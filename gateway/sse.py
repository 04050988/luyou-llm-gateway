"""SSE 流式处理：格式统一、有界缓冲背压、断连时取消上游。"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator, Callable, Optional

import httpx

logger = logging.getLogger("gateway.sse")

DONE = "[DONE]"
QUEUE_MAX = 32  # 有界缓冲上限：客户端慢消费时背压暂停上游读取


def _parse_data_payload(line: str) -> Optional[tuple]:
    """把上游一行解析为 (payload, is_done)。非 data 行返回 None。"""
    line = line.strip()
    if not line.startswith("data:"):
        return None
    payload = line[len("data:"):].strip()
    if payload == DONE:
        return (DONE, True)
    if not payload:
        return ("", False)
    return (payload, False)


async def stream_to_client(
    upstream: httpx.Response,
    on_usage: Optional[Callable[[dict], None]] = None,
    on_error: Optional[Callable[[str], None]] = None,
) -> AsyncIterator[str]:
    """读上游 SSE，产出 OpenAI 标准格式的 data 行。

    背压：写入队列满时暂停读取上游；客户端断开时取消上游。
    on_error: 上游中途异常时的回调（用于把流中断计入失败统计）。
    """
    queue: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=QUEUE_MAX)
    got_usage = False

    async def _reader():
        nonlocal got_usage
        try:
            async for raw_line in upstream.aiter_lines():
                parsed = _parse_data_payload(raw_line)
                if parsed is None:
                    continue
                payload, is_done = parsed
                if on_usage is not None and not got_usage and payload != DONE:
                    try:
                        obj = json.loads(payload)
                        usage = obj.get("usage")
                        if usage:
                            on_usage(usage)
                            got_usage = True
                    except (json.JSONDecodeError, AttributeError):
                        pass
                await queue.put(f"data: {payload}\n\n")
                if is_done:
                    return
            # 上游自然结束但没发 [DONE]：补一个，否则消费端会永远等待
            await queue.put("data: [DONE]\n\n")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # 上游中途断开
            logger.warning("upstream stream error: %s", exc)
            if on_error is not None:
                try:
                    on_error(str(exc))
                except Exception:
                    pass
            await queue.put(f"data: {json.dumps({'error': {'message': 'upstream stream interrupted'}})}\n\n")
            # 错误事件后必须收尾，消费端以 [DONE] 终止，不会悬挂
            await queue.put("data: [DONE]\n\n")
        finally:
            await upstream.aclose()

    reader_task = asyncio.create_task(_reader())
    try:
        while True:
            item = await queue.get()
            if item is None:
                break
            yield item
            if item.startswith("data: [DONE]"):
                break
    finally:
        if not reader_task.done():
            reader_task.cancel()
            try:
                await reader_task
            except (asyncio.CancelledError, Exception):
                pass
