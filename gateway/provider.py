"""平台适配器：openai_compatible（零代码接入）与 sensenova（格式兼容层）。"""
from __future__ import annotations

import json
from typing import Any, AsyncIterator, Dict, Optional

import httpx

from .errors import GatewayError, upstream_failed
from .models import ProviderConfig


class Provider:
    def __init__(self, cfg: ProviderConfig, client: httpx.AsyncClient):
        self.cfg = cfg
        self.client = client

    def _headers(self, key: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def _url(self, path: str) -> str:
        base = self.cfg.base_url.rstrip("/")
        path = path.lstrip("/")
        if "/v1" in base and path.startswith("v1/"):
            path = path[len("v1/"):]
        return f"{base}/{path}"

    async def chat(self, body: Dict[str, Any], key: str, timeout: float) -> httpx.Response:
        headers = self._headers(key)
        if body.get("stream"):
            req = self.client.build_request(
                "POST", self._url(self.cfg.chat_path), json=body, headers=headers, timeout=timeout
            )
            return await self.client.send(req, stream=True)
        return await self.client.post(
            self._url(self.cfg.chat_path), json=body, headers=headers, timeout=timeout
        )

    async def embeddings(self, body: Dict[str, Any], key: str, timeout: float) -> httpx.Response:
        headers = self._headers(key)
        return await self.client.post(
            self._url(self.cfg.embeddings_path), json=body, headers=headers, timeout=timeout
        )

    async def images_generations(self, body: Dict[str, Any], key: str, timeout: float) -> httpx.Response:
        headers = self._headers(key)
        return await self.client.post(
            self._url(self.cfg.images_path), json=body, headers=headers, timeout=timeout
        )

    async def models(self, key: str, timeout: float) -> Optional[list]:
        try:
            resp = await self.client.get(
                self._url(self.cfg.models_path), headers=self._headers(key), timeout=timeout
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            items = data.get("data", []) if isinstance(data, dict) else data
            return [m.get("id") for m in items if isinstance(m, dict) and m.get("id")]
        except Exception:
            return None

    def is_throttled(self, resp: httpx.Response) -> bool:
        """识别限流：429 或 200+业务限流码。"""
        if resp.status_code == 429:
            return True
        if resp.status_code >= 400:
            try:
                body = resp.json()
                msg = str(body.get("error", {}).get("message", "")).lower()
                return any(k in msg for k in ("rate limit", "too many", "throttl", "限流"))
            except Exception:
                return False
        return False

    def parse_error(self, resp: httpx.Response) -> GatewayError:
        try:
            body = resp.json()
            msg = body.get("error", {}).get("message") or resp.reason_phrase
        except Exception:
            msg = resp.reason_phrase
        return upstream_failed(f"Upstream returned {resp.status_code}", f"upstream_status={resp.status_code} body={msg}")

    @staticmethod
    def retry_after(resp: httpx.Response) -> Optional[float]:
        value = resp.headers.get("Retry-After")
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None


class OpenAICompatibleProvider(Provider):
    pass


class SenseNovaProvider(Provider):
    """商汤日日新：OpenAI 兼容格式，附商汤字段限制处理。

    商汤对未列入官方参数表的字段会直接拒绝（400），这里对常见冲突字段做剥离。
    """

    # 商汤对未列入官方参数表的字段会直接拒绝（400），对冲突字段做统一剥离
    STRIP_FIELDS = ("response_format",)

    def _sanitize(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in body.items() if k not in self.STRIP_FIELDS}

    async def chat(self, body: Dict[str, Any], key: str, timeout: float) -> httpx.Response:
        clean = self._sanitize(body)
        return await super().chat(clean, key, timeout)


def with_stream_usage(body: Dict[str, Any]) -> Dict[str, Any]:
    """流式请求自动注入 include_usage，让上游回传 token 用量（配额调度依赖它）。

    客户端已携带 stream_options 时完全不动（是否要 usage 由客户端决定）。
    返回副本，不改调用方对象。
    """
    if "stream_options" in body:
        return body
    out = dict(body)
    out["stream_options"] = {"include_usage": True}
    return out


def create_provider(cfg: ProviderConfig, client: httpx.AsyncClient) -> Provider:
    if cfg.type == "sensenova":
        return SenseNovaProvider(cfg, client)
    return OpenAICompatibleProvider(cfg, client)