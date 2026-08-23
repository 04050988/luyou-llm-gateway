"""网关核心服务：路由、配额感知调度、故障切换、重试与统计回填。"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Set

import httpx

from .catalog import ModelCatalog
from .errors import GatewayError, model_not_found, no_available_key, upstream_failed
from .models import ProviderConfig
from .provider import Provider, create_provider, with_stream_usage
from .registry import Registry, RegistryManager
from .scheduler import RoundRobin, select_key
from .sse import stream_to_client
from .stats import KeyStats, KeyStatsStore
from .usage import UsageLedger

logger = logging.getLogger("gateway.service")

# 429 短冷却基数（秒）；连击时按指数升级：15s → 30s → 60s（上限 cooldown_seconds）
THROTTLE_BASE_COOLDOWN = 15.0


def mask_key(key: str) -> str:
    if len(key) <= 8:
        return "****"
    return f"{key[:4]}****{key[-4:]}"


class GatewayService:
    def __init__(self, registry_manager: RegistryManager, settings=None):
        self.rm = registry_manager
        self.settings = settings or registry_manager.current().config.gateway
        self.stats = KeyStatsStore()
        self._rr: Dict[str, RoundRobin] = {}
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=self.settings.connect_timeout,
                read=self.settings.read_timeout,
                write=self.settings.connect_timeout,
                pool=self.settings.connect_timeout * 2,
            ),
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )
        self.catalog = ModelCatalog(self.client)
        # 每日用量台账（SQLite，与配置同目录）
        db_dir = Path(self.rm.config_path).parent
        self.usage = UsageLedger(db_dir / "usage.db")

    def _book_usage(
        self,
        provider: str,
        model: str,
        key: str,
        ok: bool,
        throttled: bool = False,
        usage: Optional[dict] = None,
    ) -> None:
        """记一次用量台账（异步落盘，不阻塞请求路径）。"""
        try:
            u = usage or {}
            self.usage.record(
                provider=provider,
                model=model,
                key_masked=mask_key(key),
                ok=ok,
                throttled=throttled,
                prompt_tokens=int(u.get("prompt_tokens") or 0),
                completion_tokens=int(u.get("completion_tokens") or 0),
            )
        except Exception as exc:  # 台账失败绝不影响主流程
            logger.warning("usage record failed: %s", exc)

    # ---- 公共接口 ----
    async def chat(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """非流式聊天。仅在未收到上游成功响应前重试/切换。"""
        if body.get("stream"):
            raise GatewayError(400, "stream must use streaming endpoint", "invalid_request_error")
        model = body.get("model", "")
        provider_cfg = await self.resolve_provider_async(model)
        if provider_cfg is None:
            raise model_not_found(model)
        return await self._chat_with_retry(provider_cfg, body)

    def _record_model(self, model: str, ok: Optional[bool], throttled: bool = False, error: Optional[str] = None) -> None:
        """按模型维度记录一次调用结果（供 admin/stats 观测）。"""
        if model:
            self.stats.model(model).record(ok=ok, throttled=throttled, error=error)

    async def chat_stream(self, body: Dict[str, Any]) -> AsyncIterator[str]:
        """流式聊天：2xx 发起成功后绝不再重试；发起阶段失败以 SSE 错误事件返回。"""
        model = body.get("model", "")
        st = None
        last_error: Optional[GatewayError] = None
        key = ""
        try:
            provider_cfg = await self.resolve_provider_async(model)
            if provider_cfg is None:
                raise model_not_found(model)
            provider = create_provider(provider_cfg, self.client)
            # 流式自动请求 usage（配额调度依赖）；个别平台不认 stream_options 会 400，
            # 由下方 _strip_stream_options_and_retry 回退
            upstream_body = with_stream_usage(body)
            key = self._pick_key(provider_cfg)
            if key is None:
                raise no_available_key(provider_cfg.name, detail="stream",
                                       retry_after=self._min_cooldown_remaining(provider_cfg))
            self._record_model(model, None)  # 占位计数：成功/失败在下方回调中补记

            st = self._acquire(key, provider_cfg)
            started = time.monotonic()
            try:
                try:
                    resp = await provider.chat(upstream_body, key, self.settings.request_timeout)
                except httpx.HTTPError as exc:
                    self._record_model(model, False, error=f"connect: {exc}")
                    raise upstream_failed("Upstream connection failed", detail=str(exc))
                # 平台不认识 stream_options 时会 400 拒绝：剥掉后立即原 key 重试一次
                if resp.status_code == 400 and upstream_body is not body and _rejects_stream_options(resp):
                    await resp.aclose()
                    logger.info("provider '%s' rejected stream_options; retrying without it", provider_cfg.name)
                    upstream_body = body
                    resp = await provider.chat(upstream_body, key, self.settings.request_timeout)
                # 发起失败（非 2xx）：尚未产生任何内容，允许切换重试
                if resp.status_code != 200:
                    await self._ensure_body(resp)
                    # 客户端请求错误（4xx 且非 401/429）：换 key 无意义，直接透传、不重试、不罚 key
                    if 400 <= resp.status_code < 500 and resp.status_code not in (401, 429):
                        await resp.aclose()
                        self._record_model(model, False, error=_upstream_message(resp))
                        raise _passthrough_error(resp)
                    throttled = resp.status_code == 429 or _looks_throttled(resp)
                    self._record_model(
                        model, False, throttled=throttled,
                        error=_upstream_message(resp) or f"status={resp.status_code}",
                    )
                    last_error = self._record_upstream_error(key, st, resp, provider_cfg)
                    await resp.aclose()
                    for attempt in range(1, self.settings.max_retries + 1):
                        await asyncio.sleep(self._backoff(attempt))
                        key = self._pick_key(provider_cfg)
                        if key is None:
                            raise no_available_key(provider_cfg.name, detail="stream retry",
                                                   retry_after=self._min_cooldown_remaining(provider_cfg))
                        st = self._acquire(key, provider_cfg)
                        resp = await provider.chat(upstream_body, key, self.settings.request_timeout)
                        if resp.status_code == 200:
                            break
                        await self._ensure_body(resp)
                        if 400 <= resp.status_code < 500 and resp.status_code not in (401, 429):
                            await resp.aclose()
                            self._record_model(model, False, error=_upstream_message(resp))
                            raise _passthrough_error(resp)
                        throttled = resp.status_code == 429 or _looks_throttled(resp)
                        self._record_model(
                            model, False, throttled=throttled,
                            error=_upstream_message(resp) or f"status={resp.status_code}",
                        )
                        last_error = self._record_upstream_error(key, st, resp, provider_cfg)
                        await resp.aclose()
                    else:
                        raise last_error or no_available_key(provider_cfg.name, detail="stream all retries failed")

                def _on_usage(usage: dict):
                    tokens = int((usage or {}).get("total_tokens") or 0)
                    st.record_success(tokens)
                    self._record_model(model, True)
                    self._book_usage(provider_cfg.name, model, key, ok=True, usage=usage)

                def _on_stream_error(err: str):
                    # 上游中途断流：计入该 key 失败与该模型的失败统计
                    st.record_failure()
                    self._record_model(model, False, error=f"stream interrupted: {err}")

                async for chunk in stream_to_client(resp, on_usage=_on_usage, on_error=_on_stream_error):
                    yield chunk
            finally:
                if st is not None:
                    st.release()
                logger.info(
                    "stream key=%s model=%s elapsed=%.2fs",
                    mask_key(key), model, time.monotonic() - started,
                )
        except asyncio.CancelledError:
            raise
        except GatewayError as exc:
            # 响应头已发出，无法回退为 JSON；以 SSE 错误事件返回，避免客户端解码失败
            payload = {"error": {"message": exc.message, "type": exc.code, "code": "gateway"}}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            yield "data: [DONE]\n\n"

    async def _passthrough(self, resp) -> None:
        if 400 <= resp.status_code < 500 and resp.status_code not in (401, 429):
            raise _passthrough_error(resp)

    async def embeddings(self, body: Dict[str, Any]) -> Dict[str, Any]:
        model = body.get("model", "")
        provider_cfg = await self.resolve_provider_async(model)
        if provider_cfg is None:
            raise model_not_found(model)
        provider = create_provider(provider_cfg, self.client)
        key = self._pick_key(provider_cfg)
        if key is None:
            raise no_available_key(provider_cfg.name, detail="embeddings")
        st = self._acquire(key, provider_cfg)
        try:
            resp = await provider.embeddings(body, key, self.settings.request_timeout)
            if resp.status_code != 200:
                await self._ensure_body(resp)
                await self._passthrough(resp)
                throttled = resp.status_code == 429 or _looks_throttled(resp)
                self._record_model(model, False, throttled=throttled, error=_upstream_message(resp))
                self._book_usage(provider_cfg.name, model, key, ok=False, throttled=throttled)
                self._handle_error(key, st, resp, provider_cfg)
                await resp.aclose()
                raise upstream_failed("Upstream embeddings failed", f"status={resp.status_code}")
            data = resp.json()
            st.record_success()
            self._record_model(model, True)
            self._book_usage(provider_cfg.name, model, key, ok=True,
                             usage=data.get("usage") or {})
            return data
        finally:
            st.release()

    async def images_generations(self, body: Dict[str, Any]) -> Dict[str, Any]:
        model = body.get("model", "")
        provider_cfg = await self.resolve_provider_async(model)
        if provider_cfg is None:
            raise model_not_found(model)
        provider = create_provider(provider_cfg, self.client)
        key = self._pick_key(provider_cfg)
        if key is None:
            raise no_available_key(provider_cfg.name, detail="images")
        st = self._acquire(key, provider_cfg)
        try:
            resp = await provider.images_generations(body, key, self.settings.request_timeout)
            if resp.status_code != 200:
                await self._ensure_body(resp)
                await self._passthrough(resp)
                throttled = resp.status_code == 429 or _looks_throttled(resp)
                self._record_model(model, False, throttled=throttled, error=_upstream_message(resp))
                self._book_usage(provider_cfg.name, model, key, ok=False, throttled=throttled)
                self._handle_error(key, st, resp, provider_cfg)
                await resp.aclose()
                raise upstream_failed("Upstream images generation failed", f"status={resp.status_code}")
            data = resp.json()
            st.record_success()
            self._record_model(model, True)
            self._book_usage(provider_cfg.name, model, key, ok=True,
                             usage=data.get("usage") or {})
            return data
        finally:
            st.release()

    async def list_models(self) -> List[str]:
        """静态配置列表 + 各平台动态拉取合并；动态失败回退静态。"""
        registry = self.rm.current()
        models = set(registry.list_models())
        for p in registry.providers.values():
            dynamic = await self.catalog.dynamic_models(p)
            if dynamic:
                models.update(dynamic)
        return sorted(models)

    # ---- 路由解析：别名改写 > route_to 显式 > 静态 models > 动态目录 ----
    def canonical_model(self, model: str) -> str:
        """把别名解析为真实模型名（最多跟一层，防环由配置校验拦截）。"""
        aliases = getattr(self.rm.current().config, "aliases", None) or {}
        return aliases.get(model, model)

    def resolve_provider(self, model: str) -> Optional[ProviderConfig]:
        """同步解析（仅静态两级），供 preflight 等无事件循环上下文处使用。"""
        return self.rm.current().resolve_provider(model)

    async def resolve_provider_async(self, model: str) -> Optional[ProviderConfig]:
        """三级路由解析；动态级带缓存与负缓存，失败自动回退静态判定。"""
        reg = self.rm.current()
        # 1. route_to 显式指定
        for p in reg.providers.values():
            if model in p.route_to:
                return reg.provider(p.route_to[model])
        # 2. 静态 models
        static_hit = None
        for p in reg.providers.values():
            if model in p.models:
                static_hit = p
                break
        if static_hit is not None:
            return static_hit
        # 3. 动态目录：逐个平台查（各自有独立缓存/负缓存）
        for p in reg.providers.values():
            dynamic = await self.catalog.dynamic_models(p)
            if dynamic and model in dynamic:
                logger.info("model=%s resolved via dynamic catalog of '%s'", model, p.name)
                return p
        return None

    def _fallback_candidates(self, primary: ProviderConfig) -> List[ProviderConfig]:
        """跨平台切换候选：fallback_chain 中排在主平台之后的平台。

        每次实时读当前配置快照（热更新 fallback_chain 立即生效）。
        """
        chain = list(getattr(self.rm.current().config.gateway, "fallback_chain", None) or [])
        if not chain:
            return [primary]
        candidates: List[ProviderConfig] = []
        started = False
        for name in chain:
            p = self.rm.current().provider(name)
            if p is None:
                continue
            if p.name == primary.name:
                started = True
                continue
            if started:
                candidates.append(p)
        return candidates

    # ---- 非流式重试（含跨平台切换）----
    async def _chat_with_retry(self, provider_cfg: ProviderConfig, body: Dict[str, Any]) -> Dict[str, Any]:
        provider = create_provider(provider_cfg, self.client)
        model = body.get("model", "")
        attempted: Set[str] = set()
        tried_providers: Set[str] = {provider_cfg.name}
        current = provider_cfg
        for attempt in range(1, self.settings.max_retries + 2):
            key = self._pick_key(current, exclude=attempted)
            if key is None:
                # 本平台 key 全部不可用：沿 fallback_chain 换平台
                nxt = None
                for cand in self._fallback_candidates(current):
                    if cand.name in tried_providers:
                        continue
                    k2 = self._pick_key(cand, exclude=set())
                    if k2 is not None:
                        nxt = cand
                        break
                if nxt is None:
                    raise no_available_key(
                        current.name,
                        detail=f"after trying {sorted(tried_providers)}",
                        retry_after=self._min_cooldown_remaining_chain(provider_cfg),
                    )
                logger.warning("model=%s switching provider '%s' -> '%s'", model, current.name, nxt.name)
                current = nxt
                provider_cfg = nxt
                tried_providers.add(nxt.name)
                continue
            attempted.add(key)
            st = self._acquire(key, provider_cfg)
            started = time.monotonic()
            try:
                try:
                    resp = await provider.chat(body, key, self.settings.request_timeout)
                except httpx.HTTPError as exc:
                    st.record_failure()
                    logger.warning("attempt=%d key=%s network error: %s", attempt, mask_key(key), exc)
                    if attempt <= self.settings.max_retries:
                        await asyncio.sleep(self._backoff(attempt))
                        continue
                    self._record_model(model, False, error=f"connect: {exc}")
                    raise upstream_failed("All retries failed", detail=str(exc))
            finally:
                st.release()

            if resp.status_code == 200:
                data = resp.json()
                usage = data.get("usage") or {}
                tokens = int(usage.get("total_tokens") or 0)
                st.record_success(tokens)
                self._record_model(model, True)
                self._book_usage(provider_cfg.name, model, key, ok=True, usage=usage)
                logger.info(
                    "chat key=%s model=%s attempt=%d status=200 elapsed=%.2fs tokens=%d",
                    mask_key(key), body.get("model"), attempt, time.monotonic() - started, tokens,
                )
                return data

            await self._ensure_body(resp)
            # 客户端请求错误（4xx 且非 401/429）：换 key 无意义，直接透传、不重试、不罚 key
            if 400 <= resp.status_code < 500 and resp.status_code not in (401, 429):
                await resp.aclose()
                self._record_model(model, False, error=_upstream_message(resp))
                raise _passthrough_error(resp)

            throttled = resp.status_code == 429 or _looks_throttled(resp)
            self._record_model(
                model, False, throttled=throttled,
                error=_upstream_message(resp) or f"status={resp.status_code}",
            )
            self._handle_error(key, st, resp, provider_cfg)
            await resp.aclose()
            if attempt <= self.settings.max_retries:
                await asyncio.sleep(self._backoff(attempt))
                continue
            if resp.status_code == 429:
                raise _passthrough_error(resp)
            raise upstream_failed("All retries failed", f"last status={resp.status_code}")

    # ---- 内部工具 ----
    def _stats_params(self, cfg: ProviderConfig) -> dict:
        return {
            "tpm_threshold": cfg.tpm_threshold,
            "rpm_threshold": cfg.rpm_threshold,
            "concurrency_limit": cfg.concurrency_limit,
            "cooldown_seconds": cfg.cooldown_seconds,
            "failure_threshold": self.settings.failure_threshold,
        }

    def _acquire(self, key: str, cfg: ProviderConfig) -> KeyStats:
        st = self.stats.ensure(key, self._stats_params(cfg))
        st.acquire()
        return st

    def _pick_key(self, cfg: ProviderConfig, exclude: Optional[Set[str]] = None) -> Optional[str]:
        exclude = exclude or set()
        keys = [k for k in cfg.keys if k not in exclude]
        if not keys:
            return None
        params = self._stats_params(cfg)
        for k in keys:
            self.stats.ensure(k, params)
        stats_map = {k: self.stats.get(k) for k in keys}
        if cfg.strategy == "round_robin":
            rr = self._rr.setdefault(cfg.name, RoundRobin())
            return select_key(keys, stats_map, strategy="round_robin", round_robin_counter=rr)
        return select_key(keys, stats_map, strategy="quota")

    def _backoff(self, attempt: int) -> float:
        base = self.settings.retry_backoff_base * (2 ** (attempt - 1))
        jitter = base * 0.2
        return max(0.0, base - jitter + (jitter * 2) * _rand())

    @staticmethod
    async def _ensure_body(resp) -> None:
        """流式响应未读 body 时先读取，使 resp.json()/text 可用。"""
        try:
            if not resp.is_stream_consumed:
                await resp.aread()
        except Exception:
            pass

    def _throttle_cooldown(self, st: KeyStats, cfg: ProviderConfig, resp=None) -> float:
        """429 冷却时长：基数 15s，按连续限流指数升级，封顶配置的 cooldown_seconds。

        上游给了 Retry-After 时优先采用（同样封顶）。
        """
        cap = float(cfg.cooldown_seconds)
        seconds = min(THROTTLE_BASE_COOLDOWN * (2 ** max(0, st.throttle_streak - 1)), cap)
        retry_after = Provider.retry_after(resp) if resp is not None else None
        if retry_after and retry_after > 0:
            seconds = min(retry_after, cap)
        return seconds

    def _record_upstream_error(self, key: str, st: KeyStats, resp, cfg: ProviderConfig) -> GatewayError:
        """记录上游错误并返回可回传客户端的错误（透传上游 message，便于定位）。"""
        self._handle_error(key, st, resp, cfg)
        msg = _upstream_message(resp) or f"Upstream returned {resp.status_code}"
        if resp.status_code == 429:
            return GatewayError(429, msg, "rate_limit_error", "upstream rate limited")
        return upstream_failed(msg, f"status={resp.status_code}")

    def _handle_error(self, key: str, st: KeyStats, resp, cfg: ProviderConfig) -> None:
        status = resp.status_code
        if status == 401:
            st.enter_cooldown(cfg.cooldown_seconds * 5)
            st.record_failure()
            logger.warning("key=%s 401 invalid, extended cooldown", mask_key(key))
        elif status == 429 or (status >= 400 and _looks_throttled(resp)):
            # 限流多为平台级，重罚会锁死全部 key；短冷却让位其余 key，
            # 连续限流按指数升级（15→30→60…封顶 cooldown_seconds）
            st.record_throttled()
            st.enter_cooldown(self._throttle_cooldown(st, cfg, resp))
            logger.warning(
                "key=%s throttled (status=%s streak=%d), cooldown=%.0fs",
                mask_key(key), status, st.throttle_streak, self._cooldown_remaining(st),
            )
        else:
            st.record_failure()
            logger.warning("key=%s failed status=%s body=%s", mask_key(key), status, (_upstream_message(resp) or "")[:300])

    @staticmethod
    def _cooldown_remaining(st: KeyStats) -> float:
        return max(0.0, st._cooldown_until - time.monotonic())

    # ---- 后台 key 探活 ----
    async def probe_all(self) -> List[dict]:
        """对每个平台用 /models 探活各 key。

        - 401：确认失效，重罚长冷却（cooldown×5）
        - 其余失败/成功：只记录状态，不罚不奖（429 可能只是平台级限流）
        返回每个 key 的探活结果。
        """
        results: List[dict] = []
        for p in self.rm.current().providers.values():
            provider = create_provider(p, self.client)
            params = self._stats_params(p)
            for k in p.keys:
                st = self.stats.ensure(k, params)
                status = "unknown"
                try:
                    ids = await provider.models(k, timeout=self.settings.connect_timeout)
                except Exception as exc:
                    logger.info("probe key=%s provider=%s network error: %s", mask_key(k), p.name, exc)
                    ids = None
                    status = "network_error"
                if ids is not None:
                    status = "ok"
                    logger.info("probe key=%s provider=%s ok (%d models)", mask_key(k), p.name, len(ids))
                results.append({
                    "provider": p.name,
                    "key": mask_key(k),
                    "status": status,
                    "state": st.state(),
                })
        return results

    async def start_probe_loop(self) -> None:
        """后台探活循环：每 probe_interval 秒探一轮；0 或负值关闭。"""
        interval = getattr(self.settings, "probe_interval", 300)
        if interval <= 0:
            logger.info("probe loop disabled (probe_interval=%s)", interval)
            return
        async def _loop():
            await asyncio.sleep(5)  # 启动后稍等再首轮探测
            while True:
                try:
                    await self.probe_all()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("probe loop error: %s", exc)
                await asyncio.sleep(interval)

        self._probe_task = asyncio.create_task(_loop())
        logger.info("probe loop started (interval=%ss)", interval)

    def _min_cooldown_remaining(self, cfg: ProviderConfig) -> Optional[float]:
        """该平台所有 key 中最短的剩余冷却时间；无 key 在冷却时返回 None。"""
        remainings = []
        for k in cfg.keys:
            st = self.stats.get(k)
            if st is not None and st.in_cooldown():
                remainings.append(self._cooldown_remaining(st))
        return min(remainings) if remainings else None

    def _min_cooldown_remaining_chain(self, primary: ProviderConfig) -> Optional[float]:
        """主平台+所有 fallback 平台中最短的剩余冷却。"""
        values = []
        for p in self._fallback_candidates(primary):
            v = self._min_cooldown_remaining(p)
            if v is not None and v > 0:
                values.append(v)
        return min(values) if values else None

    def _provider_with_capacity(self, primary: ProviderConfig) -> Optional[ProviderConfig]:
        """主平台没有可用 key 时，沿 fallback_chain 找第一个有余量的平台；都满返回 None。"""
        if self._pick_key(primary) is not None:
            return primary
        for p in self._fallback_candidates(primary):
            if self._pick_key(p) is not None:
                logger.info("primary '%s' exhausted; capacity found on fallback '%s'", primary.name, p.name)
                return p
        return None


def _rand() -> float:
    import random
    return random.random()


def _upstream_message(resp) -> Optional[str]:
    try:
        body = resp.json()
        m = body.get("error", {}).get("message")
        return m if isinstance(m, str) else None
    except Exception:
        return None


def _passthrough_error(resp) -> GatewayError:
    """客户端请求类错误：保持上游状态码透传，便于客户端/用户定位。"""
    msg = _upstream_message(resp) or f"Upstream returned {resp.status_code}"
    return GatewayError(resp.status_code, msg, "invalid_request_error", f"upstream status={resp.status_code}")


def _looks_throttled(resp) -> bool:
    try:
        body = resp.json()
        msg = str(body.get("error", {}).get("message", "")).lower()
        return any(k in msg for k in ("rate limit", "too many", "throttl", "限流"))
    except Exception:
        return False


def _rejects_stream_options(resp) -> bool:
    """识别"上游因不认识 stream_options 而拒绝"的 400。"""
    try:
        body = resp.json()
        msg = str(body.get("error", {}).get("message", "")).lower()
    except Exception:
        return False
    return "stream_options" in msg or "stream options" in msg or "unknown field" in msg and "stream" in msg
