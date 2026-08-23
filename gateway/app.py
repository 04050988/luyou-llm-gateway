"""FastAPI 应用与路由。"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from . import __version__
from .auth import verify_master_key
from .errors import GatewayError, invalid_request, unauthorized
from .service import GatewayService, mask_key

logger = logging.getLogger("gateway.app")


def _message_summary(messages) -> str:
    """提取消息结构摘要用于日志：role 与 content 的类型构成。"""
    if not isinstance(messages, list):
        return f"not-list({type(messages).__name__})"
    parts = []
    for m in messages[:10]:
        role = m.get("role", "?") if isinstance(m, dict) else "?"
        content = m.get("content") if isinstance(m, dict) else None
        if isinstance(content, str):
            parts.append(f"{role}:str({len(content)})")
        elif isinstance(content, list):
            types = []
            for item in content[:5]:
                if isinstance(item, dict):
                    t = item.get("type", "no-type")
                    if t == "image_url":
                        url = item.get("image_url")
                        url = url.get("url", "") if isinstance(url, dict) else ""
                        types.append(f"image_url:{str(url)[:40]}")
                    else:
                        types.append(str(t))
                else:
                    types.append(type(item).__name__)
            parts.append(f"{role}:[{','.join(types)}]")
        else:
            parts.append(f"{role}:{type(content).__name__}")
    return "|".join(parts)


def create_app(service: GatewayService) -> FastAPI:
    app = FastAPI(title="Unified LLM API Gateway", version=__version__)

    # ---- 异常处理：错误体与日志详情隔离 ----
    @app.exception_handler(GatewayError)
    async def _gateway_error_handler(request: Request, exc: GatewayError):
        logger.warning("%s %s %s", request.method, request.url.path, exc.to_log())
        headers = {}
        if exc.retry_after is not None and exc.retry_after > 0:
            # 标准 Retry-After 头 + 错误体字段，客户端可据此安排重试
            headers["Retry-After"] = str(max(1, int(exc.retry_after + 0.999)))
        return JSONResponse(status_code=exc.status_code, content=exc.to_client(), headers=headers)

    @app.exception_handler(HTTPException)
    async def _http_error_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"message": str(exc.detail), "type": "http_error", "code": "gateway"}},
        )

    # ---- 鉴权依赖 ----
    def require_master(authorization: Optional[str] = Header(default=None)) -> None:
        master_key = service.rm.current().config.master_key
        if not verify_master_key(authorization, master_key):
            raise unauthorized(detail="missing or invalid master_key")

    # ---- 预检：把路由/无可用 key 等错误在流开始前转成 JSON ----
    async def _preflight(body: Dict[str, Any], stream: bool) -> None:
        model = body.get("model", "")
        if not model:
            raise invalid_request("model is required")
        cfg = await service.resolve_provider_async(model)
        if cfg is None:
            raise GatewayError(404, f"Model '{model}' not found", "model_not_found")
        if service._pick_key(cfg) is None:
            raise GatewayError(
                429,
                f"All keys for provider '{cfg.name}' are exhausted or cooling down",
                "provider_overloaded",
            )
        if stream:
            body["stream"] = True

    @app.get("/v1/health")
    async def health():
        reg = service.rm.current()
        providers = {}
        for name, p in reg.providers.items():
            cooling = 0
            for k in p.keys:
                st = service.stats.get(k)
                if st is not None and st.in_cooldown():
                    cooling += 1
            providers[name] = {"keys": len(p.keys), "cooling": cooling, "models": p.models}
        return {"status": "ok", "version": __version__, "providers": providers}

    @app.get("/v1/models")
    async def list_models(authorization: Optional[str] = Header(default=None)):
        require_master(authorization)
        models = await service.list_models()
        return {"object": "list", "data": [{"id": m, "object": "model", "owned_by": "gateway"} for m in models]}

    @app.post("/v1/chat/completions")
    async def chat_completions(
        request: Request,
        authorization: Optional[str] = Header(default=None),
    ):
        require_master(authorization)
        try:
            body: Dict[str, Any] = await request.json()
        except Exception:
            raise invalid_request("invalid JSON body")

        logger.info("chat model=%s messages=%s", body.get("model"), _message_summary(body.get("messages")))

        if body.get("stream"):
            await _preflight(body, stream=True)
            return StreamingResponse(
                service.chat_stream(body),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        await _preflight(body, stream=False)
        result = await service.chat(body)
        return JSONResponse(content=result)

    @app.post("/v1/embeddings")
    async def embeddings(
        request: Request,
        authorization: Optional[str] = Header(default=None),
    ):
        require_master(authorization)
        try:
            body: Dict[str, Any] = await request.json()
        except Exception:
            raise invalid_request("invalid JSON body")
        await _preflight(body, stream=False)
        result = await service.embeddings(body)
        return JSONResponse(content=result)

    @app.post("/v1/images/generations")
    async def images_generations(
        request: Request,
        authorization: Optional[str] = Header(default=None),
    ):
        require_master(authorization)
        try:
            body: Dict[str, Any] = await request.json()
        except Exception:
            raise invalid_request("invalid JSON body")
        await _preflight(body, stream=False)
        result = await service.images_generations(body)
        return JSONResponse(content=result)

    @app.post("/admin/reload")
    async def admin_reload(authorization: Optional[str] = Header(default=None)):
        require_master(authorization)
        try:
            service.rm.reload()
        except GatewayError as exc:
            logger.warning("reload rejected: %s", exc.to_log())
            raise exc
        # 配置已变：失效动态模型目录，下次按新配置重新拉取
        service.catalog.invalidate()
        return {"status": "reloaded"}

    @app.get("/admin/stats")
    async def admin_stats(authorization: Optional[str] = Header(default=None)):
        require_master(authorization)
        out = {}
        reg = service.rm.current()
        for name, p in reg.providers.items():
            per_key = []
            for k in p.keys:
                st = service.stats.get(k)
                if st is None:
                    per_key.append({"key": mask_key(k), "state": "unknown"})
                    continue
                per_key.append({
                    "key": mask_key(k),
                    "state": st.state(),
                    "tpm_window": st.tpm_window(),
                    "rpm_window": st.rpm_window(),
                    "concurrency": st.concurrency,
                    "failures": st.failures,
                    "throttled": st.throttled,
                    "success_rate_60s": st.success_rate(),
                    "throttle_streak": st.throttle_streak,
                })
            out[name] = per_key
        out["_catalog"] = {
            name: sorted(models) for name, models in service.catalog.cached_models().items()
        }
        out["_models"] = service.stats.models_snapshot()
        return out

    return app