"""统一错误模型：客户端错误体与日志详情隔离。"""
from __future__ import annotations

from typing import Optional


class GatewayError(Exception):
    """网关层错误。message 会回传客户端，detail 只进日志。"""

    def __init__(
        self,
        status_code: int,
        message: str,
        code: str,
        detail: str = "",
        retry_after: Optional[float] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.code = code
        self.detail = detail
        self.retry_after = retry_after  # 秒；429 时建议客户端等待的时长

    def to_client(self) -> dict:
        body = {"error": {"message": self.message, "type": self.code, "code": "gateway"}}
        if self.retry_after is not None:
            body["error"]["retry_after"] = round(self.retry_after, 1)
        return body

    def to_log(self) -> str:
        return f"code={self.code} detail={self.detail or self.message}"


def unauthorized(detail: str = "") -> GatewayError:
    return GatewayError(401, "Unauthorized", "authentication_error", detail)


def invalid_request(message: str, detail: str = "") -> GatewayError:
    return GatewayError(400, message, "invalid_request_error", detail)


def model_not_found(model: str) -> GatewayError:
    return GatewayError(404, f"Model '{model}' not found", "model_not_found")


def no_available_key(provider: str, detail: str = "", retry_after: float | None = None) -> GatewayError:
    """所有 key 均不可用。retry_after 取各 key 最短剩余冷却，让客户端知道何时可重试。"""
    msg = f"All keys for provider '{provider}' are exhausted or cooling down"
    if retry_after and retry_after > 0:
        msg += f"; retry after {int(retry_after + 0.999)}s"
    return GatewayError(429, msg, "provider_overloaded", detail, retry_after=retry_after)


def upstream_failed(message: str, detail: str = "") -> GatewayError:
    return GatewayError(502, message, "upstream_error", detail)
