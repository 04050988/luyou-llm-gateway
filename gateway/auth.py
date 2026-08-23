"""master_key 鉴权（常数时间比较 + 失败限速，防时序攻击与在线爆破）。"""
from __future__ import annotations

import hmac
import re
import time
from collections import deque
from threading import Lock
from typing import Optional

BEARER_RE = re.compile(r"^Bearer\s+(.+)$", re.IGNORECASE)

# 爆破防护：窗口期内最多 MAX_AUTH_FAILURES 次失败，超时后锁定 LOCKOUT_SECONDS 秒
MAX_AUTH_FAILURES = 10
FAILURE_WINDOW = 300.0      # 失败计数滑动窗口（秒）
LOCKOUT_SECONDS = 60.0      # 触发后的锁定期（秒）


class AuthRateLimiter:
    """全局失败计数（网关只有一个 master_key，无需按 IP 区分）。"""

    def __init__(self, max_failures: int = MAX_AUTH_FAILURES,
                 window: float = FAILURE_WINDOW, lockout: float = LOCKOUT_SECONDS):
        self.max_failures = max_failures
        self.window = window
        self.lockout = lockout
        self._failures: deque[float] = deque()
        self._locked_until = 0.0
        self._lock = Lock()

    def locked(self) -> bool:
        """是否处于爆破锁定中。到期自动解锁。"""
        now = time.monotonic()
        with self._lock:
            if now < self._locked_until:
                return True
            # 清理过期失败记录
            cutoff = now - self.window
            while self._failures and self._failures[0] < cutoff:
                self._failures.popleft()
            return False

    def record_failure(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._failures.append(now)
            if len(self._failures) >= self.max_failures:
                self._locked_until = now + self.lockout
                self._failures.clear()

    def record_success(self) -> None:
        """成功鉴权：清空失败计数并解除锁定。"""
        with self._lock:
            self._failures.clear()
            self._locked_until = 0.0


# 进程级单例：鉴权限速随服务生命周期
limiter = AuthRateLimiter()


def extract_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    m = BEARER_RE.match(authorization.strip())
    return m.group(1).strip() if m else None


def verify_master_key(authorization: Optional[str], master_key: str) -> bool:
    """验证 Bearer token。常数时间比较；连续失败触发临时锁定。"""
    if limiter.locked():
        return False
    token = extract_bearer(authorization)
    ok = bool(token) and hmac.compare_digest(
        token.encode("utf-8"), master_key.encode("utf-8")
    )
    if ok:
        limiter.record_success()
    else:
        limiter.record_failure()
    return ok
