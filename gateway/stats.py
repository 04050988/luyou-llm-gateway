"""key 使用统计：滑动窗口 TPM/RPM、瞬时并发、失败计数、历史成功率、半开探测与冷却管理。

冷却语义（熔断器模式）：
  - cooldown 到期后 key 不直接恢复，进入 probation（半开）状态
  - 半开期间放行的首个请求成功 → 完全恢复；失败 → 重新冷却（时长翻倍）
"""
from __future__ import annotations

import time
from collections import deque
from threading import RLock
from typing import Dict, Optional


class KeyStats:
    def __init__(
        self,
        tpm_threshold: int,
        rpm_threshold: int,
        concurrency_limit: int,
        cooldown_seconds: int,
        failure_threshold: int,
        window: float = 60.0,
    ):
        self.tpm_threshold = tpm_threshold
        self.rpm_threshold = rpm_threshold
        self.concurrency_limit = concurrency_limit
        self.cooldown_seconds = cooldown_seconds
        self.failure_threshold = failure_threshold
        self.window = window
        self._tokens: deque[tuple[float, int]] = deque()
        self._calls: deque[tuple[float, int]] = deque()
        # 成功率窗口：(时间, 是否成功)。仅统计已判定结果（成功或失败），限流不计入。
        self._outcomes: deque[tuple[float, bool]] = deque()
        self.concurrency = 0
        self.failures = 0
        self.throttled = 0          # 累计限流次数
        self.throttle_streak = 0    # 连续限流计数（无成功则递增）
        self._cooldown_until = 0.0
        self.probation = False      # 冷却结束后的半开状态
        self._probation_token: Optional[object] = None  # 当前半开探测请求的令牌

    # ---- 窗口统计 ----
    def _prune(self, now: float) -> None:
        cutoff = now - self.window
        while self._tokens and self._tokens[0][0] < cutoff:
            self._tokens.popleft()
        while self._calls and self._calls[0][0] < cutoff:
            self._calls.popleft()
        while self._outcomes and self._outcomes[0][0] < cutoff:
            self._outcomes.popleft()

    def _window_sum(self, dq: deque[tuple[float, int]], now: float) -> int:
        self._prune(now)
        return sum(v for _, v in dq)

    def tpm_window(self) -> int:
        return self._window_sum(self._tokens, time.monotonic())

    def rpm_window(self) -> int:
        return self._window_sum(self._calls, time.monotonic())

    # ---- 记录 ----
    def record_request(self, tokens: int = 0) -> None:
        now = time.monotonic()
        self._prune(now)
        self._calls.append((now, 1))
        if tokens > 0:
            self._tokens.append((now, tokens))

    def record_success(self, tokens: int = 0) -> None:
        now = time.monotonic()
        self.record_request(tokens)
        self.failures = 0
        self.throttle_streak = 0
        if self.probation:
            self._close_probation()
        self._outcomes.append((now, True))

    def record_failure(self) -> None:
        now = time.monotonic()
        self.record_request()
        self.failures += 1
        self.throttle_streak += 1
        if self.probation:
            # 半开探测失败：重新冷却，时长翻倍（指数升级）
            self._reopen_cooldown()
        elif self.failures >= self.failure_threshold:
            self.enter_cooldown()
        self._outcomes.append((now, False))

    def record_throttled(self) -> None:
        """记录一次限流（不自动冷却，由调用方决定惩罚时长）。"""
        self.throttled += 1
        self.throttle_streak += 1
        if self.probation:
            self._reopen_cooldown()

    # ---- 历史成功率 ----
    def success_rate(self) -> Optional[float]:
        """近 window 秒内的成功率；无样本返回 None（视为中性，不奖不罚）。"""
        self._prune(time.monotonic())
        n = len(self._outcomes)
        if n == 0:
            return None
        return sum(1 for _, ok in self._outcomes if ok) / n

    # ---- 并发 ----
    def acquire(self) -> None:
        self.concurrency += 1

    def release(self) -> None:
        if self.concurrency > 0:
            self.concurrency -= 1

    # ---- 冷却与半开 ----
    def enter_cooldown(self, seconds: Optional[float] = None) -> None:
        self._cooldown_until = time.monotonic() + (
            seconds if seconds is not None else self.cooldown_seconds
        )
        self.probation = False
        self._probation_token = None

    def in_cooldown(self) -> bool:
        if time.monotonic() < self._cooldown_until:
            return True
        if self._cooldown_until > 0 and not self.probation and self._probation_token is None:
            # 冷却刚到期：转入半开，放行下一个请求做探测
            self.probation = True
        return False

    def begin_probe(self) -> object:
        """半开状态下领取探测令牌；同一时刻只允许一个探测请求。"""
        token = object()
        self._probation_token = token
        return token

    def probe_in_flight(self) -> bool:
        return self._probation_token is not None

    def settle_probe_success(self, token: object) -> None:
        if self._probation_token is token:
            self.record_success()

    def settle_probe_failure(self, token: object) -> None:
        if self._probation_token is token:
            self.record_failure()

    def _close_probation(self) -> None:
        self.probation = False
        self._probation_token = None
        self._cooldown_until = 0.0

    def _reopen_cooldown(self) -> None:
        base = self.cooldown_seconds or 15
        elapsed_penalty = min(base * (2 ** max(1, self.throttle_streak)), base * 8)
        self.enter_cooldown(elapsed_penalty)

    def state(self) -> str:
        """当前状态：active / cooldown / probation。"""
        if self.in_cooldown():
            return "cooldown"
        if self.probation or self._probation_token is not None:
            return "probation"
        return "active"

    # ---- 可用性/余量 ----
    def tpm_margin(self) -> float:
        if self.tpm_threshold <= 0:
            return 1.0
        return max(0.0, 1.0 - self.tpm_window() / self.tpm_threshold)

    def rpm_margin(self) -> float:
        if self.rpm_threshold <= 0:
            return 1.0
        return max(0.0, 1.0 - self.rpm_window() / self.rpm_threshold)

    def concurrency_margin(self) -> float:
        if self.concurrency_limit <= 0:
            return 1.0
        return max(0.0, 1.0 - self.concurrency / self.concurrency_limit)

    def available(self) -> bool:
        if self.in_cooldown():
            return False
        # 半开且已有探测在途：不再接新流量
        if self.probation and self.probe_in_flight():
            return False
        return self.concurrency_margin() > 0 and (self.rpm_margin() > 0 or self.rpm_threshold <= 0)


class ModelHealth:
    """按模型维度的健康统计：总/成功/失败/限流计数与最近一次错误。"""

    def __init__(self) -> None:
        self.total = 0
        self.success = 0
        self.failures = 0
        self.throttled = 0
        self.last_error: Optional[str] = None
        self.last_error_at: Optional[float] = None

    def record(self, ok: Optional[bool], throttled: bool = False, error: Optional[str] = None) -> None:
        self.total += 1
        if ok:
            self.success += 1
        elif ok is False:
            self.failures += 1
        if throttled:
            self.throttled += 1
        if error:
            self.last_error = error[:300]
            self.last_error_at = time.time()

    def snapshot(self) -> dict:
        rate = round(self.success / self.total, 3) if self.total else None
        return {
            "total": self.total,
            "success": self.success,
            "failures": self.failures,
            "throttled": self.throttled,
            "success_rate": rate,
            "last_error": self.last_error,
        }


class KeyStatsStore:
    """每个 key 一份统计 + 每个模型一份健康统计，线程安全。"""

    def __init__(self) -> None:
        self._lock = RLock()
        self._stats: Dict[str, KeyStats] = {}
        self._models: Dict[str, ModelHealth] = {}

    def get(self, key: str) -> Optional[KeyStats]:
        with self._lock:
            return self._stats.get(key)

    def ensure(self, key: str, params: dict) -> KeyStats:
        with self._lock:
            st = self._stats.get(key)
            if st is None:
                st = KeyStats(
                    tpm_threshold=params.get("tpm_threshold", 60000),
                    rpm_threshold=params.get("rpm_threshold", 100),
                    concurrency_limit=params.get("concurrency_limit", 8),
                    cooldown_seconds=params.get("cooldown_seconds", 60),
                    failure_threshold=params.get("failure_threshold", 3),
                )
                self._stats[key] = st
            return st

    def model(self, name: str) -> ModelHealth:
        with self._lock:
            mh = self._models.get(name)
            if mh is None:
                mh = ModelHealth()
                self._models[name] = mh
            return mh

    def models_snapshot(self) -> Dict[str, dict]:
        with self._lock:
            return {name: mh.snapshot() for name, mh in self._models.items()}

    def reset(self) -> None:
        with self._lock:
            self._stats.clear()
            self._models.clear()
