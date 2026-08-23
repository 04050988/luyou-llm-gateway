"""配额感知调度器：按 TPM/RPM/并发归一化余量 × 历史成功率选 key。"""
from __future__ import annotations

import itertools
from typing import List, Optional

from .stats import KeyStats

# 历史成功率的权重（0~1）。成功率样本不足时按中性 1.0 处理。
SUCCESS_WEIGHT = 0.4
# 连续限流时的额外降权系数底数；streak=1 → ×0.7，streak=2 → ×0.49 …
THROTTLE_DECAY = 0.7


def _score(stats: KeyStats) -> float:
    quota_score = min(stats.tpm_margin(), stats.rpm_margin(), stats.concurrency_margin())
    rate = stats.success_rate()
    if rate is not None and rate < 1.0:
        # 成功率越低得分越低；全败(0.0)时几乎不再被选中，除非没有别的 key 可用
        quota_score *= (1 - SUCCESS_WEIGHT) + SUCCESS_WEIGHT * rate
    if stats.throttle_streak > 0:
        quota_score *= THROTTLE_DECAY ** stats.throttle_streak
    return quota_score


def select_key(
    keys: List[str],
    stats_map,
    strategy: str = "quota",
    round_robin_counter=None,
) -> Optional[str]:
    """返回选中的 key；无可用的返回 None。

    stats_map: callable(key) -> Optional[KeyStats] 或 dict 映射。
    round_robin_counter: 供 round_robin 轮询用的可迭代计数器（如 itertools.cycle）。
    """
    get_stats = stats_map.get if isinstance(stats_map, dict) else stats_map

    available = [k for k in keys if (s := get_stats(k)) is not None and s.available()]

    if strategy == "round_robin":
        if not available:
            return None
        if round_robin_counter is None:
            return available[0]
        n = next(round_robin_counter) % len(available)
        return available[n]

    # quota 策略：余量 × 历史成功率综合得分最高的优先
    if not available:
        return None
    return max(available, key=lambda k: _score(get_stats(k)))


class RoundRobin:
    def __init__(self) -> None:
        self._counter = itertools.count()

    def __next__(self) -> int:
        return next(self._counter)
