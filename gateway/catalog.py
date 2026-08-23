"""动态模型目录：从各平台拉取真实模型列表，带 TTL 缓存与失败回退。

路由解析优先级（resolve 时逐级回退）：
  1. route_to 显式指定
  2. 静态 models 配置
  3. 动态目录（上游 /models 实时返回）
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple

from .models import ProviderConfig
from .provider import create_provider

logger = logging.getLogger("gateway.catalog")

DEFAULT_TTL = 300.0        # 动态模型缓存有效期（秒）
NEGATIVE_TTL = 60.0        # 拉取失败后的负缓存时长，避免每次请求都打上游


class ModelCatalog:
    def __init__(self, client, ttl: float = DEFAULT_TTL):
        self.client = client
        self.ttl = ttl
        # provider_name -> (fetched_at, set(models))
        self._cache: Dict[str, Tuple[float, set]] = {}
        # provider_name -> (failed_at,) 负缓存
        self._negative: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def dynamic_models(self, cfg: ProviderConfig) -> Optional[set]:
        """返回该平台动态模型集合；失败/负缓存期内返回 None。"""
        now = time.monotonic()
        hit = self._cache.get(cfg.name)
        if hit and (now - hit[0]) < self.ttl:
            return hit[1]
        neg = self._negative.get(cfg.name)
        if neg is not None and (now - neg) < NEGATIVE_TTL:
            return None

        async with self._lock:
            # 双重检查：等锁期间可能已被并发填充
            now = time.monotonic()
            hit = self._cache.get(cfg.name)
            if hit and (now - hit[0]) < self.ttl:
                return hit[1]
            neg = self._negative.get(cfg.name)
            if neg is not None and (now - neg) < NEGATIVE_TTL:
                return None

            provider = create_provider(cfg, self.client)
            fetched: set = set()
            for key in cfg.keys:
                try:
                    ids = await provider.models(key, timeout=10.0)
                except Exception as exc:  # 网络异常按拉取失败处理
                    logger.warning("catalog fetch %s failed: %s", cfg.name, exc)
                    ids = None
                if ids:
                    fetched.update(ids)
                    break
            if fetched:
                self._cache[cfg.name] = (now, fetched)
                self._negative.pop(cfg.name, None)
                return fetched
            self._negative[cfg.name] = now
            return None

    def invalidate(self, provider_name: Optional[str] = None) -> None:
        """热更新配置后失效缓存；provider_name 为空则全清。"""
        if provider_name is None:
            self._cache.clear()
            self._negative.clear()
        else:
            self._cache.pop(provider_name, None)
            self._negative.pop(provider_name, None)

    def cached_models(self) -> Dict[str, set]:
        """当前缓存内容快照（供 admin 观测）。"""
        return {name: set(models) for name, (_, models) in self._cache.items()}
