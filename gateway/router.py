"""模型 → 平台 路由（冲突规则见 registry.resolve_provider）。"""
from __future__ import annotations

from .errors import model_not_found
from .registry import Registry


def route(registry: Registry, model: str):
    p = registry.resolve_provider(model)
    if p is None:
        raise model_not_found(model)
    return p