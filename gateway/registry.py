"""平台注册表：配置加载、校验、环境变量替换与原子热更新。"""
from __future__ import annotations

import logging
import os
import re
from typing import Dict, List, Optional

import yaml

from .errors import invalid_request
from .models import GatewayConfig, GatewaySettings, ProviderConfig

logger = logging.getLogger("gateway.registry")

ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env(value):
    """把字符串里的 ${VAR} 替换为环境变量值；缺失时保留原样。"""
    if not isinstance(value, str):
        return value

    def _sub(m):
        name = m.group(1)
        return os.environ.get(name, m.group(0))

    return ENV_RE.sub(_sub, value)


class Registry:
    """不可变配置快照；热更新时整体替换。"""

    def __init__(self, config: GatewayConfig):
        self.config = config

    # ---- 查询 ----
    def provider(self, name: str) -> Optional[ProviderConfig]:
        return self.config.providers.get(name)

    @property
    def providers(self) -> Dict[str, ProviderConfig]:
        return self.config.providers

    def resolve_provider(self, model: str) -> Optional[ProviderConfig]:
        """模型名 → 平台。优先 route_to 显式指定，否则按声明顺序取第一个含该模型的平台。"""
        for p in self.config.providers.values():
            if model in p.route_to:
                return self.provider(p.route_to[model])
        for p in self.config.providers.values():
            if model in p.models:
                return p
        return None

    def list_models(self) -> List[str]:
        seen = set()
        out = []
        for p in self.config.providers.values():
            for m in p.models:
                if m not in seen:
                    seen.add(m)
                    out.append(m)
        return out


def _build_settings(raw_gateway: Optional[dict]) -> GatewaySettings:
    s = GatewaySettings()
    if not raw_gateway:
        return s
    known = ("host", "port", "connect_timeout", "read_timeout", "request_timeout",
             "max_retries", "retry_backoff_base", "cooldown_seconds", "failure_threshold",
             "reload_debounce")
    unknown = set(raw_gateway) - set(known)
    if unknown:
        raise invalid_request(
            f"gateway: unknown fields {sorted(unknown)}", detail="config validation failed"
        )
    for f in known:
        if f in raw_gateway:
            setattr(s, f, raw_gateway[f])
    return s


# ProviderConfig 支持的配置字段；写错字（如 strategy 拼错之外的未知键）直接拒绝
_PROVIDER_FIELDS = {
    "type", "base_url", "keys", "models", "strategy", "tpm_threshold", "rpm_threshold",
    "concurrency_limit", "cooldown_seconds", "route_to",
    "chat_path", "models_path", "embeddings_path", "images_path",
}


def _build_provider(name: str, raw: dict) -> ProviderConfig:
    unknown = set(raw) - _PROVIDER_FIELDS
    if unknown:
        raise invalid_request(
            f"provider '{name}': unknown fields {sorted(unknown)}",
            detail="config validation failed",
        )
    keys = [expand_env(k) for k in raw.get("keys", [])]
    p = ProviderConfig(
        name=name,
        type=raw.get("type", "openai_compatible"),
        base_url=str(raw.get("base_url", "")).rstrip("/"),
        keys=keys,
        models=list(raw.get("models", [])),
        strategy=raw.get("strategy", "round_robin"),
        tpm_threshold=int(raw.get("tpm_threshold", 60000)),
        rpm_threshold=int(raw.get("rpm_threshold", 100)),
        concurrency_limit=int(raw.get("concurrency_limit", 8)),
        cooldown_seconds=int(raw.get("cooldown_seconds", 60)),
        route_to=raw.get("route_to", {}),
        chat_path=raw.get("chat_path", "/chat/completions"),
        models_path=raw.get("models_path", "/models"),
        embeddings_path=raw.get("embeddings_path", "/embeddings"),
        images_path=raw.get("images_path", "/images/generations"),
    )
    return p


def validate(config: GatewayConfig) -> List[str]:
    errors: List[str] = []
    warnings: List[str] = []
    if not config.master_key:
        errors.append("master_key 未配置")
    # route_to 指向的平台必须存在
    for name, p in config.providers.items():
        for model, target in p.route_to.items():
            if target not in config.providers:
                errors.append(f"provider '{name}': route_to['{model}'] 指向不存在的平台 '{target}'")
    # 同名模型出现在多个平台：路由按声明顺序取第一个，提醒用户避免歧义
    owner: dict[str, str] = {}
    for name, p in config.providers.items():
        for m in p.models:
            if m in owner and owner[m] != name:
                warnings.append(f"模型 '{m}' 同时配置在 '{owner[m]}' 和 '{name}'，路由优先使用 '{owner[m]}'（可用 route_to 显式指定）")
            else:
                owner.setdefault(m, name)
    for name, p in config.providers.items():
        if p.type not in ("sensenova", "openai_compatible"):
            errors.append(f"provider '{name}': 未知 type '{p.type}'")
        if not p.base_url:
            errors.append(f"provider '{name}': base_url 为空")
        if not p.keys:
            errors.append(f"provider '{name}': 没有配置任何 key")
        if p.strategy not in ("quota", "round_robin"):
            errors.append(f"provider '{name}': 未知 strategy '{p.strategy}'（可选 quota / round_robin）")
        if not p.models:
            errors.append(f"provider '{name}': models 为空")
    for w in warnings:
        logger.warning("config warning: %s", w)
    return errors


def load_config(path: str) -> GatewayConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    raw_master = expand_env(raw.get("master_key", ""))
    cfg = GatewayConfig(
        master_key=raw_master,
        gateway=_build_settings(raw.get("gateway")),
        providers={name: _build_provider(name, p) for name, p in (raw.get("providers") or {}).items()},
    )
    errs = validate(cfg)
    if errs:
        raise invalid_request("; ".join(errs), detail="config validation failed")
    return cfg


class RegistryManager:
    """持有当前 Registry，负责原子热更新。"""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self._registry = Registry(load_config(config_path))
        logger.info("config loaded from %s", config_path)

    def current(self) -> Registry:
        return self._registry

    def reload(self) -> Registry:
        """先完整解析校验，成功后整体替换引用；失败抛错、保留旧配置。"""
        new_cfg = load_config(self.config_path)
        new_reg = Registry(new_cfg)
        self._registry = new_reg
        logger.info("config reloaded (providers=%s)", list(new_reg.providers))
        return new_reg