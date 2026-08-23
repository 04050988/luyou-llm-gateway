"""数据模型定义。"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class ProviderConfig:
    name: str
    type: str
    base_url: str
    keys: List[str]
    models: List[str] = field(default_factory=list)
    strategy: str = "round_robin"  # quota / round_robin
    tpm_threshold: int = 60000
    rpm_threshold: int = 100
    concurrency_limit: int = 8
    cooldown_seconds: int = 60
    route_to: Dict[str, str] = field(default_factory=dict)  # model -> provider name
    chat_path: str = "/chat/completions"
    models_path: str = "/models"
    embeddings_path: str = "/embeddings"
    images_path: str = "/images/generations"


@dataclass
class GatewaySettings:
    host: str = "127.0.0.1"
    port: int = 8000
    connect_timeout: float = 10.0
    read_timeout: float = 60.0
    request_timeout: float = 600.0
    max_retries: int = 2
    retry_backoff_base: float = 1.0
    cooldown_seconds: int = 60
    failure_threshold: int = 3
    reload_debounce: int = 500


@dataclass
class GatewayConfig:
    master_key: str
    gateway: GatewaySettings = field(default_factory=GatewaySettings)
    providers: Dict[str, ProviderConfig] = field(default_factory=dict)