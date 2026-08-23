"""启动入口：加载配置、启动服务、监听配置热加载。"""
from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import sys
import time
from pathlib import Path

import uvicorn
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from gateway.app import create_app
from gateway.errors import GatewayError
from gateway.registry import RegistryManager
from gateway.service import GatewayService


def setup_logging(log_dir: Path | None = None) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)

    if log_dir is not None:
        # 轮转文件日志：单文件 10MB，保留 5 个备份，防止无限增长吃满磁盘
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / "gw.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)


class ConfigWatcher(FileSystemEventHandler):
    def __init__(self, manager: RegistryManager, debounce_ms: int):
        self.manager = manager
        self.debounce = debounce_ms / 1000.0
        self._last = 0.0

    def on_modified(self, event):
        if event.is_directory:
            return
        now = time.monotonic()
        if now - self._last < self.debounce:
            return
        self._last = now
        try:
            self.manager.reload()
        except GatewayError as exc:
            logging.getLogger("gateway.main").warning("reload rejected, keeping old config: %s", exc.to_log())


def main() -> None:
    parser = argparse.ArgumentParser(description="Unified LLM API Gateway")
    parser.add_argument("--config", default="config.yaml", help="path to config.yaml")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    if not config_path.exists():
        log_tmp = logging.getLogger("gateway.main")
        log_tmp.error("config file not found: %s", config_path)
        sys.exit(1)

    setup_logging(config_path.parent)
    log = logging.getLogger("gateway.main")

    rm = RegistryManager(str(config_path))
    settings = rm.current().config.gateway
    service = GatewayService(rm, settings)
    app = create_app(service)

    # 后台 key 探活（probe_interval=0 可关闭）
    service._loop_probe = asyncio.new_event_loop()
    import threading

    def _run_probe_loop():
        asyncio.set_event_loop(service._loop_probe)
        service._loop_probe.run_until_complete(service.start_probe_loop())

    probe_thread = threading.Thread(target=_run_probe_loop, daemon=True, name="gateway-probe")
    probe_thread.start()

    watcher = ConfigWatcher(rm, settings.reload_debounce)
    observer = Observer()
    observer.schedule(watcher, str(config_path.parent), recursive=False)
    observer.start()
    log.info("config watcher started on %s", config_path)

    try:
        uvicorn.run(app, host=settings.host, port=settings.port, log_level="warning")
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()