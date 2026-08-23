"""每日用量台账：SQLite 落盘，按 天/平台/模型/key 维度聚合 token 与调用数。

设计：
  - 异步安全：所有写操作经 asyncio.to_thread 进线程，内部 RLock 保护
  - 幂等聚合：同一天同维度累加，不产生重复行
  - 自动清理：默认保留 90 天
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Dict, List, Optional

logger = logging.getLogger("gateway.usage")

RETENTION_DAYS = 90


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


class UsageLedger:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._lock = RLock()
        self._pending: set = set()  # 在途写库任务，防 GC
        self._init_db()
        self._last_cleanup_day: str = ""

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS usage_daily (
                        day TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        model TEXT NOT NULL,
                        key_masked TEXT NOT NULL,
                        requests INTEGER DEFAULT 0,
                        successes INTEGER DEFAULT 0,
                        failures INTEGER DEFAULT 0,
                        throttled INTEGER DEFAULT 0,
                        prompt_tokens INTEGER DEFAULT 0,
                        completion_tokens INTEGER DEFAULT 0,
                        total_tokens INTEGER DEFAULT 0,
                        PRIMARY KEY (day, provider, model, key_masked)
                    )
                """)
                conn.commit()
            finally:
                conn.close()

    def record(
        self,
        provider: str,
        model: str,
        key_masked: str,
        ok: bool,
        throttled: bool = False,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
    ) -> None:
        """记录一次调用。total_tokens 由前两者相加；流式无 usage 时为 0 也照常计次。"""
        total = max(0, prompt_tokens) + max(0, completion_tokens)
        day = _today()

        def _write() -> None:
            with self._lock:
                conn = self._connect()
                try:
                    conn.execute(
                        """
                        INSERT INTO usage_daily
                          (day, provider, model, key_masked, requests, successes,
                           failures, throttled, prompt_tokens, completion_tokens, total_tokens)
                        VALUES (?,?,?,?,1,?,?,?,?,?,?)
                        ON CONFLICT(day, provider, model, key_masked) DO UPDATE SET
                          requests = requests + 1,
                          successes = successes + excluded.successes,
                          failures = failures + excluded.failures,
                          throttled = throttled + excluded.throttled,
                          prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                          completion_tokens = completion_tokens + excluded.completion_tokens,
                          total_tokens = total_tokens + excluded.total_tokens
                        """,
                        (
                            day, provider, model, key_masked,
                            1 if ok else 0,
                            0 if ok else 1,
                            1 if throttled else 0,
                            max(0, prompt_tokens), max(0, completion_tokens), total,
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
                self._maybe_cleanup()

        # 写库放线程池，不阻塞事件循环；持有引用防止任务被 GC
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _write()
            return
        task = loop.run_in_executor(None, _write)
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    def _maybe_cleanup(self) -> None:
        """每天最多清理一次过期数据。"""
        today = _today()
        if self._last_cleanup_day == today:
            return
        self._last_cleanup_day = today
        try:
            conn = self._connect()
            try:
                conn.execute(
                    "DELETE FROM usage_daily WHERE day < date('now', ?)",
                    (f"-{RETENTION_DAYS} days",),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("usage cleanup failed: %s", exc)

    def query(self, days: int = 7, provider: Optional[str] = None) -> List[dict]:
        """查询最近 N 天的台账，按天/模型/key 汇总返回。"""
        conn = self._connect()
        try:
            sql = """
                SELECT day, provider, model, key_masked,
                       SUM(requests), SUM(successes), SUM(failures), SUM(throttled),
                       SUM(prompt_tokens), SUM(completion_tokens), SUM(total_tokens)
                FROM usage_daily
                WHERE day >= date('now', ?)
            """
            params: list = [f"-{max(1, days)} days"]
            if provider:
                sql += " AND provider = ?"
                params.append(provider)
            sql += """
                GROUP BY day, provider, model, key_masked
                ORDER BY day DESC, total_tokens DESC
            """
            rows = conn.execute(sql, params).fetchall()
            out = []
            for r in rows:
                (day, prov, model, key, req, ok_n, fail_n, thr_n,
                 pt, ct, tt) = r
                out.append({
                    "day": day, "provider": prov, "model": model, "key": key,
                    "requests": req, "successes": ok_n, "failures": fail_n,
                    "throttled": thr_n,
                    "prompt_tokens": pt, "completion_tokens": ct, "total_tokens": tt,
                })
            return out
        finally:
            conn.close()

    def summary(self, days: int = 7, provider: Optional[str] = None) -> dict:
        """按模型聚合的摘要（不带 key 维度），便于一眼看额度消耗。"""
        rows = self.query(days=days, provider=provider)
        agg: Dict[str, dict] = {}
        for r in rows:
            a = agg.setdefault(r["model"], {
                "requests": 0, "successes": 0, "failures": 0, "throttled": 0,
                "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0,
            })
            for f in ("requests", "successes", "failures", "throttled",
                      "prompt_tokens", "completion_tokens", "total_tokens"):
                a[f] += r[f]
        return {"days": days, "models": dict(sorted(agg.items(), key=lambda kv: -kv[1]["total_tokens"]))}
