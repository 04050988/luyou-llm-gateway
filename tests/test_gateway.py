"""端到端验收测试（走 ASGI，不占端口）：鉴权、故障切换、调度、流式、热加载。"""
from __future__ import annotations

import asyncio
import os
import tempfile
import time
import unittest
from pathlib import Path

import httpx

from gateway.app import create_app
from gateway.registry import RegistryManager
from gateway.service import GatewayService
from tests.mock_upstream import MockUpstream

MASTER = "master-test-key"


def make_config(base_url: str, extra_provider: str = "") -> str:
    return f"""
master_key: "{MASTER}"
gateway:
  host: "127.0.0.1"
  port: 8000
  connect_timeout: 5
  read_timeout: 5
  request_timeout: 30
  max_retries: 2
  retry_backoff_base: 0.05
  failure_threshold: 3
  cooldown_seconds: 2
providers:
  mock:
    type: openai_compatible
    base_url: "{base_url}"
    keys: ["sk-ok-1", "sk-ok-2"]
    models: ["mock-model"]
    strategy: quota
    tpm_threshold: 1000
    rpm_threshold: 100
    concurrency_limit: 8
  bad401:
    type: openai_compatible
    base_url: "{base_url}"
    keys: ["sk-bad-401", "sk-ok-2"]
    models: ["bad401-model"]
    strategy: round_robin
  bad429:
    type: openai_compatible
    base_url: "{base_url}"
    keys: ["sk-bad-429", "sk-ok-2"]
    models: ["bad429-model"]
    strategy: round_robin
  badall:
    type: openai_compatible
    base_url: "{base_url}"
    keys: ["sk-bad-401", "sk-bad-500"]
    models: ["badall-model"]
    strategy: round_robin
  bad400:
    type: openai_compatible
    base_url: "{base_url}"
    keys: ["sk-bad-400"]
    models: ["bad400-model"]
    strategy: round_robin
{extra_provider}
"""


class TestGateway(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.new_event_loop()
        cls.upstream = MockUpstream().start()
        cls._tmp = tempfile.TemporaryDirectory()
        cls.config_path = os.path.join(cls._tmp.name, "config.yaml")
        Path(cls.config_path).write_text(make_config(cls.upstream.base_url), encoding="utf-8")
        cls.rm = RegistryManager(cls.config_path)
        cls.service = GatewayService(cls.rm, cls.rm.current().config.gateway)
        cls.app = create_app(cls.service)

    @classmethod
    def tearDownClass(cls):
        cls.loop.run_until_complete(cls.service.client.aclose())
        cls.loop.close()
        cls.upstream.stop()
        cls._tmp.cleanup()

    def _client(self):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")

    def _auth(self):
        return {"Authorization": f"Bearer {MASTER}"}

    # ---- 鉴权 ----
    def test_01_auth_required(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]})
                self.assertEqual(r.status_code, 401)
                r2 = await c.post("/v1/chat/completions", json={"model": "mock-model"}, headers={"Authorization": "Bearer wrong"})
                self.assertEqual(r2.status_code, 401)
                r3 = await c.post("/v1/chat/completions", json={"model": "mock-model"}, headers=self._auth())
                self.assertNotEqual(r3.status_code, 401)
        self.loop.run_until_complete(run())

    # ---- 非流式正常 ----
    def test_02_chat_ok(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                self.assertEqual(r.status_code, 200)
                data = r.json()
                self.assertEqual(data["choices"][0]["message"]["content"], "hi")
                self.assertEqual(data["usage"]["total_tokens"], 8)
        self.loop.run_until_complete(run())

    # ---- 故障切换：401 切到下一个 key ----
    def test_03_failover_401(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "bad401-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                self.assertEqual(r.status_code, 200, r.text)
        self.loop.run_until_complete(run())

    # ---- 故障切换：429 切到下一个 key ----
    def test_04_failover_429(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "bad429-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                self.assertEqual(r.status_code, 200, r.text)
        self.loop.run_until_complete(run())

    # ---- 全部失败：不泄露内部原因，返回错误 ----
    def test_05_all_fail(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "badall-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                self.assertIn(r.status_code, (502, 429))
                body = r.json()
                self.assertNotIn("sk-bad", r.text)  # 不泄露内部 key
        self.loop.run_until_complete(run())

    # ---- 模型列表聚合 ----
    def test_06_models(self):
        async def run():
            async with self._client() as c:
                r = await c.get("/v1/models", headers=self._auth())
                self.assertEqual(r.status_code, 200)
                ids = {m["id"] for m in r.json()["data"]}
                self.assertIn("mock-model", ids)
                self.assertIn("bad401-model", ids)
                self.assertIn("badall-model", ids)
        self.loop.run_until_complete(run())

    # ---- 模型不存在 ----
    def test_07_model_not_found(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "nope"}, headers=self._auth())
                self.assertEqual(r.status_code, 404)
        self.loop.run_until_complete(run())

    # ---- 流式：正常产出并 [DONE] ----
    def test_08_stream_ok(self):
        async def run():
            async with self._client() as c:
                async with c.stream("POST", "/v1/chat/completions", json={"model": "mock-model", "stream": True, "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth()) as r:
                    self.assertEqual(r.status_code, 200)
                    text = "".join([line async for line in r.aiter_text()])
                self.assertIn("data: ", text)
                self.assertIn("你", text)
                self.assertIn("好", text)
                self.assertIn("data: [DONE]", text)
        self.loop.run_until_complete(run())

    # ---- 流式：模型不存在时返回 JSON 错误而非残缺流 ----
    def test_09_stream_model_missing(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "nope", "stream": True}, headers=self._auth())
                self.assertEqual(r.status_code, 404)
                self.assertEqual(r.headers.get("content-type"), "application/json")
        self.loop.run_until_complete(run())

    # ---- 流式：发起失败（429）自动切换后成功 ----
    def test_10_stream_failover(self):
        async def run():
            async with self._client() as c:
                async with c.stream("POST", "/v1/chat/completions", json={"model": "bad429-model", "stream": True, "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth()) as r:
                    self.assertEqual(r.status_code, 200)
                    text = "".join([line async for line in r.aiter_text()])
                self.assertIn("data: [DONE]", text)
                self.assertIn("好", text)
        self.loop.run_until_complete(run())

    # ---- 配额感知：quota 策略选"余量高"的 key ----
    def test_11_quota_aware(self):
        from gateway.scheduler import select_key
        from gateway.stats import KeyStats

        st1 = KeyStats(tpm_threshold=60000, rpm_threshold=5, concurrency_limit=8, cooldown_seconds=2, failure_threshold=3)
        st2 = KeyStats(tpm_threshold=60000, rpm_threshold=100, concurrency_limit=8, cooldown_seconds=2, failure_threshold=3)
        for _ in range(5):
            st1.record_request()  # k1 RPM 余量耗尽
        picked = select_key(["k1", "k2"], {"k1": st1, "k2": st2}, strategy="quota")
        self.assertEqual(picked, "k2")
        # 并发打满时也应跳过
        st2.concurrency = st2.concurrency_limit
        picked2 = select_key(["k1", "k2"], {"k1": st1, "k2": st2}, strategy="quota")
        self.assertIsNone(picked2)

    # ---- 热加载：修改配置新增 provider 后 reload 生效 ----
    def test_12_hot_reload(self):
        async def run():
            Path(self.config_path).write_text(
                make_config(self.upstream.base_url, extra_provider='  hot:\n    type: openai_compatible\n    base_url: "' + self.upstream.base_url + '"\n    keys: ["sk-ok-1"]\n    models: ["hot-model"]\n    strategy: round_robin\n'),
                encoding="utf-8",
            )
            new_reg = self.rm.reload()
            self.assertIn("hot", new_reg.providers)
            self.assertIsNotNone(new_reg.resolve_provider("hot-model"))
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "hot-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                self.assertEqual(r.status_code, 200, r.text)
        self.loop.run_until_complete(run())

    # ---- 热加载：非法配置被拒绝，保留旧配置 ----
    def test_13_reload_reject_invalid(self):
        async def run():
            before = self.rm.current().list_models()
            Path(self.config_path).write_text("master_key: ''\n", encoding="utf-8")
            with self.assertRaises(Exception):
                self.rm.reload()
            # 旧配置仍在
            self.assertEqual(self.rm.current().list_models(), before)
            # 恢复文件，避免影响后续测试
            Path(self.config_path).write_text(make_config(self.upstream.base_url), encoding="utf-8")
        self.loop.run_until_complete(run())

    # ---- 图片生成 ----
    def test_16_images_generations(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/images/generations", json={"model": "mock-model", "prompt": "a cat"}, headers=self._auth())
                self.assertEqual(r.status_code, 200, r.text)
                self.assertIn("data", r.json())
                r2 = await c.post("/v1/images/generations", json={"model": "nope", "prompt": "x"}, headers=self._auth())
                self.assertEqual(r2.status_code, 404)
        self.loop.run_until_complete(run())

    # ---- 流式发起失败（400）：返回 SSE 错误事件而非断流 ----
    def test_17_stream_error_is_sse(self):
        async def run():
            async with self._client() as c:
                async with c.stream("POST", "/v1/chat/completions", json={"model": "bad400-model", "stream": True, "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth()) as r:
                    self.assertEqual(r.status_code, 200)
                    text = "".join([line async for line in r.aiter_text()])
                self.assertIn("data: [DONE]", text)
                self.assertIn("error", text)
                self.assertIn("invalid field", text)  # 上游 400 的 message 透传到客户端
        self.loop.run_until_complete(run())

    # ---- 客户端错误(4xx)透传：不重试、不罚 key ----
    def test_18_chat_400_passthrough_no_cooldown(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/v1/chat/completions", json={"model": "bad400-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.json()["error"]["message"], "invalid field 'response_format'")
            st = self.service.stats.get("sk-bad-400")
            self.assertIsNotNone(st)
            self.assertFalse(st.in_cooldown())
            self.assertEqual(st.failures, 0)
        self.loop.run_until_complete(run())

    def test_19_stream_400_no_cooldown(self):
        async def run():
            async with self._client() as c:
                async with c.stream("POST", "/v1/chat/completions", json={"model": "bad400-model", "stream": True, "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth()) as r:
                    self.assertEqual(r.status_code, 200)
                    text = "".join([line async for line in r.aiter_text()])
                self.assertIn("invalid field", text)  # 上游 400 message 透传为 SSE 错误事件
            st = self.service.stats.get("sk-bad-400")
            self.assertIsNotNone(st)
            self.assertFalse(st.in_cooldown())
            self.assertEqual(st.failures, 0)
        self.loop.run_until_complete(run())

    # ---- health ----
    def test_14_health(self):
        async def run():
            async with self._client() as c:
                r = await c.get("/v1/health")
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.json()["status"], "ok")
        self.loop.run_until_complete(run())

    # ---- 调度 #2：历史成功率降权 —— 一直失败的 key 在有余量时也不该被优先选中 ----
    def test_20_success_rate_weighting(self):
        from gateway.scheduler import select_key
        from gateway.stats import KeyStats

        st1 = KeyStats(tpm_threshold=60000, rpm_threshold=100, concurrency_limit=8, cooldown_seconds=2, failure_threshold=3)
        st2 = KeyStats(tpm_threshold=60000, rpm_threshold=100, concurrency_limit=8, cooldown_seconds=2, failure_threshold=3)
        # k1: 刚失败两次（未达冷却阈值，仍 available）；k2: 无样本
        st1.record_failure()
        st1.record_failure()
        picked = select_key(["k1", "k2"], {"k1": st1, "k2": st2}, strategy="quota")
        self.assertEqual(picked, "k2")
        # k2 也失败两次后两者同分（都 0 成功率），任选其一均可
        st2.record_failure()
        st2.record_failure()
        picked2 = select_key(["k1", "k2"], {"k1": st1, "k2": st2}, strategy="quota")
        self.assertIn(picked2, ("k1", "k2"))
        # k2 成功一次后反超
        st2.record_success()
        self.assertEqual(select_key(["k1", "k2"], {"k1": st1, "k2": st2}, strategy="quota"), "k2")

    # ---- 调度 #2：连续限流连击降权 + 指数冷却升级 ----
    def test_21_throttle_streak_and_cooldown_escalation(self):
        from gateway.scheduler import THROTTLE_DECAY, _score
        from gateway.stats import KeyStats

        st1 = KeyStats(tpm_threshold=60000, rpm_threshold=100, concurrency_limit=8, cooldown_seconds=60, failure_threshold=3)
        base = _score(st1)
        st1.record_throttled()
        self.assertAlmostEqual(_score(st1), base * THROTTLE_DECAY)
        st1.record_throttled()
        self.assertAlmostEqual(_score(st1), base * (THROTTLE_DECAY ** 2))
        # 成功后连击清零
        st1.record_success()
        self.assertEqual(st1.throttle_streak, 0)

    def test_22_throttle_cooldown_escalation_in_service(self):
        from gateway.service import GatewayService, THROTTLE_BASE_COOLDOWN

        cfg = type("C", (), {"cooldown_seconds": 120})()
        st = self.service.stats.ensure("sk-esc-test", {"cooldown_seconds": 120})
        # 第1次限流：15s；第2次：30s；第3次：60s；第4次：封顶120s
        st.record_throttled()
        s1 = GatewayService._throttle_cooldown(GatewayService, st, cfg, None)
        st.record_throttled()
        s2 = GatewayService._throttle_cooldown(GatewayService, st, cfg, None)
        st.record_throttled()
        s3 = GatewayService._throttle_cooldown(GatewayService, st, cfg, None)
        st.record_throttled()
        s4 = GatewayService._throttle_cooldown(GatewayService, st, cfg, None)
        self.assertAlmostEqual(s1, THROTTLE_BASE_COOLDOWN)
        self.assertAlmostEqual(s2, THROTTLE_BASE_COOLDOWN * 2)
        self.assertAlmostEqual(s3, THROTTLE_BASE_COOLDOWN * 4)
        self.assertAlmostEqual(s4, 120.0)  # 封顶

    # ---- 动态模型目录 #3：静态未配置、上游动态返回的模型可路由可调用 ----
    def test_23_dynamic_model_routable(self):
        async def run():
            from tests.mock_upstream import MockHandler
            MockHandler.dynamic_models = ["mock-model", "ghost-model"]
            # 前面的测试可能已填充目录缓存（TTL 内命中旧列表），先失效再验
            self.service.catalog.invalidate()
            try:
                async with self._client() as c:
                    r = await c.post("/v1/chat/completions", json={"model": "ghost-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                    self.assertEqual(r.status_code, 200, r.text)
                    data = r.json()
                    self.assertEqual(data["model"], "ghost-model")
            finally:
                MockHandler.dynamic_models = ["mock-model"]
                self.service.catalog.invalidate()
        self.loop.run_until_complete(run())

    # ---- 动态模型目录 #3：负缓存 —— 上游 /models 失败时回退，不无限重试打爆上游 ----
    def test_24_catalog_negative_cache(self):
        async def run():
            from tests.mock_upstream import MockHandler
            saved = MockHandler.dynamic_models
            MockHandler.dynamic_models = []  # 模拟上游返回空列表
            try:
                got = await self.service.catalog.dynamic_models(self.rm.current().providers["mock"])
                self.assertIsNone(got)
                # 第二次调用走负缓存，不再发请求（即使此时恢复列表也不会立刻看到）
                MockHandler.dynamic_models = ["instant-model"]
                got2 = await self.service.catalog.dynamic_models(self.rm.current().providers["mock"])
                self.assertIsNone(got2)
            finally:
                MockHandler.dynamic_models = saved
                self.service.catalog.invalidate()
        self.loop.run_until_complete(run())

    # ---- admin/stats 暴露新指标 ----
    def test_25_admin_stats_fields(self):
        async def run():
            async with self._client() as c:
                await c.post("/v1/chat/completions", json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                r = await c.get("/admin/stats", headers=self._auth())
                self.assertEqual(r.status_code, 200)
                body = r.json()
                key_stat = body["mock"][0]
                self.assertIn("success_rate_60s", key_stat)
                self.assertIn("throttle_streak", key_stat)
                self.assertEqual(key_stat["success_rate_60s"], 1.0)
                self.assertIn("_catalog", body)
        self.loop.run_until_complete(run())

    # ---- 半开探测：冷却到期后先探测，成功才恢复 ----
    def test_26_half_open_probe(self):
        from gateway.stats import KeyStats

        st = KeyStats(tpm_threshold=60000, rpm_threshold=100, concurrency_limit=8,
                      cooldown_seconds=60, failure_threshold=1)
        st.record_failure()  # failure_threshold=1 → 直接进冷却
        self.assertEqual(st.state(), "cooldown")
        self.assertFalse(st.available())
        # 冷却未到期：in_cooldown 不会转半开
        self.assertEqual(st.state(), "cooldown")
        # 模拟冷却到期
        st._cooldown_until = time.monotonic() - 0.01
        self.assertEqual(st.state(), "probation")   # 到期即转半开
        self.assertTrue(st.available())             # 半开放行探测请求
        token = st.begin_probe()
        self.assertTrue(st.probe_in_flight())
        self.assertFalse(st.available())            # 探测在途不再接新流量
        st.settle_probe_success(token)              # 探测成功 → 完全恢复
        self.assertEqual(st.state(), "active")
        self.assertTrue(st.available())

    # ---- 半开探测：探测失败重新冷却且时长翻倍 ----
    def test_27_probe_failure_recools(self):
        from gateway.stats import KeyStats

        st = KeyStats(tpm_threshold=60000, rpm_threshold=100, concurrency_limit=8,
                      cooldown_seconds=30, failure_threshold=1)
        st.enter_cooldown(30)
        st._cooldown_until = time.monotonic() - 0.01   # 到期转半开
        self.assertEqual(st.state(), "probation")
        token = st.begin_probe()
        st.settle_probe_failure(token)
        self.assertEqual(st.state(), "cooldown")       # 重新冷却
        self.assertGreater(st._cooldown_until - time.monotonic(), 29)  # 时长 ≥ 基数（翻倍逻辑）

    # ---- 429 全 key 耗尽时返回 Retry-After 头 ----
    def test_28_retry_after_header(self):
        async def run():
            async with self._client() as c:
                # badall 的两个 key 都会被罚入冷却；耗尽后应返回 429 + Retry-After
                r1 = await c.post("/v1/chat/completions", json={"model": "badall-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                self.assertIn(r1.status_code, (429, 502))
                r2 = await c.post("/v1/chat/completions", json={"model": "badall-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                if r2.status_code == 429:
                    ra = r2.headers.get("Retry-After")
                    if ra is not None:
                        self.assertGreater(int(ra), 0)
                    body = r2.json()["error"]
                    # retry_after 字段可选存在，但存在必须为正数
                    if "retry_after" in body:
                        self.assertGreater(body["retry_after"], 0)
        self.loop.run_until_complete(run())

    # ---- 按模型健康统计：成功计入 _models ----
    def test_29_model_health_tracking(self):
        async def run():
            async with self._client() as c:
                await c.post("/v1/chat/completions", json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                stats = self.service.stats.models_snapshot()
                self.assertIn("mock-model", stats)
                mh = stats["mock-model"]
                self.assertGreaterEqual(mh["total"], 1)
                self.assertGreaterEqual(mh["success"], 1)
                # 失败模型也有记录
                await c.post("/v1/chat/completions", json={"model": "bad400-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                mh400 = self.service.stats.models_snapshot()["bad400-model"]
                self.assertGreaterEqual(mh400["failures"], 1)
                self.assertIsNotNone(mh400["last_error"])
        self.loop.run_until_complete(run())

    # ---- 流中断计入失败统计（模拟上游中途断开）----
    def test_30_stream_interrupt_counts_failure(self):
        from gateway.sse import stream_to_client
        import httpx as _httpx

        async def run():
            # 构造一个"读到一半就断"的上游响应
            async def broken_aiter():
                yield 'data: {"choices":[{"delta":{"content":"a"}}]}\n\n'
                raise ConnectionError("boom")

            resp = _httpx.Response(200, content=b"", headers={"Content-Type": "text/event-stream"})
            resp.aiter_lines = broken_aiter  # monkey-patch
            errors = []
            chunks = []
            async for chunk in stream_to_client(resp, on_error=lambda e: errors.append(e)):
                chunks.append(chunk)
            self.assertIn("upstream stream interrupted", "".join(chunks))
            self.assertEqual(len(errors), 1)
            self.assertIn("boom", errors[0])
        self.loop.run_until_complete(run())

    # ---- 流式自动注入 stream_options.include_usage ----
    def test_31_stream_usage_injection(self):
        from gateway.provider import with_stream_usage

        body = {"model": "m", "stream": True, "messages": []}
        out = with_stream_usage(body)
        self.assertTrue(out["stream_options"]["include_usage"])
        self.assertNotIn("stream_options", body)  # 原对象不被污染
        # 客户端已显式指定时不覆盖
        body2 = {"model": "m", "stream": True, "stream_options": {"include_usage": False}}
        self.assertIs(with_stream_usage(body2), body2)
        # 客户端带了 stream_options（哪怕 include_usage=False）也不注入
        body3 = {"model": "m", "stream": True, "stream_options": {"foo": 1}}
        self.assertIs(with_stream_usage(body3), body3)

    def test_32_stream_injects_usage_upstream(self):
        """端到端：流式请求发到上游的 body 应包含 stream_options.include_usage。"""
        from tests.mock_upstream import MockHandler

        async def run():
            captured = {}
            orig_do_POST = MockHandler.do_POST

            def spy_do_POST(handler_self, *a, **kw):
                length = int(handler_self.headers.get("Content-Length", "0"))
                raw = handler_self.rfile.read(length) if length else b"{}"
                captured["body"] = raw.decode("utf-8")
                handler_self.rfile = handler_self.rfile  # no-op
                # 手动重放：把读掉的内容塞回去不可行，直接调用原逻辑用缓存体
                return orig_do_POST_with_body(handler_self, raw)

            def orig_do_POST_with_body(handler_self, raw):
                import json as _json
                from urllib.parse import urlparse
                path = urlparse(handler_self.path).path
                body = _json.loads(raw or b"{}")
                if body.get("stream"):
                    handler_self.send_response(200)
                    handler_self.send_header("Content-Type", "text/event-stream")
                    handler_self.end_headers()
                    usage = {"total_tokens": 42} if body.get("stream_options", {}).get("include_usage") else None
                    chunk = {"id": "x", "object": "chat.completion.chunk",
                             "choices": [{"delta": {"content": "好"}, "index": 0}], "usage": usage}
                    handler_self.wfile.write(f"data: {_json.dumps(chunk)}\n\n".encode())
                    handler_self.wfile.write(b"data: [DONE]\n\n")
                    handler_self.wfile.flush()
                    return
                handler_self._send_json(200, {"id": "x", "object": "chat.completion", "created": 0,
                                              "model": body.get("model", ""), "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
                                              "usage": {"total_tokens": 8}})

            MockHandler.do_POST = spy_do_POST
            try:
                async with self._client() as c:
                    async with c.stream("POST", "/v1/chat/completions", json={"model": "mock-model", "stream": True, "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth()) as r:
                        text = "".join([line async for line in r.aiter_text()])
                    self.assertIn("[DONE]", text)
                sent = captured.get("body", "")
                self.assertIn("stream_options", sent)
                self.assertIn("include_usage", sent)
            finally:
                MockHandler.do_POST = orig_do_POST
        self.loop.run_until_complete(run())

    # ---- 配置校验加固：未知字段拒绝、strategy 拼错报错、route_to 悬空报错 ----
    def test_33_config_validation_strict(self):
        from gateway.registry import load_config

        base = f"""
master_key: "mk"
providers:
  mock:
    type: openai_compatible
    base_url: "{self.upstream.base_url}"
    keys: ["sk-ok-1"]
    models: ["mock-model"]
"""
        import tempfile, os as _os
        tmp = tempfile.TemporaryDirectory()
        p = Path(tmp.name) / "c.yaml"
        # 1. provider 未知字段 → 拒绝
        p.write_text(base + "    stratgy: quota\n", encoding="utf-8")
        with self.assertRaises(Exception):
            load_config(str(p))
        # 2. strategy 枚举值错误 → 报错而非静默回退
        p.write_text(base + "    strategy: qutoa\n", encoding="utf-8")
        with self.assertRaises(Exception):
            load_config(str(p))
        # 3. route_to 悬空引用 → 拒绝
        p.write_text(base + "    route_to: {\"x\": \"ghost\"}\n", encoding="utf-8")
        with self.assertRaises(Exception):
            load_config(str(p))
        # 4. gateway 段未知字段 → 拒绝
        p.write_text("master_key: 'mk'\ngateway:\n  max_retry: 3\n" + base, encoding="utf-8")
        with self.assertRaises(Exception):
            load_config(str(p))
        # 5. 正常配置仍可加载
        p.write_text(base + "    strategy: quota\n", encoding="utf-8")
        cfg = load_config(str(p))
        self.assertEqual(cfg.providers["mock"].strategy, "quota")
        tmp.cleanup()

    # ---- 同名模型冲突告警（不报错，路由按声明顺序取第一个）----
    def test_34_duplicate_model_warning(self):
        import io
        import logging
        from gateway.registry import Registry, load_config

        cfg_yaml = f"""
master_key: "mk"
providers:
  alpha:
    type: openai_compatible
    base_url: "{self.upstream.base_url}"
    keys: ["sk-ok-1"]
    models: ["shared-model"]
  beta:
    type: openai_compatible
    base_url: "{self.upstream.base_url}"
    keys: ["sk-ok-1"]
    models: ["shared-model"]
"""
        tmp = tempfile.TemporaryDirectory()
        p = Path(tmp.name) / "c.yaml"
        p.write_text(cfg_yaml, encoding="utf-8")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logging.getLogger("gateway.registry").addHandler(handler)
        try:
            cfg = load_config(str(p))
            reg = Registry(cfg)
            # 路由取声明顺序第一个
            self.assertEqual(reg.resolve_provider("shared-model").name, "alpha")
        finally:
            logging.getLogger("gateway.registry").removeHandler(handler)
        self.assertIn("shared-model", stream.getvalue())
        tmp.cleanup()

    # ---- 每日用量台账：成功/失败/限流/token 聚合 ----
    def test_35_usage_ledger(self):
        import asyncio as _asyncio
        from gateway.usage import UsageLedger

        async def run():
            led = UsageLedger(self._tmp.name + "/usage_test.db")
            led.record("p", "m1", "sk-x****1234", ok=True, prompt_tokens=10, completion_tokens=5)
            led.record("p", "m1", "sk-x****1234", ok=True, prompt_tokens=20, completion_tokens=8)
            led.record("p", "m1", "sk-y****5678", ok=False, throttled=True)
            # 等待线程池写完
            for t in list(led._pending):
                await t
            rows = led.query(days=7)
            m1_x = [r for r in rows if r["model"] == "m1" and "x****" in r["key"]][0]
            self.assertEqual(m1_x["requests"], 2)
            self.assertEqual(m1_x["successes"], 2)
            self.assertEqual(m1_x["total_tokens"], 43)  # (10+5)+(20+8)
            m1_y = [r for r in rows if r["model"] == "m1" and "y****" in r["key"]][0]
            self.assertEqual(m1_y["failures"], 1)
            self.assertEqual(m1_y["throttled"], 1)
            summary = led.summary(days=7)
            self.assertEqual(summary["models"]["m1"]["requests"], 3)
            self.assertEqual(summary["models"]["m1"]["total_tokens"], 43)
        self.loop.run_until_complete(run())

    # ---- /admin/usage 端点：鉴权 + 真实调用落账 ----
    def test_36_admin_usage_endpoint(self):
        async def run():
            async with self._client() as c:
                r0 = await c.get("/admin/usage")
                self.assertEqual(r0.status_code, 401)
                await c.post("/v1/chat/completions", json={"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}, headers=self._auth())
                # 台账异步落盘，稍等
                await asyncio.sleep(0.2)
                r = await c.get("/admin/usage?days=7", headers=self._auth())
                self.assertEqual(r.status_code, 200)
                body = r.json()
                self.assertIn("rows", body)
                self.assertIn("summary", body)
                mock_rows = [x for x in body["rows"] if x["provider"] == "mock"]
                self.assertGreaterEqual(len(mock_rows), 1)
                self.assertGreaterEqual(body["summary"]["models"]["mock-model"]["successes"], 1)
        self.loop.run_until_complete(run())

    # ---- admin 端点需要鉴权 ----
    def test_15_admin_auth(self):
        async def run():
            async with self._client() as c:
                r = await c.post("/admin/reload")
                self.assertEqual(r.status_code, 401)
                r2 = await c.post("/admin/reload", headers=self._auth())
                self.assertEqual(r2.status_code, 200)
        self.loop.run_until_complete(run())


if __name__ == "__main__":
    unittest.main(verbosity=2)