"""Mock OpenAI 兼容上游：用于本地验证网关调度/切换/流式，不发真实请求。"""
from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlparse


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # 动态模型目录：测试可注入"仅出现在 /models、不在静态配置里"的模型
    dynamic_models = ["mock-model"]

    def log_message(self, fmt, *args):
        pass

    def _auth_key(self) -> str:
        auth = self.headers.get("Authorization", "")
        return auth.replace("Bearer ", "", 1).strip() if auth else ""

    def _send_json(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path.endswith("/models"):
            self._send_json(200, {"object": "list", "data": [{"id": m} for m in MockHandler.dynamic_models]})
        else:
            self._send_json(404, {"error": {"message": "not found"}})

    def do_POST(self):
        path = urlparse(self.path).path
        if not path.endswith("/chat/completions") and not path.endswith("/embeddings") and not path.endswith("/images/generations"):
            self._send_json(404, {"error": {"message": "not found"}})
            return

        key = self._auth_key()
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except Exception:
            body = {}

        # 故障注入：按 key 触发不同错误
        if key == "sk-bad-401":
            self._send_json(401, {"error": {"message": "invalid api key", "type": "invalid_request_error"}})
            return
        if key == "sk-bad-429":
            self._send_json(429, {"error": {"message": "rate limit exceeded", "type": "rate_limit_error"}})
            return
        if key == "sk-bad-500":
            self._send_json(500, {"error": {"message": "server error"}})
            return
        if key == "sk-bad-400":
            self._send_json(400, {"error": {"message": "invalid field 'response_format'", "type": "invalid_request_error"}})
            return

        if path.endswith("/images/generations"):
            self._send_json(200, {"created": int(time.time()), "data": [{"url": "http://mock/img.png"}]})
            return

        if path.endswith("/embeddings"):
            self._send_json(200, {"object": "list", "data": [{"embedding": [0.1] * 4, "index": 0}], "model": body.get("model", "")})
            return

        if body.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            chunks = [
                {"id": "mock", "object": "chat.completion.chunk", "choices": [{"delta": {"content": "你"}, "index": 0}]},
                {"id": "mock", "object": "chat.completion.chunk", "choices": [{"delta": {"content": "好"}, "index": 0}]},
                {"id": "mock", "object": "chat.completion.chunk", "choices": [{"delta": {}, "index": 0}], "usage": {"total_tokens": 42}},
            ]
            for c in chunks:
                self.wfile.write(f"data: {json.dumps(c, ensure_ascii=False)}\n\n".encode("utf-8"))
                self.wfile.flush()
                time.sleep(0.01)
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return

        self._send_json(200, {
            "id": "mock",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": body.get("model", ""),
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8},
        })


class MockUpstream:
    def __init__(self, port: int = 0):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), MockHandler)
        self.port = self.httpd.server_address[1]
        self.thread = Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def base_url(self):
        return f"http://127.0.0.1:{self.port}"


if __name__ == "__main__":
    up = MockUpstream().start()
    print(f"mock upstream on {up.base_url}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        up.stop()