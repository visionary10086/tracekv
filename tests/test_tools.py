"""Protocol/measurement tests use a synthetic HTTP server, NOT a model benchmark."""
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import benchmark
import preflight


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"data":[{"id":"fixture-model"}]}' if self.path == "/v1/models" else b"fixture_counter 1\n")

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        mode = payload["messages"][-1]["content"]
        if mode == "http_error":
            self.send_error(503, "Fixture unavailable")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        if mode == "invalid_json":
            self.wfile.write(b"data: NOT_JSON\n\n")
            return
        def emit(obj):
            self.wfile.write(("data: " + json.dumps(obj, ensure_ascii=False) + "\r\n\r\n").encode("utf-8"))
            self.wfile.flush()
        emit({"choices": [{"index": 0, "delta": {"role": "assistant"}}]})
        if mode != "empty":
            emit({"choices": [{"index": 0, "delta": {"content": "测试"}}]})
            emit({"choices": [{"index": 0, "delta": {"content": "成功"}}]})
        if mode != "missing_finish":
            emit({"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
        if mode != "no_usage":
            emit({"choices": [], "usage": {"completion_tokens": 7, "prompt_tokens": 11}})
        if mode != "truncated":
            self.wfile.write(b"data: [DONE]\n\n")


class ToolsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def run_request(self, mode="normal"):
        return benchmark.request_one(self.url, "fixture-model",
            {"id": mode, "messages": [{"role": "user", "content": mode}]}, 0, 64, 3)

    def test_stream_unicode_and_usage_not_chunk_count(self):
        r = self.run_request()
        self.assertTrue(r["success"])
        self.assertEqual(r["output_text"], "测试成功")
        self.assertEqual(r["completion_tokens"], 7)
        self.assertLessEqual(r["ttft_ms"], r["e2e_ms"])

    def test_missing_usage_is_unknown_not_zero(self):
        r = self.run_request("no_usage")
        self.assertTrue(r["success"])
        self.assertIsNone(benchmark.summarize([r], 1)["successful_output_tokens_per_second"])

    def test_truncated_stream_fails(self):
        self.assertFalse(self.run_request("truncated")["success"])

    def test_missing_finish_fails(self):
        self.assertFalse(self.run_request("missing_finish")["success"])

    def test_empty_content_fails(self):
        r = self.run_request("empty")
        self.assertFalse(r["success"])
        self.assertIsNone(r["ttft_ms"])

    def test_http_error_recorded(self):
        r = self.run_request("http_error")
        self.assertFalse(r["success"])
        self.assertIn("503", r["error"])

    def test_invalid_json_fails(self):
        self.assertFalse(self.run_request("invalid_json")["success"])

    def test_sse_comments_crlf_multiline(self):
        data = io.BytesIO(b': comment\r\ndata: {"x":\r\ndata: 1}\r\n\r\ndata: [DONE]\n\n')
        self.assertEqual(list(benchmark.events(data)), ['{"x":\n1}', '[DONE]'])

    def test_summary_failure_denominator(self):
        good, bad = self.run_request(), self.run_request("http_error")
        summary = benchmark.summarize([good, bad], 2)
        self.assertEqual(summary["failure_rate"], 0.5)
        self.assertEqual(summary["successful_requests_per_second"], 0.5)
        self.assertEqual(summary["successful_output_tokens_per_second"], 3.5)
        self.assertTrue(summary["small_sample_warning"])

    def test_windows_wsl_decode(self):
        self.assertEqual(preflight.decode("没有已安装的分发。".encode("utf-16-le"), "utf-16-le"), "没有已安装的分发。")

    def test_workload_invalid_duplicate_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cases.jsonl"
            case = {"id": "x", "messages": [{"role": "user", "content": "x"}]}
            path.write_text((json.dumps(case) + "\n") * 2, encoding="utf-8")
            with self.assertRaises(ValueError):
                benchmark.load_cases(path)

    def test_cli_parallel_artifacts_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "synthetic_protocol_test_only"
            command = [sys.executable, str(ROOT / "tools" / "benchmark.py"),
                       "--base-url", self.url, "--model", "fixture-model",
                       "--workload", str(ROOT / "workloads" / "smoke.jsonl"),
                       "--out", str(out), "--requests", "6", "--concurrency", "2", "--warmup", "1"]
            result = subprocess.run(command, capture_output=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
            summary = json.loads((out / "summary.json").read_text())
            self.assertEqual(summary["successful_requests"], 6)
            self.assertEqual(summary["successful_output_tokens"], 42)
            self.assertEqual(len((out / "requests.jsonl").read_text(encoding="utf-8").splitlines()), 6)
            self.assertTrue((out / "metrics_after.txt").exists())
            before = (out / "manifest.json").read_bytes()
            repeat = subprocess.run(command, capture_output=True, timeout=10)
            self.assertNotEqual(repeat.returncode, 0)
            self.assertEqual((out / "manifest.json").read_bytes(), before)

    def test_cold_cache_rejects_warmup(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, str(ROOT / "tools" / "benchmark.py"),
                "--model", "fixture-model", "--workload", str(ROOT / "workloads" / "smoke.jsonl"),
                "--out", str(Path(directory) / "unused"), "--cache-state", "cold"],
                capture_output=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((Path(directory) / "unused").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
