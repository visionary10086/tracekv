import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from collector.demo import collect_system_metrics
from contracts.schema import validate_snapshot
from engine.vllm_adapter import VllmAdapter, parse_metrics
from policy.demo import recommend_policy


SAMPLE = json.loads((Path(__file__).resolve().parents[1] / "contracts/sample_snapshot.json").read_text())


class ContractTests(unittest.TestCase):
    def test_sample_and_recommendation(self):
        self.assertIs(validate_snapshot(SAMPLE), SAMPLE)
        result = recommend_policy(SAMPLE)
        self.assertEqual((result["policy"], result["bottleneck"]), ("os_aware", "io_pressure"))
        self.assertEqual(result["apply_mode"], "next_run")

    def test_invalid_ratios_and_time(self):
        for key, value in (("prefix_hit_ratio", -0.1), ("usage_ratio", 1.2)):
            changed = json.loads(json.dumps(SAMPLE))
            changed["kv"][key] = value
            with self.assertRaises(ValueError):
                validate_snapshot(changed)
        changed = json.loads(json.dumps(SAMPLE))
        changed["timestamp"] = "2026-11-01T12:00:00"
        with self.assertRaises(ValueError):
            validate_snapshot(changed)

    def test_memory_and_unknown_rules(self):
        changed = json.loads(json.dumps(SAMPLE))
        changed["system"].update(disk_read_p95_ms=None, mem_available_mb=512)
        self.assertEqual(recommend_policy(changed)["bottleneck"], "memory_pressure")
        changed["system"]["mem_available_mb"] = None
        changed["kv"]["prefix_hit_ratio"] = None
        self.assertEqual(recommend_policy(changed)["bottleneck"], "unknown")

    def test_proc_memory_source(self):
        with patch("pathlib.Path.read_text", return_value="MemTotal: 5000 kB\nMemAvailable: 2000 kB\n"):
            result = collect_system_metrics("/fake/meminfo")
        self.assertEqual(result["mem_available_mb"], 2.048)
        self.assertEqual(result["source"], "proc")
        self.assertIsNone(result["disk_read_p95_ms"])


class VllmMetricTests(unittest.TestCase):
    def test_window_delta_and_counter_reset(self):
        bodies = [
            b'vllm:kv_cache_usage_perc{engine="0"} 0.8\nvllm:num_requests_running{engine="0"} 2\n'
            b'vllm:prefix_cache_queries_total{engine="0"} 100\nvllm:prefix_cache_hits_total{engine="0"} 20\n',
            b'vllm:kv_cache_usage_perc{engine="0"} 0.7\nvllm:num_requests_running{engine="0"} 1\n'
            b'vllm:prefix_cache_queries_total{engine="0"} 120\nvllm:prefix_cache_hits_total{engine="0"} 30\n',
            b'vllm:prefix_cache_queries_total{engine="0"} 3\nvllm:prefix_cache_hits_total{engine="0"} 1\n',
        ]
        adapter = VllmAdapter()
        with patch("urllib.request.urlopen", side_effect=[io.BytesIO(x) for x in bodies]):
            self.assertIsNone(adapter.sample()["prefix_hit_ratio"])
            self.assertEqual(adapter.sample()["prefix_hit_ratio"], 0.5)
            self.assertIsNone(adapter.sample()["prefix_hit_ratio"])

    def test_parse_recorded_metric_names(self):
        parsed = parse_metrics('vllm:kv_cache_usage_perc{engine="0"} 0.25\n'
                               'vllm:prefix_cache_queries_total{engine="0"} 12\n')
        self.assertEqual(parsed["kv_cache_usage_perc"], 0.25)
        self.assertEqual(parsed["prefix_cache_queries_total"], 12)


try:
    from fastapi.testclient import TestClient
    from app.main import app
except ImportError:
    TestClient = None


@unittest.skipIf(TestClient is None, "FastAPI MVP dependencies not installed")
class ApiTests(unittest.TestCase):
    def test_http_flow(self):
        client = TestClient(app)
        response = client.get("/api/v1/snapshot")
        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertIs(validate_snapshot(snapshot), snapshot)
        self.assertEqual(snapshot["sources"]["kv"], "unavailable")
        response = client.post("/api/v1/policy/evaluate", json=SAMPLE)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["policy"], "os_aware")
        invalid = json.loads(json.dumps(SAMPLE))
        invalid["kv"]["usage_ratio"] = 5
        self.assertEqual(client.post("/api/v1/policy/evaluate", json=invalid).status_code, 422)
