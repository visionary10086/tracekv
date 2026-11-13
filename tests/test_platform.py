"""Behavior checks for experiment isolation, missing data and UTC window boundaries."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from collections import deque

from fastapi.testclient import TestClient
from app.main import app, monitor
from app.jobs import Jobs
from app.monitor import Monitor
from tools.run_experiment import validate_config, analyze, write_json, append_json

ROOT = Path(__file__).resolve().parents[1]


class PlatformTests(unittest.TestCase):
    def test_ui_and_waiting_sample(self):
        client = TestClient(app)
        self.assertEqual(client.get("/").status_code, 200)
        self.assertIn("实验工作台", client.get("/").text)
        with patch.object(monitor, "get_history", return_value=[]):
            self.assertEqual(client.get("/api/v1/snapshot").status_code, 503)

    def test_cross_origin_cannot_start_or_stop(self):
        client = TestClient(app)
        self.assertEqual(client.post("/api/v1/runs", json={}, headers={"Origin": "https://other.test"}).status_code, 403)
        self.assertEqual(client.post("/api/v1/runs/fake/stop", headers={"Origin": "https://other.test"}).status_code, 403)

    def test_config_rejects_invalid_budget_and_boolean(self):
        for config in ({"policy": "cpu_lru", "cpu_kv_blocks": 0},
                       {"requests": True}, {"requests": 0}, {"command": "echo nope"}):
            with self.assertRaises(ValueError):
                validate_config(config)

    def test_no_model_or_kv_data_fabricated(self):
        sample = Monitor().sample()
        self.assertIsNone(sample["kv"]["usage_ratio"])
        self.assertEqual(sample["sources"]["kv"], "unavailable")
        self.assertIsNone(sample["system"]["disk_read_p95_ms"])

    def test_supervisor_conflict_and_nonowned_stop(self):
        with tempfile.TemporaryDirectory() as folder:
            jobs = Jobs(folder)
            jobs.process = MagicMock()
            jobs.process.poll.return_value = None
            with patch("app.jobs.validate_config", return_value={"model_path": "/models/test", "workload_path": str(ROOT / "workloads/smoke.jsonl")}), patch.object(jobs, "models", return_value=["/models/test"]):
                with self.assertRaises(RuntimeError):
                    jobs.start({})
            with self.assertRaises(ValueError):
                jobs.stop("other")
            jobs.process.send_signal.assert_not_called()

    def test_optional_gpu_power_unavailable_preserves_memory(self):
        from collector.gpu import collect_gpu_metrics
        process = MagicMock(returncode=0, stdout="100, 1000, 0, 49, N/A\\n")
        with patch("collector.gpu.subprocess.run", return_value=process):
            sample = collect_gpu_metrics("/fake/nvidia-smi")
        self.assertEqual(sample["source"], "nvidia-smi")
        self.assertEqual(sample["memory_used_mb"], 104.86)
        self.assertIsNone(sample["power_w"])

    def test_cancel_before_runner_ready_is_not_lost(self):
        with tempfile.TemporaryDirectory() as folder:
            jobs = Jobs(folder)
            out = Path(folder) / "early-run"
            out.mkdir()
            write_json(out / "manifest.json", {"status": "running"})
            jobs.active_id = "early-run"
            jobs.process = MagicMock()
            jobs.process.poll.return_value = None
            result = jobs.stop("early-run")
            self.assertEqual(result["status"], "stopping")
            self.assertTrue((out / "cancel.flag").exists())
            jobs.process.send_signal.assert_not_called()

    def test_path_escape_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            jobs = Jobs(folder)
            with self.assertRaises(ValueError):
                jobs.resolve("../private")
            target = Path(folder) / "symlink"
            target.symlink_to("/tmp", target_is_directory=True)
            with self.assertRaises(FileNotFoundError):
                jobs.resolve("symlink")

    def test_fixed_boundary_sample_not_dropped_and_missing_window_is_null(self):
        with tempfile.TemporaryDirectory() as folder:
            out = Path(folder)
            manifest = {"run_id":"test", "status":"completed", "config":{"policy":"gpu_only","requests":0,"sample_interval_ms":1000},
                        "measurement_started_at_utc":"2026-11-13T00:00:00Z",
                        "measurement_finished_at_utc":"2026-11-13T00:00:02.100000Z","measurement_seconds":2.1}
            write_json(out / "manifest.json",manifest)
            sample={"timestamp":"2026-11-13T00:00:02Z","window_ms":1000,
                    "system":{"mem_available_mb":100}, "gpu":{"memory_used_mb":42},
                    "kv":{"usage_ratio":0.2,"prefix_hit_ratio":0.5,"running_requests":0},
                    "sources":{"system":"proc","gpu":"nvidia-smi","kv":"vllm"}}
            append_json(out / "snapshots.jsonl",sample)
            analyze(out)
            rows=[json.loads(line) for line in (out/"aligned_snapshots.jsonl").read_text().splitlines()]
            self.assertEqual(len(rows),3)
            self.assertIsNone(rows[0]["gpu"]["memory_used_mb"])
            self.assertEqual(rows[-1]["gpu"]["memory_used_mb"],42)
            self.assertIsNone(rows[-1]["kv"]["prefix_hit_ratio"])
            self.assertEqual(rows[-1]["kv_interval"]["prefix_hit_ratio"],0.5)
            self.assertTrue((out/"report.md").exists())


if __name__ == "__main__":
    unittest.main()
