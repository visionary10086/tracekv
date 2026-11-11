"""Check the CPU LRU experiment's launch and verification logic without a GPU."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import launch_vllm
import run_kv_lru


class CacheExperimentTests(unittest.TestCase):
    def test_launcher_enables_native_lru_through_pinned_spec(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "server"
            arguments = ["launch_vllm.py", "--model", directory, "--out", str(output),
                         "--expected-version", "0.11.2", "--cpu-kv-blocks", "128",
                         "--num-gpu-blocks-override", "33"]
            with (patch.object(sys, "argv", arguments),
                  patch.object(launch_vllm.platform, "system", return_value="Linux"),
                  patch.object(launch_vllm.importlib.metadata, "version", return_value="0.11.2"),
                  patch.object(launch_vllm.subprocess, "run") as run):
                run.return_value.returncode = 0
                run.return_value.stdout = ""
                self.assertEqual(launch_vllm.main(), 0)

            command = json.loads((output / "server_manifest.json").read_text())["command"]
            config = json.loads(command[command.index("--kv-transfer-config") + 1])
            self.assertEqual(config["kv_connector"], "OffloadingConnector")
            self.assertEqual(config["kv_role"], "kv_both")
            extra = config["kv_connector_extra_config"]
            self.assertEqual(extra["num_cpu_blocks"], 128)
            self.assertEqual(extra["eviction_policy"], "lru")
            self.assertEqual(extra["trace_dir"], str(output.resolve()))
            self.assertIn("--enable-prefix-caching", command)

    def test_metrics_preserve_missing_and_reset_counters(self):
        body = '\n'.join((
            'vllm:prefix_cache_hits_total{engine="0"} 64.0',
            'vllm:external_prefix_cache_hits_total{engine="0"} 32',
            'vllm:kv_cache_usage_perc{engine="0"} 0.5',
            'vllm:request_success_total{engine="0"} 1',
        ))
        values = run_kv_lru.parse_metrics(body)
        self.assertEqual(values["kv_cache_usage_perc"], 0.5)
        delta = run_kv_lru.counter_deltas(
            {"prefix_cache_hits_total": 128, "external_prefix_cache_hits_total": 0}, values
        )
        self.assertIsNone(delta["prefix_cache_hits_total"])
        self.assertIsNone(delta["external_prefix_cache_queries_total"])
        self.assertEqual(delta["external_prefix_cache_hits_total"], 32)

    def test_revisit_uses_same_messages_after_distinct_pressure(self):
        older, seed, first, second, revisit = run_kv_lru.workload()
        self.assertEqual(seed["messages"], revisit["messages"])
        self.assertEqual(len({case["id"] for case in (older, seed, first, second, revisit)}), 5)
        self.assertNotEqual(older["messages"], seed["messages"])
        self.assertNotEqual(first["messages"], second["messages"])

    def test_experiment_bypasses_proxy_for_loopback(self):
        with tempfile.TemporaryDirectory() as directory:
            arguments = ["run_kv_lru.py", "--model", directory, "--out", str(Path(directory) / "run")]
            with (patch.object(sys, "argv", arguments),
                  patch.object(sys, "platform", "linux"),
                  patch.dict(os.environ, {"no_proxy": "example.test"}),
                  patch.object(run_kv_lru, "run_arm", side_effect=RuntimeError("stop before launch"))):
                with self.assertRaisesRegex(RuntimeError, "stop before launch"):
                    run_kv_lru.main()
                self.assertEqual(os.environ["no_proxy"], "example.test,127.0.0.1")

    def test_proof_requires_reload_during_revisit(self):
        hit = "external_prefix_cache_hits_total"
        baseline = [{"output_text": "1, 2", "hit_tokens": {hit: 0}}]
        traced = [{"output_text": "1, 2", "hit_tokens": {hit: 32},
                   "started_ns": 100, "finished_ns": 200}]
        jobs = [
            {"direction": "gpu_to_cpu", "submitted_ns": 10, "observed_ns": 20},
            {"direction": "cpu_to_gpu", "submitted_ns": 110, "observed_ns": 150},
        ]
        self.assertTrue(all(run_kv_lru.verify_replay(baseline, traced, jobs).values()))
        jobs[1]["observed_ns"] = 201
        self.assertFalse(run_kv_lru.verify_replay(
            baseline, traced, jobs
        )["cpu_reload_completed_during_revisit"])
        traced[0]["output_text"] = "different output"
        self.assertFalse(run_kv_lru.verify_replay(baseline, traced, jobs)["all_outputs_match"])


if __name__ == "__main__":
    unittest.main()
