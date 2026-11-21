"""Page isolation and real integration boundary behavior; fixtures are synthetic."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from app.main import app
from app.jobs import Jobs

SAMPLE = json.loads((Path(__file__).resolve().parents[1] / "contracts/sample_snapshot.json").read_text())


class PlatformInterfaceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.jobs = Jobs(self.temp.name)
        self.out = Path(self.temp.name) / "integration-test"
        self.out.mkdir()
        (self.out / "manifest.json").write_text(json.dumps({
            "run_id": self.out.name, "status": "completed",
            "config": {"policy": "gpu_only", "requests": 0, "sample_interval_ms": 1000},
            "recorded_requests": 0}))
        self.override = patch("app.main.jobs", self.jobs)
        self.override.start()
        self.addCleanup(self.override.stop)
        self.client = TestClient(app)
        self.base = "/api/v1/runs/integration-test"

    def packet(self, source="ebpf"):
        packet = {
            "interface_version": "1.0", "run_id": "integration-test", "batch_id": "b1",
            "collector_id": "test-collector", "clock": {"wall_ns": 1000000000, "monotonic_ns": 100},
            "window_start_ns": 100, "window_end_ns": 10000,
            "status": "ok", "dropped_events": None,
        }
        if source == "ebpf":
            packet.update(source="ebpf_sched", scope="target_threads", target_tids=[42],
                          events=[{"metric": "wakeup_to_run_latency", "timestamp_ns": 200,
                                   "tid": 42, "pid": None, "value": 10000, "unit": "ns"}])
        else:
            packet.update(source="cache", events=[{"metric": "transfer", "timestamp_ns": 200,
                           "worker_id": "w1", "tier": "cpu", "job_id": "j1",
                           "direction": "gpu_to_cpu", "status": "completed", "bytes": 0}])
        return packet

    def test_three_routes_only_contain_their_own_content(self):
        for route, own, absent in [
            ("/monitor", 'id="resource-chart"', ['id="run-form"', 'id="run-list"']),
            ("/experiments", 'id="run-form"', ['id="resource-chart"', 'id="run-list"']),
            ("/runs", 'id="run-list"', ['id="resource-chart"', 'id="run-form"']),
        ]:
            response = self.client.get(route)
            self.assertEqual(response.status_code, 200)
            self.assertIn(own, response.text)
            for other in absent:
                self.assertNotIn(other, response.text)
            self.assertIn('aria-current="page"', response.text)
            self.assertNotIn('href="#experiment"', response.text)
        self.assertEqual(self.client.get("/", follow_redirects=False).headers["location"], "/monitor")

    def test_contract_and_openapi_have_request_and_response_schemas(self):
        contracts = self.client.get("/api/v1/interfaces").json()
        self.assertEqual(contracts["interface_version"], "1.0")
        self.assertIn("strategy_advice", contracts["schemas"])
        paths = self.client.get("/openapi.json").json()["paths"]
        self.assertIn("/api/v1/runs/{run_id}/telemetry/ebpf", paths)
        response = paths["/api/v1/strategies/evaluate"]["post"]["responses"]["200"]
        self.assertIn("StrategyAdvice", json.dumps(response))

    def test_published_examples_are_accessible_and_explicitly_synthetic(self):
        self.assertEqual(self.client.get("/api/v1/interfaces/guide").status_code, 200)
        for source in ("ebpf", "cache"):
            response = self.client.get("/api/v1/interfaces/examples/" + source)
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["synthetic"])
        self.assertEqual(self.client.get("/api/v1/interfaces/examples/unknown").status_code, 404)

    def test_policy_candidate_is_executable_and_does_not_change_gpu_budget(self):
        snapshot = copy.deepcopy(SAMPLE)
        snapshot["system"]["mem_available_mb"] = 8000
        snapshot["kv"]["usage_ratio"] = .95
        request = {"snapshot": snapshot, "current_config": {"policy": "gpu_only", "gpu_kv_blocks": 33, "cpu_kv_blocks": 0}}
        response = self.client.post("/api/v1/strategies/evaluate", json=request)
        self.assertEqual(response.status_code, 200)
        advice = response.json()
        self.assertEqual(advice["apply_mode"], "next_run")
        self.assertEqual(advice["config_patch"], {"policy": "cpu_lru", "gpu_kv_blocks": 33, "cpu_kv_blocks": 72})
        self.assertFalse(advice["causality_established"])

    def test_policy_unknown_data_keeps_current_and_rejects_invalid_budget(self):
        snapshot = copy.deepcopy(SAMPLE)
        snapshot["system"]["mem_available_mb"] = None
        snapshot["kv"]["usage_ratio"] = None
        request = {"snapshot": snapshot, "current_config": {"policy": "gpu_only", "cpu_kv_blocks": 0}}
        self.assertEqual(self.client.post("/api/v1/strategies/evaluate", json=request).json()["strategy_id"], "gpu_only")
        request["current_config"]["cpu_kv_blocks"] = 1
        self.assertEqual(self.client.post("/api/v1/strategies/evaluate", json=request).status_code, 422)

    def test_idempotent_retry_and_conflicting_batch(self):
        packet = self.packet()
        url = self.base + "/telemetry/ebpf"
        self.assertEqual(self.client.post(url, json=packet).status_code, 201)
        self.assertTrue(self.client.post(url, json=packet).json()["duplicate"])
        state = self.client.get(self.base + "/telemetry").json()["sources"]["ebpf"]
        self.assertEqual(state["event_count"], 1)
        self.assertEqual(state["groups"][0]["metrics"]["wakeup_to_run_latency"]["p95_us"], 10)
        self.assertIsNone(state["groups"][0]["dropped_events"])
        packet["events"][0]["value"] = 5
        self.assertEqual(self.client.post(url, json=packet).status_code, 409)
        stored = self.client.get(self.base + "/telemetry/ebpf/batches/b1").json()
        self.assertEqual(stored["payload"]["events"][0]["value"], 10000)

    def test_overlapping_window_is_rejected_but_different_target_is_separate(self):
        packet = self.packet()
        url = self.base + "/telemetry/ebpf"
        self.client.post(url, json=packet)
        packet["batch_id"] = "b2"
        self.assertEqual(self.client.post(url, json=packet).status_code, 409)
        packet["target_tids"] = [43]
        packet["events"][0]["tid"] = 43
        self.assertEqual(self.client.post(url, json=packet).status_code, 201)
        self.assertEqual(len(self.client.get(self.base + "/telemetry").json()["sources"]["ebpf"]["groups"]), 2)

    def test_bad_clock_target_window_and_run_are_rejected(self):
        changes = [
            lambda p: p.pop("clock"),
            lambda p: p.update(target_tids=[99]),
            lambda p: p["events"][0].update(timestamp_ns=p["window_end_ns"]),
            lambda p: p.update(run_id="wrong-run"),
            lambda p: p.update(dropped_events=-1),
            lambda p: p.update(command="arbitrary command"),
        ]
        for change in changes:
            packet = self.packet()
            change(packet)
            self.assertEqual(self.client.post(self.base + "/telemetry/ebpf", json=packet).status_code, 422)

    def test_target_order_is_canonical_for_retry_and_overlap_checks(self):
        packet = self.packet()
        packet["target_tids"] = [43, 42]
        url = self.base + "/telemetry/ebpf"
        self.assertEqual(self.client.post(url, json=packet).status_code, 201)
        packet["target_tids"] = [42, 43]
        self.assertTrue(self.client.post(url, json=packet).json()["duplicate"])
        packet["batch_id"] = "same-window-new-id"
        self.assertEqual(self.client.post(url, json=packet).status_code, 409)

    def test_synthetic_batches_never_count_as_real_observations(self):
        packet = self.packet()
        packet["synthetic"] = True
        self.client.post(self.base + "/telemetry/ebpf", json=packet)
        state = self.client.get(self.base + "/telemetry").json()["sources"]["ebpf"]
        self.assertEqual(state["event_count"], 0)
        self.assertEqual(state["synthetic_batch_count"], 1)
        self.assertEqual(state["status"], "unavailable")

    def test_failure_needs_reason_and_does_not_make_up_events(self):
        packet = self.packet()
        packet.update(status="unavailable", events=[])
        url = self.base + "/telemetry/ebpf"
        self.assertEqual(self.client.post(url, json=packet).status_code, 422)
        packet["error"] = "Permission denied"
        self.assertEqual(self.client.post(url, json=packet).status_code, 201)
        state = self.client.get(self.base + "/telemetry").json()["sources"]["ebpf"]
        self.assertEqual(state["event_count"], 0)
        self.assertEqual(state["errors"], ["Permission denied"])

    def test_cache_zero_bytes_and_transfer_stages_stay_distinct(self):
        packet = self.packet("cache")
        submitted = dict(packet["events"][0], status="submitted", bytes=100)
        packet["events"].append(submitted)
        self.assertEqual(self.client.post(self.base + "/telemetry/cache", json=packet).status_code, 201)
        metrics = self.client.get(self.base + "/telemetry").json()["sources"]["cache"]["groups"][0]["metrics"]
        self.assertEqual(metrics["transfer:cpu:gpu_to_cpu:completed"]["bytes"], 0)
        self.assertEqual(metrics["transfer:cpu:gpu_to_cpu:submitted"]["bytes"], 100)

    def test_cache_hits_require_logical_identity(self):
        packet = self.packet("cache")
        packet["events"] = [{"metric": "hit", "timestamp_ns": 200, "worker_id": "w1", "tier": "cpu"}]
        self.assertEqual(self.client.post(self.base + "/telemetry/cache", json=packet).status_code, 422)

    def test_report_reanalysis_includes_external_data_and_tracks_freshness(self):
        self.client.post(self.base + "/telemetry/ebpf", json=self.packet())
        self.assertTrue(self.client.get(self.base).json()["report_outdated"])
        self.assertEqual(self.client.post(self.base + "/analyze").status_code, 200)
        self.assertFalse(self.client.get(self.base).json()["report_outdated"])
        summary = json.loads((self.out / "telemetry_summary.json").read_text())
        self.assertEqual(summary["sources"]["ebpf"]["event_count"], 1)
        self.assertIn("eBPF 事件数: 1", (self.out / "report.md").read_text())
        self.assertEqual(self.client.get(self.base + "/files/telemetry_summary.json").status_code, 200)

    def test_cannot_reanalyze_running_job_or_ingest_unknown_run(self):
        manifest = json.loads((self.out / "manifest.json").read_text())
        manifest["status"] = "running"
        (self.out / "manifest.json").write_text(json.dumps(manifest))
        self.assertEqual(self.client.post(self.base + "/analyze").status_code, 409)
        packet = self.packet()
        packet["run_id"] = "missing"
        self.assertEqual(self.client.post("/api/v1/runs/missing/telemetry/ebpf", json=packet).status_code, 404)
