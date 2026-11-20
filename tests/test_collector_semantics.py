"""Regression checks for misleading attribution, missing sources and window loss."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from collector.correlator import (
    analyze_kv_pcie_transfers, compute_lagged_correlation,
    correlate_experiment_data, diagnose_system_bottlenecks, generate_aligned_snapshots,
)
from collector.cpu_scheduler import process_stream
from collector.gpu import collect_gpu_metrics
from collector.system import collect_system_sample, _read_disk_sectors


class CollectorSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def rows(self, name, records):
        path = self.root / name
        path.write_text("".join(json.dumps(row) + "\n" for row in records))
        return path

    def test_final_boundary_and_empty_windows_keep_source_semantics(self):
        gpu = self.rows("gpu.jsonl", [
            {"timestamp": "2026-11-20T00:00:00Z", "memory_used_mb": 10},
            {"timestamp": "2026-11-20T00:00:02Z", "memory_used_mb": 20},
        ])
        rows = generate_aligned_snapshots("r", gpu_samples_path=gpu)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[-1]["gpu"]["memory_used_mb"], 20)
        self.assertIsNone(rows[1]["gpu"]["memory_used_mb"])
        self.assertEqual(rows[1]["sources"]["gpu"], "unavailable")
        buckets = correlate_experiment_data(gpu_samples_path=gpu)
        self.assertEqual(sum(row["gpu"]["sample_count"] for row in buckets), 2)

    def test_missing_anchor_never_reinterprets_uptime_as_utc(self):
        events = self.rows("sched.jsonl", [{"timestamp_ns": 1234, "value": 100}])
        gpu = self.rows("gpu.jsonl", [{"timestamp": "2026-11-20T00:00:00Z", "memory_used_mb": 10}])
        buckets = correlate_experiment_data(sched_events_path=events, gpu_samples_path=gpu)
        self.assertEqual(len(buckets), 1)
        self.assertEqual(buckets[0]["sched"]["event_count"], 0)

    def test_kv_ratios_retain_intervals_instead_of_unweighted_mean(self):
        kv = self.rows("kv.jsonl", [
            {"timestamp": "2026-11-20T00:00:00Z", "prefix_hit_ratio": 1.0, "window_ms": 100, "usage_ratio": .1},
            {"timestamp": "2026-11-20T00:00:00.500Z", "prefix_hit_ratio": 0.0, "window_ms": 500, "usage_ratio": .8},
        ])
        row = generate_aligned_snapshots("r", kv_samples_path=kv)[0]
        self.assertIsNone(row["kv"]["prefix_hit_ratio"])
        self.assertEqual(row["kv"]["usage_ratio"], .8)
        self.assertEqual([item["window_ms"] for item in row["kv_intervals"]], [100, 500])

    def test_pcie_zero_missing_and_unknown_capacity_are_distinct(self):
        transfers = self.rows("kv.jsonl", [{"status": "completed", "elapsed_ms": 5, "gpu_blocks": 1}])
        missing = self.rows("missing.jsonl", [{"timestamp": "2026-11-20T00:00:00Z", "memory_used_mb": 10}])
        result = analyze_kv_pcie_transfers(transfers, missing)
        self.assertEqual(result["bottleneck"], "NO_PCIE_TELEMETRY")
        self.assertIsNone(result["pcie_physical"]["peak_tx_mb_per_s"])
        zero = self.rows("zero.jsonl", [{"timestamp": "2026-11-20T00:00:00Z", "pcie_tx_kb_per_s": 0}])
        result = analyze_kv_pcie_transfers(transfers, zero)
        self.assertEqual(result["pcie_physical"]["peak_tx_mb_per_s"], 0)
        self.assertIsNone(result["pcie_physical"]["peak_rx_mb_per_s"])
        self.assertIsNone(result["pcie_physical"]["bus_saturation_pct"])
        self.assertNotEqual(result["bottleneck"], "NO_PCIE_TELEMETRY")
        self.assertFalse(result["causality_established"])

    def test_missing_diagnosis_data_is_unknown_not_healthy(self):
        result = diagnose_system_bottlenecks([{"run_id": "r"}])
        self.assertEqual(result["primary_bottleneck"], "INSUFFICIENT_DATA")
        self.assertIsNone(result["metrics_summary"]["avg_gpu_utilization_pct"])
        self.assertFalse(result["causality_established"])

    def test_constant_and_missing_lag_data_do_not_create_evidence(self):
        result = compute_lagged_correlation([1] * 10, [2] * 10)
        self.assertIsNone(result["peak_correlation"])
        self.assertEqual(result["temporal_direction"], "undefined")
        sparse = compute_lagged_correlation([1, None, 2, None, 3], [1, 9, 2, 8, 3], max_lag=0)
        self.assertEqual(sparse["pair_counts"], [3])
        self.assertFalse(sparse["causality_established"])

    def test_gpu_optional_nan_and_fallback_preserve_requested_device(self):
        outputs = [
            MagicMock(returncode=1, stdout=""),
            MagicMock(returncode=0, stdout="100, 1000, 10, 40, NaN"),
        ]
        with patch("collector.gpu.subprocess.run", side_effect=outputs) as run:
            result = collect_gpu_metrics("/fake", gpu_index=3)
        self.assertIn("--id=3", run.call_args.args[0])
        self.assertIsNone(result["power_w"])
        self.assertEqual(result["gpu_index"], 3)

    def test_disk_partitions_are_not_double_counted_and_missing_is_unknown(self):
        path = self.root / "diskstats"
        path.write_text("8 0 sda 1 0 10 0 2 0 20 0 0 0 0\n8 1 sda1 1 0 10 0 2 0 20 0 0 0 0\n")
        self.assertEqual(_read_disk_sectors(path), (10, 20, 1, 2))
        self.assertIsNone(_read_disk_sectors(self.root / "absent"))

    def test_tid_and_comm_filter_agree_between_summary_and_events(self):
        output = io.StringIO()
        result = process_stream(io.StringIO("SCHED|200|100|42|0|100|other\nSCHED|400|200|42|0|200|vllm\n"),
                                "r", output=output, target_pids=[42], target_comm="vllm")
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(result.target_stats.event_count, 1)
        self.assertEqual([record["is_target"] for record in records], [False, True])
        self.assertTrue(all(record["pid"] is None for record in records))

    def test_invalid_bucket_rejected_before_alignment(self):
        for width in [0, -1, float("nan"), float("inf")]:
            with self.assertRaises(ValueError):
                generate_aligned_snapshots("r", bucket_sec=width)
