"""Unit tests for expanded broad telemetry (Page Faults, Reclaim, Compaction, Futex).

Tests cover:
- Multi-subsystem event stream parsing (E1, E2, E3, E4, E5)
- Telemetry contract conformance (pid is None, valid metric names)
- Stats serialization to sched_summary.json dict
- Dense window snapshot alignment passing contracts/schema.py:validate_snapshot
- Integration with collector/ebpf_uploader.py batch builder
- Large perf ring buffer configuration to prevent event drops

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import io
import json
import unittest
from pathlib import Path

from collector.cpu_scheduler import (
    DEFAULT_PERF_RB_PAGES,
    process_stream,
    stats_to_dict,
    TimeAnchor,
    CollectorResult,
)
from collector.correlator import (
    generate_aligned_snapshots,
    diagnose_system_bottlenecks,
)
from collector.ebpf_uploader import build_ebpf_batch, VALID_METRICS
from contracts.schema import validate_snapshot


class TestBroadTelemetryStreamParsing(unittest.TestCase):
    """Test parsing of multi-subsystem kernel event streams."""

    def test_multi_subsystem_stream_parsing(self):
        sample_stream = io.StringIO(
            "CLOCK|wall_ns=1700000000000000000|mono_ns=1000000000\n"
            "HEADER|type|data\n"
            "SCHED|1000100000|1000050000|101|0|50000|vllm_worker\n"
            "RUNWAIT|1000200000|1000150000|101|0|50000|vllm_worker\n"
            "ONCPU|1000300000|101|0|100000|vllm_worker\n"
            "FAULT|1000350000|101|0x7fff1000|0x401000|4|vllm_worker|minor_fault\n"
            "FAULT|1000380000|101|0x7fff2000|0x401050|0|vllm_worker|major_fault\n"
            "RECLAIM|1000450000|101|2000000|128\n"
            "COMPACT|1000550000|101|3000000|1\n"
            "FUTEX|1000650000|101|1500000|0|vllm_worker\n"
            "STAT|matched|1\n"
            "STAT|runwait_matched|1\n"
            "STAT|oncpu_matched|1\n"
            "STAT|minor_faults|1\n"
            "STAT|major_faults|1\n"
            "STAT|reclaim_matched|1\n"
            "STAT|compact_matched|1\n"
            "STAT|futex_matched|1\n"
            "STAT|map_overflow|0\n"
        )
        output = io.StringIO()
        res = process_stream(
            sample_stream,
            run_id="broad-test-run",
            output=output,
            target_pids=[101],
            target_comm="vllm_worker",
        )

        s = res.stats
        self.assertEqual(s.event_count, 1)
        self.assertEqual(s.runwait_event_count, 1)
        self.assertEqual(s.oncpu_event_count, 1)
        self.assertEqual(s.minor_fault_count, 1)
        self.assertEqual(s.major_fault_count, 1)
        self.assertEqual(s.page_fault_count, 2)
        self.assertEqual(s.reclaim_event_count, 1)
        self.assertEqual(s.compaction_event_count, 1)
        self.assertEqual(s.futex_event_count, 1)

        # Parse generated JSONL events
        lines = [line.strip() for line in output.getvalue().splitlines() if line.strip()]
        self.assertGreaterEqual(len(lines), 7)

        records = [json.loads(line) for line in lines]
        metrics = [r["metric"] for r in records]
        self.assertIn("wakeup_to_run_latency", metrics)
        self.assertIn("runnable_wait", metrics)
        self.assertIn("on_cpu", metrics)
        self.assertIn("minor_fault", metrics)
        self.assertIn("major_fault", metrics)
        self.assertIn("direct_reclaim", metrics)
        self.assertIn("blocked", metrics)

        # Telemetry contract verification: raw kernel events leave pid: None
        for r in records:
            self.assertIsNone(r["pid"])
            self.assertIn(r["metric"], VALID_METRICS)
            self.assertIsInstance(r["timestamp_ns"], int)
            self.assertGreater(r["timestamp_ns"], 0)

    def test_stats_to_dict_broad_telemetry_fields(self):
        anchor = TimeAnchor(wall_ns=1700000000000000000, monotonic_ns=1000000000)
        res = CollectorResult(run_id="dict-test", start_anchor=anchor)
        res.stats.page_fault_count = 12
        res.stats.minor_fault_count = 10
        res.stats.major_fault_count = 2
        res.stats.reclaim_event_count = 3
        res.stats.compaction_event_count = 1
        res.stats.futex_event_count = 5

        d = stats_to_dict(res)
        self.assertEqual(d["page_fault_count"], 12)
        self.assertEqual(d["minor_fault_count"], 10)
        self.assertEqual(d["major_fault_count"], 2)
        self.assertEqual(d["reclaim_event_count"], 3)
        self.assertEqual(d["compaction_event_count"], 1)
        self.assertEqual(d["futex_event_count"], 5)

    def test_perf_ring_buffer_pages_enlarged(self):
        """Verify perf ring buffer pages default is enlarged (>= 512) for high event rates."""
        self.assertGreaterEqual(DEFAULT_PERF_RB_PAGES, 512)
        self.assertEqual(DEFAULT_PERF_RB_PAGES, 1024)


class TestBroadTelemetryCorrelatorAndSnapshots(unittest.TestCase):
    """Test aligned snapshot generation with multi-subsystem telemetry."""

    def test_correlator_produces_dense_snapshots_passing_schema(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sched_path = tmppath / "sched_events.jsonl"
            summary_path = tmppath / "sched_summary.json"
            gpu_path = tmppath / "gpu_samples.jsonl"
            sys_path = tmppath / "system_samples.jsonl"

            # 1. Write summary with clock anchors
            summary_path.write_text(
                json.dumps({
                    "start_wall_ns": 1700000000000000000,
                    "start_monotonic_ns": 1000000000,
                    "scope": "target_threads",
                })
            )

            # 2. Write multi-subsystem events across multiple windows
            events = [
                {"metric": "wakeup_to_run_latency", "timestamp_ns": 1000000000, "value": 15000, "is_target": True, "comm": "vllm", "tid": 10},
                {"metric": "runnable_wait", "timestamp_ns": 1100000000, "value": 25000, "is_target": True, "comm": "vllm", "tid": 10},
                {"metric": "on_cpu", "timestamp_ns": 1200000000, "value": 800000, "is_target": True, "comm": "vllm", "tid": 10},
                {"metric": "minor_fault", "timestamp_ns": 1300000000, "value": 0, "is_target": True, "comm": "vllm", "tid": 10},
                {"metric": "direct_reclaim", "timestamp_ns": 1400000000, "value": 5000000, "is_target": True, "comm": "vllm", "tid": 10},
                {"metric": "blocked", "timestamp_ns": 1500000000, "value": 3000000, "is_target": True, "comm": "vllm", "tid": 10},
            ]
            sched_path.write_text("\n".join(json.dumps(e) for e in events) + "\n")

            # 3. Write continuous system and GPU samples
            sys_samples = [
                {"timestamp": "2023-11-14T22:13:20.000000Z", "mem_available_mb": 16000.0, "disk_read_bytes_per_s": 0.0},
                {"timestamp": "2023-11-14T22:13:20.500000Z", "mem_available_mb": 15900.0, "disk_read_bytes_per_s": 1024.0},
                {"timestamp": "2023-11-14T22:13:21.000000Z", "mem_available_mb": 15850.0, "disk_read_bytes_per_s": 2048.0},
            ]
            sys_path.write_text("\n".join(json.dumps(s) for s in sys_samples) + "\n")

            gpu_samples = [
                {"timestamp": "2023-11-14T22:13:20.000000Z", "memory_used_mb": 4096.0, "utilization_pct": 80.0},
                {"timestamp": "2023-11-14T22:13:20.500000Z", "memory_used_mb": 4200.0, "utilization_pct": 85.0},
                {"timestamp": "2023-11-14T22:13:21.000000Z", "memory_used_mb": 4250.0, "utilization_pct": 90.0},
            ]
            gpu_path.write_text("\n".join(json.dumps(g) for g in gpu_samples) + "\n")

            # 4. Generate aligned snapshots with sub-second buckets
            snaps = generate_aligned_snapshots(
                run_id="dense-test-run",
                sched_events_path=sched_path,
                sched_summary_path=summary_path,
                gpu_samples_path=gpu_path,
                system_samples_path=sys_path,
                bucket_sec=0.5,
            )

            self.assertGreaterEqual(len(snaps), 2)
            for snap in snaps:
                # Strictly validate schema
                validated = validate_snapshot(snap)
                self.assertIsNotNone(validated)

                # Check multi-subsystem sections exist
                self.assertIn("memory_stalls", snap)
                self.assertIn("synchronization", snap)
                self.assertIn("sched", snap)
                self.assertIn("system", snap)
                self.assertIn("gpu", snap)

    def test_broad_telemetry_uploader_batch_conformance(self):
        """Verify that multi-subsystem events can be packaged into valid EbpfBatch."""
        clock = {"wall_ns": 1700000000000000000, "monotonic_ns": 1000000000}
        events = [
            {"metric": "wakeup_to_run_latency", "timestamp_ns": 1000100000, "value": 50000, "tid": 42},
            {"metric": "runnable_wait", "timestamp_ns": 1000200000, "value": 30000, "tid": 42},
            {"metric": "on_cpu", "timestamp_ns": 1000300000, "value": 100000, "tid": 42},
            {"metric": "minor_fault", "timestamp_ns": 1000400000, "value": 0, "tid": 42},
            {"metric": "major_fault", "timestamp_ns": 1000500000, "value": 0, "tid": 42},
            {"metric": "direct_reclaim", "timestamp_ns": 1000600000, "value": 2000000, "tid": 42},
            {"metric": "blocked", "timestamp_ns": 1000700000, "value": 1500000, "tid": 42},
        ]
        batch = build_ebpf_batch(
            run_id="batch-test",
            collector_id="test-broad-collector",
            scope="target_threads",
            clock=clock,
            window_start_ns=1000000000,
            window_end_ns=1001000000,
            events=events,
            target_tids=[42],
        )
        self.assertEqual(batch["run_id"], "batch-test")
        self.assertEqual(len(batch["events"]), 7)


if __name__ == "__main__":
    unittest.main()
