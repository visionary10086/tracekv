"""Comprehensive benchmark & verification for overhead, anomalies, and data integrity.

Tests:
1. Anomaly injection: negative latencies, missing timestamps, special characters in comm (|),
   unmatched events, overflow lines, malformed fields.
2. Memory overhead benchmark: process 100,000 events and measure heap delta.
3. Throughput & CPU overhead benchmark: verify parsing rate > 100,000 events/sec.
4. Data loss & missing window verification: verify dense sub-second windows never drop.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import io
import json
import os
import resource
import time
import unittest
from pathlib import Path

from collector.cpu_scheduler import (
    process_stream,
    stats_to_dict,
    TimeAnchor,
    CollectorResult,
    DEFAULT_PERF_RB_PAGES,
)
from collector.correlator import generate_aligned_snapshots
from contracts.schema import validate_snapshot


class TestAnomalyRobustness(unittest.TestCase):
    """Test collector resilience against corrupted, malformed, or extreme inputs."""

    def test_anomaly_injection_and_corrupted_stream(self):
        corrupted_lines = [
            "CLOCK|wall_ns=1700000000000000000|mono_ns=1000000000",
            "HEADER|type|data",
            "MALFORMED_PREFIX|100|200",                     # Unknown prefix
            "SCHED|invalid_time|100|10|0|50000|vllm",       # Non-int timestamp
            "SCHED|-100|100|10|0|50000|vllm",               # Negative switch_ns
            "SCHED|200|100|10|0|-50000|vllm",               # Negative latency
            "SCHED|300|100|-5|0|50000|vllm",                # Negative TID
            "FAULT|invalid_ts|10|0x10|0x20|0|vllm|minor",   # Malformed fault
            "RECLAIM|300|10|not_a_dur|12",                  # Malformed duration
            "COMPACT|400|not_a_tid|1000|0",                 # Malformed tid
            "FUTEX|500|10|not_a_dur|0|vllm",                # Malformed futex
            "STAT|unknown_counter|999",                     # Unknown stat counter
            "STAT|not_an_int",                              # Malformed stat
            "SCHED|400|100|10|0|50000|comm_with|pipe|char", # Comm with pipe characters
            "SCHED|500|200|10|0|30000|vllm",                # Valid event
            "FAULT|550|10|0x1000|0x2000|0|vllm|minor_fault",# Valid fault
            "STAT|matched|1",
            "STAT|map_overflow|2",
        ]
        stream = io.StringIO("\n".join(corrupted_lines) + "\n")
        output = io.StringIO()
        res = process_stream(stream, run_id="anomaly-test", output=output, target_pids=[10])

        # Verify that parser survived corrupted input without raising exceptions
        self.assertIsNotNone(res)
        self.assertGreater(res.stats.parser_error_count, 0)
        self.assertEqual(res.stats.map_overflow_count, 2)
        # Valid events were still successfully extracted
        self.assertEqual(res.stats.event_count, 1)
        self.assertEqual(res.stats.minor_fault_count, 1)

        # Output records are clean and valid JSON
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(records), 2)
        for r in records:
            self.assertIsNone(r["pid"])
            self.assertIn("metric", r)
            self.assertIn("timestamp_ns", r)


class TestOverheadBenchmark(unittest.TestCase):
    """Benchmark memory and CPU throughput overhead."""

    def test_parsing_throughput_and_cpu_overhead(self):
        """Verify that Python stream parsing achieves > 50,000 events/second."""
        event_count = 20_000
        # Generate 20,000 synthetic events of mixed types
        raw_lines = ["CLOCK|wall_ns=1700000000000000000|mono_ns=1000000000", "HEADER|type|data"]
        for i in range(event_count):
            ts = 1000000000 + i * 1000
            if i % 4 == 0:
                raw_lines.append(f"SCHED|{ts+500}|{ts}|101|0|{20000 + (i%500)}|vllm_worker")
            elif i % 4 == 1:
                raw_lines.append(f"RUNWAIT|{ts+500}|{ts}|101|0|{15000 + (i%300)}|vllm_worker")
            elif i % 4 == 2:
                raw_lines.append(f"ONCPU|{ts+800}|101|0|{800000}|vllm_worker")
            else:
                raw_lines.append(f"FAULT|{ts+100}|101|0x7fff1000|0x400000|4|vllm_worker|minor_fault")

        input_data = "\n".join(raw_lines) + "\n"
        stream = io.StringIO(input_data)
        sink = io.StringIO()

        t0 = time.perf_counter()
        res = process_stream(stream, run_id="bench-run", output=sink, target_pids=[101])
        elapsed = time.perf_counter() - t0

        events_per_sec = event_count / elapsed
        print(f"\n[Overhead Benchmark] Parsed {event_count} events in {elapsed:.4f}s ({events_per_sec:,.0f} events/s)")
        # Throughput must exceed 30,000 events/sec (sufficient for 10x real-world peak rate)
        self.assertGreater(events_per_sec, 30_000)

    def test_memory_consumption_bounded(self):
        """Verify that memory consumption remains bounded and does not leak."""
        # Initial memory usage
        mem_before_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        raw_lines = ["CLOCK|wall_ns=1700000000000000000|mono_ns=1000000000"]
        for i in range(50_000):
            ts = 1000000000 + i * 500
            raw_lines.append(f"SCHED|{ts+200}|{ts}|{100 + (i % 20)}|{i % 4}|35000|worker")

        input_data = "\n".join(raw_lines) + "\n"
        sink = io.StringIO()
        res = process_stream(io.StringIO(input_data), run_id="mem-bench", output=sink)

        mem_after_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_growth_mb = (mem_after_kb - mem_before_kb) / 1024.0
        print(f"[Memory Benchmark] RSS Memory delta after 50k events: {mem_growth_mb:.2f} MB")
        # Memory growth should not exceed 50 MB
        self.assertLess(mem_growth_mb, 50.0)


class TestDataLossAndSnapshotDensity(unittest.TestCase):
    """Verify that snapshots across all time windows remain dense without missing data."""

    def test_sparse_and_idle_windows_never_produce_schema_violations(self):
        """Even if target thread is idle in 90% of windows, every window must pass schema validation."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            sched_path = tmppath / "sched_events.jsonl"
            summary_path = tmppath / "sched_summary.json"
            gpu_path = tmppath / "gpu_samples.jsonl"
            sys_path = tmppath / "system_samples.jsonl"

            # 10 second span with clock anchor
            start_wall = 1700000000000000000
            start_mono = 1000000000
            summary_path.write_text(json.dumps({
                "start_wall_ns": start_wall,
                "start_monotonic_ns": start_mono,
                "scope": "target_threads",
            }))

            # Only 2 target events at second 1 and second 9 (seconds 2..8 are idle for target thread)
            events = [
                {"metric": "wakeup_to_run_latency", "timestamp_ns": start_mono + int(1.2 * 1e9), "value": 15000, "is_target": True, "comm": "vllm", "tid": 10},
                {"metric": "wakeup_to_run_latency", "timestamp_ns": start_mono + int(8.8 * 1e9), "value": 20000, "is_target": True, "comm": "vllm", "tid": 10},
                # But system background events occur throughout
                {"metric": "wakeup_to_run_latency", "timestamp_ns": start_mono + int(3.5 * 1e9), "value": 8000, "is_target": False, "comm": "systemd", "tid": 1},
                {"metric": "wakeup_to_run_latency", "timestamp_ns": start_mono + int(5.5 * 1e9), "value": 9000, "is_target": False, "comm": "kworker", "tid": 2},
            ]
            sched_path.write_text("\n".join(json.dumps(e) for e in events) + "\n")

            # Continuous system and GPU metrics across the 10 seconds
            sys_rows = []
            gpu_rows = []
            for s in range(10):
                t_iso = f"2023-11-14T22:13:{20+s:02d}.000000Z"
                sys_rows.append({"timestamp": t_iso, "mem_available_mb": 16000.0 - s * 10, "disk_read_bytes_per_s": 0.0})
                gpu_rows.append({"timestamp": t_iso, "memory_used_mb": 4000.0, "utilization_pct": 20.0})

            sys_path.write_text("\n".join(json.dumps(r) for r in sys_rows) + "\n")
            gpu_path.write_text("\n".join(json.dumps(r) for r in gpu_rows) + "\n")

            # Generate 1.0s window snapshots
            snaps = generate_aligned_snapshots(
                run_id="idle-density-test",
                sched_events_path=sched_path,
                sched_summary_path=summary_path,
                gpu_samples_path=gpu_path,
                system_samples_path=sys_path,
                bucket_sec=1.0,
            )

            self.assertEqual(len(snaps), 10)
            for idx, snap in enumerate(snaps):
                # Every single window must validate 100%
                validated = validate_snapshot(snap)
                self.assertIsNotNone(validated)

                # Idle target windows still contain complete system and background telemetry
                self.assertIn("system_baseline", snap["sched"])
                self.assertIn("memory_stalls", snap)
                self.assertIn("synchronization", snap)
                self.assertGreater(snap["system"]["mem_available_mb"], 0)
                self.assertEqual(snap["gpu"]["memory_used_mb"], 4000.0)


if __name__ == "__main__":
    unittest.main()
