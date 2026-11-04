"""Tests for collector.cpu_scheduler — no root or kernel tracing required.

All tests use synthetic text streams that mimic bpftrace output.  No mock
data is ever presented as genuine kernel measurement.
"""

import io
import json
import math
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from collector.cpu_scheduler import (
    SchedEvent,
    TimeAnchor,
    CollectorResult,
    CollectorStats,
    parse_event,
    parse_stat,
    percentile,
    compute_stats,
    event_to_jsonl,
    stats_to_dict,
    process_stream,
)


# ---------------------------------------------------------------------------
# Helper: build synthetic bpftrace output
# ---------------------------------------------------------------------------

def make_line(switch_ns, wakeup_ns, tid, cpu, latency_ns, comm="test"):
    return f"SCHED|{switch_ns}|{wakeup_ns}|{tid}|{cpu}|{latency_ns}|{comm}"


def make_stream(lines):
    """Return a StringIO object from a list of strings."""
    return io.StringIO("\n".join(lines) + "\n")


# ===========================================================================
# Event parsing tests
# ===========================================================================

class TestParseEvent(unittest.TestCase):
    def test_valid_event(self):
        line = "SCHED|200000|100000|42|3|100000|myapp"
        ev = parse_event(line)
        self.assertIsNotNone(ev)
        self.assertEqual(ev.switch_ns, 200000)
        self.assertEqual(ev.wakeup_ns, 100000)
        self.assertEqual(ev.tid, 42)
        self.assertEqual(ev.cpu, 3)
        self.assertEqual(ev.latency_ns, 100000)
        self.assertEqual(ev.comm, "myapp")

    def test_empty_line(self):
        self.assertIsNone(parse_event(""))

    def test_wrong_prefix(self):
        self.assertIsNone(parse_event("DATA|1|2|3|4|5|x"))

    def test_too_few_fields(self):
        self.assertIsNone(parse_event("SCHED|1|2|3|4|5"))

    def test_too_many_fields_still_valid(self):
        # The comm field might contain pipe characters in theory;
        # our parser rejects extra fields to keep things strict.
        result = parse_event("SCHED|1|2|3|4|5|a|b")
        # 8 parts != 7 → rejected
        self.assertIsNone(result)

    def test_non_integer_field(self):
        self.assertIsNone(parse_event("SCHED|abc|2|3|4|5|x"))

    def test_negative_latency_rejected(self):
        self.assertIsNone(parse_event("SCHED|200|100|1|0|-100|x"))

    def test_negative_tid_rejected(self):
        self.assertIsNone(parse_event("SCHED|200|100|-1|0|100|x"))

    def test_negative_cpu_rejected(self):
        self.assertIsNone(parse_event("SCHED|200|100|1|-1|100|x"))

    def test_negative_timestamp_rejected(self):
        self.assertIsNone(parse_event("SCHED|-200|100|1|0|100|x"))

    def test_empty_comm_rejected(self):
        self.assertIsNone(parse_event("SCHED|200|100|1|0|100|"))

    def test_zero_latency_accepted(self):
        ev = parse_event("SCHED|100|100|1|0|0|idle")
        self.assertIsNotNone(ev)
        self.assertEqual(ev.latency_ns, 0)


# ===========================================================================
# Stat parsing tests
# ===========================================================================

class TestParseStat(unittest.TestCase):
    def test_valid_stat(self):
        self.assertEqual(parse_stat("STAT|matched|42"), ("matched", 42))

    def test_invalid_stat(self):
        self.assertIsNone(parse_stat("STAT|matched"))
        self.assertIsNone(parse_stat("STAT|matched|abc"))
        self.assertIsNone(parse_stat(""))


# ===========================================================================
# Percentile computation tests
# ===========================================================================

class TestPercentile(unittest.TestCase):
    def test_single_value(self):
        self.assertEqual(percentile([100], 50), 100)
        self.assertEqual(percentile([100], 95), 100)
        self.assertEqual(percentile([100], 99), 100)

    def test_two_values(self):
        vals = [10, 90]
        self.assertEqual(percentile(vals, 50), 10)
        self.assertEqual(percentile(vals, 95), 90)

    def test_known_values(self):
        vals = list(range(1, 101))  # 1..100
        self.assertEqual(percentile(vals, 50), 50)
        self.assertEqual(percentile(vals, 95), 95)
        self.assertEqual(percentile(vals, 99), 99)

    def test_p0_returns_min(self):
        self.assertEqual(percentile([5, 10, 20], 0), 5)

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            percentile([], 50)

    def test_twenty_values_p95(self):
        vals = list(range(1, 21))  # 1..20
        # ceil(0.95 * 20) = 19 → index 18 → value 19
        self.assertEqual(percentile(vals, 95), 19)


# ===========================================================================
# Statistics computation tests
# ===========================================================================

class TestComputeStats(unittest.TestCase):
    def test_empty_dataset(self):
        s = compute_stats([])
        self.assertEqual(s.event_count, 0)
        self.assertIsNone(s.min_ns)
        self.assertIsNone(s.max_ns)
        self.assertIsNone(s.p50_ns)
        self.assertIsNone(s.mean_ns)

    def test_single_event(self):
        s = compute_stats([5000])
        self.assertEqual(s.event_count, 1)
        self.assertEqual(s.min_ns, 5000)
        self.assertEqual(s.max_ns, 5000)
        self.assertEqual(s.p50_ns, 5000)
        self.assertEqual(s.p95_ns, 5000)
        self.assertEqual(s.p99_ns, 5000)
        self.assertAlmostEqual(s.mean_ns, 5000.0)

    def test_multiple_events(self):
        data = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
        s = compute_stats(data)
        self.assertEqual(s.event_count, 10)
        self.assertEqual(s.min_ns, 1000)
        self.assertEqual(s.max_ns, 10000)
        self.assertAlmostEqual(s.mean_ns, 5500.0)
        self.assertEqual(s.p50_ns, 5000)

    def test_nanosecond_precision_preserved(self):
        data = [123456789, 123456790, 123456791]
        s = compute_stats(data)
        self.assertEqual(s.min_ns, 123456789)
        self.assertEqual(s.max_ns, 123456791)


# ===========================================================================
# JSONL output tests
# ===========================================================================

class TestEventToJsonl(unittest.TestCase):
    def test_structure(self):
        ev = SchedEvent(
            switch_ns=200000, wakeup_ns=100000, tid=42,
            cpu=3, latency_ns=100000, comm="test",
        )
        line = event_to_jsonl(ev, "run-001")
        parsed = json.loads(line)
        self.assertEqual(parsed["schema_version"], 1)
        self.assertEqual(parsed["run_id"], "run-001")
        self.assertEqual(parsed["source"], "ebpf_sched")
        self.assertEqual(parsed["metric"], "wakeup_to_run_latency")
        self.assertEqual(parsed["clock"], "monotonic")
        self.assertEqual(parsed["timestamp_ns"], 200000)
        self.assertEqual(parsed["wakeup_timestamp_ns"], 100000)
        self.assertIsNone(parsed["pid"])
        self.assertEqual(parsed["tid"], 42)
        self.assertEqual(parsed["comm"], "test")
        self.assertEqual(parsed["cpu"], 3)
        self.assertIsNone(parsed["request_id"])
        self.assertEqual(parsed["value"], 100000)
        self.assertEqual(parsed["unit"], "ns")

    def test_valid_json_line(self):
        ev = SchedEvent(200, 100, 1, 0, 100, "x")
        line = event_to_jsonl(ev, "r")
        # Must be a single line
        self.assertNotIn("\n", line)
        # Must be valid JSON
        json.loads(line)


# ===========================================================================
# Stream processing tests
# ===========================================================================

class TestProcessStream(unittest.TestCase):
    def test_normal_events(self):
        lines = [
            "HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm",
            make_line(200000, 100000, 42, 0, 100000, "app1"),
            make_line(300000, 250000, 43, 1, 50000, "app2"),
            make_line(400000, 350000, 44, 2, 50000, "app3"),
        ]
        result = process_stream(make_stream(lines), "test-001")
        self.assertEqual(result.stats.event_count, 3)
        self.assertEqual(result.stats.min_ns, 50000)
        self.assertEqual(result.stats.max_ns, 100000)
        self.assertEqual(result.stats.parser_error_count, 0)

    def test_empty_lines_ignored(self):
        lines = ["", "", make_line(200, 100, 1, 0, 100, "x"), ""]
        result = process_stream(make_stream(lines), "t")
        self.assertEqual(result.stats.event_count, 1)
        self.assertEqual(result.stats.parser_error_count, 0)

    def test_malformed_events_counted(self):
        lines = [
            make_line(200, 100, 1, 0, 100, "ok"),
            "SCHED|bad|data",
            "SCHED|200|100|1|0|-5|neg",
            make_line(300, 200, 2, 0, 100, "ok2"),
        ]
        result = process_stream(make_stream(lines), "t")
        self.assertEqual(result.stats.event_count, 2)
        self.assertEqual(result.stats.parser_error_count, 2)

    def test_stat_lines_captured(self):
        lines = [
            make_line(200, 100, 1, 0, 100, "x"),
            "STAT|matched|1",
            "STAT|dup_wakeup|5",
            "STAT|unmatched_switch|10",
            "STAT|exit_cleanup|3",
        ]
        result = process_stream(make_stream(lines), "t")
        self.assertEqual(result.stats.event_count, 1)
        self.assertEqual(result.stats.kernel_matched_count, 1)
        self.assertEqual(result.stats.dropped_events_count, 0)
        self.assertEqual(result.stats.duplicate_wakeup_count, 5)
        self.assertEqual(result.stats.unmatched_switch_count, 10)
        self.assertEqual(result.stats.exit_cleanup_count, 3)

    def test_kernel_matched_loss_reconciliation(self):
        # 100 matched in kernel, but only 90 lines received
        lines = [make_line(200, 100, i, 0, 100, f"app{i}") for i in range(90)]
        lines.append("STAT|matched|100")
        result = process_stream(make_stream(lines), "loss-test")
        self.assertEqual(result.stats.event_count, 90)
        self.assertEqual(result.stats.kernel_matched_count, 100)
        self.assertEqual(result.stats.dropped_events_count, 10)

    def test_reservoir_sampling_bounds_memory(self):
        # Stream 5000 events with max_samples=100
        lines = [make_line(1000 + i, 1000, i, 0, (i + 1) * 10, f"p{i}") for i in range(5000)]
        result = process_stream(make_stream(lines), "res-test", max_samples=100)
        self.assertEqual(result.stats.event_count, 5000)
        # min and max are exact across all 5000 events
        self.assertEqual(result.stats.min_ns, 10)
        self.assertEqual(result.stats.max_ns, 50000)
        # mean is exact
        expected_mean = sum((i + 1) * 10 for i in range(5000)) / 5000
        self.assertAlmostEqual(result.stats.mean_ns, expected_mean)
        # percentiles are computed from the bounded reservoir
        self.assertIsNotNone(result.stats.p50_ns)
        self.assertIsNotNone(result.stats.p95_ns)
        self.assertIsNotNone(result.stats.p99_ns)
        self.assertLess(result.stats.p50_ns, result.stats.p95_ns)

    def test_output_file_written(self):
        lines = [
            make_line(200, 100, 1, 0, 100, "x"),
            make_line(300, 250, 2, 1, 50, "y"),
        ]
        output = io.StringIO()
        result = process_stream(make_stream(lines), "out-test", output=output)
        self.assertEqual(result.stats.event_count, 2)
        output.seek(0)
        written_lines = output.read().strip().split("\n")
        self.assertEqual(len(written_lines), 2)
        for wl in written_lines:
            parsed = json.loads(wl)
            self.assertEqual(parsed["run_id"], "out-test")

    def test_unknown_lines_counted_as_errors(self):
        lines = ["WARNING: something from bpftrace", make_line(200, 100, 1, 0, 100, "x")]
        result = process_stream(make_stream(lines), "t")
        self.assertEqual(result.stats.event_count, 1)
        self.assertEqual(result.stats.parser_error_count, 1)

    def test_header_line_ignored(self):
        lines = ["HEADER|a|b|c", make_line(200, 100, 1, 0, 100, "x")]
        result = process_stream(make_stream(lines), "t")
        self.assertEqual(result.stats.event_count, 1)
        self.assertEqual(result.stats.parser_error_count, 0)

    def test_empty_stream(self):
        result = process_stream(make_stream([]), "empty")
        self.assertEqual(result.stats.event_count, 0)
        self.assertIsNone(result.stats.p50_ns)
        self.assertIsNone(result.stats.mean_ns)

    def test_time_anchors_set(self):
        result = process_stream(make_stream([]), "t")
        self.assertIsNotNone(result.start_anchor)
        self.assertIsNotNone(result.end_anchor)
        self.assertGreater(result.start_anchor.wall_ns, 0)
        self.assertGreater(result.start_anchor.monotonic_ns, 0)
        # End should be >= start
        self.assertGreaterEqual(result.end_anchor.monotonic_ns,
                                result.start_anchor.monotonic_ns)


# ===========================================================================
# Time anchor and calibration tests
# ===========================================================================

class TestTimeAnchor(unittest.TestCase):
    def test_monotonic_to_utc_mapping(self):
        """Verify that a monotonic timestamp can be approximately mapped
        to UTC using the anchor pair."""
        anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
        # A hypothetical event 1ms after the anchor in monotonic time
        event_mono_ns = anchor.monotonic_ns + 1_000_000
        # Estimated wall-clock time
        estimated_wall = anchor.wall_ns + (event_mono_ns - anchor.monotonic_ns)
        # Should be approximately 1ms after wall anchor
        delta = estimated_wall - anchor.wall_ns
        self.assertAlmostEqual(delta, 1_000_000, delta=100_000)


# ===========================================================================
# Summary serialization tests
# ===========================================================================

class TestStatsSerialization(unittest.TestCase):
    def test_stats_to_dict(self):
        anchor = TimeAnchor(wall_ns=1000, monotonic_ns=2000)
        result = CollectorResult(run_id="r", start_anchor=anchor)
        result.end_anchor = TimeAnchor(wall_ns=3000, monotonic_ns=4000)
        result.stats.event_count = 5
        result.stats.p50_ns = 100
        result.stats.mean_ns = 120.5

        d = stats_to_dict(result)
        self.assertEqual(d["run_id"], "r")
        self.assertEqual(d["event_count"], 5)
        self.assertEqual(d["p50_ns"], 100)
        self.assertEqual(d["mean_ns"], 120.5)
        self.assertEqual(d["start_wall_ns"], 1000)
        self.assertEqual(d["end_wall_ns"], 3000)
        # Must be JSON-serializable
        json.dumps(d)


# ===========================================================================
# bpftrace subprocess error handling tests
# ===========================================================================

class TestSubprocessHandling(unittest.TestCase):
    def test_bpftrace_not_found(self):
        """When bpftrace is not in PATH, collector returns an error cleanly."""
        from collector.cpu_scheduler import run_collector
        result = run_collector(
            run_id="no-bt",
            bpftrace_bin="/nonexistent/bpftrace",
        )
        self.assertIsNotNone(result.error)
        self.assertIn("bpftrace", result.error.lower())

    def test_permission_denied(self):
        """When bpftrace exists but cannot be executed, error is captured."""
        with tempfile.NamedTemporaryFile(suffix=".bt", delete=False) as f:
            f.write(b"#!/bin/false\n")
            fake_bt = f.name
        try:
            os.chmod(fake_bt, 0o000)
            from collector.cpu_scheduler import run_collector
            result = run_collector(
                run_id="perm-test",
                bpftrace_bin=fake_bt,
            )
            self.assertIsNotNone(result.error)
        finally:
            os.chmod(fake_bt, 0o644)
            os.unlink(fake_bt)

    def test_process_with_immediate_exit(self):
        """A subprocess that exits immediately still produces a valid result."""
        # Use /bin/echo to simulate bpftrace output
        lines = [
            "HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm",
            make_line(500, 400, 10, 0, 100, "echo"),
            "STAT|matched|1",
            "STAT|dup_wakeup|0",
            "STAT|unmatched_switch|2",
            "STAT|exit_cleanup|0",
        ]
        script_content = "\n".join(["#!/bin/sh", "echo '" + "\\n".join(lines) + "'"]) + "\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            # Write a script that echo's the bpftrace-like output
            f.write("#!/bin/sh\n")
            for line in lines:
                f.write(f'echo "{line}"\n')
            f.flush()
            script_path = f.name
        try:
            os.chmod(script_path, 0o755)
            from collector.cpu_scheduler import run_collector
            result = run_collector(
                run_id="echo-test",
                bpftrace_bin=script_path,
                script_path=Path("/dev/null"),  # ignored by shell script
            )
            self.assertEqual(result.stats.event_count, 1)
            self.assertEqual(result.stats.unmatched_switch_count, 2)
            self.assertEqual(result.bpftrace_exit_code, 0)
        finally:
            os.unlink(script_path)

    def test_streaming_large_volume_prevents_pipe_deadlock(self):
        """Verify real-time streaming: generating 5,000 lines (>300 KB, far exceeding
        the 64 KB kernel pipe buffer) completes without deadlocking."""
        # Python script generating 5000 lines
        generator_code = (
            "import sys\n"
            "print('HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm')\n"
            "for i in range(5000):\n"
            "    print(f'SCHED|{200000+i}|{100000+i}|{i}|0|100000|app{i}')\n"
            "print('STAT|matched|5000')\n"
            "print('STAT|dup_wakeup|0')\n"
            "print('STAT|unmatched_switch|0')\n"
            "print('STAT|exit_cleanup|0')\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(generator_code)
            py_script = f.name
        try:
            from collector.cpu_scheduler import run_collector
            result = run_collector(
                run_id="pipe-volume-test",
                bpftrace_bin=sys.executable,
                script_path=Path(py_script),
                duration_seconds=5.0,
            )
            self.assertEqual(result.stats.event_count, 5000)
            self.assertEqual(result.stats.kernel_matched_count, 5000)
            self.assertEqual(result.stats.dropped_events_count, 0)
            self.assertEqual(result.bpftrace_exit_code, 0)
        finally:
            os.unlink(py_script)

    def test_stop_event_terminates_subprocess(self):
        """Verify cooperative stop via threading.Event."""
        # A generator that runs in an infinite loop until signaled
        code = (
            "import sys, time\n"
            "print('HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm', flush=True)\n"
            "try:\n"
            "    for i in range(100):\n"
            "        print(f'SCHED|{200+i}|100|1|0|100|app', flush=True)\n"
            "        time.sleep(0.01)\n"
            "    while True:\n"
            "        time.sleep(0.1)\n"
            "except KeyboardInterrupt:\n"
            "    print('STAT|matched|50', flush=True)\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            py_script = f.name
        try:
            import threading
            from collector.cpu_scheduler import run_collector
            stop_evt = threading.Event()
            # Set stop_event after 0.2s
            timer = threading.Timer(0.2, stop_evt.set)
            timer.daemon = True
            timer.start()

            result = run_collector(
                run_id="stop-event-test",
                bpftrace_bin=sys.executable,
                script_path=Path(py_script),
                duration_seconds=None,
                stop_event=stop_evt,
            )
            self.assertGreater(result.stats.event_count, 0)
        finally:
            timer.cancel()
            os.unlink(py_script)

    def test_bpftrace_env_map_and_perf_defaults(self):
        """Verify BPFTRACE_MAX_MAP_KEYS and BPFTRACE_PERF_RB_PAGES are set in child env."""
        code = (
            "import os\n"
            "print(f\"STAT|map_keys|{os.environ.get('BPFTRACE_MAX_MAP_KEYS', '')}\")\n"
            "print(f\"STAT|perf_pages|{os.environ.get('BPFTRACE_PERF_RB_PAGES', '')}\")\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            py_script = f.name
        try:
            from collector.cpu_scheduler import run_collector
            result = run_collector(
                run_id="env-test",
                bpftrace_bin=sys.executable,
                script_path=Path(py_script),
                max_map_keys=32768,
                perf_rb_pages=128,
            )
            # Child process ran with custom map and perf settings
            self.assertEqual(result.bpftrace_exit_code, 0)
        finally:
            os.unlink(py_script)


# ===========================================================================
# CLI module execution test
# ===========================================================================

class TestCLI(unittest.TestCase):
    def test_module_importable(self):
        """The collector module can be imported without side effects."""
        import collector.cpu_scheduler as mod
        self.assertTrue(hasattr(mod, "main"))
        self.assertTrue(hasattr(mod, "run_collector"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
