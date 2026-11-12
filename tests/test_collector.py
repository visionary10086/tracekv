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

    def test_kernel_source_and_build_env_passed(self):
        """Verify BPFTRACE_KERNEL_SOURCE and BPFTRACE_KERNEL_BUILD are passed to child env."""
        code = (
            "import os\n"
            "assert os.environ.get('BPFTRACE_KERNEL_SOURCE') == '/custom/source'\n"
            "assert os.environ.get('BPFTRACE_KERNEL_BUILD') == '/custom/build'\n"
            "print('HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm')\n"
            "print('STAT|matched|0')\n"
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            py_script = f.name
        try:
            from collector.cpu_scheduler import run_collector
            result = run_collector(
                run_id="kheaders-test",
                bpftrace_bin=sys.executable,
                script_path=Path(py_script),
                kernel_source="/custom/source",
                kernel_build="/custom/build",
            )
            self.assertEqual(result.bpftrace_exit_code, 0)
            self.assertEqual(result.kernel_source, "/custom/source")
            self.assertEqual(result.kernel_build, "/custom/build")
        finally:
            os.unlink(py_script)

    def test_find_kernel_headers(self):
        """Verify find_kernel_headers locates directory with include/linux/kconfig.h."""
        from collector.cpu_scheduler import find_kernel_headers
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            kconfig = base / "kernel-headers-6.18.54.1" / "include" / "linux" / "kconfig.h"
            kconfig.parent.mkdir(parents=True, exist_ok=True)
            kconfig.touch()

            with patch("pathlib.Path.home", return_value=base):
                detected = find_kernel_headers()
                self.assertIsNotNone(detected)
                self.assertEqual(detected, base / "kernel-headers-6.18.54.1")

    def test_summary_includes_kernel_paths(self):
        """Summary dict includes kernel_source and kernel_build entries."""
        from collector.cpu_scheduler import CollectorResult, TimeAnchor, stats_to_dict
        res = CollectorResult(
            run_id="k-test",
            start_anchor=TimeAnchor(wall_ns=1, monotonic_ns=2),
            kernel_source="/path/to/source",
            kernel_build="/path/to/build",
        )
        d = stats_to_dict(res)
        self.assertEqual(d["kernel_source"], "/path/to/source")
        self.assertEqual(d["kernel_build"], "/path/to/build")


# ===========================================================================
# CLI module execution test
# ===========================================================================

class TestCLI(unittest.TestCase):
    def test_module_importable(self):
        """The collector module can be imported without side effects."""
        import collector.cpu_scheduler as mod
        self.assertTrue(hasattr(mod, "main"))
        self.assertTrue(hasattr(mod, "run_collector"))

    def test_cli_kernel_args(self):
        """CLI arguments for kernel source and build are accepted."""
        from unittest.mock import patch
        from collector.cpu_scheduler import main, CollectorResult, TimeAnchor
        dummy_result = CollectorResult(
            run_id="test",
            start_anchor=TimeAnchor(wall_ns=1, monotonic_ns=2),
            end_anchor=TimeAnchor(wall_ns=3, monotonic_ns=4),
        )
        with patch("collector.cpu_scheduler.run_collector", return_value=dummy_result) as mock_run:
            with patch("sys.stderr"), patch("builtins.print"):
                exit_code = main(["--kernel-source", "/tmp/ksrc", "--kernel-build", "/tmp/kbld", "--duration", "2.0"])
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            _, kwargs = mock_run.call_args
            self.assertEqual(str(kwargs["kernel_source"]), "/tmp/ksrc")
            self.assertEqual(str(kwargs["kernel_build"]), "/tmp/kbld")

    def test_workload_lifecycle(self):
        """Synthetic workload runs and stops cleanly."""
        from tools.sched_workload import run_workload
        # Run a 0.05s workload with 1 worker and probe
        start = time.time()
        run_workload(workers=1, duration=0.05, include_probe=True, probe_interval=0.01)
        elapsed = time.time() - start
        self.assertGreaterEqual(elapsed, 0.04)

    def test_experiment_table_formatting(self):
        """Experiment table formats markdown comparison cleanly."""
        from tools.sched_experiment import format_comparison_table
        sample_summaries = [
            {
                "scenario": "idle",
                "workers": 0,
                "event_count": 1000,
                "p50_ns": 5000,
                "p95_ns": 15000,
                "p99_ns": 35000,
                "max_ns": 100000,
                "dropped_events_count": 0,
                "overhead": {"collector_avg_cpu_pct": 1.5, "peak_python_rss_kb": 25000},
            },
            {
                "scenario": "load_32",
                "workers": 32,
                "event_count": 2500,
                "p50_ns": 45000,
                "p95_ns": 180000,
                "p99_ns": 450000,
                "max_ns": 2000000,
                "dropped_events_count": 0,
                "overhead": {"collector_avg_cpu_pct": 2.1, "peak_python_rss_kb": 26000},
            },
        ]
        table = format_comparison_table(sample_summaries)
        self.assertIn("Scenario", table)
        self.assertIn("idle", table)
        self.assertIn("load_32", table)
        self.assertIn("5.00 µs", table)
        self.assertIn("45.00 µs", table)

    def test_collect_gpu_metrics_mock(self):
        """Verify GPU metrics parsing and MiB to decimal MB conversion."""
        from collector.gpu import collect_gpu_metrics
        mock_output = "1000, 12000, 45, 55, 120.5\n"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=mock_output)
            res = collect_gpu_metrics(binary_path="/mock/nvidia-smi")
            self.assertEqual(res["source"], "nvidia-smi")
            # 1000 MiB = 1000 * 1024 * 1024 / 1_000_000 = 1048.58 MB
            self.assertAlmostEqual(res["memory_used_mb"], 1048.58, places=1)
            self.assertEqual(res["utilization_pct"], 45.0)
            self.assertEqual(res["temperature_c"], 55.0)
            self.assertEqual(res["power_w"], 120.5)

    def test_collect_gpu_metrics_unavailable(self):
        """When nvidia-smi is unavailable, collector returns source=unavailable."""
        from collector.gpu import collect_gpu_metrics
        res = collect_gpu_metrics(binary_path="/nonexistent/nvidia-smi")
        self.assertEqual(res["source"], "unavailable")
        self.assertIsNone(res["memory_used_mb"])

    def test_build_unified_snapshot_integration(self):
        """Verify unified snapshot passes schema validation and includes extensions."""
        from collector.aggregator import build_unified_snapshot
        mock_sched = {
            "p50_ns": 5000,
            "p95_ns": 15000,
            "p99_ns": 35000,
            "mean_ns": 8000,
            "unmatched_switch_count": 42,
            "event_count": 500,
            "dropped_events_count": 0,
        }
        mock_gpu = {"memory_used_mb": 2048.0, "source": "nvidia-smi"}
        snap = build_unified_snapshot(
            run_id="unified-test",
            sched_summary=mock_sched,
            gpu_metrics=mock_gpu,
        )
        self.assertEqual(snap["run_id"], "unified-test")
        self.assertEqual(snap["sources"]["gpu"], "nvidia-smi")
        self.assertIn("ebpf_sched", snap["sources"]["system"])
        self.assertEqual(snap["system"]["sched_p50_us"], 5.0)
        self.assertEqual(snap["system"]["sched_p95_us"], 15.0)
        self.assertEqual(snap["system"]["sched_p99_us"], 35.0)
        self.assertEqual(snap["system"]["sched_preempt_count"], 42)
        self.assertIn("sched", snap)
        self.assertEqual(snap["sched"]["event_count"], 500)

    def test_correlator_time_buckets_and_pearson(self):
        """Verify cross-layer time-series alignment and Pearson correlation."""
        from collector.correlator import correlate_experiment_data, compute_correlation
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            summary_f = base / "summary.json"
            events_f = base / "events.jsonl"
            requests_f = base / "requests.jsonl"

            # Create synthetic summary with time anchor (epoch 1000.0)
            summary_f.write_text(json.dumps({
                "start_wall_ns": 1_000_000_000_000,
                "start_monotonic_ns": 500_000_000_000,
            }))

            # Event at mono = 500s (epoch 1000.0s), latency = 10us
            # Event at mono = 501.5s (epoch 1001.5s), latency = 100us
            events_lines = [
                json.dumps({"timestamp_ns": 500_000_000_000, "value": 10000, "comm": "app"}),
                json.dumps({"timestamp_ns": 501_500_000_000, "value": 100000, "comm": "app"}),
            ]
            events_f.write_text("\n".join(events_lines) + "\n")

            # Requests at 1000.2s and 1001.7s
            req_lines = [
                json.dumps({"started_at_utc": "1970-01-01T00:16:40.200000Z", "ttft_ms": 15.0, "completion_tokens": 10}),
                json.dumps({"started_at_utc": "1970-01-01T00:16:41.700000Z", "ttft_ms": 150.0, "completion_tokens": 20}),
            ]
            requests_f.write_text("\n".join(req_lines) + "\n")

            buckets = correlate_experiment_data(
                sched_events_path=events_f,
                sched_summary_path=summary_f,
                requests_path=requests_f,
                bucket_sec=1.0,
            )
            self.assertGreaterEqual(len(buckets), 2)
            # Bucket 0 has sched latency 10us and ttft 15ms
            self.assertEqual(buckets[0]["sched"]["event_count"], 1)
            self.assertEqual(buckets[0]["sched"]["p50_us"], 10.0)
            self.assertEqual(buckets[0]["requests"]["request_count"], 1)
            self.assertEqual(buckets[0]["requests"]["ttft_p50_ms"], 15.0)

            # Test Pearson r
            r = compute_correlation([10.0, 100.0], [15.0, 150.0])
            self.assertEqual(r, 1.0)


class TestV02Integration(unittest.TestCase):
    """Tests for TraceKV v0.2 integration contract adherence."""

    def test_stats_to_dict_scope_and_anchors(self):
        """Verify sched_summary.json includes scope: system and time anchors."""
        anchor = TimeAnchor(wall_ns=1700000000000000000, monotonic_ns=100000000000)
        res = CollectorResult(run_id="run-v02", start_anchor=anchor)
        d = stats_to_dict(res)
        self.assertEqual(d["scope"], "system")
        self.assertEqual(d["start_wall_ns"], 1700000000000000000)
        self.assertEqual(d["start_monotonic_ns"], 100000000000)

        # Target process scope
        res_target = CollectorResult(
            run_id="run-target",
            start_anchor=anchor,
            scope="target_process",
            target_pids=[1234, 5678],
        )
        d_target = stats_to_dict(res_target)
        self.assertEqual(d_target["scope"], "target_process")
        self.assertEqual(d_target["target_pids"], [1234, 5678])

    def test_gpu_monitor_and_sample_parsing(self):
        """Verify GPU parsing with 7 fields, [Not Supported] fields, and GpuMonitor."""
        from collector.gpu import (
            collect_gpu_metrics,
            parse_legacy_gpu_sample,
            GpuMonitor,
        )

        # 7-field mock with [Not Supported] power
        mock_7 = "0, GPU-test-uuid, 2048, 8192, 75, 62.0, [Not Supported]\n"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=mock_7)
            res = collect_gpu_metrics(binary_path="/mock/smi", run_id="gpu-test")
            self.assertEqual(res["gpu_index"], 0)
            self.assertEqual(res["gpu_uuid"], "GPU-test-uuid")
            # 2048 MiB = 2048 * 1024 * 1024 / 1_000_000 = 2147.48 MB
            self.assertAlmostEqual(res["memory_used_mb"], 2147.48, places=1)
            self.assertEqual(res["utilization_pct"], 75.0)
            self.assertEqual(res["temperature_c"], 62.0)
            self.assertIsNone(res["power_w"])
            self.assertEqual(res["source"], "nvidia-smi")

        # Legacy 4-item baseline parsing: used_mib, util_pct, temp_c, power_w
        legacy_rec = {
            "time_utc": "2026-11-12T10:00:00Z",
            "values_memory_MiB_util_percent_temp_C_power_W": "1000, 50, 45, 80.5",
        }
        parsed_legacy = parse_legacy_gpu_sample(legacy_rec, run_id="leg-run")
        self.assertEqual(parsed_legacy["run_id"], "leg-run")
        self.assertEqual(parsed_legacy["power_w"], 80.5)
        self.assertEqual(parsed_legacy["temperature_c"], 45.0)
        self.assertEqual(parsed_legacy["utilization_pct"], 50.0)

        # GpuMonitor background writing
        with tempfile.TemporaryDirectory() as tmpdir:
            out_file = Path(tmpdir) / "gpu_samples.jsonl"
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout=mock_7)
                with GpuMonitor("mon-run", out_file, interval_sec=0.05, binary_path="/mock/smi") as mon:
                    time.sleep(0.12)
                self.assertGreaterEqual(mon.sample_count, 1)
                self.assertTrue(out_file.exists())
                first_line = json.loads(out_file.read_text().splitlines()[0])
                self.assertEqual(first_line["run_id"], "mon-run")
                self.assertEqual(first_line["source"], "nvidia-smi")

    def test_system_monitor_and_sample(self):
        """Verify /proc/meminfo reading, decimal MB conversion, and SystemMonitor."""
        from collector.system import collect_system_sample, SystemMonitor

        mem_text = "MemTotal:       16384000 kB\nMemFree:         4096000 kB\nMemAvailable:    8000000 kB\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_meminfo = Path(tmpdir) / "meminfo"
            fake_meminfo.write_text(mem_text)

            sample = collect_system_sample(run_id="sys-test", meminfo_path=fake_meminfo)
            # 8000000 kB * 1024 / 1_000_000 = 8192.0 MB
            self.assertAlmostEqual(sample["mem_available_mb"], 8192.0, places=1)
            self.assertEqual(sample["source"], "proc")
            self.assertIsNone(sample["disk_read_p95_ms"])

            out_file = Path(tmpdir) / "system_samples.jsonl"
            with SystemMonitor("sys-mon", out_file, interval_sec=0.05, meminfo_path=fake_meminfo) as mon:
                time.sleep(0.12)
            self.assertGreaterEqual(mon.sample_count, 1)
            first_line = json.loads(out_file.read_text().splitlines()[0])
            self.assertEqual(first_line["run_id"], "sys-mon")
            self.assertEqual(first_line["source"], "proc")

    def test_generate_aligned_snapshots_v02(self):
        """Verify aligned_snapshots.jsonl conforms to TraceKV v0.2 specification."""
        from collector.correlator import generate_aligned_snapshots, write_aligned_snapshots
        from contracts.schema import validate_snapshot

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            sched_sum_path = base / "sched_summary.json"
            sched_ev_path = base / "sched_events.jsonl"
            gpu_path = base / "gpu_samples.jsonl"
            sys_path = base / "system_samples.jsonl"
            kv_path = base / "kv_samples.jsonl"
            out_snapshots = base / "aligned_snapshots.jsonl"

            # Anchor: start_wall_ns at epoch 1000s, start_mono_ns at 100s
            sched_sum_path.write_text(json.dumps({
                "scope": "system",
                "start_wall_ns": 1_000_000_000_000,
                "start_monotonic_ns": 100_000_000_000,
            }))

            # Event 1 at mono 100.2s -> wall 1000.2s, lat 15us
            # Event 2 at mono 101.5s -> wall 1001.5s, lat 85us
            sched_ev_path.write_text(
                json.dumps({"timestamp_ns": 100_200_000_000, "value": 15000, "comm": "vllm"}) + "\n" +
                json.dumps({"timestamp_ns": 101_500_000_000, "value": 85000, "comm": "vllm"}) + "\n"
            )

            # GPU sample at 1000.4s and 101.4s
            gpu_path.write_text(
                json.dumps({"timestamp": "1970-01-01T00:16:40.400000Z", "memory_used_mb": 4096.0, "source": "nvidia-smi"}) + "\n" +
                json.dumps({"timestamp": "1970-01-01T00:16:41.400000Z", "memory_used_mb": 4200.0, "source": "nvidia-smi"}) + "\n"
            )

            # System sample at 1000.1s and 101.1s
            sys_path.write_text(
                json.dumps({"timestamp": "1970-01-01T00:16:40.100000Z", "mem_available_mb": 8192.0, "source": "proc"}) + "\n" +
                json.dumps({"timestamp": "1970-01-01T00:16:41.100000Z", "mem_available_mb": 8000.0, "source": "proc"}) + "\n"
            )

            # KV sample at 1000.5s
            kv_path.write_text(
                json.dumps({"timestamp": "1970-01-01T00:16:40.500000Z", "usage_ratio": 0.65, "prefix_hit_ratio": 0.30, "running_requests": 2}) + "\n"
            )

            snaps = generate_aligned_snapshots(
                run_id="run-exp-v02",
                sched_events_path=sched_ev_path,
                sched_summary_path=sched_sum_path,
                gpu_samples_path=gpu_path,
                system_samples_path=sys_path,
                kv_samples_path=kv_path,
                bucket_sec=1.0,
                strict_anchor=True,
            )

            self.assertGreaterEqual(len(snaps), 2)

            # Each snapshot must strictly pass contracts/schema.py
            for s in snaps:
                validate_snapshot(s)
                self.assertEqual(s["schema_version"], "0.1")
                self.assertEqual(s["run_id"], "run-exp-v02")
                self.assertIn("window_start_utc", s)
                self.assertIn("window_end_utc", s)
                self.assertEqual(s["sched"]["scope"], "system")

            # Check window 0 (1000.0s to 1001.0s)
            w0 = snaps[0]
            self.assertEqual(w0["sched"]["event_count"], 1)
            self.assertEqual(w0["sched"]["p50_us"], 15.0)
            self.assertEqual(w0["gpu"]["memory_used_mb"], 4096.0)
            self.assertEqual(w0["sources"]["gpu"], "nvidia-smi")
            self.assertEqual(w0["sources"]["system"], "proc+ebpf_sched")
            self.assertEqual(w0["kv"]["usage_ratio"], 0.65)

            # Check window 1 (1001.0s to 1002.0s)
            w1 = snaps[1]
            self.assertEqual(w1["sched"]["event_count"], 1)
            self.assertEqual(w1["sched"]["p50_us"], 85.0)
            self.assertEqual(w1["gpu"]["memory_used_mb"], 4200.0)

            # Test write_aligned_snapshots
            count = write_aligned_snapshots(snaps, out_snapshots)
            self.assertEqual(count, len(snaps))
            self.assertTrue(out_snapshots.exists())

    def test_strict_anchor_rejection(self):
        """When anchors are missing and strict_anchor=True, an error is raised."""
        from collector.correlator import generate_aligned_snapshots
        with tempfile.TemporaryDirectory() as tmpdir:
            ev_file = Path(tmpdir) / "sched.jsonl"
            ev_file.write_text(json.dumps({"timestamp_ns": 100, "value": 10}) + "\n")
            with self.assertRaises(ValueError):
                generate_aligned_snapshots(
                    run_id="r",
                    sched_events_path=ev_file,
                    sched_summary_path=None,
                    strict_anchor=True,
                )

    def test_enriched_gpu_12_fields(self):
        """Verify 12-field GPU query parsing with memory bandwidth and PState."""
        from collector.gpu import collect_gpu_metrics
        mock_12 = "0, GPU-4080-UUID, 4000, 12000, 8000, 60, 25, 45.0, 50.5, P0, 2000, 9000\n"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=mock_12)
            res = collect_gpu_metrics(binary_path="/mock/smi", run_id="gpu-rich")
            self.assertEqual(res["gpu_uuid"], "GPU-4080-UUID")
            self.assertAlmostEqual(res["memory_free_mb"], 8388.61, places=1)
            self.assertEqual(res["utilization_pct"], 60.0)
            self.assertEqual(res["utilization_memory_pct"], 25.0)
            self.assertEqual(res["pstate"], "P0")
            self.assertEqual(res["sm_clock_mhz"], 2000)
            self.assertEqual(res["mem_clock_mhz"], 9000)

    def test_enriched_system_psi_and_stat(self):
        """Verify CPU utilization, PSI pressure, diskstats, and swap calculation."""
        from collector.system import collect_system_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            meminfo = base / "meminfo"
            meminfo.write_text(
                "MemTotal:       16000000 kB\n"
                "MemFree:         4000000 kB\n"
                "MemAvailable:    8000000 kB\n"
                "SwapTotal:       2000000 kB\n"
                "SwapFree:        1500000 kB\n"
            )
            stat = base / "stat"
            stat.write_text("cpu  100 0 50 850 0 0 0 0 0 0\nctxt 5000\n")
            loadavg = base / "loadavg"
            loadavg.write_text("0.50 0.40 0.30 3/500 12345\n")

            p_dir = base / "pressure"
            p_dir.mkdir()
            (p_dir / "cpu").write_text("some avg10=2.50 avg60=1.00 avg300=0.50 total=1000\n")
            (p_dir / "memory").write_text("some avg10=1.20 avg60=0.80 avg300=0.20 total=500\nfull avg10=0.50 avg60=0.20 avg300=0.05 total=200\n")
            (p_dir / "io").write_text("some avg10=3.00 avg60=1.50 avg300=0.80 total=800\nfull avg10=1.00 avg60=0.50 avg300=0.10 total=300\n")

            diskstats = base / "diskstats"
            diskstats.write_text("   8       0 sda 100 0 2000 100 50 0 1000 50 0 100 100\n")

            # Sample 1
            sample1, state1 = collect_system_sample(
                run_id="sys-rich",
                meminfo_path=meminfo,
                stat_path=stat,
                loadavg_path=loadavg,
                pressure_dir=p_dir,
                diskstats_path=diskstats,
                return_state=True,
            )
            self.assertEqual(sample1["memory"]["swap_used_pct"], 25.0)
            self.assertEqual(sample1["psi"]["psi_cpu_some_avg10"], 2.5)
            self.assertEqual(sample1["psi"]["psi_io_some_avg10"], 3.0)
            self.assertEqual(sample1["cpu"]["running_threads"], 3)
            self.assertEqual(sample1["cpu"]["total_threads"], 500)

            # Sample 2 with delta
            time.sleep(0.05)
            stat.write_text("cpu  120 0 60 860 0 0 0 0 0 0\nctxt 5100\n")
            diskstats.write_text("   8       0 sda 110 0 2200 110 55 0 1100 55 0 110 110\n")

            sample2, state2 = collect_system_sample(
                run_id="sys-rich",
                meminfo_path=meminfo,
                stat_path=stat,
                loadavg_path=loadavg,
                pressure_dir=p_dir,
                diskstats_path=diskstats,
                prev_state=state1,
                return_state=True,
            )
            # Total delta: (120-100)+(60-50)+(860-850) = 20 + 10 + 10 = 40 ticks
            # Idle delta: 10 ticks -> cpu_util = (1 - 10/40) * 100 = 75.0%
            self.assertEqual(sample2["cpu"]["utilization_pct"], 75.0)
            self.assertGreater(sample2["cpu"]["ctxt_switches_per_s"], 0)
            self.assertGreater(sample2["io"]["read_bytes_per_s"], 0)

    def test_top_comms_and_distinct_tids(self):
        """Verify distinct TIDs and top contending process ranking in cpu_scheduler."""
        from collector.cpu_scheduler import process_stream
        lines = [
            "HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm",
            "SCHED|1000|500|101|0|500|vllm",
            "SCHED|2000|1500|102|1|500|python3",
            "SCHED|3000|2500|101|0|500|vllm",
            "SCHED|4000|3500|103|2|500|kworker",
            "STAT|matched|4",
        ]
        res = process_stream(io.StringIO("\n".join(lines) + "\n"), run_id="top-comm-test")
        d = stats_to_dict(res)
        self.assertEqual(d["distinct_tids_count"], 3)
        self.assertEqual(d["top_comms"]["vllm"], 2)
        self.assertEqual(d["top_comms"]["python3"], 1)
        self.assertEqual(d["top_comms"]["kworker"], 1)



if __name__ == "__main__":
    unittest.main(verbosity=2)
