"""Tests for E0/E1/E2/E5 eBPF collector modules.

Tests for:
  - parse_clock / to_wall_ns / ClockAnchor (E0)
  - parse_runwait / RunwaitEvent (E1)
  - check_ebpf_environment without bpftrace (E0)
  - parse_oncpu_event / process_oncpu_stream (E2)
  - parse_reclaim_event / process_reclaim_stream (E5)
  - build_ebpf_batch / split_into_batches (EbpfBatch uploader)
  - process_stream now handles CLOCK| and RUNWAIT| lines (integration)

All tests use synthetic data streams and do NOT require bpftrace or root.
"""

import io
import json
import unittest
import uuid


class TestClockAnchor(unittest.TestCase):
    """E0: parse_clock and to_wall_ns."""

    def _parse(self, line):
        from collector.cpu_scheduler import parse_clock
        return parse_clock(line)

    def test_parse_valid(self):
        anchor = self._parse("CLOCK|wall_ns=1700000000000000000|mono_ns=12345678901234")
        self.assertIsNotNone(anchor)
        self.assertEqual(anchor.wall_ns, 1_700_000_000_000_000_000)
        self.assertEqual(anchor.monotonic_ns, 12_345_678_901_234)

    def test_parse_wrong_prefix(self):
        self.assertIsNone(self._parse("HEADER|wall_ns=1700|mono_ns=1234"))

    def test_parse_missing_field(self):
        self.assertIsNone(self._parse("CLOCK|wall_ns=1700000000000000000"))

    def test_parse_non_numeric(self):
        self.assertIsNone(self._parse("CLOCK|wall_ns=abc|mono_ns=1234"))

    def test_parse_zero_values(self):
        self.assertIsNone(self._parse("CLOCK|wall_ns=0|mono_ns=0"))

    def test_parse_swapped_key_names(self):
        # Swapped key names should fail
        self.assertIsNone(self._parse("CLOCK|mono_ns=1234|wall_ns=1700"))

    def test_to_wall_ns_identity(self):
        from collector.cpu_scheduler import ClockAnchor, to_wall_ns
        anchor = ClockAnchor(wall_ns=1_700_000_000_000_000_000,
                              monotonic_ns=10_000_000_000)
        # Event at exactly the anchor point
        self.assertEqual(to_wall_ns(10_000_000_000, anchor), 1_700_000_000_000_000_000)

    def test_to_wall_ns_offset(self):
        from collector.cpu_scheduler import ClockAnchor, to_wall_ns
        anchor = ClockAnchor(wall_ns=1_000_000_000_000_000_000,
                              monotonic_ns=5_000_000_000)
        # Event 1 second later (1e9 ns)
        result = to_wall_ns(6_000_000_000, anchor)
        self.assertEqual(result, 1_000_000_001_000_000_000)

    def test_to_wall_ns_before_anchor(self):
        from collector.cpu_scheduler import ClockAnchor, to_wall_ns
        # Event 500ms before anchor — result < anchor.wall_ns
        anchor = ClockAnchor(wall_ns=2_000_000_000_000_000_000,
                              monotonic_ns=10_000_000_000)
        result = to_wall_ns(9_500_000_000, anchor)
        self.assertEqual(result, 2_000_000_000_000_000_000 - 500_000_000)


class TestParseRunwait(unittest.TestCase):
    """E1: parse_runwait."""

    def _parse(self, line):
        from collector.cpu_scheduler import parse_runwait
        return parse_runwait(line)

    def test_valid_line(self):
        ev = self._parse("RUNWAIT|1234567890|1234000000|9999|3|567890|python3")
        self.assertIsNotNone(ev)
        self.assertEqual(ev.switch_ns, 1_234_567_890)
        self.assertEqual(ev.preempt_ns, 1_234_000_000)
        self.assertEqual(ev.tid, 9999)
        self.assertEqual(ev.cpu, 3)
        self.assertEqual(ev.wait_ns, 567_890)
        self.assertEqual(ev.comm, "python3")

    def test_wrong_prefix(self):
        self.assertIsNone(self._parse("SCHED|1234567890|1234000000|9999|3|567890|python3"))

    def test_too_few_fields(self):
        self.assertIsNone(self._parse("RUNWAIT|1234|9999"))

    def test_negative_wait(self):
        self.assertIsNone(self._parse("RUNWAIT|1234567890|1234000000|9999|3|-1|python3"))

    def test_zero_wait_allowed(self):
        ev = self._parse("RUNWAIT|1000|999|42|0|0|idle_thread")
        self.assertIsNotNone(ev)
        self.assertEqual(ev.wait_ns, 0)

    def test_empty_comm_rejected(self):
        self.assertIsNone(self._parse("RUNWAIT|1234567890|1234000000|9999|3|100|"))

    def test_non_numeric_tid(self):
        self.assertIsNone(self._parse("RUNWAIT|1234567890|1234000000|abc|3|100|python3"))


class TestCheckEbpfEnvironment(unittest.TestCase):
    """E0: check_ebpf_environment when bpftrace is not available."""

    def test_no_bpftrace_returns_error(self):
        from collector.cpu_scheduler import check_ebpf_environment
        result = check_ebpf_environment(bpftrace_path="/nonexistent/bpftrace")
        self.assertFalse(result["bpftrace_ok"])
        self.assertFalse(result["liveness_ok"])
        self.assertIn("errors", result)
        self.assertTrue(len(result["errors"]) > 0)
        self.assertIn("details", result)
        self.assertIn("NOT READY", result["details"])

    def test_result_has_all_keys(self):
        from collector.cpu_scheduler import check_ebpf_environment
        result = check_ebpf_environment(bpftrace_path="/nonexistent/bpftrace")
        for key in ("bpftrace_ok", "bpftrace_version", "version_ok",
                    "tracefs_ok", "privilege_ok", "liveness_ok",
                    "details", "errors"):
            self.assertIn(key, result, f"Missing key: {key}")

    def test_tracefs_check(self):
        from collector.cpu_scheduler import check_ebpf_environment
        result = check_ebpf_environment(bpftrace_path="/nonexistent/bpftrace")
        # tracefs_ok is a bool (may be True in WSL2 with tracefs mounted)
        self.assertIsInstance(result["tracefs_ok"], bool)


class TestProcessStreamWithClockAndRunwait(unittest.TestCase):
    """Integration: process_stream handles CLOCK| and RUNWAIT| lines (E0+E1)."""

    def _make_stream(self, lines):
        return io.StringIO("\n".join(lines) + "\n")

    def test_clock_anchor_parsed(self):
        from collector.cpu_scheduler import process_stream
        lines = [
            "CLOCK|wall_ns=1700000000000000000|mono_ns=10000000000",
            "HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm",
            "SCHED|10000500000|10000000000|1234|0|500000|python3",
        ]
        result = process_stream(self._make_stream(lines), run_id="test-clock")
        self.assertIsNotNone(result.kernel_clock_anchor)
        self.assertEqual(result.kernel_clock_anchor.wall_ns, 1_700_000_000_000_000_000)
        self.assertEqual(result.kernel_clock_anchor.monotonic_ns, 10_000_000_000)

    def test_runwait_counted_in_stats(self):
        from collector.cpu_scheduler import process_stream
        lines = [
            "CLOCK|wall_ns=1700000000000000000|mono_ns=10000000000",
            "HEADER|switch_ns|wakeup_ns|tid|cpu|latency_ns|comm",
            "SCHED|10000500000|10000000000|1234|0|500000|python3",
            "RUNWAIT|10001000000|10000800000|5678|1|200000|vllm_worker",
            "RUNWAIT|10002000000|10001500000|5678|1|500000|vllm_worker",
        ]
        out = io.StringIO()
        result = process_stream(self._make_stream(lines), run_id="test-rw", output=out)
        self.assertEqual(result.stats.runwait_event_count, 2)
        self.assertEqual(result.stats.event_count, 1)  # only SCHED events count here

        # RUNWAIT events written to JSONL output
        out.seek(0)
        records = [json.loads(l) for l in out if l.strip()]
        rw_records = [r for r in records if r.get("metric") == "runnable_wait"]
        sched_records = [r for r in records if r.get("metric") is None or "latency" in str(r)]
        self.assertEqual(len(rw_records), 2)
        self.assertEqual(rw_records[0]["value"], 200_000)
        self.assertEqual(rw_records[0]["tid"], 5678)

    def test_runwait_wall_ns_converted(self):
        from collector.cpu_scheduler import process_stream
        lines = [
            "CLOCK|wall_ns=2000000000000000000|mono_ns=5000000000",
            "RUNWAIT|6000000000|5500000000|42|0|500000000|test",
        ]
        out = io.StringIO()
        process_stream(self._make_stream(lines), run_id="ts-test", output=out)
        out.seek(0)
        records = [json.loads(l) for l in out if l.strip()]
        rw = [r for r in records if r.get("metric") == "runnable_wait"]
        self.assertEqual(len(rw), 1)
        # wall_ns = 2e18 + (6e9 - 5e9) = 2e18 + 1e9
        expected_wall = 2_000_000_000_000_000_000 + 1_000_000_000
        self.assertEqual(rw[0]["timestamp_wall_ns"], expected_wall)

    def test_new_stat_fields_parsed(self):
        from collector.cpu_scheduler import process_stream
        lines = [
            "STAT|matched|100",
            "STAT|runwait_matched|25",
            "STAT|map_overflow|3",
            "STAT|dup_wakeup|7",
            "STAT|exit_cleanup|2",
            "STAT|unmatched_switch|50",
        ]
        result = process_stream(self._make_stream(lines), run_id="stat-test")
        self.assertEqual(result.stats.kernel_matched_count, 100)
        self.assertEqual(result.stats.kernel_runwait_matched, 25)
        self.assertEqual(result.stats.map_overflow_count, 3)
        self.assertEqual(result.stats.duplicate_wakeup_count, 7)
        self.assertEqual(result.stats.exit_cleanup_count, 2)
        self.assertEqual(result.stats.unmatched_switch_count, 50)


class TestOnCpuCollector(unittest.TestCase):
    """E2: parse_oncpu_event and process_oncpu_stream."""

    def test_parse_valid(self):
        from collector.cpu_runtime import parse_oncpu_event
        ev = parse_oncpu_event("ONCPU|9000000000|12345|2|450000|python3")
        self.assertIsNotNone(ev)
        self.assertEqual(ev.end_ns, 9_000_000_000)
        self.assertEqual(ev.tid, 12345)
        self.assertEqual(ev.cpu, 2)
        self.assertEqual(ev.duration_ns, 450_000)
        self.assertEqual(ev.comm, "python3")

    def test_parse_wrong_prefix(self):
        from collector.cpu_runtime import parse_oncpu_event
        self.assertIsNone(parse_oncpu_event("SCHED|9000000000|12345|2|450000|python3"))

    def test_parse_negative_duration(self):
        from collector.cpu_runtime import parse_oncpu_event
        self.assertIsNone(parse_oncpu_event("ONCPU|9000000000|12345|2|-1|python3"))

    def test_parse_empty_comm(self):
        from collector.cpu_runtime import parse_oncpu_event
        self.assertIsNone(parse_oncpu_event("ONCPU|9000000000|12345|2|450000|"))

    def test_process_stream_basic(self):
        from collector.cpu_runtime import process_oncpu_stream
        lines = [
            "CLOCK|wall_ns=1700000000000000000|mono_ns=10000000000",
            "HEADER|end_ns|tid|cpu|duration_ns|comm",
            "ONCPU|10001000000|1111|0|500000|python3",
            "ONCPU|10002000000|1111|0|300000|python3",
            "ONCPU|10003000000|2222|1|800000|vllm_worker",
        ]
        result = process_oncpu_stream(io.StringIO("\n".join(lines)), run_id="e2-test")
        self.assertEqual(result.stats.event_count, 3)
        self.assertIsNotNone(result.kernel_clock_anchor)
        self.assertEqual(result.stats.distinct_tids_count, 2)
        self.assertEqual(result.stats.min_ns, 300_000)
        self.assertEqual(result.stats.max_ns, 800_000)

    def test_process_stream_jsonl_output(self):
        from collector.cpu_runtime import process_oncpu_stream
        lines = [
            "CLOCK|wall_ns=2000000000000000000|mono_ns=5000000000",
            "ONCPU|6000000000|42|0|1000000|target_proc",
        ]
        out = io.StringIO()
        process_oncpu_stream(
            io.StringIO("\n".join(lines)), run_id="out-test",
            output=out, target_tid=42
        )
        out.seek(0)
        records = [json.loads(l) for l in out if l.strip()]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["metric"], "on_cpu")
        self.assertEqual(records[0]["value"], 1_000_000)
        self.assertTrue(records[0]["is_target"])
        # wall_ns = 2e18 + (6e9 - 5e9) = 2e18 + 1e9
        self.assertEqual(records[0]["timestamp_wall_ns"],
                         2_000_000_000_000_000_000 + 1_000_000_000)

    def test_process_stream_target_tid_filter(self):
        from collector.cpu_runtime import process_oncpu_stream
        lines = [
            "ONCPU|1000|100|0|500|target_app",
            "ONCPU|2000|200|0|300|other_app",
        ]
        out = io.StringIO()
        process_oncpu_stream(
            io.StringIO("\n".join(lines)), run_id="filter-test",
            output=out, target_tid=100
        )
        out.seek(0)
        records = [json.loads(l) for l in out if l.strip()]
        target_records = [r for r in records if r["is_target"]]
        non_target = [r for r in records if not r["is_target"]]
        self.assertEqual(len(target_records), 1)
        self.assertEqual(len(non_target), 1)
        self.assertEqual(target_records[0]["tid"], 100)

    def test_stat_lines_parsed(self):
        from collector.cpu_runtime import process_oncpu_stream
        lines = [
            "STAT|matched|50",
            "STAT|map_overflow|2",
            "STAT|exit_cleanup|1",
            "STAT|unmatched_out|5",
        ]
        result = process_oncpu_stream(io.StringIO("\n".join(lines)), run_id="stat-test")
        self.assertEqual(result.stats.kernel_matched_count, 50)
        self.assertEqual(result.stats.map_overflow_count, 2)
        self.assertEqual(result.stats.exit_cleanup_count, 1)
        self.assertEqual(result.stats.unmatched_switch_count, 5)

    def test_percentiles_computed(self):
        from collector.cpu_runtime import process_oncpu_stream
        # 10 events with known durations
        lines = [f"ONCPU|{i*1000}|{i}|0|{i*100_000}|proc" for i in range(1, 11)]
        result = process_oncpu_stream(io.StringIO("\n".join(lines)), run_id="pct-test")
        self.assertIsNotNone(result.stats.p50_ns)
        self.assertIsNotNone(result.stats.p95_ns)
        self.assertIsNotNone(result.stats.p99_ns)
        self.assertLessEqual(result.stats.p50_ns, result.stats.p95_ns)


class TestMemReclaimCollector(unittest.TestCase):
    """E5: parse_reclaim_event and process_reclaim_stream."""

    def test_parse_valid(self):
        from collector.mem_reclaim import parse_reclaim_event
        ev = parse_reclaim_event("RECLAIM|9000000000|5678|12000000|64")
        self.assertIsNotNone(ev)
        self.assertEqual(ev.end_ns, 9_000_000_000)
        self.assertEqual(ev.tid, 5678)
        self.assertEqual(ev.duration_ns, 12_000_000)
        self.assertEqual(ev.nr_reclaimed, 64)

    def test_parse_wrong_prefix(self):
        from collector.mem_reclaim import parse_reclaim_event
        self.assertIsNone(parse_reclaim_event("SCHED|9000000000|5678|12000000|64"))

    def test_parse_too_few_fields(self):
        from collector.mem_reclaim import parse_reclaim_event
        self.assertIsNone(parse_reclaim_event("RECLAIM|9000000000|5678|12000000"))

    def test_parse_negative_duration(self):
        from collector.mem_reclaim import parse_reclaim_event
        self.assertIsNone(parse_reclaim_event("RECLAIM|9000000000|5678|-1|64"))

    def test_parse_zero_pages_reclaimed(self):
        from collector.mem_reclaim import parse_reclaim_event
        ev = parse_reclaim_event("RECLAIM|9000000000|5678|12000000|0")
        self.assertIsNotNone(ev)
        self.assertEqual(ev.nr_reclaimed, 0)

    def test_process_stream_basic(self):
        from collector.mem_reclaim import process_reclaim_stream
        lines = [
            "CLOCK|wall_ns=1700000000000000000|mono_ns=10000000000",
            "HEADER|end_ns|tid|duration_ns|nr_reclaimed",
            "RECLAIM|10001000000|1111|5000000|32",
            "RECLAIM|10002000000|1111|8000000|48",
            "RECLAIM|10003000000|2222|2000000|16",
        ]
        result = process_reclaim_stream(io.StringIO("\n".join(lines)), run_id="e5-test")
        self.assertEqual(result.stats.event_count, 3)
        self.assertEqual(result.total_pages_reclaimed, 96)
        self.assertIsNotNone(result.kernel_clock_anchor)
        self.assertEqual(result.stats.min_ns, 2_000_000)
        self.assertEqual(result.stats.max_ns, 8_000_000)

    def test_process_stream_jsonl_output_with_wall_ns(self):
        from collector.mem_reclaim import process_reclaim_stream
        lines = [
            "CLOCK|wall_ns=2000000000000000000|mono_ns=5000000000",
            "RECLAIM|6000000000|42|3000000|8",
        ]
        out = io.StringIO()
        process_reclaim_stream(
            io.StringIO("\n".join(lines)), run_id="e5-out",
            output=out, target_tid=42
        )
        out.seek(0)
        records = [json.loads(l) for l in out if l.strip()]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["metric"], "direct_reclaim")
        self.assertEqual(records[0]["value"], 3_000_000)
        self.assertEqual(records[0]["nr_reclaimed"], 8)
        self.assertTrue(records[0]["is_target"])
        expected_wall = 2_000_000_000_000_000_000 + 1_000_000_000
        self.assertEqual(records[0]["timestamp_wall_ns"], expected_wall)

    def test_unavailable_tracepoint_detected(self):
        from collector.mem_reclaim import process_reclaim_stream
        lines = [
            "No probes to attach",
        ]
        result = process_reclaim_stream(io.StringIO("\n".join(lines)), run_id="unavail")
        self.assertTrue(result.unavailable)
        self.assertIsNotNone(result.error)
        self.assertEqual(result.stats.event_count, 0)

    def test_stat_lines_parsed(self):
        from collector.mem_reclaim import process_reclaim_stream
        lines = [
            "STAT|matched|20",
            "STAT|unmatched_end|3",
            "STAT|exit_cleanup|1",
        ]
        result = process_reclaim_stream(io.StringIO("\n".join(lines)), run_id="stat5")
        self.assertEqual(result.stats.kernel_matched_count, 20)
        self.assertEqual(result.stats.unmatched_switch_count, 3)
        self.assertEqual(result.stats.exit_cleanup_count, 1)

    def test_dropped_events_reconciled(self):
        from collector.mem_reclaim import process_reclaim_stream
        # kernel says 20 matched, but we only receive 18 → 2 dropped
        lines = (
            ["RECLAIM|%d|1|1000000|4" % (i * 1000) for i in range(1, 19)] +
            ["STAT|matched|20"]
        )
        result = process_reclaim_stream(io.StringIO("\n".join(lines)), run_id="drop5")
        self.assertEqual(result.stats.dropped_events_count, 2)


class TestEbpfBatchBuilder(unittest.TestCase):
    """EbpfBatch uploader: build_ebpf_batch and split_into_batches."""

    def _clock(self):
        return {"wall_ns": 1_700_000_000_000_000_000, "monotonic_ns": 10_000_000_000}

    def _event(self, metric="wakeup_to_run_latency", ts=10_000_500_000, tid=1234):
        return {"metric": metric, "timestamp_ns": ts, "value": 500_000, "tid": tid}

    def test_build_basic_system_scope(self):
        from collector.ebpf_uploader import build_ebpf_batch
        batch = build_ebpf_batch(
            run_id="run-001",
            collector_id="dager-sched",
            scope="system",
            clock=self._clock(),
            window_start_ns=10_000_000_000,
            window_end_ns=10_010_000_000,
            events=[self._event()],
        )
        self.assertEqual(batch["run_id"], "run-001")
        self.assertEqual(batch["scope"], "system")
        self.assertEqual(len(batch["events"]), 1)
        self.assertIn("batch_id", batch)
        self.assertIsNotNone(batch["batch_id"])

    def test_target_threads_requires_tids(self):
        from collector.ebpf_uploader import build_ebpf_batch
        with self.assertRaises(ValueError):
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="target_threads",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=[], target_tids=None,
            )

    def test_comm_match_requires_target_comm(self):
        from collector.ebpf_uploader import build_ebpf_batch
        with self.assertRaises(ValueError):
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="comm_match",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=[], target_comm=None,
            )

    def test_invalid_scope_rejected(self):
        from collector.ebpf_uploader import build_ebpf_batch
        with self.assertRaises(ValueError):
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="unknown_scope",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=[],
            )

    def test_invalid_metric_rejected(self):
        from collector.ebpf_uploader import build_ebpf_batch
        bad_event = {"metric": "invalid_metric", "timestamp_ns": 10_000_500_000,
                     "value": 100, "tid": 1}
        with self.assertRaises(ValueError):
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="system",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=[bad_event],
            )

    def test_event_outside_window_rejected(self):
        from collector.ebpf_uploader import build_ebpf_batch
        # timestamp outside window
        bad_event = self._event(ts=5_000_000_000)  # before window start
        with self.assertRaises(ValueError):
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="system",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=[bad_event],
            )

    def test_target_threads_tid_not_in_set_rejected(self):
        from collector.ebpf_uploader import build_ebpf_batch
        event = self._event(tid=9999)  # not in target_tids
        with self.assertRaises(ValueError):
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="target_threads",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=[event], target_tids=[1234],
            )

    def test_too_many_events_rejected(self):
        from collector.ebpf_uploader import build_ebpf_batch, MAX_EVENTS_PER_BATCH
        events = [self._event() for _ in range(MAX_EVENTS_PER_BATCH + 1)]
        with self.assertRaises(ValueError):
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="system",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=events,
            )

    def test_split_into_batches_basic(self):
        from collector.ebpf_uploader import split_into_batches, MAX_EVENTS_PER_BATCH
        # 2001 events → 2 batches
        events = [self._event() for _ in range(MAX_EVENTS_PER_BATCH + 1)]
        batches = split_into_batches(
            run_id="r", collector_id="c", scope="system",
            clock=self._clock(),
            window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
            events=events,
            dropped_events=5,
        )
        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0]["events"]), MAX_EVENTS_PER_BATCH)
        self.assertEqual(len(batches[1]["events"]), 1)
        # dropped_events only in first batch
        self.assertEqual(batches[0]["dropped_events"], 5)
        self.assertIsNone(batches[1]["dropped_events"])
        # unique batch_ids
        self.assertNotEqual(batches[0]["batch_id"], batches[1]["batch_id"])

    def test_split_empty_events_gives_one_batch(self):
        from collector.ebpf_uploader import split_into_batches
        batches = split_into_batches(
            run_id="r", collector_id="c", scope="system",
            clock=self._clock(),
            window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
            events=[],
        )
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["events"], [])

    def test_all_valid_metrics_accepted(self):
        from collector.ebpf_uploader import build_ebpf_batch, VALID_METRICS
        for metric in VALID_METRICS:
            event = {"metric": metric, "timestamp_ns": 10_000_500_000,
                     "value": 100, "tid": 1}
            # Should not raise
            build_ebpf_batch(
                run_id="r", collector_id="c", scope="system",
                clock=self._clock(),
                window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
                events=[event],
            )

    def test_stable_batch_id_on_retry(self):
        from collector.ebpf_uploader import build_ebpf_batch
        fixed_id = str(uuid.uuid4())
        batch = build_ebpf_batch(
            run_id="r", collector_id="c", scope="system",
            clock=self._clock(),
            window_start_ns=10_000_000_000, window_end_ns=10_010_000_000,
            events=[], batch_id=fixed_id,
        )
        self.assertEqual(batch["batch_id"], fixed_id)

    def test_jsonl_to_events_filter(self):
        from collector.ebpf_uploader import jsonl_to_events
        import tempfile, os
        records = [
            {"metric": "on_cpu", "timestamp_ns": 10_000_500_000, "value": 1000, "tid": 1, "is_target": True},
            {"metric": "runnable_wait", "timestamp_ns": 10_000_600_000, "value": 500, "tid": 2, "is_target": False},
            {"metric": "on_cpu", "timestamp_ns": 10_000_700_000, "value": 800, "tid": 3, "is_target": False},
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
            fname = f.name
        try:
            events = jsonl_to_events(fname, metric_filter="on_cpu")
            self.assertEqual(len(events), 2)
            events_target = jsonl_to_events(fname, metric_filter="on_cpu", target_only=True)
            self.assertEqual(len(events_target), 1)
            self.assertEqual(events_target[0]["tid"], 1)
        finally:
            os.unlink(fname)


class TestP0Fixes(unittest.TestCase):
    """Unit tests for P0 fixes: ThreadTracker and streaming batch uploader."""

    def test_thread_tracker_basic_match(self):
        from collector.cpu_scheduler import ThreadTracker
        tracker = ThreadTracker(target_pids=[100], auto_expand=False)
        self.assertTrue(tracker.matches(100))
        self.assertFalse(tracker.matches(101))
        # Without /proc verification, synthetic target_pids remain TID filters (tgid is None)
        self.assertIsNone(tracker.resolve_tgid(100))
        self.assertIsNone(tracker.resolve_tgid(101))

    def test_thread_tracker_manual_subthread_resolution(self):
        from collector.cpu_scheduler import ThreadTracker
        tracker = ThreadTracker(target_pids=[500], auto_expand=False)
        # Manually register worker thread 501 under main TGID 500
        tracker._target_tids.add(501)
        tracker._tid_to_tgid[501] = 500

        self.assertTrue(tracker.matches(501))
        self.assertEqual(tracker.resolve_tgid(501), 500)
        self.assertIn(501, tracker.all_target_tids)

    def test_event_to_jsonl_populates_real_tgid(self):
        from collector.cpu_scheduler import SchedEvent, ThreadTracker, event_to_jsonl
        tracker = ThreadTracker(target_pids=[1234], auto_expand=False)
        tracker._target_tids.add(1235)  # worker thread
        tracker._tid_to_tgid[1235] = 1234  # TGID is 1234

        ev = SchedEvent(switch_ns=1000, wakeup_ns=900, tid=1235, cpu=0, latency_ns=100, comm="vllm_worker")
        line = event_to_jsonl(ev, run_id="tgid-test", tracker=tracker)
        rec = json.loads(line)
        # Real TGID is 1234, while tid is 1235
        self.assertEqual(rec["pid"], 1234)
        self.assertEqual(rec["tid"], 1235)
        self.assertTrue(rec["is_target"])

    def test_iter_jsonl_events_streaming(self):
        from collector.ebpf_uploader import iter_jsonl_events
        import tempfile, os
        records = [
            {"metric": "on_cpu", "timestamp_ns": 1000, "value": 50, "tid": 10},
            {"metric": "runnable_wait", "timestamp_ns": 1100, "value": 30, "tid": 11},
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
            fname = f.name
        try:
            evs = list(iter_jsonl_events(fname))
            self.assertEqual(len(evs), 2)
            self.assertEqual(evs[0]["metric"], "on_cpu")
            self.assertEqual(evs[1]["metric"], "runnable_wait")
        finally:
            os.unlink(fname)

    def test_stream_batches_from_jsonl(self):
        from collector.ebpf_uploader import stream_batches_from_jsonl
        import tempfile, os
        clock = {"wall_ns": 1_700_000_000_000_000_000, "monotonic_ns": 1000}
        # 5 events
        records = [
            {"metric": "on_cpu", "timestamp_ns": 1000 + i*10, "value": 50, "tid": 10}
            for i in range(5)
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
            fname = f.name
        try:
            batches = list(stream_batches_from_jsonl(
                fname,
                run_id="stream-test",
                collector_id="test-coll",
                scope="system",
                clock=clock,
                window_start_ns=900,
                window_end_ns=2000,
            ))
            self.assertEqual(len(batches), 1)
            self.assertEqual(len(batches[0]["events"]), 5)
            self.assertEqual(batches[0]["run_id"], "stream-test")
        finally:
            os.unlink(fname)


if __name__ == "__main__":
    unittest.main(verbosity=2)
