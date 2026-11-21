"""Thread actual CPU execution time collector — E2 (on_cpu).

Launches ``collector/ebpf/cpu_runtime.bt`` as a subprocess, parses the
pipe-delimited ONCPU event stream, and writes JSONL output compatible with
the platform's EbpfBatch ``metric: "on_cpu"`` format.

Each ONCPU event records one continuous CPU-execution slice:
  - start: when the thread was switched IN (sched_switch next_pid)
  - end: when the thread was switched OUT (sched_switch prev_pid)
  - value: duration in nanoseconds (the actual compute time)

This complements wakeup_to_run_latency (how long waiting for CPU) with
on_cpu (how long actually running on CPU), enabling full thread life-cycle
accounting: sleep → runnable → on_cpu → sleep/preempt → ...

Zero external dependencies beyond Python 3.12 standard library.

Author: 邓智文 (DAGER)
Project: TraceKV — collector/cpu_runtime.py
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, TextIO

from collector.cpu_scheduler import (
    ClockAnchor,
    CollectorStats,
    ThreadTracker,
    TimeAnchor,
    parse_clock,
    parse_stat,
    percentile,
    to_wall_ns,
    _find_bpftrace,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCRIPT_PATH = Path(__file__).resolve().parent / "ebpf" / "cpu_runtime.bt"
_ONCPU_PREFIX = "ONCPU|"
_STAT_PREFIX = "STAT|"
_CLOCK_PREFIX = "CLOCK|"
_HEADER_PREFIX = "HEADER|"

DEFAULT_COLLECTION_DURATION = 10.0
DEFAULT_MAX_SAMPLES = 100_000


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class OnCpuEvent:
    """One on-CPU execution slice observation (E2).

    Captured when a thread is switched off the CPU (sched_switch prev_pid).
    duration_ns = time from switch-in to switch-out for this TID.
    """
    end_ns: int        # monotonic ns when thread left CPU
    tid: int
    cpu: int
    duration_ns: int   # actual execution time (ns)
    comm: str


@dataclass
class CpuRuntimeResult:
    """Result of one cpu_runtime collection session."""
    run_id: str
    start_anchor: TimeAnchor
    target_tid: int | None = None
    target_comm: str | None = None
    end_anchor: TimeAnchor | None = None
    kernel_clock_anchor: ClockAnchor | None = None
    stats: CollectorStats = field(default_factory=CollectorStats)
    bpftrace_exit_code: int | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_oncpu_event(line: str) -> OnCpuEvent | None:
    """Parse a single ``ONCPU|…`` line into an :class:`OnCpuEvent`.

    Format::

        ONCPU|<end_ns>|<tid>|<cpu>|<duration_ns>|<comm>

    Returns ``None`` if the line is malformed or values are invalid.
    """
    parts = line.split("|")
    if len(parts) != 6 or parts[0] != "ONCPU":
        return None
    try:
        end_ns = int(parts[1])
        tid = int(parts[2])
        cpu_id = int(parts[3])
        duration_ns = int(parts[4])
        comm = parts[5].strip()
    except (ValueError, IndexError):
        return None
    if end_ns < 0 or tid < 0 or cpu_id < 0 or duration_ns < 0:
        return None
    if not comm:
        return None
    return OnCpuEvent(
        end_ns=end_ns,
        tid=tid,
        cpu=cpu_id,
        duration_ns=duration_ns,
        comm=comm,
    )


def oncpu_event_to_jsonl(
    event: OnCpuEvent,
    run_id: str,
    clock_anchor: ClockAnchor | None = None,
    target_tid: int | None = None,
    target_comm: str | None = None,
    tracker: ThreadTracker | None = None,
) -> str:
    """Serialise an :class:`OnCpuEvent` to a JSONL string.

    Produces the ``metric: "on_cpu"`` format expected by the platform's
    EbpfBatch interface.  ``timestamp_wall_ns`` is set when a
    :class:`ClockAnchor` is available; otherwise ``null``.  ``pid`` is populated
    with the real process TGID if resolved via :class:`ThreadTracker`.
    """
    wall_ns = to_wall_ns(event.end_ns, clock_anchor) if clock_anchor else None
    is_target = False
    pid_val = None

    if tracker is not None:
        is_target = tracker.matches(event.tid, event.comm)
        if is_target:
            pid_val = tracker.resolve_tgid(event.tid)
    elif target_tid is not None and event.tid == target_tid:
        is_target = True
        pid_val = target_tid
    elif target_comm and event.comm.startswith(target_comm[:15]):  # kernel truncates at 15
        is_target = True

    record = {
        "metric": "on_cpu",
        "run_id": run_id,
        "timestamp_ns": event.end_ns,
        "timestamp_wall_ns": wall_ns,
        "value": event.duration_ns,
        "pid": pid_val,
        "tid": event.tid,
        "cpu": event.cpu,
        "comm": event.comm,
        "is_target": is_target,
    }
    return json.dumps(record)


# ---------------------------------------------------------------------------
# Stream processor
# ---------------------------------------------------------------------------

def process_oncpu_stream(
    stream: IO[str],
    run_id: str,
    output: TextIO | None = None,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    target_tid: int | None = None,
    target_comm: str | None = None,
) -> CpuRuntimeResult:
    """Read lines from *stream*, parse ONCPU events, optionally write JSONL.

    Uses reservoir sampling for memory-bounded percentile computation.
    Supports dynamic worker thread discovery via :class:`ThreadTracker`.
    Returns a :class:`CpuRuntimeResult` with aggregate statistics.

    This function does **not** launch bpftrace — it only processes an
    already-open text stream, making it easy to test with synthetic data.
    """
    import random
    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    result = CpuRuntimeResult(
        run_id=run_id,
        start_anchor=anchor,
        target_tid=target_tid,
        target_comm=target_comm,
    )
    target_pids = [target_tid] if target_tid is not None else None
    tracker = ThreadTracker(target_pids=target_pids, target_comm=target_comm)
    reservoir: list[int] = []
    rng = random.Random(42)
    seen_tids: set[int] = set()
    comm_counts: dict[str, int] = {}

    for raw_line in stream:
        line = raw_line.rstrip("\n").rstrip("\r")
        if not line:
            continue
        if line.startswith(_HEADER_PREFIX) or line.startswith("Attaching "):
            continue

        # E0: kernel clock anchor
        if line.startswith(_CLOCK_PREFIX):
            anchor_parsed = parse_clock(line)
            if anchor_parsed is not None:
                result.kernel_clock_anchor = anchor_parsed
            continue

        if line.startswith(_STAT_PREFIX):
            parsed = parse_stat(line)
            if parsed is not None:
                name, value = parsed
                if name == "matched":
                    result.stats.kernel_matched_count = value
                elif name == "unmatched_out":
                    result.stats.unmatched_switch_count = value
                elif name == "exit_cleanup":
                    result.stats.exit_cleanup_count = value
                elif name == "map_overflow":
                    result.stats.map_overflow_count = value
            continue

        if line.startswith(_ONCPU_PREFIX):
            event = parse_oncpu_event(line)
            if event is None:
                result.stats.parser_error_count += 1
                continue

            result.stats.event_count += 1
            result.stats.sum_ns += event.duration_ns
            seen_tids.add(event.tid)
            comm_counts[event.comm] = comm_counts.get(event.comm, 0) + 1

            if result.stats.min_ns is None or event.duration_ns < result.stats.min_ns:
                result.stats.min_ns = event.duration_ns
            if result.stats.max_ns is None or event.duration_ns > result.stats.max_ns:
                result.stats.max_ns = event.duration_ns

            # Reservoir sampling for percentiles
            if len(reservoir) < max_samples:
                reservoir.append(event.duration_ns)
            else:
                j = rng.randint(0, result.stats.event_count - 1)
                if j < max_samples:
                    reservoir[j] = event.duration_ns

            if output is not None:
                output.write(
                    oncpu_event_to_jsonl(
                        event, run_id,
                        clock_anchor=result.kernel_clock_anchor,
                        target_tid=target_tid,
                        target_comm=target_comm,
                        tracker=tracker,
                    ) + "\n"
                )
            continue

        # Unknown line
        result.stats.parser_error_count += 1

    # Finalize stats
    result.stats.distinct_tids_count = len(seen_tids)
    sorted_comms = sorted(comm_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    result.stats.top_comms = dict(sorted_comms)

    if result.stats.event_count > 0:
        result.stats.mean_ns = result.stats.sum_ns / result.stats.event_count
        if reservoir:
            reservoir.sort()
            result.stats.p50_ns = percentile(reservoir, 50)
            result.stats.p95_ns = percentile(reservoir, 95)
            result.stats.p99_ns = percentile(reservoir, 99)

    if result.stats.kernel_matched_count is not None:
        received = result.stats.event_count + result.stats.parser_error_count
        result.stats.dropped_events_count = max(
            0, result.stats.kernel_matched_count - received
        )

    result.end_anchor = TimeAnchor(
        wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns()
    )
    return result


# ---------------------------------------------------------------------------
# bpftrace subprocess runner
# ---------------------------------------------------------------------------

def run_cpu_runtime_collector(
    run_id: str,
    duration_s: float = DEFAULT_COLLECTION_DURATION,
    out_path: Path | None = None,
    target_tid: int | None = None,
    target_comm: str | None = None,
    target_tgid: int | None = None,
    bpftrace_path: str | None = None,
) -> CpuRuntimeResult:
    """Launch cpu_runtime.bt and collect on-cpu events for *duration_s* seconds.

    Args:
        run_id:        Experiment identifier (propagated to every JSONL record).
        duration_s:    How many seconds to collect before sending SIGINT.
        out_path:      If set, write JSONL events to this file.
        target_tid:    Only emit ``is_target=True`` for this TID.
        target_comm:   Only emit ``is_target=True`` for threads with this name.
        target_tgid:   Target process TGID for in-kernel early filtering.
        bpftrace_path: Override bpftrace binary path.

    Returns:
        :class:`CpuRuntimeResult` with aggregate statistics.  On error
        (bpftrace not found, insufficient privileges) ``error`` is set and
        ``stats.event_count == 0``.
    """
    bt = bpftrace_path or _find_bpftrace()
    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    empty = CpuRuntimeResult(run_id=run_id, start_anchor=anchor,
                              target_tid=target_tid, target_comm=target_comm)

    if bt is None:
        empty.error = "bpftrace not found in PATH"
        return empty
    if not _SCRIPT_PATH.exists():
        empty.error = f"cpu_runtime.bt not found at {_SCRIPT_PATH}"
        return empty

    cmd = [bt]
    if target_tgid is not None:
        cmd.extend(["-D", f"TARGET_TGID={target_tgid}"])
    if target_tid is not None:
        cmd.extend(["-D", f"TARGET_TID={target_tid}"])
    cmd.append(str(_SCRIPT_PATH))

    out_file = open(out_path, "w", encoding="utf-8") if out_path else None
    result: CpuRuntimeResult | None = None

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        stderr_lines: list[str] = []

        def _drain_stderr():
            for line in proc.stderr:
                stderr_lines.append(line)

        stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
        stderr_thread.start()

        def _send_sigint():
            time.sleep(duration_s)
            try:
                proc.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass

        timer = threading.Thread(target=_send_sigint, daemon=True)
        timer.start()

        result = process_oncpu_stream(
            proc.stdout,
            run_id=run_id,
            output=out_file,
            target_tid=target_tid,
            target_comm=target_comm,
        )

        proc.wait(timeout=30)
        stderr_thread.join(timeout=5)
        result.bpftrace_exit_code = proc.returncode

    except Exception as exc:
        if result is None:
            result = empty
        result.error = str(exc)
    finally:
        if out_file:
            out_file.close()

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description="Collect per-thread on-CPU execution time via eBPF (E2)"
    )
    parser.add_argument("--run-id", default="run-oncpu", help="Experiment identifier")
    parser.add_argument("--duration", type=float, default=DEFAULT_COLLECTION_DURATION,
                        help="Collection duration in seconds")
    parser.add_argument("--target-tid", type=int, default=None,
                        help="Only track this TID (0 = all)")
    parser.add_argument("--target-comm", default=None,
                        help="Mark threads with this name as target")
    parser.add_argument("--out", default=None,
                        help="Output JSONL file path")
    args = parser.parse_args(argv)

    result = run_cpu_runtime_collector(
        run_id=args.run_id,
        duration_s=args.duration,
        out_path=Path(args.out) if args.out else None,
        target_tid=args.target_tid,
        target_comm=args.target_comm,
    )

    if result.error:
        print(f"[ERROR] {result.error}", file=sys.stderr)
        return 1

    s = result.stats
    print(json.dumps({
        "run_id": result.run_id,
        "event_count": s.event_count,
        "dropped": s.dropped_events_count,
        "map_overflow": s.map_overflow_count,
        "p50_us": round(s.p50_ns / 1000, 1) if s.p50_ns else None,
        "p95_us": round(s.p95_ns / 1000, 1) if s.p95_ns else None,
        "p99_us": round(s.p99_ns / 1000, 1) if s.p99_ns else None,
        "mean_us": round(s.mean_ns / 1000, 1) if s.mean_ns else None,
        "distinct_tids": s.distinct_tids_count,
        "clock_anchor_ok": result.kernel_clock_anchor is not None,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
