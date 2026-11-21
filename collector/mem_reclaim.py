"""Direct memory reclaim stall collector — E5 (direct_reclaim).

Launches ``collector/ebpf/mem_reclaim.bt`` as a subprocess, parses the
pipe-delimited RECLAIM event stream, and writes JSONL output compatible with
the platform's EbpfBatch ``metric: "direct_reclaim"`` format.

Direct reclaim occurs when the kernel's memory allocator cannot satisfy a
request from free page lists and must synchronously reclaim pages, stalling
the allocating thread until enough pages are freed.  For LLM inference
workloads this can appear as unexplained latency spikes during KV cache
allocation or model weight loading when the system is under memory pressure.

Each RECLAIM event records:
  - tid: the thread that triggered and was stalled by direct reclaim
  - duration_ns: how long (ns) the thread was blocked inside direct reclaim
  - nr_reclaimed: how many pages were reclaimed during this stall

Zero external dependencies beyond Python 3.12 standard library.

Author: 邓智文 (DAGER)
Project: TraceKV — collector/mem_reclaim.py
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

_SCRIPT_PATH = Path(__file__).resolve().parent / "ebpf" / "mem_reclaim.bt"
_RECLAIM_PREFIX = "RECLAIM|"
_STAT_PREFIX = "STAT|"
_CLOCK_PREFIX = "CLOCK|"
_HEADER_PREFIX = "HEADER|"

DEFAULT_COLLECTION_DURATION = 30.0   # longer default: reclaim events are rare
DEFAULT_MAX_SAMPLES = 50_000


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ReclaimEvent:
    """One direct memory reclaim stall observation (E5).

    Captured on vmscan:mm_vmscan_direct_reclaim_end.
    duration_ns = end_ts - begin_ts (begin on mm_vmscan_direct_reclaim_begin).
    """
    end_ns: int          # monotonic ns when reclaim exited
    tid: int             # thread that triggered reclaim (TGID unknown here)
    duration_ns: int     # stall duration in nanoseconds
    nr_reclaimed: int    # pages reclaimed (from tracepoint args)


@dataclass
class MemReclaimResult:
    """Result of one mem_reclaim collection session."""
    run_id: str
    start_anchor: TimeAnchor
    target_tid: int | None = None
    end_anchor: TimeAnchor | None = None
    kernel_clock_anchor: ClockAnchor | None = None
    stats: CollectorStats = field(default_factory=CollectorStats)
    total_pages_reclaimed: int = 0
    bpftrace_exit_code: int | None = None
    error: str | None = None
    unavailable: bool = False   # True if vmscan tracepoint not found in kernel


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_reclaim_event(line: str) -> ReclaimEvent | None:
    """Parse a single ``RECLAIM|…`` line into a :class:`ReclaimEvent`.

    Format::

        RECLAIM|<end_ns>|<tid>|<duration_ns>|<nr_reclaimed>

    Returns ``None`` if the line is malformed or values are invalid.
    """
    parts = line.split("|")
    if len(parts) != 5 or parts[0] != "RECLAIM":
        return None
    try:
        end_ns = int(parts[1])
        tid = int(parts[2])
        duration_ns = int(parts[3])
        nr_reclaimed = int(parts[4])
    except (ValueError, IndexError):
        return None
    if end_ns < 0 or tid < 0 or duration_ns < 0 or nr_reclaimed < 0:
        return None
    return ReclaimEvent(
        end_ns=end_ns,
        tid=tid,
        duration_ns=duration_ns,
        nr_reclaimed=nr_reclaimed,
    )


def reclaim_event_to_jsonl(
    event: ReclaimEvent,
    run_id: str,
    clock_anchor: ClockAnchor | None = None,
    target_tid: int | None = None,
) -> str:
    """Serialise a :class:`ReclaimEvent` to a JSONL string.

    Produces the ``metric: "direct_reclaim"`` format expected by the
    platform's EbpfBatch interface.
    """
    wall_ns = to_wall_ns(event.end_ns, clock_anchor) if clock_anchor else None
    is_target = target_tid is not None and event.tid == target_tid
    record = {
        "metric": "direct_reclaim",
        "run_id": run_id,
        "timestamp_ns": event.end_ns,
        "timestamp_wall_ns": wall_ns,
        "value": event.duration_ns,
        "tid": event.tid,
        "nr_reclaimed": event.nr_reclaimed,
        "is_target": is_target,
    }
    return json.dumps(record)


# ---------------------------------------------------------------------------
# Stream processor
# ---------------------------------------------------------------------------

def process_reclaim_stream(
    stream: IO[str],
    run_id: str,
    output: TextIO | None = None,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    target_tid: int | None = None,
) -> MemReclaimResult:
    """Read lines from *stream*, parse RECLAIM events, optionally write JSONL.

    Returns a :class:`MemReclaimResult` with aggregate statistics.

    This function does **not** launch bpftrace — it only processes an
    already-open text stream, making it easy to test with synthetic data.
    """
    import random
    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    result = MemReclaimResult(
        run_id=run_id,
        start_anchor=anchor,
        target_tid=target_tid,
    )
    reservoir: list[int] = []
    rng = random.Random(42)
    seen_tids: set[int] = set()

    for raw_line in stream:
        line = raw_line.rstrip("\n").rstrip("\r")
        if not line:
            continue
        if line.startswith(_HEADER_PREFIX) or line.startswith("Attaching "):
            continue

        # Detect unavailable tracepoint
        if "No probes to attach" in line or "tracepoint not found" in line.lower():
            result.unavailable = True
            result.error = (
                "vmscan tracepoints unavailable in this kernel. "
                "Try kprobe:try_to_free_pages as fallback."
            )
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
                elif name == "unmatched_end":
                    result.stats.unmatched_switch_count = value
                elif name == "exit_cleanup":
                    result.stats.exit_cleanup_count = value
            continue

        if line.startswith(_RECLAIM_PREFIX):
            event = parse_reclaim_event(line)
            if event is None:
                result.stats.parser_error_count += 1
                continue

            result.stats.event_count += 1
            result.stats.sum_ns += event.duration_ns
            result.total_pages_reclaimed += event.nr_reclaimed
            seen_tids.add(event.tid)

            if result.stats.min_ns is None or event.duration_ns < result.stats.min_ns:
                result.stats.min_ns = event.duration_ns
            if result.stats.max_ns is None or event.duration_ns > result.stats.max_ns:
                result.stats.max_ns = event.duration_ns

            if len(reservoir) < max_samples:
                reservoir.append(event.duration_ns)
            else:
                j = rng.randint(0, result.stats.event_count - 1)
                if j < max_samples:
                    reservoir[j] = event.duration_ns

            if output is not None:
                output.write(
                    reclaim_event_to_jsonl(
                        event, run_id,
                        clock_anchor=result.kernel_clock_anchor,
                        target_tid=target_tid,
                    ) + "\n"
                )
            continue

        # Unknown line
        result.stats.parser_error_count += 1

    # Finalize stats
    result.stats.distinct_tids_count = len(seen_tids)
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

def run_mem_reclaim_collector(
    run_id: str,
    duration_s: float = DEFAULT_COLLECTION_DURATION,
    out_path: Path | None = None,
    target_tid: int | None = None,
    bpftrace_path: str | None = None,
) -> MemReclaimResult:
    """Launch mem_reclaim.bt and collect direct-reclaim stall events.

    Args:
        run_id:        Experiment identifier (propagated to every JSONL record).
        duration_s:    How many seconds to collect before sending SIGINT.
        out_path:      If set, write JSONL events to this file.
        target_tid:    Mark events from this TID with ``is_target=True``.
        bpftrace_path: Override bpftrace binary path.

    Returns:
        :class:`MemReclaimResult`. When vmscan tracepoints are not available
        in the kernel, ``unavailable=True`` and ``error`` explains the
        fallback option.
    """
    bt = bpftrace_path or _find_bpftrace()
    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    empty = MemReclaimResult(run_id=run_id, start_anchor=anchor, target_tid=target_tid)

    if bt is None:
        empty.error = "bpftrace not found in PATH"
        return empty
    if not _SCRIPT_PATH.exists():
        empty.error = f"mem_reclaim.bt not found at {_SCRIPT_PATH}"
        return empty

    cmd = [bt, str(_SCRIPT_PATH)]
    if target_tid is not None:
        cmd = [bt, "-D", f"TARGET_TID={target_tid}", str(_SCRIPT_PATH)]

    out_file = open(out_path, "w", encoding="utf-8") if out_path else None
    result: MemReclaimResult | None = None

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

        result = process_reclaim_stream(
            proc.stdout,
            run_id=run_id,
            output=out_file,
            target_tid=target_tid,
        )

        proc.wait(timeout=30)
        stderr_thread.join(timeout=5)
        result.bpftrace_exit_code = proc.returncode

        # Check stderr for "No probes to attach" if not already detected
        stderr_text = "".join(stderr_lines)
        if not result.unavailable and "No probes to attach" in stderr_text:
            result.unavailable = True
            result.error = (
                "vmscan tracepoints not available in this kernel configuration. "
                "Verify with: sudo bpftrace -l 'tracepoint:vmscan:*'"
            )

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
        description="Collect direct memory reclaim stall events via eBPF (E5)"
    )
    parser.add_argument("--run-id", default="run-reclaim", help="Experiment identifier")
    parser.add_argument("--duration", type=float, default=DEFAULT_COLLECTION_DURATION,
                        help="Collection duration in seconds")
    parser.add_argument("--target-tid", type=int, default=None,
                        help="Mark this TID as target in output")
    parser.add_argument("--out", default=None,
                        help="Output JSONL file path")
    args = parser.parse_args(argv)

    result = run_mem_reclaim_collector(
        run_id=args.run_id,
        duration_s=args.duration,
        out_path=Path(args.out) if args.out else None,
        target_tid=args.target_tid,
    )

    if result.unavailable:
        print(f"[WARN] {result.error}", file=sys.stderr)
        # Not a hard failure — still print empty stats
    elif result.error:
        print(f"[ERROR] {result.error}", file=sys.stderr)
        return 1

    s = result.stats
    print(json.dumps({
        "run_id": result.run_id,
        "event_count": s.event_count,
        "total_pages_reclaimed": result.total_pages_reclaimed,
        "dropped": s.dropped_events_count,
        "p50_ms": round(s.p50_ns / 1e6, 2) if s.p50_ns else None,
        "p95_ms": round(s.p95_ns / 1e6, 2) if s.p95_ns else None,
        "p99_ms": round(s.p99_ns / 1e6, 2) if s.p99_ns else None,
        "mean_ms": round(s.mean_ns / 1e6, 2) if s.mean_ns else None,
        "distinct_tids": s.distinct_tids_count,
        "clock_anchor_ok": result.kernel_clock_anchor is not None,
        "unavailable": result.unavailable,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
