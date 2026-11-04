"""CPU scheduler wakeup-to-run latency collector using bpftrace.

Launches ``collector/ebpf/sched_latency.bt`` as a subprocess, parses the
pipe-delimited event stream, computes percentile statistics, and writes
JSONL output.  Designed to run without third-party dependencies beyond the
Python 3.12 standard library.

Requires ``bpftrace`` and sufficient privileges (typically root) to attach
to kernel tracepoints.  When privileges are insufficient the collector
exits cleanly with an informative message rather than crashing.

Author: 邓智文 (DAGER)
"""

from __future__ import annotations

import json
import math
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, TextIO


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCRIPT_PATH = Path(__file__).resolve().parent / "ebpf" / "sched_latency.bt"

_EVENT_PREFIX = "SCHED|"
_STAT_PREFIX = "STAT|"
_HEADER_PREFIX = "HEADER|"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SchedEvent:
    """One matched wakeup→switch latency observation."""
    switch_ns: int
    wakeup_ns: int
    tid: int
    cpu: int
    latency_ns: int
    comm: str


@dataclass
class TimeAnchor:
    """Pair of wall-clock and monotonic readings for time calibration."""
    wall_ns: int          # time.time_ns()
    monotonic_ns: int     # time.monotonic_ns()


@dataclass
class CollectorStats:
    """Aggregate statistics for one collection run."""
    event_count: int = 0
    unmatched_switch_count: int = 0
    duplicate_wakeup_count: int = 0
    exit_cleanup_count: int = 0
    parser_error_count: int = 0
    min_ns: int | None = None
    max_ns: int | None = None
    sum_ns: int = 0
    p50_ns: int | None = None
    p95_ns: int | None = None
    p99_ns: int | None = None
    mean_ns: float | None = None


@dataclass
class CollectorResult:
    """Full result of a collection session."""
    run_id: str
    start_anchor: TimeAnchor
    end_anchor: TimeAnchor | None = None
    stats: CollectorStats = field(default_factory=CollectorStats)
    bpftrace_exit_code: int | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_event(line: str) -> SchedEvent | None:
    """Parse a single ``SCHED|…`` line into a :class:`SchedEvent`.

    Returns ``None`` if the line is malformed or contains invalid values.
    Negative latencies are rejected.
    """
    parts = line.split("|")
    if len(parts) != 7 or parts[0] != "SCHED":
        return None
    try:
        switch_ns = int(parts[1])
        wakeup_ns = int(parts[2])
        tid = int(parts[3])
        cpu_id = int(parts[4])
        latency_ns = int(parts[5])
        comm = parts[6]
    except (ValueError, IndexError):
        return None

    # Sanity checks
    if latency_ns < 0:
        return None
    if tid < 0:
        return None
    if switch_ns < 0 or wakeup_ns < 0:
        return None
    if cpu_id < 0:
        return None
    if not comm:
        return None

    return SchedEvent(
        switch_ns=switch_ns,
        wakeup_ns=wakeup_ns,
        tid=tid,
        cpu=cpu_id,
        latency_ns=latency_ns,
        comm=comm,
    )


def parse_stat(line: str) -> tuple[str, int] | None:
    """Parse a ``STAT|name|value`` line.  Returns ``(name, value)``."""
    parts = line.split("|")
    if len(parts) != 3 or parts[0] != "STAT":
        return None
    try:
        return parts[1], int(parts[2])
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Percentile computation
# ---------------------------------------------------------------------------

def percentile(sorted_values: list[int], p: float) -> int:
    """Nearest-rank percentile on a *pre-sorted* list of integers.

    Uses the *exclusive* definition: ``rank = ceil(p/100 * N)`` where
    ``N = len(sorted_values)``.  For ``p == 0`` returns the minimum.

    This is documented in ``docs/collector-scheduler.md`` so that results
    are reproducible.
    """
    if not sorted_values:
        raise ValueError("percentile requires a non-empty sequence")
    if p <= 0:
        return sorted_values[0]
    n = len(sorted_values)
    rank = math.ceil(p / 100.0 * n)
    return sorted_values[min(rank, n) - 1]


def compute_stats(latencies_ns: list[int]) -> CollectorStats:
    """Compute summary statistics from raw latency values in nanoseconds.

    The input list is sorted in-place for percentile computation.
    """
    stats = CollectorStats()
    stats.event_count = len(latencies_ns)
    if not latencies_ns:
        return stats
    latencies_ns.sort()
    stats.min_ns = latencies_ns[0]
    stats.max_ns = latencies_ns[-1]
    stats.sum_ns = sum(latencies_ns)
    stats.mean_ns = stats.sum_ns / len(latencies_ns)
    stats.p50_ns = percentile(latencies_ns, 50)
    stats.p95_ns = percentile(latencies_ns, 95)
    stats.p99_ns = percentile(latencies_ns, 99)
    return stats


# ---------------------------------------------------------------------------
# JSONL formatting
# ---------------------------------------------------------------------------

def event_to_jsonl(event: SchedEvent, run_id: str) -> str:
    """Serialize one event to a JSON line (no trailing newline)."""
    return json.dumps({
        "schema_version": 1,
        "run_id": run_id,
        "source": "ebpf_sched",
        "metric": "wakeup_to_run_latency",
        "clock": "monotonic",
        "timestamp_ns": event.switch_ns,
        "wakeup_timestamp_ns": event.wakeup_ns,
        "pid": None,
        "tid": event.tid,
        "comm": event.comm,
        "cpu": event.cpu,
        "request_id": None,
        "value": event.latency_ns,
        "unit": "ns",
    }, ensure_ascii=False)


def stats_to_dict(result: CollectorResult) -> dict:
    """Build a serializable summary dictionary."""
    s = result.stats
    d: dict = {
        "schema_version": 1,
        "run_id": result.run_id,
        "source": "ebpf_sched",
        "metric": "wakeup_to_run_latency_summary",
        "start_wall_ns": result.start_anchor.wall_ns,
        "start_monotonic_ns": result.start_anchor.monotonic_ns,
        "event_count": s.event_count,
        "unmatched_switch_count": s.unmatched_switch_count,
        "duplicate_wakeup_count": s.duplicate_wakeup_count,
        "exit_cleanup_count": s.exit_cleanup_count,
        "parser_error_count": s.parser_error_count,
        "min_ns": s.min_ns,
        "max_ns": s.max_ns,
        "mean_ns": round(s.mean_ns, 2) if s.mean_ns is not None else None,
        "p50_ns": s.p50_ns,
        "p95_ns": s.p95_ns,
        "p99_ns": s.p99_ns,
        "bpftrace_exit_code": result.bpftrace_exit_code,
        "error": result.error,
    }
    if result.end_anchor is not None:
        d["end_wall_ns"] = result.end_anchor.wall_ns
        d["end_monotonic_ns"] = result.end_anchor.monotonic_ns
    return d


# ---------------------------------------------------------------------------
# Stream processor (can be used with any text stream, not just bpftrace)
# ---------------------------------------------------------------------------

def process_stream(
    stream: IO[str],
    run_id: str,
    output: TextIO | None = None,
) -> CollectorResult:
    """Read lines from *stream*, parse events, optionally write JSONL to *output*.

    Returns a :class:`CollectorResult` with computed statistics.  This
    function does **not** launch bpftrace — it only processes an already-open
    text stream, making it easy to test with synthetic data.
    """
    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    result = CollectorResult(run_id=run_id, start_anchor=anchor)
    latencies: list[int] = []

    for raw_line in stream:
        line = raw_line.rstrip("\n").rstrip("\r")
        if not line:
            continue
        if line.startswith(_HEADER_PREFIX):
            continue

        if line.startswith(_STAT_PREFIX):
            parsed = parse_stat(line)
            if parsed is not None:
                name, value = parsed
                if name == "unmatched_switch":
                    result.stats.unmatched_switch_count = value
                elif name == "dup_wakeup":
                    result.stats.duplicate_wakeup_count = value
                elif name == "exit_cleanup":
                    result.stats.exit_cleanup_count = value
                # matched count is implicit from event_count
            continue

        if line.startswith(_EVENT_PREFIX):
            event = parse_event(line)
            if event is None:
                result.stats.parser_error_count += 1
                continue
            latencies.append(event.latency_ns)
            if output is not None:
                output.write(event_to_jsonl(event, run_id) + "\n")
            continue

        # Unknown line — could be bpftrace warning; count as parse error
        result.stats.parser_error_count += 1

    computed = compute_stats(latencies)
    # Preserve kernel-reported counters already stored
    computed.unmatched_switch_count = result.stats.unmatched_switch_count
    computed.duplicate_wakeup_count = result.stats.duplicate_wakeup_count
    computed.exit_cleanup_count = result.stats.exit_cleanup_count
    computed.parser_error_count = result.stats.parser_error_count
    result.stats = computed

    result.end_anchor = TimeAnchor(
        wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
    )
    return result


# ---------------------------------------------------------------------------
# bpftrace subprocess management
# ---------------------------------------------------------------------------

def _find_bpftrace() -> str | None:
    """Locate the bpftrace binary."""
    from shutil import which
    return which("bpftrace")


def run_collector(
    run_id: str,
    output_path: Path | None = None,
    duration_seconds: float | None = None,
    bpftrace_bin: str | None = None,
    script_path: Path | None = None,
) -> CollectorResult:
    """Launch bpftrace and collect scheduler latency events.

    Parameters
    ----------
    run_id:
        Experiment identifier written into every JSONL record.
    output_path:
        File path for the raw JSONL event log.  If ``None`` events are
        still parsed for statistics but not persisted.
    duration_seconds:
        Optional wall-clock limit.  After this many seconds ``SIGINT`` is
        sent to bpftrace so it flushes its ``END`` block.
    bpftrace_bin:
        Override the bpftrace executable path.
    script_path:
        Override the ``.bt`` script path.

    Returns
    -------
    CollectorResult
        Includes statistics, time anchors and subprocess exit code.
    """
    bt = bpftrace_bin or _find_bpftrace()
    if bt is None:
        anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
        return CollectorResult(
            run_id=run_id,
            start_anchor=anchor,
            end_anchor=anchor,
            error="bpftrace not found in PATH",
        )

    script = str(script_path or _SCRIPT_PATH)
    cmd = [bt, script]

    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    result = CollectorResult(run_id=run_id, start_anchor=anchor)
    output_file: TextIO | None = None

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except PermissionError as exc:
        result.end_anchor = TimeAnchor(
            wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
        )
        result.error = f"Permission denied launching bpftrace: {exc}"
        return result
    except OSError as exc:
        result.end_anchor = TimeAnchor(
            wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
        )
        result.error = f"Failed to launch bpftrace: {exc}"
        return result

    try:
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_file = open(output_path, "w", encoding="utf-8")

        if duration_seconds is not None:
            # Let bpftrace run for the specified duration then signal it
            time.sleep(duration_seconds)
            try:
                os.killpg(proc.pid, signal.SIGINT)
            except ProcessLookupError:
                pass

        assert proc.stdout is not None
        stream_result = process_stream(proc.stdout, run_id, output_file)

        # Merge
        result.stats = stream_result.stats
        result.end_anchor = stream_result.end_anchor

        proc.wait(timeout=10)
        result.bpftrace_exit_code = proc.returncode

        stderr_text = ""
        if proc.stderr:
            stderr_text = proc.stderr.read().strip()
            proc.stderr.close()
        if proc.returncode != 0 and stderr_text:
            result.error = f"bpftrace exited {proc.returncode}: {stderr_text}"
        elif proc.returncode != 0:
            result.error = f"bpftrace exited with code {proc.returncode}"

    except Exception as exc:
        result.error = str(exc)
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        proc.wait(timeout=5)
        result.bpftrace_exit_code = proc.returncode
    finally:
        if output_file is not None:
            output_file.close()
        # Ensure subprocess pipes are closed to avoid ResourceWarning
        if proc.stdout and not proc.stdout.closed:
            proc.stdout.close()
        if proc.stderr and not proc.stderr.closed:
            proc.stderr.close()
        result.end_anchor = result.end_anchor or TimeAnchor(
            wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
        )

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """Simple CLI wrapper for manual testing."""
    import argparse

    parser = argparse.ArgumentParser(
        description="CPU scheduler wakeup-to-run latency collector",
    )
    parser.add_argument("--run-id", default="manual-001",
                        help="Experiment run identifier")
    parser.add_argument("--out", type=Path, default=None,
                        help="Path for JSONL event output")
    parser.add_argument("--duration", type=float, default=None,
                        help="Collection duration in seconds")
    parser.add_argument("--summary", type=Path, default=None,
                        help="Path for JSON summary output")
    args = parser.parse_args(argv)

    print(f"Starting scheduler latency collector (run_id={args.run_id})",
          file=sys.stderr, flush=True)

    result = run_collector(
        run_id=args.run_id,
        output_path=args.out,
        duration_seconds=args.duration,
    )

    summary = stats_to_dict(result)
    if args.summary is not None:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        with open(args.summary, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if result.error is None else 2


if __name__ == "__main__":
    raise SystemExit(main())
