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
import random
import signal
import subprocess
import sys
import threading
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

DEFAULT_MAX_MAP_KEYS = 65536
DEFAULT_PERF_RB_PAGES = 256
DEFAULT_COLLECTION_DURATION = 10.0
DEFAULT_MAX_SAMPLES = 100_000


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
    kernel_matched_count: int | None = None
    dropped_events_count: int = 0
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
    distinct_tids_count: int = 0
    top_comms: dict[str, int] = field(default_factory=dict)


@dataclass
class CollectorResult:
    """Full result of a collection session."""
    run_id: str
    start_anchor: TimeAnchor
    end_anchor: TimeAnchor | None = None
    stats: CollectorStats = field(default_factory=CollectorStats)
    bpftrace_exit_code: int | None = None
    error: str | None = None
    kernel_source: str | None = None
    kernel_build: str | None = None
    scope: str = "system"
    target_pids: list[int] | None = None
    target_comm: str | None = None
    target_stats: CollectorStats | None = None


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

def event_to_jsonl(
    event: SchedEvent,
    run_id: str,
    target_pids: list[int] | None = None,
    target_comm: str | None = None,
) -> str:
    """Serialize one event to a JSON line (no trailing newline)."""
    pid_val: int | None = None
    is_target: bool | None = None

    if target_pids is not None:
        if event.tid in target_pids:
            is_target = True
            pid_val = event.tid
        else:
            is_target = False
    elif target_comm is not None:
        if event.comm == target_comm:
            is_target = True
            pid_val = event.tid
        else:
            is_target = False

    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "source": "ebpf_sched",
        "metric": "wakeup_to_run_latency",
        "clock": "monotonic",
        "timestamp_ns": event.switch_ns,
        "wakeup_timestamp_ns": event.wakeup_ns,
        "pid": pid_val,
        "tid": event.tid,
        "comm": event.comm,
        "cpu": event.cpu,
        "request_id": None,
        "value": event.latency_ns,
        "unit": "ns",
    }
    if is_target is not None:
        payload["is_target"] = is_target
    return json.dumps(payload, ensure_ascii=False)


def stats_to_dict(result: CollectorResult) -> dict:
    """Build a serializable summary dictionary conforming to v0.2 contract.

    Notes on metric semantics per docs/联调接口规范_v0.2.md:
    - scope: "system" for unconstrained tracing, "target_process" if filtered.
    - unmatched_switch_count: switch-ins without observed wakeup event (unpartnered).
    - dropped_events_count: reconciliation discrepancy between kernel matched count
      and userspace parsed events.
    """
    s = result.stats
    d: dict = {
        "schema_version": 1,
        "run_id": result.run_id,
        "source": "ebpf_sched",
        "scope": result.scope,
        "metric": "wakeup_to_run_latency_summary",
        "start_wall_ns": result.start_anchor.wall_ns,
        "start_monotonic_ns": result.start_anchor.monotonic_ns,
        "event_count": s.event_count,
        "kernel_matched_count": s.kernel_matched_count,
        "dropped_events_count": s.dropped_events_count,
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
        "distinct_tids_count": s.distinct_tids_count,
        "top_comms": s.top_comms,
        "kernel_source": result.kernel_source,
        "kernel_build": result.kernel_build,
        "bpftrace_exit_code": result.bpftrace_exit_code,
        "error": result.error,
    }
    if result.target_pids is not None:
        d["target_pids"] = result.target_pids
    if result.target_comm is not None:
        d["target_comm"] = result.target_comm
    if result.scope == "target_process" and result.target_stats is not None:
        ts = result.target_stats
        d["event_count"] = ts.event_count
        d["min_ns"] = ts.min_ns
        d["max_ns"] = ts.max_ns
        d["mean_ns"] = round(ts.mean_ns, 2) if ts.mean_ns is not None else None
        d["p50_ns"] = ts.p50_ns
        d["p95_ns"] = ts.p95_ns
        d["p99_ns"] = ts.p99_ns
        d["distinct_tids_count"] = ts.distinct_tids_count
        d["system_baseline"] = {
            "event_count": s.event_count,
            "p50_ns": s.p50_ns,
            "p95_ns": s.p95_ns,
            "p99_ns": s.p99_ns,
            "mean_ns": round(s.mean_ns, 2) if s.mean_ns is not None else None,
            "top_comms": s.top_comms,
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
    max_samples: int = DEFAULT_MAX_SAMPLES,
    random_seed: int | None = 42,
    target_pids: list[int] | None = None,
    target_comm: str | None = None,
) -> CollectorResult:
    """Read lines from *stream*, parse events, optionally write JSONL to *output*.

    Uses streaming accumulation for exact count/min/max/mean and reservoir
    sampling for memory-bounded percentile computation (at most *max_samples*
    stored in memory).

    Supports target process isolation (`target_pids` or `target_comm`):
    records per-process scheduler metrics alongside system-wide background baseline.

    Returns a :class:`CollectorResult` with computed statistics.  This
    function does **not** launch bpftrace — it only processes an already-open
    text stream, making it easy to test with synthetic data.
    """
    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    scope = "target_process" if (target_pids is not None or target_comm is not None) else "system"
    result = CollectorResult(
        run_id=run_id,
        start_anchor=anchor,
        scope=scope,
        target_pids=target_pids,
        target_comm=target_comm,
    )
    sample_reservoir: list[int] = []
    target_reservoir: list[int] = []
    target_stats = CollectorStats() if scope == "target_process" else None
    target_seen_tids: set[int] = set()

    rng = random.Random(random_seed) if random_seed is not None else random.Random()
    seen_tids: set[int] = set()
    comm_counts: dict[str, int] = {}
    target_pids_set = set(target_pids) if target_pids is not None else None

    for raw_line in stream:
        line = raw_line.rstrip("\n").rstrip("\r")
        if not line:
            continue
        if line.startswith(_HEADER_PREFIX) or line.startswith("Attaching "):
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
                elif name == "matched":
                    result.stats.kernel_matched_count = value
            continue

        if line.startswith(_EVENT_PREFIX):
            event = parse_event(line)
            if event is None:
                result.stats.parser_error_count += 1
                continue

            # Exact online streaming aggregations (system-wide)
            result.stats.event_count += 1
            result.stats.sum_ns += event.latency_ns
            seen_tids.add(event.tid)
            comm_counts[event.comm] = comm_counts.get(event.comm, 0) + 1
            if result.stats.min_ns is None or event.latency_ns < result.stats.min_ns:
                result.stats.min_ns = event.latency_ns
            if result.stats.max_ns is None or event.latency_ns > result.stats.max_ns:
                result.stats.max_ns = event.latency_ns

            # System reservoir sampling: bounded memory for percentiles
            if len(sample_reservoir) < max_samples:
                sample_reservoir.append(event.latency_ns)
            else:
                j = rng.randint(0, result.stats.event_count - 1)
                if j < max_samples:
                    sample_reservoir[j] = event.latency_ns

            # Target process filtering
            is_target = False
            if target_pids_set is not None and event.tid in target_pids_set:
                is_target = True
            elif target_comm is not None and event.comm == target_comm:
                is_target = True

            if is_target and target_stats is not None:
                target_stats.event_count += 1
                target_stats.sum_ns += event.latency_ns
                target_seen_tids.add(event.tid)
                if target_stats.min_ns is None or event.latency_ns < target_stats.min_ns:
                    target_stats.min_ns = event.latency_ns
                if target_stats.max_ns is None or event.latency_ns > target_stats.max_ns:
                    target_stats.max_ns = event.latency_ns

                if len(target_reservoir) < max_samples:
                    target_reservoir.append(event.latency_ns)
                else:
                    k = rng.randint(0, target_stats.event_count - 1)
                    if k < max_samples:
                        target_reservoir[k] = event.latency_ns

            if output is not None:
                output.write(
                    event_to_jsonl(
                        event,
                        run_id,
                        target_pids=target_pids,
                        target_comm=target_comm,
                    ) + "\n"
                )
            continue

        # Unknown line — could be bpftrace warning; count as parse error
        result.stats.parser_error_count += 1

    # Finalize summary statistics (system-wide)
    result.stats.distinct_tids_count = len(seen_tids)
    sorted_comms = sorted(comm_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    result.stats.top_comms = dict(sorted_comms)

    if result.stats.event_count > 0:
        result.stats.mean_ns = result.stats.sum_ns / result.stats.event_count
        if sample_reservoir:
            sample_reservoir.sort()
            result.stats.p50_ns = percentile(sample_reservoir, 50)
            result.stats.p95_ns = percentile(sample_reservoir, 95)
            result.stats.p99_ns = percentile(sample_reservoir, 99)

    # Finalize target summary statistics
    if target_stats is not None:
        target_stats.distinct_tids_count = len(target_seen_tids)
        if target_stats.event_count > 0:
            target_stats.mean_ns = target_stats.sum_ns / target_stats.event_count
            if target_reservoir:
                target_reservoir.sort()
                target_stats.p50_ns = percentile(target_reservoir, 50)
                target_stats.p95_ns = percentile(target_reservoir, 95)
                target_stats.p99_ns = percentile(target_reservoir, 99)
        result.target_stats = target_stats

    # Reconcile kernel matched count vs user-space received events
    if result.stats.kernel_matched_count is not None:
        received_total = result.stats.event_count + result.stats.parser_error_count
        result.stats.dropped_events_count = max(
            0, result.stats.kernel_matched_count - received_total
        )

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


def find_kernel_headers() -> Path | None:
    """Locate kernel headers directory containing include/linux/kconfig.h.

    Checks standard WSL extraction targets across full release string,
    base version, and common search directories.
    """
    import glob
    release = os.uname().release
    base_version = release.split("-")[0]  # e.g. '6.18.54.1' from '6.18.54.1-microsoft-standard-WSL2'

    search_roots = [
        Path.home(),
        Path("/home/dager"),
        Path("/tmp"),
    ]
    suffixes = [
        f"kernel-headers-{release}",
        f"kernel-headers-{base_version}",
        f"linux-headers-{release}",
        f"linux-headers-{base_version}",
    ]

    for root in search_roots:
        for suffix in suffixes:
            cand = root / suffix
            if (cand / "include" / "linux" / "kconfig.h").exists():
                return cand

    # Fallback: wildcard glob for any kernel-headers* directory with kconfig.h
    for pat in ["/home/dager/kernel-headers*", "/tmp/kernel-headers*", str(Path.home() / "kernel-headers*")]:
        for p in sorted(glob.glob(pat)):
            cand = Path(p)
            if (cand / "include" / "linux" / "kconfig.h").exists():
                return cand

    return None


def run_collector(
    run_id: str,
    output_path: Path | None = None,
    duration_seconds: float | None = DEFAULT_COLLECTION_DURATION,
    stop_event: threading.Event | None = None,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    bpftrace_bin: str | None = None,
    script_path: Path | None = None,
    max_map_keys: int = DEFAULT_MAX_MAP_KEYS,
    perf_rb_pages: int = DEFAULT_PERF_RB_PAGES,
    kernel_source: Path | str | None = None,
    kernel_build: Path | str | None = None,
    target_pid: int | None = None,
    target_pids: list[int] | None = None,
    target_comm: str | None = None,
) -> CollectorResult:
    """Launch bpftrace and collect scheduler latency events in real-time.

    Parameters
    ----------
    run_id:
        Experiment identifier written into every JSONL record.
    output_path:
        File path for the raw JSONL event log.  If ``None`` events are
        still parsed for statistics but not persisted.
    duration_seconds:
        Wall-clock duration in seconds.  Defaults to 10.0 seconds to prevent
        unbounded execution.  If set to None, collection proceeds until
        stop_event is set or the subprocess exits.
    stop_event:
        Optional threading.Event to signal collection stop cooperatively.
    max_samples:
        Maximum reservoir sample size for percentile computation.
    bpftrace_bin:
        Override the bpftrace executable path.
    script_path:
        Override the ``.bt`` script path.
    max_map_keys:
        Value for BPFTRACE_MAX_MAP_KEYS environment variable.
    perf_rb_pages:
        Value for BPFTRACE_PERF_RB_PAGES environment variable.
    kernel_source:
        Override BPFTRACE_KERNEL_SOURCE directory.
    kernel_build:
        Override BPFTRACE_KERNEL_BUILD directory.
    target_pid:
        Single target PID to filter (sets scope="target_process").
    target_pids:
        List of target PIDs to filter (sets scope="target_process").
    target_comm:
        Target process command name to filter (sets scope="target_process").

    Returns
    -------
    CollectorResult
        Includes statistics, time anchors and subprocess exit code.
    """
    if target_pid is not None and target_pids is None:
        target_pids = [target_pid]

    bt = bpftrace_bin or _find_bpftrace()
    if bt is None:
        anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
        return CollectorResult(
            run_id=run_id,
            start_anchor=anchor,
            end_anchor=anchor,
            error="bpftrace not found in PATH",
            scope="target_process" if (target_pids or target_comm) else "system",
            target_pids=target_pids,
            target_comm=target_comm,
        )

    script = str(script_path or _SCRIPT_PATH)
    cmd = [bt, script]

    env = os.environ.copy()
    env.setdefault("BPFTRACE_MAX_MAP_KEYS", str(max_map_keys))
    env.setdefault("BPFTRACE_PERF_RB_PAGES", str(perf_rb_pages))

    # Resolve kernel headers
    resolved_source = kernel_source or env.get("BPFTRACE_KERNEL_SOURCE")
    resolved_build = kernel_build or env.get("BPFTRACE_KERNEL_BUILD")

    if resolved_source is None or resolved_build is None:
        release = os.uname().release
        default_kconfig = Path(f"/lib/modules/{release}/build/include/linux/kconfig.h")
        if not default_kconfig.exists():
            detected = find_kernel_headers()
            if detected is not None:
                resolved_source = resolved_source or str(detected)
                resolved_build = resolved_build or str(detected)

    if resolved_source is not None:
        env["BPFTRACE_KERNEL_SOURCE"] = str(resolved_source)
    if resolved_build is not None:
        env["BPFTRACE_KERNEL_BUILD"] = str(resolved_build)

    anchor = TimeAnchor(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
    scope = "target_process" if (target_pids or target_comm) else "system"
    result = CollectorResult(
        run_id=run_id,
        start_anchor=anchor,
        kernel_source=env.get("BPFTRACE_KERNEL_SOURCE"),
        kernel_build=env.get("BPFTRACE_KERNEL_BUILD"),
        scope=scope,
        target_pids=target_pids,
        target_comm=target_comm,
    )
    output_file: TextIO | None = None

    if result.kernel_source:
        print(f"[collector] Using kernel headers from {result.kernel_source}", file=sys.stderr, flush=True)
    else:
        print("[collector] Using system default kernel headers", file=sys.stderr, flush=True)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
            env=env,
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

    # Read stderr asynchronously in a daemon thread so it never fills its pipe buffer
    stderr_chunks: list[str] = []
    def _drain_stderr():
        if proc.stderr is not None:
            try:
                for err_line in proc.stderr:
                    stderr_chunks.append(err_line)
            except Exception:
                pass

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    def _send_sigint():
        try:
            os.killpg(proc.pid, signal.SIGINT)
        except (ProcessLookupError, PermissionError):
            pass

    # Timer/Stopper thread to trigger SIGINT after duration_seconds or when stop_event is set
    stop_thread: threading.Thread | None = None
    if duration_seconds is not None or stop_event is not None:
        def _stopper():
            if duration_seconds is not None and stop_event is not None:
                stop_event.wait(timeout=duration_seconds)
            elif duration_seconds is not None:
                time.sleep(duration_seconds)
            elif stop_event is not None:
                stop_event.wait()

            _send_sigint()

            # Escalation safety: if process has not terminated after 10s grace, SIGKILL
            time.sleep(10.0)
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass

        stop_thread = threading.Thread(target=_stopper, daemon=True)
        stop_thread.start()

    try:
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_file = open(output_path, "w", encoding="utf-8")

        # Stream stdout in REAL-TIME on the main thread (avoids pipe deadlock!)
        assert proc.stdout is not None
        try:
            stream_result = process_stream(
                proc.stdout,
                run_id,
                output=output_file,
                max_samples=max_samples,
                target_pids=target_pids,
                target_comm=target_comm,
            )
        except KeyboardInterrupt:
            _send_sigint()
            stream_result = process_stream(
                proc.stdout,
                run_id,
                output=output_file,
                max_samples=max_samples,
                target_pids=target_pids,
                target_comm=target_comm,
            )

        # Merge results
        result.stats = stream_result.stats
        result.target_stats = stream_result.target_stats
        result.scope = stream_result.scope
        result.target_pids = stream_result.target_pids
        result.target_comm = stream_result.target_comm
        result.end_anchor = stream_result.end_anchor

        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            proc.wait(timeout=5)

        result.bpftrace_exit_code = proc.returncode

        stderr_thread.join(timeout=2)
        stderr_text = "".join(stderr_chunks).strip()

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
    parser.add_argument("--duration", type=float, default=DEFAULT_COLLECTION_DURATION,
                        help=f"Collection duration in seconds (default: {DEFAULT_COLLECTION_DURATION})")
    parser.add_argument("--max-samples", type=int, default=DEFAULT_MAX_SAMPLES,
                        help=f"Max reservoir samples for percentiles (default: {DEFAULT_MAX_SAMPLES})")
    parser.add_argument("--summary", type=Path, default=None,
                        help="Path for JSON summary output")
    parser.add_argument("--kernel-source", type=Path, default=None,
                        help="Override BPFTRACE_KERNEL_SOURCE directory")
    parser.add_argument("--kernel-build", type=Path, default=None,
                        help="Override BPFTRACE_KERNEL_BUILD directory")
    parser.add_argument("--pid", type=int, default=None,
                        help="Target process PID to isolate scheduler latency")
    parser.add_argument("--comm", type=str, default=None,
                        help="Target process command name (comm) to isolate scheduler latency")
    args = parser.parse_args(argv)

    print(f"Starting scheduler latency collector (run_id={args.run_id}, duration={args.duration}s)",
          file=sys.stderr, flush=True)

    result = run_collector(
        run_id=args.run_id,
        output_path=args.out,
        duration_seconds=args.duration,
        max_samples=args.max_samples,
        kernel_source=args.kernel_source,
        kernel_build=args.kernel_build,
        target_pid=args.pid,
        target_comm=args.comm,
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
