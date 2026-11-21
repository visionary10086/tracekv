"""CPU scheduler wakeup-to-run latency collector using bpftrace.

Launches ``collector/ebpf/sched_latency.bt`` as a subprocess, parses the
pipe-delimited event stream, computes percentile statistics, and writes
JSONL output.  Designed to run without third-party dependencies beyond the
Python 3.12 standard library.

Requires ``bpftrace`` and sufficient privileges (typically root) to attach
to kernel tracepoints.  When privileges are insufficient the collector
exits cleanly with an informative message rather than crashing.

E0 additions (2026-11-21):
  - ClockAnchor: kernel realtime+monotonic clock pair from bpftrace CLOCK| line
  - parse_clock(): parse CLOCK|wall_ns=...|mono_ns=... emitted by BEGIN block
  - to_wall_ns(): convert kernel monotonic timestamp → UTC wall nanoseconds
  - RunwaitEvent / parse_runwait(): Preempt-to-Run (RUNWAIT) latency path (E1)
  - check_ebpf_environment(): bpftrace version, tracefs mount, privilege check
  - map_overflow_count in CollectorStats

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
_ENV_CHECK_SCRIPT = Path(__file__).resolve().parent / "ebpf" / "env_check.bt"

_EVENT_PREFIX = "SCHED|"
_RUNWAIT_PREFIX = "RUNWAIT|"
_ONCPU_PREFIX = "ONCPU|"
_CLOCK_PREFIX = "CLOCK|"
_STAT_PREFIX = "STAT|"
_HEADER_PREFIX = "HEADER|"

_MIN_BPFTRACE_VERSION = (0, 20, 0)

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
class RunwaitEvent:
    """One Preempt-to-Run latency observation (E1).

    Captured when a task was preempted (prev_state==0 in sched_switch) and
    subsequently rescheduled.  Covers the runnable-queue wait time that
    wakeup_to_run_latency does NOT measure.
    """
    switch_ns: int     # monotonic ns when task re-enters CPU
    preempt_ns: int    # monotonic ns when task was preempted off CPU
    tid: int
    cpu: int
    wait_ns: int       # = switch_ns - preempt_ns
    comm: str


@dataclass
class ClockAnchor:
    """Kernel-emitted clock pair for monotonic→wall conversion (E0).

    Extracted from the ``CLOCK|wall_ns=...|mono_ns=...`` line that
    sched_latency.bt emits in its BEGIN block.  wall_ns is CLOCK_REALTIME
    (nanoseconds since epoch); mono_ns is CLOCK_MONOTONIC at the same
    instant, as seen by the kernel's bpftrace nsecs(realtime/monotonic).
    """
    wall_ns: int        # CLOCK_REALTIME  (kernel, nanoseconds)
    monotonic_ns: int   # CLOCK_MONOTONIC (kernel, nanoseconds)


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
    map_overflow_count: int = 0          # E0: BPF map capacity overflow events
    runwait_event_count: int = 0         # E1: preempt-to-run events parsed
    kernel_runwait_matched: int | None = None  # E1: STAT|runwait_matched from kernel
    oncpu_event_count: int = 0           # E2: on-cpu execution slices parsed
    kernel_oncpu_matched: int | None = None    # E2: STAT|oncpu_matched from kernel
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
    kernel_clock_anchor: ClockAnchor | None = None  # E0: from CLOCK| line
    target_tids: list[int] | None = None            # Dynamically discovered or observed TIDs


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


def parse_clock(line: str) -> ClockAnchor | None:
    """Parse a ``CLOCK|wall_ns=<n>|mono_ns=<n>`` line emitted by bpftrace BEGIN.

    Returns a :class:`ClockAnchor` on success, ``None`` on malformed input.

    Example input::

        CLOCK|wall_ns=1700000000000000000|mono_ns=12345678901234

    The two timestamps are captured by the kernel at the same instant,
    providing an anchor for converting subsequent CLOCK_MONOTONIC event
    timestamps to UTC wall-clock nanoseconds via :func:`to_wall_ns`.
    """
    if not line.startswith("CLOCK|"):
        return None
    parts = line.split("|")
    if len(parts) != 3:
        return None
    try:
        wall_part = parts[1]   # "wall_ns=<n>"
        mono_part = parts[2]   # "mono_ns=<n>"
        if not wall_part.startswith("wall_ns=") or not mono_part.startswith("mono_ns="):
            return None
        wall_ns = int(wall_part[len("wall_ns="):])
        mono_ns = int(mono_part[len("mono_ns="):])
    except (ValueError, IndexError):
        return None
    if wall_ns <= 0 or mono_ns <= 0:
        return None
    return ClockAnchor(wall_ns=wall_ns, monotonic_ns=mono_ns)


def to_wall_ns(mono_ns: int, anchor: ClockAnchor) -> int:
    """Convert a kernel CLOCK_MONOTONIC timestamp to UTC wall-clock nanoseconds.

    Uses the kernel-emitted :class:`ClockAnchor` (wall + monotonic captured
    simultaneously) to shift the monotonic reading into absolute time::

        wall_ns = anchor.wall_ns + (mono_ns - anchor.monotonic_ns)

    This provides sub-microsecond accuracy for event timestamps when the
    anchor is captured close to the start of collection.

    Args:
        mono_ns:  Kernel monotonic nanoseconds from a bpftrace event.
        anchor:   :class:`ClockAnchor` from :func:`parse_clock`.

    Returns:
        UTC nanoseconds since the Unix epoch.
    """
    return anchor.wall_ns + (mono_ns - anchor.monotonic_ns)


def parse_runwait(line: str) -> RunwaitEvent | None:
    """Parse a ``RUNWAIT|…`` line into a :class:`RunwaitEvent` (E1).

    Format::

        RUNWAIT|<switch_ns>|<preempt_ns>|<tid>|<cpu>|<wait_ns>|<comm>

    Returns ``None`` if the line is malformed or values are invalid.
    """
    parts = line.split("|")
    if len(parts) != 7 or parts[0] != "RUNWAIT":
        return None
    try:
        switch_ns = int(parts[1])
        preempt_ns = int(parts[2])
        tid = int(parts[3])
        cpu_id = int(parts[4])
        wait_ns = int(parts[5])
        comm = parts[6].strip()
    except (ValueError, IndexError):
        return None
    if wait_ns < 0 or tid < 0 or switch_ns < 0 or preempt_ns < 0 or cpu_id < 0:
        return None
    if not comm:
        return None
    return RunwaitEvent(
        switch_ns=switch_ns,
        preempt_ns=preempt_ns,
        tid=tid,
        cpu=cpu_id,
        wait_ns=wait_ns,
        comm=comm,
    )


def check_ebpf_environment(bpftrace_path: str | None = None) -> dict:
    """Check whether the eBPF collection environment is functional (E0).

    Performs three independent checks and returns a summary dict:

    * ``bpftrace_ok``: bpftrace binary found and responds to ``--version``.
    * ``bpftrace_version``: detected version tuple, e.g. ``(0, 20, 4)``.
    * ``version_ok``: ``True`` if version >= ``(0, 20, 0)``.
    * ``tracefs_ok``: ``/sys/kernel/debug/tracing`` or
      ``/sys/kernel/tracing`` is readable (tracefs mounted).
    * ``privilege_ok``: running as root (``euid == 0``) or CAP_BPF present.
    * ``liveness_ok``: env_check.bt executed successfully (bpftrace can
      attach to kernel).  Only attempted when bpftrace_ok is True.
    * ``details``: human-readable summary string.
    * ``errors``: list of error strings for failed checks.

    This function never raises; failures are captured in the return value.
    """
    result: dict = {
        "bpftrace_ok": False,
        "bpftrace_version": None,
        "version_ok": False,
        "tracefs_ok": False,
        "privilege_ok": False,
        "liveness_ok": False,
        "details": "",
        "errors": [],
    }

    # --- bpftrace binary check ---
    bt = bpftrace_path or _find_bpftrace()
    if bt is None:
        result["errors"].append("bpftrace not found in PATH")
    else:
        try:
            out = subprocess.run(
                [bt, "--version"], capture_output=True, text=True, timeout=5
            )
            version_str = (out.stdout + out.stderr).strip()
            # Parse "bpftrace v0.20.4" or "bpftrace 0.20.4"
            import re as _re
            m = _re.search(r"(\d+)\.(\d+)\.(\d+)", version_str)
            if m:
                ver = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
                result["bpftrace_ok"] = True
                result["bpftrace_version"] = ver
                result["version_ok"] = ver >= _MIN_BPFTRACE_VERSION
                if not result["version_ok"]:
                    result["errors"].append(
                        f"bpftrace {ver} < required {_MIN_BPFTRACE_VERSION}"
                    )
            else:
                result["errors"].append(f"Cannot parse bpftrace version: {version_str!r}")
        except (OSError, subprocess.TimeoutExpired) as exc:
            result["errors"].append(f"bpftrace --version failed: {exc}")

    # --- tracefs mount check ---
    for tracefs_path in ("/sys/kernel/debug/tracing", "/sys/kernel/tracing"):
        if Path(tracefs_path).is_dir() and os.access(tracefs_path, os.R_OK):
            result["tracefs_ok"] = True
            break
    if not result["tracefs_ok"]:
        result["errors"].append(
            "tracefs not readable at /sys/kernel/debug/tracing or /sys/kernel/tracing"
        )

    # --- privilege check ---
    if os.geteuid() == 0:
        result["privilege_ok"] = True
    else:
        # Check for CAP_BPF via /proc/self/status CapEff
        try:
            status = Path("/proc/self/status").read_text(encoding="utf-8")
            for line in status.splitlines():
                if line.startswith("CapEff:"):
                    cap_eff = int(line.split(":")[1].strip(), 16)
                    # CAP_BPF = bit 39 (kernel 5.8+)
                    if cap_eff & (1 << 39):
                        result["privilege_ok"] = True
                    break
        except (OSError, ValueError):
            pass
    if not result["privilege_ok"]:
        result["errors"].append("Not root and CAP_BPF not detected; bpftrace needs elevated privileges")

    # --- liveness check (env_check.bt) ---
    if result["bpftrace_ok"] and result["version_ok"]:
        env_bt = _ENV_CHECK_SCRIPT
        if env_bt.exists():
            try:
                out = subprocess.run(
                    [bt, str(env_bt)],
                    capture_output=True, text=True, timeout=10
                )
                if "OK|bpftrace_alive" in out.stdout:
                    result["liveness_ok"] = True
                else:
                    result["errors"].append(
                        f"env_check.bt did not emit OK|bpftrace_alive; "
                        f"stdout={out.stdout!r} stderr={out.stderr!r}"
                    )
            except (OSError, subprocess.TimeoutExpired) as exc:
                result["errors"].append(f"env_check.bt execution failed: {exc}")
        else:
            result["errors"].append(f"env_check.bt not found at {env_bt}")

    all_ok = (
        result["bpftrace_ok"]
        and result["version_ok"]
        and result["tracefs_ok"]
        and result["privilege_ok"]
        and result["liveness_ok"]
    )
    result["details"] = (
        f"bpftrace={'ok' if result['bpftrace_ok'] else 'missing'} "
        f"version={result['bpftrace_version']} "
        f"tracefs={'ok' if result['tracefs_ok'] else 'missing'} "
        f"privilege={'ok' if result['privilege_ok'] else 'missing'} "
        f"liveness={'ok' if result['liveness_ok'] else 'skipped/fail'} "
        f"→ {'READY' if all_ok else 'NOT READY'}"
    )
    return result


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
# Dynamic thread discovery and TGID resolution
# ---------------------------------------------------------------------------

def discover_process_tids(pid: int) -> set[int]:
    """Scan /proc/<pid>/task to discover all active thread IDs (TIDs) for a process.

    Returns a set containing at least ``{pid}`` (the main thread).  If /proc
    is unavailable or permission is denied, falls back gracefully to ``{pid}``.
    """
    tids = {pid}
    task_dir = Path(f"/proc/{pid}/task")
    if task_dir.is_dir():
        try:
            for entry in task_dir.iterdir():
                if entry.name.isdigit():
                    tids.add(int(entry.name))
        except (OSError, PermissionError):
            pass
    return tids


class ThreadTracker:
    """Tracks thread IDs (TIDs) for target processes and resolves their owning TGID.

    In Linux, a multi-threaded process (such as a vLLM engine, PyTorch worker pool,
    or OpenMP runtime) shares a single Process Group ID (TGID = pid), while each
    thread has a unique Thread ID (TID).  The kernel scheduler tracepoints report
    individual TIDs.

    This tracker:
    1. Scans /proc/<pid>/task to automatically expand a target process PID into
       all of its constituent worker thread TIDs.
    2. Maps each discovered TID back to its owning process TGID, satisfying the
       platform contract: 'pid is real TGID, unknown filled as null, cannot be
       substituted by tid'.
    3. Supports dynamic TID resolution on cache miss via /proc/<tid>/status.
    """

    def __init__(
        self,
        target_pids: list[int] | set[int] | None = None,
        target_comm: str | None = None,
        auto_expand: bool = True,
    ):
        self.target_pids: list[int] | None = list(target_pids) if target_pids is not None else None
        self.target_comm: str | None = target_comm
        self.auto_expand: bool = auto_expand
        self._target_tids: set[int] = set(self.target_pids) if self.target_pids else set()
        self._tid_to_tgid: dict[int, int] = {}
        if self.target_pids and self.auto_expand:
            self.refresh()

    def refresh(self) -> None:
        """Scan /proc/<pid>/task for all target PIDs to update active TIDs and TGID map."""
        if not self.target_pids:
            return
        for pid in self.target_pids:
            task_dir = Path(f"/proc/{pid}/task")
            if task_dir.is_dir():
                try:
                    for entry in task_dir.iterdir():
                        if entry.name.isdigit():
                            tid = int(entry.name)
                            self._target_tids.add(tid)
                            self._tid_to_tgid[tid] = pid
                except (OSError, PermissionError):
                    pass

    def matches(self, tid: int, comm: str | None = None) -> bool:
        """Check whether (tid, comm) matches the tracked target process/comm.

        When both target_pids and target_comm are specified, both must match (AND semantics).
        """
        if self.target_pids is None and self.target_comm is None:
            return False

        pid_match = True
        if self.target_pids is not None:
            if tid in self._target_tids:
                pid_match = True
            elif self.auto_expand:
                tgid = self.resolve_tgid_from_proc(tid)
                if tgid is not None and tgid in self.target_pids:
                    self._target_tids.add(tid)
                    self._tid_to_tgid[tid] = tgid
                    pid_match = True
                else:
                    pid_match = False
            else:
                pid_match = False

        comm_match = True
        if self.target_comm is not None:
            comm_match = (comm == self.target_comm)

        return pid_match and comm_match

    def resolve_tgid(self, tid: int) -> int | None:
        """Return the owning TGID for this TID, or None if unknown."""
        return self._tid_to_tgid.get(tid)

    @staticmethod
    def resolve_tgid_from_proc(tid: int) -> int | None:
        """Inspect /proc/<tid>/status to read the Tgid field."""
        try:
            status_path = Path(f"/proc/{tid}/status")
            if not status_path.exists():
                return None
            for line in status_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("Tgid:"):
                    return int(line.split(":")[1].strip())
        except (OSError, ValueError, IndexError):
            pass
        return None

    @property
    def all_target_tids(self) -> list[int]:
        """Return a sorted list of all known TIDs belonging to the target."""
        return sorted(self._target_tids)


# ---------------------------------------------------------------------------
# JSONL formatting
# ---------------------------------------------------------------------------

def _target_scope(target_pids, target_comm):
    if target_pids is not None:
        return "target_threads"
    return "comm_match" if target_comm is not None else "system"


def _matches_target(event, target_pids, target_comm, tracker: ThreadTracker | None = None):
    if tracker is not None:
        return tracker.matches(event.tid, event.comm)
    return (target_pids is not None or target_comm is not None) and (
        target_pids is None or event.tid in target_pids
    ) and (target_comm is None or event.comm == target_comm)


def event_to_jsonl(
    event: SchedEvent,
    run_id: str,
    target_pids: list[int] | None = None,
    target_comm: str | None = None,
    tracker: ThreadTracker | None = None,
) -> str:
    """Serialize one event to a JSON line (no trailing newline)."""
    # sched_switch supplies a TID and comm, not the process TGID.
    pid_val = None
    if tracker is not None:
        is_target = tracker.matches(event.tid, event.comm)
    else:
        is_target = _matches_target(event, target_pids, target_comm) if (
            target_pids is not None or target_comm is not None
        ) else None

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
    - scope: system, target_threads (TID filter), or comm_match (thread name).
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
        "selection_semantics": "target_pids are TID filters; comm is name matching, not process identity",
        "metric": "wakeup_to_run_latency_summary",
        "start_wall_ns": result.start_anchor.wall_ns,
        "start_monotonic_ns": result.start_anchor.monotonic_ns,
        "event_count": s.event_count,
        "kernel_matched_count": s.kernel_matched_count,
        "dropped_events_count": s.dropped_events_count if s.kernel_matched_count is not None else None,
        "loss_reconciliation_available": s.kernel_matched_count is not None,
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
    if result.target_tids is not None:
        d["target_tids"] = result.target_tids
    if result.target_comm is not None:
        d["target_comm"] = result.target_comm
    if result.scope != "system" and result.target_stats is not None:
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
    scope = _target_scope(target_pids, target_comm)
    result = CollectorResult(
        run_id=run_id,
        start_anchor=anchor,
        scope=scope,
        target_pids=target_pids,
        target_comm=target_comm,
    )
    tracker = ThreadTracker(target_pids=target_pids, target_comm=target_comm)
    sample_reservoir: list[int] = []
    target_reservoir: list[int] = []
    target_stats = CollectorStats() if scope != "system" else None
    target_seen_tids: set[int] = set()

    rng = random.Random(random_seed) if random_seed is not None else random.Random()
    seen_tids: set[int] = set()
    comm_counts: dict[str, int] = {}

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
                elif name == "runwait_matched":          # E1
                    result.stats.kernel_runwait_matched = value
                elif name == "oncpu_matched":            # E2
                    result.stats.kernel_oncpu_matched = value
                elif name == "map_overflow":             # E0
                    result.stats.map_overflow_count = value
            continue

        # E0: kernel clock anchor line
        if line.startswith(_CLOCK_PREFIX):
            anchor_parsed = parse_clock(line)
            if anchor_parsed is not None:
                result.kernel_clock_anchor = anchor_parsed
            continue

        # E1: Preempt-to-Run runwait event
        if line.startswith(_RUNWAIT_PREFIX):
            rw = parse_runwait(line)
            if rw is None:
                result.stats.parser_error_count += 1
                continue
            result.stats.runwait_event_count += 1
            # Write RUNWAIT events to JSONL output with metric tag and resolved TGID
            if output is not None:
                rw_wall_ns = (
                    to_wall_ns(rw.switch_ns, result.kernel_clock_anchor)
                    if hasattr(result, "kernel_clock_anchor") and result.kernel_clock_anchor
                    else None
                )
                rw_is_target = tracker.matches(rw.tid, rw.comm)
                rw_tgid = tracker.resolve_tgid(rw.tid) if rw_is_target else None
                rw_record = {
                    "metric": "runnable_wait",
                    "run_id": run_id,
                    "timestamp_ns": rw.switch_ns,
                    "timestamp_wall_ns": rw_wall_ns,
                    "value": rw.wait_ns,
                    "pid": rw_tgid,
                    "tid": rw.tid,
                    "cpu": rw.cpu,
                    "comm": rw.comm,
                    "is_target": rw_is_target,
                }
                output.write(json.dumps(rw_record) + "\n")
            continue

        # E2: Unified ONCPU execution slice event
        if line.startswith(_ONCPU_PREFIX):
            parts = line.split("|")
            if len(parts) == 6:
                try:
                    oncpu_end_ns = int(parts[1])
                    oncpu_tid = int(parts[2])
                    oncpu_cpu = int(parts[3])
                    oncpu_dur_ns = int(parts[4])
                    oncpu_comm = parts[5].strip()
                    if oncpu_dur_ns >= 0 and oncpu_tid >= 0:
                        result.stats.oncpu_event_count += 1
                        if output is not None:
                            oncpu_wall = (
                                to_wall_ns(oncpu_end_ns, result.kernel_clock_anchor)
                                if hasattr(result, "kernel_clock_anchor") and result.kernel_clock_anchor
                                else None
                            )
                            oncpu_is_tgt = tracker.matches(oncpu_tid, oncpu_comm)
                            oncpu_tgid = tracker.resolve_tgid(oncpu_tid) if oncpu_is_tgt else None
                            oncpu_record = {
                                "metric": "on_cpu",
                                "run_id": run_id,
                                "timestamp_ns": oncpu_end_ns,
                                "timestamp_wall_ns": oncpu_wall,
                                "value": oncpu_dur_ns,
                                "pid": oncpu_tgid,
                                "tid": oncpu_tid,
                                "cpu": oncpu_cpu,
                                "comm": oncpu_comm,
                                "is_target": oncpu_is_tgt,
                            }
                            output.write(json.dumps(oncpu_record) + "\n")
                        continue
                except (ValueError, IndexError):
                    pass
            result.stats.parser_error_count += 1
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

            # Target process filtering with dynamic thread tree awareness
            is_target = tracker.matches(event.tid, event.comm)

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
                        tracker=tracker,
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

    # Reflect all dynamically discovered worker thread TIDs in result.target_tids
    # while preserving original user configuration in result.target_pids
    if tracker.target_pids is not None:
        result.target_tids = tracker.all_target_tids

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
        Single target thread ID (legacy parameter name); does not expand to all process threads.
    target_pids:
        List of target thread IDs (legacy parameter name).
    target_comm:
        Thread command name to match; this does not prove process identity.

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
            scope=_target_scope(target_pids, target_comm),
            target_pids=target_pids,
            target_comm=target_comm,
        )

    script = str(script_path or _SCRIPT_PATH)
    cmd = [bt]
    if target_pids and len(target_pids) == 1:
        cmd.extend(["-D", f"TARGET_TGID={target_pids[0]}"])
    cmd.append(script)

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
    scope = _target_scope(target_pids, target_comm)
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
                        help="Target TID (legacy --pid option; does not include all process threads)")
    parser.add_argument("--comm", type=str, default=None,
                        help="Thread name match (comm); does not establish process identity")
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
