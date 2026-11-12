"""CPU Scheduler Latency & Collector Overhead Experiment Runner.

Orchestrates controlled CPU load scenarios (idle, half-load, full-load,
oversubscribed), collects scheduler wakeup-to-run latency via
`collector.cpu_scheduler`, monitors collector resource overhead (CPU%, RSS MB),
and produces comparative performance reports.

Zero third-party dependencies beyond standard Python 3.12.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

# Add project root to sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from collector.cpu_scheduler import run_collector, stats_to_dict
from tools.sched_workload import _cpu_worker, _probe_worker


@dataclass
class OverheadStats:
    """Resource overhead metrics for the collector during the run."""
    peak_python_rss_kb: int = 0
    peak_bpftrace_rss_kb: int = 0
    collector_avg_cpu_pct: float = 0.0
    sample_count: int = 0


def _get_rss_kb(pid: int) -> int:
    """Read VmRSS in kilobytes for a process from /proc/<pid>/status."""
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    return int(parts[1])
    except (FileNotFoundError, ProcessLookupError, IndexError, ValueError, PermissionError):
        pass
    return 0


def _get_cpu_ticks(pid: int) -> int:
    """Read utime + stime ticks from /proc/<pid>/stat."""
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as f:
            fields = f.read().split()
            # fields[13] is utime, fields[14] is stime (0-indexed)
            return int(fields[13]) + int(fields[14])
    except (FileNotFoundError, ProcessLookupError, IndexError, ValueError, PermissionError):
        pass
    return 0


class OverheadMonitor:
    """Background thread that samples resource usage of collector processes."""

    def __init__(self, target_pids: list[int], interval_sec: float = 0.2):
        self.target_pids = target_pids
        self.interval = interval_sec
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.stats = OverheadStats()
        self._ticks_history: list[int] = []

    def start(self) -> None:
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> OverheadStats:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        return self.stats

    def _monitor_loop(self) -> None:
        my_pid = os.getpid()
        last_ticks = _get_cpu_ticks(my_pid)
        last_time = time.time()

        while not self._stop_event.is_set():
            time.sleep(self.interval)
            now = time.time()
            dt = max(0.001, now - last_time)

            # Python RSS
            py_rss = _get_rss_kb(my_pid)
            if py_rss > self.stats.peak_python_rss_kb:
                self.stats.peak_python_rss_kb = py_rss

            # Subprocess RSS (e.g. bpftrace)
            for pid in self.target_pids:
                bt_rss = _get_rss_kb(pid)
                if bt_rss > self.stats.peak_bpftrace_rss_kb:
                    self.stats.peak_bpftrace_rss_kb = bt_rss

            # Self CPU ticks
            cur_ticks = _get_cpu_ticks(my_pid)
            delta_ticks = max(0, cur_ticks - last_ticks)
            # Clock ticks per second is typically 100 on Linux (sysconf(_SC_CLK_TCK))
            clk_tck = os.sysconf(os.sysconf_names.get("SC_CLK_TCK", 100)) or 100
            cpu_pct = (delta_ticks / clk_tck) / dt * 100.0
            self._ticks_history.append(cpu_pct)

            last_ticks = cur_ticks
            last_time = now
            self.stats.sample_count += 1

        if self._ticks_history:
            self.stats.collector_avg_cpu_pct = round(
                sum(self._ticks_history) / len(self._ticks_history), 2
            )


def run_experiment_scenario(
    scenario: str,
    workers: int,
    duration: float,
    out_dir: Path,
    include_probe: bool = True,
) -> dict:
    """Run one load scenario with concurrent latency tracing and overhead monitoring."""
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / f"{scenario}_events.jsonl"
    summary_path = out_dir / f"{scenario}_summary.json"

    stop_event = mp.Event()
    worker_procs: list[mp.Process] = []

    print(
        f"\n{'='*70}\n[exp] SCENARIO: {scenario} (workers={workers}, duration={duration:.1f}s)\n{'='*70}",
        file=sys.stderr,
        flush=True,
    )

    # 1. Start background CPU workers (if workers > 0)
    if workers > 0:
        print(f"[exp] Launching {workers} CPU-bound contention workers...", file=sys.stderr, flush=True)
        for i in range(workers):
            p = mp.Process(target=_cpu_worker, args=(stop_event, i), daemon=True)
            p.start()
            worker_procs.append(p)

    if include_probe:
        probe = mp.Process(target=_probe_worker, args=(stop_event, 0.005), daemon=True)
        probe.start()
        worker_procs.append(probe)

    # Small warmup to ensure workers saturate CPUs
    if workers > 0:
        time.sleep(0.5)

    # 2. Run latency collector with overhead monitoring
    overhead_monitor = OverheadMonitor(target_pids=[])
    overhead_monitor.start()

    try:
        collector_result = run_collector(
            run_id=scenario,
            output_path=jsonl_path,
            duration_seconds=duration,
        )
    finally:
        # Stop workload workers cleanly
        stop_event.set()
        for p in worker_procs:
            p.join(timeout=1.0)
            if p.is_alive():
                p.kill()

    overhead_stats = overhead_monitor.stop()

    # 3. Assemble and persist summary
    summary = stats_to_dict(collector_result)
    summary["scenario"] = scenario
    summary["workers"] = workers
    summary["duration_requested"] = duration
    summary["overhead"] = asdict(overhead_stats)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[exp] Scenario {scenario} complete. Results written to {summary_path}", file=sys.stderr, flush=True)
    return summary


def format_comparison_table(summaries: list[dict]) -> str:
    """Format a clean markdown table comparing all executed scenarios."""
    lines = [
        "| Scenario | Workers | Events | P50 Latency | P95 Latency | P99 Latency | Max Latency | Drops | Collector CPU% | Peak RSS (MB) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]

    for s in summaries:
        name = s.get("scenario", s.get("run_id", "unknown"))
        w = s.get("workers", 0)
        ev = s.get("event_count", 0)
        p50 = f"{s.get('p50_ns', 0) / 1000:.2f} µs" if s.get("p50_ns") is not None else "N/A"
        p95 = f"{s.get('p95_ns', 0) / 1000:.2f} µs" if s.get("p95_ns") is not None else "N/A"
        p99 = f"{s.get('p99_ns', 0) / 1000:.2f} µs" if s.get("p99_ns") is not None else "N/A"
        max_l = f"{s.get('max_ns', 0) / 1000:.2f} µs" if s.get("max_ns") is not None else "N/A"
        drops = s.get("dropped_events_count", 0)

        ov = s.get("overhead", {})
        cpu = f"{ov.get('collector_avg_cpu_pct', 0.0):.1f}%"
        peak_rss_kb = max(ov.get("peak_python_rss_kb", 0), ov.get("peak_bpftrace_rss_kb", 0))
        rss_mb = f"{peak_rss_kb / 1024:.1f} MB" if peak_rss_kb > 0 else "< 20 MB"

        lines.append(f"| **{name}** | {w} | {ev} | {p50} | {p95} | {p99} | {max_l} | {drops} | {cpu} | {rss_mb} |")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="TraceKV CPU Scheduler Latency & Overhead Benchmark Runner",
    )
    parser.add_argument(
        "--scenario",
        default="load_test",
        help="Scenario identifier (e.g. idle, load_16, load_32, load_48)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of CPU contention workers (0 = idle baseline)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Collection duration in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("results/sched_experiments"),
        help="Directory to save experiment logs and summaries",
    )
    parser.add_argument(
        "--compare",
        type=Path,
        default=None,
        help="Directory containing summary JSON files to build comparison report",
    )
    args = parser.parse_args(argv)

    if args.compare is not None:
        summary_files = sorted(args.compare.glob("*_summary.json"))
        if not summary_files:
            print(f"No summary files found in {args.compare}", file=sys.stderr)
            return 1
        summaries = []
        for sf in summary_files:
            with open(sf, "r", encoding="utf-8") as f:
                summaries.append(json.load(f))
        print("\n### CPU Scheduler Latency & Collector Overhead Comparison\n")
        print(format_comparison_table(summaries))
        return 0

    summary = run_experiment_scenario(
        scenario=args.scenario,
        workers=args.workers,
        duration=args.duration,
        out_dir=args.out_dir,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary.get("error") is None else 2


if __name__ == "__main__":
    raise SystemExit(main())
