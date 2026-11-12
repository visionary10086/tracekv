"""Synthetic CPU contention workload generator for scheduler latency evaluation.

Spawns a configurable number of CPU-intensive worker processes to create
predictable CPU saturation and scheduler runqueue congestion, enabling
comparative evaluation of wakeup-to-run latency under varying load levels.

Uses only Python 3.12 standard library.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import argparse
import hashlib
import multiprocessing as mp
import os
import signal
import sys
import time


def _cpu_worker(stop_event: mp.Event, worker_id: int) -> None:
    """CPU-bound worker that spins computing SHA-256 hashes until stopped."""
    # Set comm if possible on Linux
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        prctl = libc.prctl
        PR_SET_NAME = 15
        name = f"tracekv_load_{worker_id}".encode("utf-8")[:15]
        prctl(PR_SET_NAME, name, 0, 0, 0)
    except Exception:
        pass

    data = b"tracekv_cpu_burn_" + str(worker_id).encode("utf-8")
    while not stop_event.is_set():
        # Tight CPU loop
        for _ in range(10_000):
            data = hashlib.sha256(data).digest()


def _probe_worker(stop_event: mp.Event, sleep_interval: float = 0.005) -> None:
    """Intermittent probe worker: sleeps and wakes up periodically.

    Generates predictable Sleep-to-Run wakeup events under load.
    """
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        PR_SET_NAME = 15
        prctl = libc.prctl
        prctl(PR_SET_NAME, b"tracekv_probe\0", 0, 0, 0)
    except Exception:
        pass

    while not stop_event.is_set():
        time.sleep(sleep_interval)


def run_workload(
    workers: int,
    duration: float,
    include_probe: bool = True,
    probe_interval: float = 0.005,
) -> None:
    """Launch *workers* CPU-bound processes and run for *duration* seconds."""
    stop_event = mp.Event()
    processes: list[mp.Process] = []

    # Handle graceful exit
    def _handle_signal(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print(
        f"[load] Starting {workers} CPU workers (duration={duration:.1f}s, probe={include_probe})",
        file=sys.stderr,
        flush=True,
    )

    # Launch CPU workers
    for i in range(workers):
        p = mp.Process(target=_cpu_worker, args=(stop_event, i), daemon=True)
        p.start()
        processes.append(p)

    # Launch probe worker
    probe_p: mp.Process | None = None
    if include_probe:
        probe_p = mp.Process(
            target=_probe_worker,
            args=(stop_event, probe_interval),
            daemon=True,
        )
        probe_p.start()
        processes.append(probe_p)

    start_time = time.time()
    try:
        while time.time() - start_time < duration and not stop_event.is_set():
            time.sleep(0.1)
    finally:
        stop_event.set()
        for p in processes:
            p.join(timeout=1.0)
            if p.is_alive():
                p.kill()

    elapsed = time.time() - start_time
    print(f"[load] Workload finished after {elapsed:.2f}s", file=sys.stderr, flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="TraceKV CPU Contention Workload Generator",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 4,
        help=f"Number of CPU-bound workers (default: {os.cpu_count() or 4})",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Duration in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--no-probe",
        action="store_true",
        help="Disable periodic probe worker",
    )
    parser.add_argument(
        "--probe-interval",
        type=float,
        default=0.005,
        help="Probe worker sleep interval in seconds (default: 0.005)",
    )
    args = parser.parse_args(argv)

    run_workload(
        workers=args.workers,
        duration=args.duration,
        include_probe=not args.no_probe,
        probe_interval=args.probe_interval,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
