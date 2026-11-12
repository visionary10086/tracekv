"""System telemetry sampler for Linux/WSL (/proc/meminfo).

Strictly satisfies TraceKV v0.2 integration specification for `system_samples.jsonl`.
Provides single-shot sampling and continuous background monitoring.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time
from typing import Any


def _utc_now_iso() -> str:
    """Return current ISO 8601 UTC timestamp ending in 'Z'."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def collect_system_sample(
    run_id: str = "",
    meminfo_path: str | Path = "/proc/meminfo",
) -> dict[str, Any]:
    """Collect a single system telemetry sample conforming to v0.2 contract.

    Returns a dict with:
        run_id: str
        timestamp: ISO 8601 UTC string (ending in Z)
        mem_available_mb: Decimal megabytes (1,000,000 bytes)
        disk_read_p95_ms: null (eBPF disk tracker roadmap)
        disk_read_bytes_per_s: null (eBPF disk tracker roadmap)
        source: 'proc' or 'unavailable'
    """
    available = None
    source = "unavailable"
    try:
        content = Path(meminfo_path).read_text(encoding="utf-8")
        for line in content.splitlines():
            if line.startswith("MemAvailable:"):
                parts = line.split()
                # /proc/meminfo reports in kB (1024 bytes)
                # contracts/schema.py defines decimal MB: kB * 1024 / 1_000_000
                available = round(int(parts[1]) * 1024 / 1_000_000, 3)
                source = "proc"
                break
    except (OSError, ValueError, IndexError):
        pass

    return {
        "run_id": run_id,
        "timestamp": _utc_now_iso(),
        "mem_available_mb": available,
        "disk_read_p95_ms": None,
        "disk_read_bytes_per_s": None,
        "source": source,
    }


class SystemMonitor:
    """Continuous background system telemetry monitor writing `system_samples.jsonl`."""

    def __init__(
        self,
        run_id: str,
        output_path: str | Path,
        interval_sec: float = 1.0,
        meminfo_path: str | Path = "/proc/meminfo",
    ):
        self.run_id = run_id
        self.output_path = Path(output_path)
        self.interval_sec = interval_sec
        self.meminfo_path = Path(meminfo_path)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.sample_count = 0

    def start(self) -> None:
        """Start the background sampling thread."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> int:
        """Stop sampling and wait for the thread to exit. Returns sample count."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        return self.sample_count

    def _monitor_loop(self) -> None:
        with open(self.output_path, "a", encoding="utf-8") as f:
            while not self._stop_event.is_set():
                sample = collect_system_sample(
                    run_id=self.run_id,
                    meminfo_path=self.meminfo_path,
                )
                f.write(json.dumps(sample, ensure_ascii=False) + "\n")
                f.flush()
                self.sample_count += 1
                self._stop_event.wait(self.interval_sec)

    def __enter__(self) -> "SystemMonitor":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="TraceKV System Telemetry Sampler (v0.2)")
    parser.add_argument("--run-id", default="test-sys-run", help="Experiment run ID")
    parser.add_argument("--out", type=Path, default=None, help="Output JSONL path")
    parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Monitoring duration (0 = single shot)")
    parser.add_argument("--meminfo", default="/proc/meminfo", help="Path to meminfo")
    args = parser.parse_args()

    if args.duration <= 0 and args.out is None:
        sample = collect_system_sample(run_id=args.run_id, meminfo_path=args.meminfo)
        print(json.dumps(sample, indent=2, ensure_ascii=False))
        return

    out_path = args.out or Path(f"results/{args.run_id}/system_samples.jsonl")
    duration = args.duration if args.duration > 0 else 2.0
    print(f"Monitoring system metrics for {duration}s -> {out_path}")
    with SystemMonitor(args.run_id, out_path, interval_sec=args.interval, meminfo_path=args.meminfo) as mon:
        time.sleep(duration)
    print(f"Done. Collected {mon.sample_count} samples.")


if __name__ == "__main__":
    main()
