"""System telemetry sampler for Linux/WSL (/proc/meminfo, /proc/stat, PSI, diskstats).

Strictly satisfies TraceKV v0.2 integration specification for `system_samples.jsonl`
while providing rich, enterprise-grade operating system performance metrics:
- CPU utilization (user, system, iowait), load averages (1m/5m/15m), runnable/total threads
- Detailed memory distribution: total, available, used, free, and swap utilization
- Linux Pressure Stall Information (PSI): CPU, Memory, and I/O pressure stall percentages
- Real disk I/O throughput (read/write bytes/s) and IOPS from /proc/diskstats
- Target process profiling (e.g. vLLM): CPU%, RSS MB, thread count, voluntary vs involuntary context switches (preemption!), major/minor page faults

Zero external dependencies beyond standard Python 3.12 library.

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


def _read_meminfo(meminfo_path: str | Path = "/proc/meminfo") -> dict[str, float | None]:
    """Parse /proc/meminfo into decimal MB fields (1 MB = 1,000,000 bytes)."""
    data: dict[str, int] = {}
    try:
        content = Path(meminfo_path).read_text(encoding="utf-8")
        for line in content.splitlines():
            parts = line.split(":")
            if len(parts) == 2:
                key = parts[0].strip()
                val_parts = parts[1].strip().split()
                if val_parts and val_parts[0].isdigit():
                    data[key] = int(val_parts[0])
    except (OSError, ValueError, IndexError):
        pass

    def to_mb(kb_key: str) -> float | None:
        if kb_key in data:
            return round(data[kb_key] * 1024 / 1_000_000, 3)
        return None

    tot_mb = to_mb("MemTotal")
    avail_mb = to_mb("MemAvailable")
    free_mb = to_mb("MemFree")
    used_mb = round(tot_mb - avail_mb, 3) if tot_mb is not None and avail_mb is not None else None
    used_pct = round((used_mb / tot_mb) * 100, 1) if tot_mb and used_mb is not None else None

    swap_tot_mb = to_mb("SwapTotal")
    swap_free_mb = to_mb("SwapFree")
    swap_used_mb = (
        round(swap_tot_mb - swap_free_mb, 3)
        if swap_tot_mb is not None and swap_free_mb is not None
        else None
    )
    swap_used_pct = (
        round((swap_used_mb / swap_tot_mb) * 100, 1)
        if swap_tot_mb and swap_used_mb is not None and swap_tot_mb > 0
        else 0.0
    )

    return {
        "mem_total_mb": tot_mb,
        "mem_available_mb": avail_mb,
        "mem_free_mb": free_mb,
        "mem_used_mb": used_mb,
        "mem_used_pct": used_pct,
        "swap_total_mb": swap_tot_mb,
        "swap_used_mb": swap_used_mb,
        "swap_free_mb": swap_free_mb,
        "swap_used_pct": swap_used_pct,
    }


def _read_loadavg(loadavg_path: str | Path = "/proc/loadavg") -> dict[str, Any]:
    """Parse /proc/loadavg for load averages and thread counts."""
    res = {
        "loadavg_1m": None,
        "loadavg_5m": None,
        "loadavg_15m": None,
        "running_threads": None,
        "total_threads": None,
    }
    try:
        content = Path(loadavg_path).read_text(encoding="utf-8").strip()
        parts = content.split()
        if len(parts) >= 4:
            res["loadavg_1m"] = float(parts[0])
            res["loadavg_5m"] = float(parts[1])
            res["loadavg_15m"] = float(parts[2])
            sched_parts = parts[3].split("/")
            if len(sched_parts) == 2:
                res["running_threads"] = int(sched_parts[0])
                res["total_threads"] = int(sched_parts[1])
    except (OSError, ValueError, IndexError):
        pass
    return res


def _read_psi(pressure_dir: str | Path = "/proc/pressure") -> dict[str, float | None]:
    """Parse Linux Pressure Stall Information (PSI)."""
    base = Path(pressure_dir)
    res: dict[str, float | None] = {
        "psi_cpu_some_avg10": None,
        "psi_mem_some_avg10": None,
        "psi_mem_full_avg10": None,
        "psi_io_some_avg10": None,
        "psi_io_full_avg10": None,
    }

    def parse_psi_file(path: Path) -> dict[str, float]:
        out: dict[str, float] = {}
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                parts = line.split()
                if not parts:
                    continue
                tag = parts[0]
                for item in parts[1:]:
                    if item.startswith("avg10="):
                        out[tag] = float(item.split("=")[1])
        except (OSError, ValueError, IndexError):
            pass
        return out

    cpu_p = parse_psi_file(base / "cpu")
    mem_p = parse_psi_file(base / "memory")
    io_p = parse_psi_file(base / "io")

    res["psi_cpu_some_avg10"] = cpu_p.get("some")
    res["psi_mem_some_avg10"] = mem_p.get("some")
    res["psi_mem_full_avg10"] = mem_p.get("full")
    res["psi_io_some_avg10"] = io_p.get("some")
    res["psi_io_full_avg10"] = io_p.get("full")
    return res


def _read_cpu_ticks_and_ctxt(stat_path: str | Path = "/proc/stat") -> tuple[dict[str, int] | None, int | None]:
    """Read CPU ticks and total context switches from /proc/stat."""
    cpu_ticks: dict[str, int] = {}
    ctxt_total: int | None = None
    try:
        content = Path(stat_path).read_text(encoding="utf-8")
        for line in content.splitlines():
            parts = line.split()
            if not parts:
                continue
            if parts[0] == "cpu" and len(parts) >= 8:
                # user, nice, system, idle, iowait, irq, softirq
                cpu_ticks = {
                    "user": int(parts[1]),
                    "nice": int(parts[2]),
                    "system": int(parts[3]),
                    "idle": int(parts[4]),
                    "iowait": int(parts[5]),
                    "irq": int(parts[6]),
                    "softirq": int(parts[7]),
                }
            elif parts[0] == "ctxt" and len(parts) >= 2:
                ctxt_total = int(parts[1])
    except (OSError, ValueError, IndexError):
        pass
    return (cpu_ticks if cpu_ticks else None, ctxt_total)


def _read_disk_sectors(diskstats_path: str | Path = "/proc/diskstats") -> tuple[int, int, int, int]:
    """Read total read/write sectors and ops across main block devices (sd*, nvme*, vd*)."""
    tot_read_sec = 0
    tot_write_sec = 0
    tot_read_ops = 0
    tot_write_ops = 0
    try:
        content = Path(diskstats_path).read_text(encoding="utf-8")
        for line in content.splitlines():
            parts = line.split()
            if len(parts) < 14:
                continue
            dev_name = parts[2]
            # Match physical block disks (e.g. sda, sdb, nvme0n1, vda), ignore loop/ram partitions
            if dev_name.startswith(("loop", "ram")):
                continue
            # Parts mapping: 3: read_ops, 5: sectors_read, 7: write_ops, 9: sectors_written
            tot_read_ops += int(parts[3])
            tot_read_sec += int(parts[5])
            tot_write_ops += int(parts[7])
            tot_write_sec += int(parts[9])
    except (OSError, ValueError, IndexError):
        pass
    return tot_read_sec, tot_write_sec, tot_read_ops, tot_write_ops


def _read_target_proc_metrics(
    pid: int,
    prev_utime_stime: tuple[int, int] | None = None,
    dt_sec: float | None = None,
) -> tuple[dict[str, Any] | None, tuple[int, int] | None]:
    """Collect process-level CPU, memory, thread count, context switches, and page faults."""
    stat_file = Path(f"/proc/{pid}/stat")
    status_file = Path(f"/proc/{pid}/status")
    if not stat_file.exists():
        return None, None

    try:
        stat_parts = stat_file.read_text(encoding="utf-8").split()
        # utime=13, stime=14, minflt=9, majflt=11 (0-indexed)
        utime = int(stat_parts[13])
        stime = int(stat_parts[14])
        minflt = int(stat_parts[9])
        majflt = int(stat_parts[11])

        # Status file for memory and context switches
        status_text = status_file.read_text(encoding="utf-8")
        rss_kb = 0
        vsz_kb = 0
        threads = 1
        vol_ctxt = 0
        nonvol_ctxt = 0

        for line in status_text.splitlines():
            if line.startswith("VmRSS:"):
                rss_kb = int(line.split()[1])
            elif line.startswith("VmSize:"):
                vsz_kb = int(line.split()[1])
            elif line.startswith("Threads:"):
                threads = int(line.split()[1])
            elif line.startswith("voluntary_ctxt_switches:"):
                vol_ctxt = int(line.split()[1])
            elif line.startswith("nonvoluntary_ctxt_switches:"):
                nonvol_ctxt = int(line.split()[1])

        cpu_pct: float | None = None
        if prev_utime_stime and dt_sec and dt_sec > 0:
            delta_ticks = (utime - prev_utime_stime[0]) + (stime - prev_utime_stime[1])
            ticks_per_sec = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
            cpu_pct = round((delta_ticks / ticks_per_sec / dt_sec) * 100.0, 1)

        proc_info = {
            "pid": pid,
            "cpu_pct": cpu_pct,
            "rss_mb": round(rss_kb * 1024 / 1_000_000, 2),
            "vsz_mb": round(vsz_kb * 1024 / 1_000_000, 2),
            "threads": threads,
            "voluntary_ctxt_switches": vol_ctxt,
            "nonvoluntary_ctxt_switches": nonvol_ctxt,  # Involuntary preemptions!
            "minor_page_faults": minflt,
            "major_page_faults": majflt,  # Triggered disk/swap I/O
        }
        return proc_info, (utime, stime)
    except (OSError, ValueError, IndexError, ProcessLookupError, PermissionError):
        return None, None


def collect_system_sample(
    run_id: str = "",
    meminfo_path: str | Path = "/proc/meminfo",
    stat_path: str | Path = "/proc/stat",
    loadavg_path: str | Path = "/proc/loadavg",
    pressure_dir: str | Path = "/proc/pressure",
    diskstats_path: str | Path = "/proc/diskstats",
    target_pid: int | None = None,
    prev_state: dict[str, Any] | None = None,
    return_state: bool = False,
) -> dict[str, Any] | tuple[dict[str, Any], dict[str, Any]]:
    """Collect a complete system telemetry sample conforming to TraceKV v0.2.

    Parameters:
        return_state: If True, returns (sample_dict, new_state_dict) for stateful
                      rate tracking across iterations. If False, returns sample_dict.
    """
    now_ts = time.time()
    now_iso = _utc_now_iso()

    # 1. Memory metrics
    mem_dict = _read_meminfo(meminfo_path)
    avail_mb = mem_dict.get("mem_available_mb")
    source = "proc" if avail_mb is not None else "unavailable"

    # 2. CPU metrics & Context switches from /proc/stat
    cpu_ticks, ctxt_total = _read_cpu_ticks_and_ctxt(stat_path)
    cpu_util: float | None = None
    cpu_user: float | None = None
    cpu_sys: float | None = None
    cpu_iowait: float | None = None
    ctxt_rate: float | None = None

    dt_sec: float | None = None
    if prev_state and "last_time" in prev_state:
        dt_sec = max(0.001, now_ts - prev_state["last_time"])

    if cpu_ticks and prev_state and "cpu_ticks" in prev_state and dt_sec:
        old_ticks = prev_state["cpu_ticks"]
        delta_user = cpu_ticks["user"] - old_ticks.get("user", 0)
        delta_sys = cpu_ticks["system"] - old_ticks.get("system", 0)
        delta_iowait = cpu_ticks["iowait"] - old_ticks.get("iowait", 0)
        delta_idle = cpu_ticks["idle"] - old_ticks.get("idle", 0)
        delta_tot = sum(cpu_ticks.values()) - sum(old_ticks.values())

        if delta_tot > 0:
            cpu_util = round((1.0 - (delta_idle / delta_tot)) * 100.0, 1)
            cpu_user = round((delta_user / delta_tot) * 100.0, 1)
            cpu_sys = round((delta_sys / delta_tot) * 100.0, 1)
            cpu_iowait = round((delta_iowait / delta_tot) * 100.0, 1)

    if ctxt_total is not None and prev_state and "ctxt_total" in prev_state and dt_sec:
        delta_ctxt = ctxt_total - prev_state["ctxt_total"]
        if delta_ctxt >= 0:
            ctxt_rate = round(delta_ctxt / dt_sec, 1)

    # 3. Load average & runnable thread counts
    load_dict = _read_loadavg(loadavg_path)

    # 4. Linux Pressure Stall Information (PSI)
    psi_dict = _read_psi(pressure_dir)

    # 5. Disk I/O from /proc/diskstats
    r_sec, w_sec, r_ops, w_ops = _read_disk_sectors(diskstats_path)
    disk_read_bytes_s: float | None = None
    disk_write_bytes_s: float | None = None
    disk_read_iops: float | None = None
    disk_write_iops: float | None = None

    if prev_state and "disk_sectors" in prev_state and dt_sec:
        old_r_sec, old_w_sec, old_r_ops, old_w_ops = prev_state["disk_sectors"]
        delta_r_bytes = max(0, (r_sec - old_r_sec) * 512)
        delta_w_bytes = max(0, (w_sec - old_w_sec) * 512)
        disk_read_bytes_s = round(delta_r_bytes / dt_sec, 1)
        disk_write_bytes_s = round(delta_w_bytes / dt_sec, 1)
        disk_read_iops = round(max(0, r_ops - old_r_ops) / dt_sec, 1)
        disk_write_iops = round(max(0, w_ops - old_w_ops) / dt_sec, 1)

    # 6. Target process profiling
    target_info: dict[str, Any] | None = None
    target_ticks_new: tuple[int, int] | None = None
    if target_pid:
        old_proc_ticks = prev_state.get("target_proc_ticks") if prev_state else None
        target_info, target_ticks_new = _read_target_proc_metrics(
            target_pid, old_proc_ticks, dt_sec
        )

    # Build updated state
    new_state = {
        "last_time": now_ts,
        "cpu_ticks": cpu_ticks,
        "ctxt_total": ctxt_total,
        "disk_sectors": (r_sec, w_sec, r_ops, w_ops),
        "target_proc_ticks": target_ticks_new,
    }

    # Assembled sample adhering to v0.2 contract
    sample: dict[str, Any] = {
        "run_id": run_id,
        "timestamp": now_iso,
        "mem_available_mb": avail_mb,
        "disk_read_p95_ms": None,
        "disk_read_bytes_per_s": disk_read_bytes_s,
        "source": source,
        "cpu": {
            "utilization_pct": cpu_util,
            "user_pct": cpu_user,
            "system_pct": cpu_sys,
            "iowait_pct": cpu_iowait,
            "loadavg_1m": load_dict["loadavg_1m"],
            "loadavg_5m": load_dict["loadavg_5m"],
            "loadavg_15m": load_dict["loadavg_15m"],
            "running_threads": load_dict["running_threads"],
            "total_threads": load_dict["total_threads"],
            "ctxt_switches_per_s": ctxt_rate,
        },
        "memory": mem_dict,
        "psi": psi_dict,
        "io": {
            "read_bytes_per_s": disk_read_bytes_s,
            "write_bytes_per_s": disk_write_bytes_s,
            "read_iops": disk_read_iops,
            "write_iops": disk_write_iops,
        },
    }

    if target_info is not None:
        sample["target_process"] = target_info

    if return_state:
        return sample, new_state
    return sample


class SystemMonitor:
    """Continuous background system telemetry monitor writing `system_samples.jsonl`."""

    def __init__(
        self,
        run_id: str,
        output_path: str | Path,
        interval_sec: float = 1.0,
        meminfo_path: str | Path = "/proc/meminfo",
        stat_path: str | Path = "/proc/stat",
        loadavg_path: str | Path = "/proc/loadavg",
        pressure_dir: str | Path = "/proc/pressure",
        diskstats_path: str | Path = "/proc/diskstats",
        target_pid: int | None = None,
    ):
        self.run_id = run_id
        self.output_path = Path(output_path)
        self.interval_sec = interval_sec
        self.meminfo_path = Path(meminfo_path)
        self.stat_path = Path(stat_path)
        self.loadavg_path = Path(loadavg_path)
        self.pressure_dir = Path(pressure_dir)
        self.diskstats_path = Path(diskstats_path)
        self.target_pid = target_pid

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.sample_count = 0
        self._state: dict[str, Any] = {}

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
                sample, self._state = collect_system_sample(
                    run_id=self.run_id,
                    meminfo_path=self.meminfo_path,
                    stat_path=self.stat_path,
                    loadavg_path=self.loadavg_path,
                    pressure_dir=self.pressure_dir,
                    diskstats_path=self.diskstats_path,
                    target_pid=self.target_pid,
                    prev_state=self._state,
                    return_state=True,
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
    parser = argparse.ArgumentParser(description="TraceKV Enriched System Telemetry Sampler (v0.2)")
    parser.add_argument("--run-id", default="test-sys-run", help="Experiment run ID")
    parser.add_argument("--out", type=Path, default=None, help="Output JSONL path")
    parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Monitoring duration (0 = single shot)")
    parser.add_argument("--pid", type=int, default=None, help="Target process PID to inspect")
    parser.add_argument("--meminfo", default="/proc/meminfo", help="Path to meminfo")
    args = parser.parse_args()

    if args.duration <= 0 and args.out is None:
        sample = collect_system_sample(
            run_id=args.run_id,
            meminfo_path=args.meminfo,
            target_pid=args.pid,
        )
        print(json.dumps(sample, indent=2, ensure_ascii=False))
        return

    out_path = args.out or Path(f"results/{args.run_id}/system_samples.jsonl")
    duration = args.duration if args.duration > 0 else 2.0
    print(f"Monitoring enriched system metrics for {duration}s -> {out_path}")
    with SystemMonitor(
        args.run_id,
        out_path,
        interval_sec=args.interval,
        meminfo_path=args.meminfo,
        target_pid=args.pid,
    ) as mon:
        time.sleep(duration)
    print(f"Done. Collected {mon.sample_count} samples.")


if __name__ == "__main__":
    main()
