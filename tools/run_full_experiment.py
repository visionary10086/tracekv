#!/usr/bin/env python3
"""TraceKV End-to-End Experiment & Diagnostic Orchestrator.

Orchestrates the entire telemetry collection and diagnosis pipeline:
1. GpuMonitor (NVML ctypes direct driver, <0.2ms, PCIe Tx/Rx, 12 GPU dims)
2. SystemMonitor (/proc Linux telemetry: PSI, diskstats, memory, CPU)
3. CPU Scheduler Tracer (eBPF: wakeup_to_run_latency, runnable_wait, on_cpu)
4. Correlator Snapshot Alignment (windowed sub-second cross-layer alignment)
5. Automated Bottleneck Diagnosis Engine (PCIe, CPU contention, Direct Reclaim)

Usage:
    # Run a 10-second end-to-end telemetry and diagnosis session:
    python3 tools/run_full_experiment.py --duration 10 --out-dir results/my_run

    # Target specific process (e.g. vLLM):
    python3 tools/run_full_experiment.py --duration 30 --target-pid 1234 --out-dir results/vllm_exp

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

# Ensure project root is in sys.path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from collector.gpu import GpuMonitor
from collector.system import SystemMonitor
from collector.cpu_scheduler import run_collector, stats_to_dict
from collector.correlator import (
    generate_aligned_snapshots,
    diagnose_system_bottlenecks,
    analyze_kv_pcie_transfers,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def print_banner(text: str) -> None:
    print(f"\n{'='*70}\n  {text}\n{'='*70}", flush=True)


def run_orchestrated_experiment(
    run_id: str,
    duration_s: float,
    out_dir: Path,
    target_pid: int | None = None,
    target_comm: str | None = None,
    kv_transfers_path: Path | None = None,
    bucket_sec: float = 1.0,
    script_path: Path | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    print_banner(f"TraceKV Telemetry & Diagnostic Orchestrator (run_id: {run_id})")
    print(f"[*] Duration:      {duration_s} seconds")
    print(f"[*] Target PID:    {target_pid or 'system-wide'}")
    print(f"[*] Target COMM:   {target_comm or 'system-wide'}")
    print(f"[*] Output Dir:    {out_dir}")

    gpu_path = out_dir / "gpu_samples.jsonl"
    sys_path = out_dir / "system_samples.jsonl"
    sched_events_path = out_dir / "sched_events.jsonl"
    sched_summary_path = out_dir / "sched_summary.json"
    aligned_snapshots_path = out_dir / "aligned_snapshots.jsonl"
    diagnosis_path = out_dir / "bottleneck_diagnosis.json"

    # 1. Start continuous monitors
    print("\n[+] Starting GpuMonitor (NVML ctypes direct driver)...")
    gpu_mon = GpuMonitor(run_id=run_id, output_path=gpu_path, interval_sec=bucket_sec)
    gpu_mon.start()

    print("[+] Starting SystemMonitor (/proc Linux telemetry + PSI)...")
    sys_mon = SystemMonitor(run_id=run_id, output_path=sys_path, interval_sec=bucket_sec, target_pid=target_pid)
    sys_mon.start()

    # 2. Run eBPF Unified Scheduler Tracer
    used_script = script_path or (_REPO_ROOT / "collector" / "ebpf" / "telemetry_full.bt")
    if not Path(used_script).exists():
        used_script = _REPO_ROOT / "collector" / "ebpf" / "sched_latency.bt"
    print(f"[+] Starting eBPF Unified Tracer ({Path(used_script).name}) for {duration_s}s...")
    target_pids = [target_pid] if target_pid is not None else None
    t0 = time.time()

    sched_res = run_collector(
        run_id=run_id,
        duration_seconds=duration_s,
        output_path=sched_events_path,
        target_pids=target_pids,
        target_comm=target_comm,
        script_path=used_script,
    )
    elapsed = time.time() - t0
    print(f"[+] eBPF collection completed in {elapsed:.2f}s.")

    # 3. Stop background monitors
    print("[+] Stopping background GPU and System monitors...")
    gpu_count = gpu_mon.stop()
    sys_count = sys_mon.stop()
    print(f"    - GPU samples collected:    {gpu_count}")
    print(f"    - System samples collected: {sys_count}")

    # Write sched summary
    summary_dict = stats_to_dict(sched_res)
    sched_summary_path.write_text(json.dumps(summary_dict, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"    - Sched events parsed:      {sched_res.stats.event_count}")
    if sched_res.error:
        print(f"    [WARN] Sched tracer warning/error: {sched_res.error}")

    # 4. Snapshot Alignment (sub-second cross-layer correlation)
    print("\n[+] Generating cross-layer aligned snapshots...")
    snapshots = generate_aligned_snapshots(
        run_id=run_id,
        sched_events_path=sched_events_path,
        sched_summary_path=sched_summary_path,
        gpu_samples_path=gpu_path,
        system_samples_path=sys_path,
        bucket_sec=bucket_sec,
    )
    with open(aligned_snapshots_path, "w", encoding="utf-8") as f:
        for snap in snapshots:
            f.write(json.dumps(snap, ensure_ascii=False) + "\n")
    print(f"    - Generated {len(snapshots)} aligned snapshots -> {aligned_snapshots_path}")

    # 5. KV-PCIe transfer analysis (if available)
    kv_pcie_res = None
    if kv_transfers_path and kv_transfers_path.exists():
        print(f"[+] Correlating software KV transfers from {kv_transfers_path} with PCIe telemetry...")
        kv_pcie_res = analyze_kv_pcie_transfers(
            kv_transfers_path=kv_transfers_path,
            gpu_samples_path=gpu_path,
            sched_summary_path=sched_summary_path,
        )

    # 6. Automated Bottleneck Diagnosis
    print("\n[+] Running Automated Bottleneck Diagnosis Engine...")
    diagnosis = diagnose_system_bottlenecks(
        snapshots=snapshots,
        kv_pcie=kv_pcie_res,
    )
    diagnosis_path.write_text(json.dumps(diagnosis, indent=2, ensure_ascii=False), encoding="utf-8")

    # Print summary card
    print_banner("DIAGNOSTIC VERDICT")
    print(f"Primary Bottleneck: {diagnosis.get('primary_bottleneck')}")
    print(f"Severity:           {diagnosis.get('severity')}")
    print(f"Summary:            {diagnosis.get('summary')}")
    print("Recommendations:")
    for rec in diagnosis.get("recommendations", []):
        print(f"  * {rec}")
    print(f"\nFull report written to: {diagnosis_path}")
    print("=" * 70)

    return diagnosis


def main() -> int:
    parser = argparse.ArgumentParser(description="TraceKV Full Experiment & Telemetry Orchestrator")
    parser.add_argument("--run-id", default=f"exp-{int(time.time())}", help="Unique experiment run ID")
    parser.add_argument("--duration", type=float, default=5.0, help="Duration in seconds")
    parser.add_argument("--out-dir", type=Path, default=Path("results/quick_exp"), help="Results output directory")
    parser.add_argument("--target-pid", type=int, default=None, help="Target process PID to isolate")
    parser.add_argument("--target-comm", default=None, help="Target command name to isolate")
    parser.add_argument("--kv-transfers", type=Path, default=None, help="Path to kv_transfers.jsonl")
    parser.add_argument("--bucket-sec", type=float, default=1.0, help="Snapshot window width in seconds")
    parser.add_argument("--script", type=Path, default=None, help="Path to custom bpftrace script (defaults to telemetry_full.bt)")
    args = parser.parse_args()

    run_orchestrated_experiment(
        run_id=args.run_id,
        duration_s=args.duration,
        out_dir=args.out_dir,
        target_pid=args.target_pid,
        target_comm=args.target_comm,
        kv_transfers_path=args.kv_transfers,
        bucket_sec=args.bucket_sec,
        script_path=args.script,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
