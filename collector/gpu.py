"""Real GPU memory and utilization sampler for NVIDIA GPUs (WSL2 / Linux).

Uses `nvidia-smi` to query current VRAM usage, utilization, temperature, and power,
converting to standard decimal MB consistent with `contracts/schema.py` and
`docs/联调接口规范_v0.2.md`.

Supports one-shot collection, continuous background monitoring (`GpuMonitor`),
and parsing of legacy sample logs.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import Any


_WSL_NVIDIA_SMI = Path("/usr/lib/wsl/lib/nvidia-smi")


def _find_nvidia_smi() -> str | None:
    """Locate nvidia-smi executable on Linux or WSL2."""
    if _WSL_NVIDIA_SMI.exists():
        return str(_WSL_NVIDIA_SMI)
    return shutil.which("nvidia-smi")


def _utc_now_iso() -> str:
    """Return current ISO 8601 UTC timestamp ending in 'Z'."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_float(val: str | None) -> float | None:
    """Parse a float string, returning None for missing or unsupported fields."""
    if val is None:
        return None
    cleaned = val.strip()
    if not cleaned or cleaned.lower() in ("n/a", "[not supported]", "not supported", "[n/a]"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_int(val: str | None) -> int | None:
    """Parse an integer string, returning None for missing or unsupported fields."""
    if val is None:
        return None
    cleaned = val.strip()
    if not cleaned or cleaned.lower() in ("n/a", "[not supported]", "not supported", "[n/a]"):
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def collect_gpu_metrics(
    binary_path: str | None = None,
    gpu_index: int = 0,
    run_id: str = "",
) -> dict[str, Any]:
    """Query current GPU metrics conforming to TraceKV v0.2 specification.

    Returns a dict with:
        run_id: str
        timestamp: ISO 8601 UTC string (ending in Z)
        gpu_index: int
        gpu_uuid: str | None
        memory_used_mb: Decimal megabytes (1,000,000 bytes)
        memory_total_mb: Decimal megabytes
        utilization_pct: GPU core utilization percentage (0 - 100)
        temperature_c: Temperature in degrees Celsius
        power_w: Power draw in Watts
        source: 'nvidia-smi' or 'unavailable'
    """
    now_iso = _utc_now_iso()
    base_unavailable: dict[str, Any] = {
        "run_id": run_id,
        "timestamp": now_iso,
        "gpu_index": gpu_index,
        "gpu_uuid": None,
        "memory_used_mb": None,
        "memory_total_mb": None,
        "utilization_pct": None,
        "temperature_c": None,
        "power_w": None,
        "source": "unavailable",
    }

    smi = binary_path or _find_nvidia_smi()
    if not smi:
        return base_unavailable

    cmd = [
        smi,
        f"--id={gpu_index}",
        "--query-gpu=index,gpu_uuid,memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw",
        "--format=csv,noheader,nounits",
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3.0,
            check=False,
        )
        if proc.returncode != 0:
            # Fallback to query without --id if specific index failed
            fallback_cmd = [
                smi,
                "--query-gpu=memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw",
                "--format=csv,noheader,nounits",
            ]
            proc = subprocess.run(fallback_cmd, capture_output=True, text=True, timeout=3.0, check=False)
            if proc.returncode != 0 or not proc.stdout.strip():
                return base_unavailable

        first_line = proc.stdout.strip().splitlines()[0]
        parts = [p.strip() for p in first_line.split(",")]

        eff_index = gpu_index
        uuid_str: str | None = None
        used_mib: float | None = None
        total_mib: float | None = None
        util_pct: float | None = None
        temp_c: float | None = None
        power_w: float | None = None

        if len(parts) >= 7:
            parsed_index = _parse_int(parts[0])
            eff_index = parsed_index if parsed_index is not None else gpu_index
            uuid_str = parts[1].strip() if parts[1].strip() else None
            used_mib = _parse_float(parts[2])
            total_mib = _parse_float(parts[3])
            util_pct = _parse_float(parts[4])
            temp_c = _parse_float(parts[5])
            power_w = _parse_float(parts[6])
        elif len(parts) == 5:
            used_mib = _parse_float(parts[0])
            total_mib = _parse_float(parts[1])
            util_pct = _parse_float(parts[2])
            temp_c = _parse_float(parts[3])
            power_w = _parse_float(parts[4])
        elif len(parts) == 4:
            used_mib = _parse_float(parts[0])
            util_pct = _parse_float(parts[1])
            temp_c = _parse_float(parts[2])
            power_w = _parse_float(parts[3])
        elif len(parts) >= 1:
            used_mib = _parse_float(parts[0])

        # Convert MiB to decimal MB (1,000,000 bytes)
        used_mb = round(used_mib * 1024 * 1024 / 1_000_000, 2) if used_mib is not None else None
        total_mb = round(total_mib * 1024 * 1024 / 1_000_000, 2) if total_mib is not None else None

        return {
            "run_id": run_id,
            "timestamp": now_iso,
            "gpu_index": eff_index,
            "gpu_uuid": uuid_str,
            "memory_used_mb": used_mb,
            "memory_total_mb": total_mb,
            "utilization_pct": util_pct,
            "temperature_c": temp_c,
            "power_w": power_w,
            "source": "nvidia-smi",
        }
    except (subprocess.TimeoutExpired, OSError, ValueError, IndexError):
        return base_unavailable


def parse_legacy_gpu_sample(record: dict[str, Any] | str, run_id: str = "") -> dict[str, Any]:
    """Parse a legacy or v0.2 GPU sample into a standardized v0.2 sample dict.

    Handles legacy format from `tools/run_wsl_baselines.py`:
        time_utc, values_memory_MiB_util_percent_temp_C_power_W
    Note per v0.2 spec: legacy string has 4 items:
        used_mib, util_pct, temp_c, power_w (power is item index 3, NOT 4).
    """
    if isinstance(record, str):
        record = json.loads(record)

    # 1. Native v0.2 format
    if "memory_used_mb" in record and "source" in record:
        res = dict(record)
        if run_id and not res.get("run_id"):
            res["run_id"] = run_id
        return res

    # 2. Legacy baseline format
    time_str = record.get("time_utc") or record.get("timestamp") or _utc_now_iso()
    raw_vals = record.get("values_memory_MiB_util_percent_temp_C_power_W", "")
    parts = [p.strip() for p in raw_vals.split(",")] if raw_vals else []

    used_mib: float | None = None
    total_mib: float | None = None
    util_pct: float | None = None
    temp_c: float | None = None
    power_w: float | None = None

    if len(parts) == 4:
        # Legacy: used_mib, util_pct, temp_c, power_w
        used_mib = _parse_float(parts[0])
        util_pct = _parse_float(parts[1])
        temp_c = _parse_float(parts[2])
        power_w = _parse_float(parts[3])
    elif len(parts) >= 5:
        # 5-field: used_mib, total_mib, util_pct, temp_c, power_w
        used_mib = _parse_float(parts[0])
        total_mib = _parse_float(parts[1])
        util_pct = _parse_float(parts[2])
        temp_c = _parse_float(parts[3])
        power_w = _parse_float(parts[4])

    used_mb = round(used_mib * 1024 * 1024 / 1_000_000, 2) if used_mib is not None else None
    total_mb = round(total_mib * 1024 * 1024 / 1_000_000, 2) if total_mib is not None else None

    return {
        "run_id": record.get("run_id") or run_id,
        "timestamp": time_str,
        "gpu_index": record.get("gpu_index", 0),
        "gpu_uuid": record.get("gpu_uuid"),
        "memory_used_mb": used_mb,
        "memory_total_mb": total_mb,
        "utilization_pct": util_pct,
        "temperature_c": temp_c,
        "power_w": power_w,
        "source": "nvidia-smi" if used_mb is not None else "unavailable",
    }


class GpuMonitor:
    """Continuous background GPU telemetry monitor writing `gpu_samples.jsonl`.

    Strictly satisfies TraceKV v0.2 integration contract.
    """

    def __init__(
        self,
        run_id: str,
        output_path: str | Path,
        interval_sec: float = 1.0,
        binary_path: str | None = None,
        gpu_index: int = 0,
    ):
        self.run_id = run_id
        self.output_path = Path(output_path)
        self.interval_sec = interval_sec
        self.binary_path = binary_path
        self.gpu_index = gpu_index

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
                sample = collect_gpu_metrics(
                    binary_path=self.binary_path,
                    gpu_index=self.gpu_index,
                    run_id=self.run_id,
                )
                f.write(json.dumps(sample, ensure_ascii=False) + "\n")
                f.flush()
                self.sample_count += 1
                self._stop_event.wait(self.interval_sec)

    def __enter__(self) -> "GpuMonitor":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="TraceKV GPU Telemetry Sampler (v0.2)")
    parser.add_argument("--run-id", default="test-gpu-run", help="Experiment run ID")
    parser.add_argument("--out", type=Path, default=None, help="Output JSONL path")
    parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Monitoring duration (0 = single shot)")
    parser.add_argument("--gpu-index", type=int, default=0, help="GPU index to query")
    args = parser.parse_args()

    if args.duration <= 0 and args.out is None:
        sample = collect_gpu_metrics(gpu_index=args.gpu_index, run_id=args.run_id)
        print(json.dumps(sample, indent=2, ensure_ascii=False))
        return

    out_path = args.out or Path(f"results/{args.run_id}/gpu_samples.jsonl")
    duration = args.duration if args.duration > 0 else 2.0
    print(f"Monitoring GPU {args.gpu_index} for {duration}s -> {out_path}")
    with GpuMonitor(args.run_id, out_path, interval_sec=args.interval, gpu_index=args.gpu_index) as mon:
        time.sleep(duration)
    print(f"Done. Collected {mon.sample_count} samples.")


if __name__ == "__main__":
    main()
