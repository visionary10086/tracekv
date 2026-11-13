"""Real GPU memory and utilization sampler for NVIDIA GPUs (WSL2 / Linux).

Uses `nvidia-smi` to query current VRAM usage, utilization, temperature, and power,
converting to standard decimal MB consistent with `contracts/schema.py`.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


_WSL_NVIDIA_SMI = Path("/usr/lib/wsl/lib/nvidia-smi")


def _find_nvidia_smi() -> str | None:
    """Locate nvidia-smi executable on Linux or WSL2."""
    if _WSL_NVIDIA_SMI.exists():
        return str(_WSL_NVIDIA_SMI)
    return shutil.which("nvidia-smi")


def collect_gpu_metrics(binary_path: str | None = None) -> dict:
    """Query current GPU metrics.

    Returns a dict with:
        memory_used_mb: Decimal megabytes (1,000,000 bytes)
        memory_total_mb: Decimal megabytes
        utilization_pct: GPU core utilization percentage (0 - 100)
        temperature_c: Temperature in degrees Celsius
        power_w: Power draw in Watts
        source: 'nvidia-smi' or 'unavailable'
    """
    smi = binary_path or _find_nvidia_smi()
    if not smi:
        return {
            "memory_used_mb": None,
            "memory_total_mb": None,
            "utilization_pct": None,
            "temperature_c": None,
            "power_w": None,
            "source": "unavailable",
        }

    cmd = [
        smi,
        "--id=0",
        "--query-gpu=memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw",
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
            return {
                "memory_used_mb": None,
                "memory_total_mb": None,
                "utilization_pct": None,
                "temperature_c": None,
                "power_w": None,
                "source": "unavailable",
            }

        first_line = proc.stdout.strip().splitlines()[0]
        parts = [p.strip() for p in first_line.split(",")]

        # nvidia-smi reports in MiB; contracts/schema.py defines decimal MB:
        # MB = MiB * 1024 * 1024 / 1_000_000
        def field(index):
            try:
                value = float(parts[index])
                import math
                return value if math.isfinite(value) and value >= 0 else None
            except (ValueError, IndexError):
                return None

        used_mib, total_mib, util_pct, temp_c, power_w = (field(i) for i in range(5))

        used_mb = round(used_mib * 1024 * 1024 / 1_000_000, 2) if used_mib is not None else None
        total_mb = round(total_mib * 1024 * 1024 / 1_000_000, 2) if total_mib is not None else None

        return {
            "memory_used_mb": used_mb,
            "memory_total_mb": total_mb,
            "utilization_pct": util_pct,
            "temperature_c": temp_c,
            "power_w": power_w,
            "source": "nvidia-smi" if used_mb is not None else "unavailable",
        }
    except (subprocess.TimeoutExpired, OSError, ValueError, IndexError):
        return {
            "memory_used_mb": None,
            "memory_total_mb": None,
            "utilization_pct": None,
            "temperature_c": None,
            "power_w": None,
            "source": "unavailable",
        }


if __name__ == "__main__":
    import json
    print(json.dumps(collect_gpu_metrics(), indent=2))
