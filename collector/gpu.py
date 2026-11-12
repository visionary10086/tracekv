"""Real GPU memory, compute & PCIe telemetry sampler for NVIDIA GPUs (WSL2 / Linux).

Supports two collection backends:
1. Direct ctypes NVML (libnvidia-ml.so): zero-subprocess query (< 0.2ms) extracting
   VRAM distribution, core compute %, memory bandwidth %, PCIe Tx/Rx throughput,
   temperature, power, clocks, and P-State.
2. nvidia-smi CLI fallback: robust subprocess parsing with individual field fault tolerance.

Converts memory to standard decimal MB consistent with `contracts/schema.py` and
`docs/联调接口规范_v0.2.md`.

Supports one-shot collection, continuous background monitoring (`GpuMonitor`),
and parsing of legacy sample logs.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import argparse
import ctypes
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
_NVML_LIB_CANDIDATES = [
    "/usr/lib/wsl/lib/libnvidia-ml.so.1",
    "/usr/lib/wsl/lib/libnvidia-ml.so",
    "libnvidia-ml.so.1",
    "libnvidia-ml.so",
]


class _nvmlMemory_t(ctypes.Structure):
    _fields_ = [
        ("total", ctypes.c_ulonglong),
        ("free", ctypes.c_ulonglong),
        ("used", ctypes.c_ulonglong),
    ]


class _nvmlUtilization_t(ctypes.Structure):
    _fields_ = [
        ("gpu", ctypes.c_uint),
        ("memory", ctypes.c_uint),
    ]


class _NvmlDriver:
    """Singleton wrapper around libnvidia-ml.so via ctypes."""

    _instance: _NvmlDriver | None = None
    _init_lock = threading.Lock()

    def __init__(self) -> None:
        self.lib: ctypes.CDLL | None = None
        self.initialized = False

        for candidate in _NVML_LIB_CANDIDATES:
            try:
                if os.path.exists(candidate) or not candidate.startswith("/"):
                    self.lib = ctypes.CDLL(candidate)
                    ret = self.lib.nvmlInit_v2()
                    if ret == 0:
                        self.initialized = True
                        break
            except Exception:
                self.lib = None
                self.initialized = False

    @classmethod
    def get_instance(cls) -> _NvmlDriver:
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def query_device(self, gpu_index: int = 0) -> dict[str, Any] | None:
        """Query GPU metrics directly via NVML in < 0.2ms."""
        if not self.initialized or self.lib is None:
            return None

        try:
            dev = ctypes.c_void_p()
            ret = self.lib.nvmlDeviceGetHandleByIndex_v2(gpu_index, ctypes.byref(dev))
            if ret != 0:
                return None

            # UUID
            uuid_buf = ctypes.create_string_buffer(64)
            self.lib.nvmlDeviceGetUUID(dev, uuid_buf, 64)
            uuid_str = uuid_buf.value.decode("utf-8") if uuid_buf.value else None

            # Memory (bytes -> decimal MB: 1 MB = 1,000,000 bytes)
            mem = _nvmlMemory_t()
            ret_mem = self.lib.nvmlDeviceGetMemoryInfo(dev, ctypes.byref(mem))
            if ret_mem == 0:
                tot_mb = round(mem.total / 1_000_000, 2)
                used_mb = round(mem.used / 1_000_000, 2)
                free_mb = round(mem.free / 1_000_000, 2)
                used_pct = round((used_mb / tot_mb) * 100.0, 1) if tot_mb > 0 else None
            else:
                tot_mb = used_mb = free_mb = used_pct = None

            # Utilization (core compute % & memory bandwidth %)
            util = _nvmlUtilization_t()
            ret_util = self.lib.nvmlDeviceGetUtilizationRates(dev, ctypes.byref(util))
            if ret_util == 0:
                util_gpu = float(util.gpu)
                util_mem = float(util.memory)
            else:
                util_gpu = util_mem = None

            # Temperature
            temp = ctypes.c_uint()
            ret_temp = self.lib.nvmlDeviceGetTemperature(dev, 0, ctypes.byref(temp))
            temp_c = float(temp.value) if ret_temp == 0 else None

            # Power (milliwatts -> W)
            power = ctypes.c_uint()
            ret_pow = self.lib.nvmlDeviceGetPowerUsage(dev, ctypes.byref(power))
            power_w = round(power.value / 1000.0, 2) if ret_pow == 0 else None

            # Performance state
            pstate = ctypes.c_uint()
            ret_ps = self.lib.nvmlDeviceGetPerformanceState(dev, ctypes.byref(pstate))
            pstate_str = f"P{pstate.value}" if ret_ps == 0 else None

            # Clocks
            sm_clk = ctypes.c_uint()
            ret_sm = self.lib.nvmlDeviceGetClockInfo(dev, 1, ctypes.byref(sm_clk))
            sm_mhz = int(sm_clk.value) if ret_sm == 0 else None

            mem_clk = ctypes.c_uint()
            ret_mem_c = self.lib.nvmlDeviceGetClockInfo(dev, 2, ctypes.byref(mem_clk))
            mem_mhz = int(mem_clk.value) if ret_mem_c == 0 else None

            # PCIe throughput (0=TX, 1=RX, in KB/s)
            tx = ctypes.c_uint()
            rx = ctypes.c_uint()
            ret_tx = self.lib.nvmlDeviceGetPcieThroughput(dev, 0, ctypes.byref(tx))
            ret_rx = self.lib.nvmlDeviceGetPcieThroughput(dev, 1, ctypes.byref(rx))
            pcie_tx_kb = int(tx.value) if ret_tx == 0 else None
            pcie_rx_kb = int(rx.value) if ret_rx == 0 else None

            return {
                "gpu_index": gpu_index,
                "gpu_uuid": uuid_str,
                "memory_used_mb": used_mb,
                "memory_total_mb": tot_mb,
                "memory_free_mb": free_mb,
                "memory_used_pct": used_pct,
                "utilization_pct": util_gpu,
                "utilization_memory_pct": util_mem,
                "temperature_c": temp_c,
                "power_w": power_w,
                "pstate": pstate_str,
                "sm_clock_mhz": sm_mhz,
                "mem_clock_mhz": mem_mhz,
                "pcie_tx_kb_per_s": pcie_tx_kb,
                "pcie_rx_kb_per_s": pcie_rx_kb,
                "source": "nvml",
            }
        except Exception:
            return None


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


def _parse_str(val: str | None) -> str | None:
    """Parse a string property, returning None if missing or unsupported."""
    if val is None:
        return None
    cleaned = val.strip()
    if not cleaned or cleaned.lower() in ("n/a", "[not supported]", "not supported", "[n/a]"):
        return None
    return cleaned


def collect_gpu_metrics(
    binary_path: str | None = None,
    gpu_index: int = 0,
    run_id: str = "",
    prefer_nvml: bool = True,
) -> dict[str, Any]:
    """Query rich GPU metrics conforming to TraceKV v0.2 specification.

    Prioritizes direct ctypes NVML (< 0.2ms, zero subprocess), automatically
    falling back to nvidia-smi CLI.
    """
    now_iso = _utc_now_iso()

    # 1. Try fast NVML path if not forcing custom binary
    if prefer_nvml and not binary_path:
        nvml_driver = _NvmlDriver.get_instance()
        nvml_data = nvml_driver.query_device(gpu_index)
        if nvml_data is not None:
            nvml_data["run_id"] = run_id
            nvml_data["timestamp"] = now_iso
            return nvml_data

    # 2. Fallback to nvidia-smi CLI
    base_unavailable: dict[str, Any] = {
        "run_id": run_id,
        "timestamp": now_iso,
        "gpu_index": gpu_index,
        "gpu_uuid": None,
        "memory_used_mb": None,
        "memory_total_mb": None,
        "memory_free_mb": None,
        "memory_used_pct": None,
        "utilization_pct": None,
        "utilization_memory_pct": None,
        "temperature_c": None,
        "power_w": None,
        "pstate": None,
        "sm_clock_mhz": None,
        "mem_clock_mhz": None,
        "pcie_tx_kb_per_s": None,
        "pcie_rx_kb_per_s": None,
        "source": "unavailable",
    }

    smi = binary_path or _find_nvidia_smi()
    if not smi:
        return base_unavailable

    cmd = [
        smi,
        f"--id={gpu_index}",
        "--query-gpu=index,gpu_uuid,memory.used,memory.total,memory.free,utilization.gpu,utilization.memory,temperature.gpu,power.draw,pstate,clocks.current.sm,clocks.current.memory",
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
        free_mib: float | None = None
        util_pct: float | None = None
        util_mem_pct: float | None = None
        temp_c: float | None = None
        power_w: float | None = None
        pstate_str: str | None = None
        sm_clk: int | None = None
        mem_clk: int | None = None

        if len(parts) >= 12:
            parsed_index = _parse_int(parts[0])
            eff_index = parsed_index if parsed_index is not None else gpu_index
            uuid_str = _parse_str(parts[1])
            used_mib = _parse_float(parts[2])
            total_mib = _parse_float(parts[3])
            free_mib = _parse_float(parts[4])
            util_pct = _parse_float(parts[5])
            util_mem_pct = _parse_float(parts[6])
            temp_c = _parse_float(parts[7])
            power_w = _parse_float(parts[8])
            pstate_str = _parse_str(parts[9])
            sm_clk = _parse_int(parts[10])
            mem_clk = _parse_int(parts[11])
        elif len(parts) == 7:
            parsed_index = _parse_int(parts[0])
            eff_index = parsed_index if parsed_index is not None else gpu_index
            uuid_str = _parse_str(parts[1])
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
        free_mb = round(free_mib * 1024 * 1024 / 1_000_000, 2) if free_mib is not None else None
        used_pct = round((used_mb / total_mb) * 100.0, 1) if used_mb is not None and total_mb else None

        return {
            "run_id": run_id,
            "timestamp": now_iso,
            "gpu_index": eff_index,
            "gpu_uuid": uuid_str,
            "memory_used_mb": used_mb,
            "memory_total_mb": total_mb,
            "memory_free_mb": free_mb,
            "memory_used_pct": used_pct,
            "utilization_pct": util_pct,
            "utilization_memory_pct": util_mem_pct,
            "temperature_c": temp_c,
            "power_w": power_w,
            "pstate": pstate_str,
            "sm_clock_mhz": sm_clk,
            "mem_clock_mhz": mem_clk,
            "pcie_tx_kb_per_s": None,
            "pcie_rx_kb_per_s": None,
            "source": "nvidia-smi",
        }
    except (subprocess.TimeoutExpired, OSError, ValueError, IndexError):
        return base_unavailable


def parse_legacy_gpu_sample(record: dict[str, Any] | str, run_id: str = "") -> dict[str, Any]:
    """Parse a legacy or v0.2 GPU sample into a standardized v0.2 sample dict."""
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
    used_pct = round((used_mb / total_mb) * 100.0, 1) if used_mb is not None and total_mb else None

    return {
        "run_id": record.get("run_id") or run_id,
        "timestamp": time_str,
        "gpu_index": record.get("gpu_index", 0),
        "gpu_uuid": record.get("gpu_uuid"),
        "memory_used_mb": used_mb,
        "memory_total_mb": total_mb,
        "memory_free_mb": None,
        "memory_used_pct": used_pct,
        "utilization_pct": util_pct,
        "utilization_memory_pct": None,
        "temperature_c": temp_c,
        "power_w": power_w,
        "pstate": None,
        "sm_clock_mhz": None,
        "mem_clock_mhz": None,
        "pcie_tx_kb_per_s": None,
        "pcie_rx_kb_per_s": None,
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
        prefer_nvml: bool = True,
    ):
        self.run_id = run_id
        self.output_path = Path(output_path)
        self.interval_sec = interval_sec
        self.binary_path = binary_path
        self.gpu_index = gpu_index
        self.prefer_nvml = prefer_nvml

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
                    prefer_nvml=self.prefer_nvml,
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
    parser = argparse.ArgumentParser(description="TraceKV NVML & GPU Telemetry Sampler (v0.2)")
    parser.add_argument("--run-id", default="test-gpu-run", help="Experiment run ID")
    parser.add_argument("--out", type=Path, default=None, help="Output JSONL path")
    parser.add_argument("--interval", type=float, default=1.0, help="Sampling interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Monitoring duration (0 = single shot)")
    parser.add_argument("--gpu-index", type=int, default=0, help="GPU index to query")
    parser.add_argument("--no-nvml", action="store_true", help="Force nvidia-smi CLI instead of NVML")
    args = parser.parse_args()

    prefer_nvml = not args.no_nvml

    if args.duration <= 0 and args.out is None:
        sample = collect_gpu_metrics(
            gpu_index=args.gpu_index,
            run_id=args.run_id,
            prefer_nvml=prefer_nvml,
        )
        print(json.dumps(sample, indent=2, ensure_ascii=False))
        return

    out_path = args.out or Path(f"results/{args.run_id}/gpu_samples.jsonl")
    duration = args.duration if args.duration > 0 else 2.0
    backend = "NVML ctypes (<0.2ms)" if prefer_nvml else "nvidia-smi"
    print(f"Monitoring GPU {args.gpu_index} via {backend} for {duration}s -> {out_path}")
    with GpuMonitor(
        args.run_id,
        out_path,
        interval_sec=args.interval,
        gpu_index=args.gpu_index,
        prefer_nvml=prefer_nvml,
    ) as mon:
        time.sleep(duration)
    print(f"Done. Collected {mon.sample_count} samples.")


if __name__ == "__main__":
    main()
