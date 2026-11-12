"""Cross-layer time-series alignment and correlation engine for TraceKV.

Aligns high-frequency eBPF scheduler latency events, GPU telemetry samples,
and vLLM inference requests into synchronized time windows, enabling empirical
correlation analysis between CPU scheduling contention and LLM TTFT / KV Cache metrics.

Zero third-party dependencies beyond standard Python 3.12.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_to_epoch_sec(iso_str: str) -> float:
    """Parse ISO 8601 string to UTC epoch seconds float."""
    clean = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean)
    return dt.timestamp()


def _percentile(values: list[float], p: float) -> float | None:
    """Calculate p-th percentile from an already sorted list of floats."""
    if not values:
        return None
    k = math.ceil((p / 100.0) * len(values)) - 1
    k = max(0, min(k, len(values) - 1))
    return values[k]


def correlate_experiment_data(
    sched_events_path: Path | str | None = None,
    sched_summary_path: Path | str | None = None,
    gpu_samples_path: Path | str | None = None,
    requests_path: Path | str | None = None,
    bucket_sec: float = 1.0,
) -> list[dict[str, Any]]:
    """Align scheduler, GPU, and request logs into uniform time buckets.

    Parameters
    ----------
    sched_events_path:
        Path to JSONL file containing `wakeup_to_run_latency` events.
    sched_summary_path:
        Path to summary JSON with `start_wall_ns` and `start_monotonic_ns` anchors.
    gpu_samples_path:
        Path to JSONL file with `gpu_samples.jsonl` from nvidia-smi monitoring.
    requests_path:
        Path to JSONL file with client request records (`requests.jsonl`).
    bucket_sec:
        Aggregation time bucket width in seconds (default 1.0s).

    Returns
    -------
    list[dict]
        Chronologically sorted list of bucket dictionaries with aligned metrics.
    """
    # 1. Load time anchor for eBPF clock calibration
    wall_anchor_ns: int | None = None
    mono_anchor_ns: int | None = None

    if sched_summary_path and Path(sched_summary_path).exists():
        with open(sched_summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
            wall_anchor_ns = summary.get("start_wall_ns")
            mono_anchor_ns = summary.get("start_monotonic_ns")

    # 2. Ingest eBPF scheduler events
    sched_items: list[tuple[float, float, str]] = []  # (epoch_sec, latency_us, comm)
    if sched_events_path and Path(sched_events_path).exists():
        with open(sched_events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    raw_mono_ns = record.get("timestamp_ns")
                    lat_ns = record.get("value", 0)
                    comm = record.get("comm", "")

                    if raw_mono_ns is not None:
                        if wall_anchor_ns is not None and mono_anchor_ns is not None:
                            wall_ns = wall_anchor_ns + (raw_mono_ns - mono_anchor_ns)
                            epoch_sec = wall_ns / 1e9
                        else:
                            # Fallback if no anchor: use relative seconds from 0
                            epoch_sec = raw_mono_ns / 1e9
                        sched_items.append((epoch_sec, lat_ns / 1000.0, comm))
                except (json.JSONDecodeError, KeyError):
                    continue

    # 3. Ingest GPU samples
    gpu_items: list[tuple[float, float, float, float]] = []  # (epoch_sec, mem_mb, util_pct, power_w)
    if gpu_samples_path and Path(gpu_samples_path).exists():
        with open(gpu_samples_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    t_str = rec.get("time_utc")
                    val_str = rec.get("values_memory_MiB_util_percent_temp_C_power_W", "")
                    if t_str and val_str:
                        sec = _iso_to_epoch_sec(t_str)
                        parts = [p.strip() for p in val_str.split(",")]
                        mem_mib = float(parts[0])
                        util = float(parts[1]) if len(parts) > 1 else 0.0
                        power = float(parts[4]) if len(parts) > 4 else 0.0
                        mem_mb = mem_mib * 1024 * 1024 / 1_000_000
                        gpu_items.append((sec, mem_mb, util, power))
                except Exception:
                    continue

    # 4. Ingest request events
    req_items: list[tuple[float, float, int]] = []  # (epoch_sec, ttft_ms, tokens)
    if requests_path and Path(requests_path).exists():
        with open(requests_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    t_str = rec.get("started_at_utc")
                    ttft = rec.get("ttft_ms")
                    tokens = rec.get("completion_tokens") or 0
                    if t_str and ttft is not None:
                        sec = _iso_to_epoch_sec(t_str)
                        req_items.append((sec, float(ttft), int(tokens)))
                except Exception:
                    continue

    # Determine overall time bounds
    all_timestamps: list[float] = []
    if sched_items:
        all_timestamps.extend([t for t, _, _ in sched_items])
    if gpu_items:
        all_timestamps.extend([t for t, _, _, _ in gpu_items])
    if req_items:
        all_timestamps.extend([t for t, _, _ in req_items])

    if not all_timestamps:
        return []

    min_t = min(all_timestamps)
    max_t = max(all_timestamps)
    num_buckets = max(1, math.ceil((max_t - min_t) / bucket_sec))

    buckets: list[dict[str, Any]] = []
    for b_idx in range(num_buckets):
        b_start = min_t + b_idx * bucket_sec
        b_end = b_start + bucket_sec

        # Scheduler events in this bucket
        b_sched = [lat for t, lat, _ in sched_items if b_start <= t < b_end]
        b_sched.sort()

        # GPU samples in this bucket
        b_gpu_mem = [m for t, m, _, _ in gpu_items if b_start <= t < b_end]
        b_gpu_util = [u for t, _, u, _ in gpu_items if b_start <= t < b_end]

        # Requests in this bucket
        b_req_ttft = [ttft for t, ttft, _ in req_items if b_start <= t < b_end]
        b_req_tokens = sum(tok for t, _, tok in req_items if b_start <= t < b_end)

        iso_time = datetime.fromtimestamp(b_start, tz=timezone.utc).isoformat().replace("+00:00", "Z")

        buckets.append({
            "bucket_index": b_idx,
            "timestamp_utc": iso_time,
            "window_duration_sec": bucket_sec,
            "sched": {
                "event_count": len(b_sched),
                "p50_us": _percentile(b_sched, 50),
                "p95_us": _percentile(b_sched, 95),
                "p99_us": _percentile(b_sched, 99),
                "mean_us": round(sum(b_sched) / len(b_sched), 2) if b_sched else None,
            },
            "gpu": {
                "sample_count": len(b_gpu_mem),
                "memory_used_mb": round(sum(b_gpu_mem) / len(b_gpu_mem), 2) if b_gpu_mem else None,
                "utilization_pct": round(sum(b_gpu_util) / len(b_gpu_util), 1) if b_gpu_util else None,
            },
            "requests": {
                "request_count": len(b_req_ttft),
                "ttft_p50_ms": _percentile(sorted(b_req_ttft), 50),
                "ttft_p95_ms": _percentile(sorted(b_req_ttft), 95),
                "completion_tokens": b_req_tokens,
            },
        })

    return buckets


def compute_correlation(x: list[float], y: list[float]) -> float | None:
    """Compute Pearson correlation coefficient r between two series."""
    if len(x) != len(y) or len(x) < 2:
        return None
    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n

    cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    var_x = sum((x[i] - mean_x) ** 2 for i in range(n))
    var_y = sum((y[i] - mean_y) ** 2 for i in range(n))

    denom = math.sqrt(var_x * var_y)
    if denom == 0:
        return 0.0
    return round(cov / denom, 4)
