"""Cross-layer time-series alignment and correlation engine for TraceKV.

Aligns high-frequency eBPF scheduler latency events, GPU telemetry samples,
Linux system memory metrics, vLLM Prometheus metrics, and inference requests into
synchronized time windows conforming strictly to TraceKV v0.2 integration contract.

Supports:
- Clock anchoring: `wall_ns = start_wall_ns + event_monotonic_ns - start_monotonic_ns`
- Left-closed, right-open time windows `[window_start_utc, window_end_utc)`
- Generating `aligned_snapshots.jsonl` passing `contracts/schema.py:validate_snapshot`
- Pearson correlation analysis between CPU scheduling contention and TTFT

Zero third-party dependencies beyond standard Python 3.12.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any

from collector.gpu import parse_legacy_gpu_sample
from contracts.schema import validate_snapshot


def _iso_to_epoch_sec(iso_str: str) -> float:
    """Parse ISO 8601 string to UTC epoch seconds float."""
    clean = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean)
    return dt.timestamp()


def _epoch_to_iso_utc(sec: float) -> str:
    """Convert UTC epoch seconds float to ISO 8601 UTC string ending in 'Z'."""
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _percentile(values: list[float], p: float) -> float | None:
    """Calculate p-th percentile from an already sorted list of floats."""
    if not values:
        return None
    k = math.ceil((p / 100.0) * len(values)) - 1
    k = max(0, min(k, len(values) - 1))
    return round(values[k], 2)


def correlate_experiment_data(
    sched_events_path: Path | str | None = None,
    sched_summary_path: Path | str | None = None,
    gpu_samples_path: Path | str | None = None,
    system_samples_path: Path | str | None = None,
    kv_samples_path: Path | str | None = None,
    requests_path: Path | str | None = None,
    bucket_sec: float = 1.0,
    strict_anchor: bool = False,
) -> list[dict[str, Any]]:
    """Align scheduler, GPU, system, KV, and request logs into uniform time buckets.

    Parameters
    ----------
    sched_events_path:
        Path to JSONL file containing `wakeup_to_run_latency` events.
    sched_summary_path:
        Path to summary JSON with `start_wall_ns` and `start_monotonic_ns` anchors.
    gpu_samples_path:
        Path to JSONL file with `gpu_samples.jsonl`.
    system_samples_path:
        Path to JSONL file with `system_samples.jsonl`.
    kv_samples_path:
        Path to JSONL file with `kv_samples.jsonl`.
    requests_path:
        Path to JSONL file with client request records (`requests.jsonl`).
    bucket_sec:
        Aggregation time bucket width in seconds (default 1.0s).
    strict_anchor:
        If True, raise ValueError when eBPF events cannot be calibrated to UTC.

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

    has_calibrated_anchor = wall_anchor_ns is not None and mono_anchor_ns is not None

    # 2. Ingest eBPF scheduler events
    sched_items: list[tuple[float, float, str]] = []  # (epoch_sec, latency_us, comm)
    if sched_events_path and Path(sched_events_path).exists():
        if strict_anchor and not has_calibrated_anchor:
            raise ValueError(
                "Missing clock calibration anchors (start_wall_ns / start_monotonic_ns) "
                "in sched_summary.json; refusing uncalibrated cross-layer alignment."
            )

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
                        if has_calibrated_anchor:
                            # Formula per v0.2 spec: wall_ns = start_wall_ns + event_mono_ns - start_mono_ns
                            wall_ns = wall_anchor_ns + (raw_mono_ns - mono_anchor_ns)
                            epoch_sec = wall_ns / 1e9
                        else:
                            # Fallback only when strict_anchor is False (e.g. standalone test)
                            epoch_sec = raw_mono_ns / 1e9
                        sched_items.append((epoch_sec, lat_ns / 1000.0, comm))
                except (json.JSONDecodeError, KeyError):
                    continue

    # 3. Ingest GPU samples (supporting v0.2 and legacy formats)
    gpu_items: list[tuple[float, float | None, float | None, float | None]] = []  # (epoch_sec, mem_mb, util_pct, power_w)
    if gpu_samples_path and Path(gpu_samples_path).exists():
        with open(gpu_samples_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    norm = parse_legacy_gpu_sample(line)
                    t_str = norm.get("timestamp")
                    if t_str:
                        sec = _iso_to_epoch_sec(t_str)
                        gpu_items.append((
                            sec,
                            norm.get("memory_used_mb"),
                            norm.get("utilization_pct"),
                            norm.get("power_w"),
                        ))
                except Exception:
                    continue

    # 4. Ingest system samples
    sys_items: list[tuple[float, float | None]] = []  # (epoch_sec, mem_available_mb)
    if system_samples_path and Path(system_samples_path).exists():
        with open(system_samples_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    t_str = rec.get("timestamp")
                    if t_str and rec.get("mem_available_mb") is not None:
                        sys_items.append((_iso_to_epoch_sec(t_str), float(rec["mem_available_mb"])))
                except Exception:
                    continue

    # 5. Ingest vLLM KV samples
    kv_items: list[tuple[float, float | None, float | None, int | None]] = []  # (sec, usage, prefix, running)
    if kv_samples_path and Path(kv_samples_path).exists():
        with open(kv_samples_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    t_str = rec.get("timestamp")
                    if t_str:
                        sec = _iso_to_epoch_sec(t_str)
                        kv_items.append((
                            sec,
                            rec.get("usage_ratio"),
                            rec.get("prefix_hit_ratio"),
                            rec.get("running_requests"),
                        ))
                except Exception:
                    continue

    # 6. Ingest request events
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
    if sys_items:
        all_timestamps.extend([t for t, _ in sys_items])
    if kv_items:
        all_timestamps.extend([t for t, _, _, _ in kv_items])
    if req_items:
        all_timestamps.extend([t for t, _, _ in req_items])

    if not all_timestamps:
        return []

    min_t = min(all_timestamps)
    max_t = max(all_timestamps)
    num_buckets = max(1, math.ceil((max_t - min_t) / bucket_sec))
    if num_buckets > 100_000:
        raise ValueError(
            f"Time span ({max_t - min_t:.1f}s) creates {num_buckets} buckets, "
            "exceeding safety limit (100,000). Check source timestamps."
        )

    buckets: list[dict[str, Any]] = []
    for b_idx in range(num_buckets):
        b_start = min_t + b_idx * bucket_sec
        b_end = b_start + bucket_sec

        # Scheduler events in this bucket [b_start, b_end)
        b_sched = [lat for t, lat, _ in sched_items if b_start <= t < b_end]
        b_sched.sort()

        # GPU samples in this bucket
        b_gpu_mem = [m for t, m, _, _ in gpu_items if b_start <= t < b_end and m is not None]
        b_gpu_util = [u for t, _, u, _ in gpu_items if b_start <= t < b_end and u is not None]

        # Requests in this bucket
        b_req_ttft = [ttft for t, ttft, _ in req_items if b_start <= t < b_end]
        b_req_tokens = sum(tok for t, _, tok in req_items if b_start <= t < b_end)

        iso_time = _epoch_to_iso_utc(b_start)

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


def generate_aligned_snapshots(
    run_id: str,
    sched_events_path: Path | str | None = None,
    sched_summary_path: Path | str | None = None,
    gpu_samples_path: Path | str | None = None,
    system_samples_path: Path | str | None = None,
    kv_samples_path: Path | str | None = None,
    bucket_sec: float = 1.0,
    strict_anchor: bool = False,
) -> list[dict[str, Any]]:
    """Generate windowed aligned snapshots strictly conforming to TraceKV v0.2 contract.

    Each window produces one snapshot validated against `contracts/schema.py:validate_snapshot`.
    Preserves all extension fields:
        `window_start_utc`, `window_end_utc`, `sched.scope: "system"`, `sched.p50_us`, etc.
    """
    # 1. Ingest calibrated anchors
    wall_anchor_ns: int | None = None
    mono_anchor_ns: int | None = None
    if sched_summary_path and Path(sched_summary_path).exists():
        with open(sched_summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
            wall_anchor_ns = summary.get("start_wall_ns")
            mono_anchor_ns = summary.get("start_monotonic_ns")

    has_anchor = wall_anchor_ns is not None and mono_anchor_ns is not None

    # 2. Ingest scheduler events
    sched_events: list[tuple[float, float, bool]] = []  # (epoch_sec, latency_us, is_target)
    sched_source = "unavailable"
    has_target_events = False
    if sched_events_path and Path(sched_events_path).exists():
        if strict_anchor and not has_anchor:
            raise ValueError("Cannot align eBPF events to UTC: missing start_wall_ns / start_monotonic_ns.")
        if has_anchor:
            with open(sched_events_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        mono_ns = record.get("timestamp_ns")
                        lat_ns = record.get("value", 0)
                        is_tgt = bool(record.get("is_target", False))
                        if is_tgt:
                            has_target_events = True
                        if mono_ns is not None:
                            wall_ns = wall_anchor_ns + (mono_ns - mono_anchor_ns)
                            sched_events.append((wall_ns / 1e9, lat_ns / 1000.0, is_tgt))
                    except Exception:
                        continue
            sched_source = "ebpf_sched"

    # 3. Ingest GPU samples
    gpu_samples: list[tuple[float, dict[str, Any]]] = []
    if gpu_samples_path and Path(gpu_samples_path).exists():
        with open(gpu_samples_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    norm = parse_legacy_gpu_sample(line)
                    t_str = norm.get("timestamp")
                    if t_str:
                        sec = _iso_to_epoch_sec(t_str)
                        gpu_samples.append((sec, norm))
                except Exception:
                    continue

    # 4. Ingest system samples
    sys_samples: list[tuple[float, dict[str, Any]]] = []
    if system_samples_path and Path(system_samples_path).exists():
        with open(system_samples_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    t_str = rec.get("timestamp")
                    if t_str and rec.get("mem_available_mb") is not None:
                        sys_samples.append((_iso_to_epoch_sec(t_str), rec))
                except Exception:
                    continue

    # 5. Ingest KV samples
    kv_samples: list[tuple[float, float | None, float | None, int | None]] = []
    if kv_samples_path and Path(kv_samples_path).exists():
        with open(kv_samples_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    t_str = rec.get("timestamp")
                    if t_str:
                        sec = _iso_to_epoch_sec(t_str)
                        kv_samples.append((
                            sec,
                            rec.get("usage_ratio"),
                            rec.get("prefix_hit_ratio"),
                            rec.get("running_requests"),
                        ))
                except Exception:
                    continue

    # Collect time bounds across available sources
    all_ts: list[float] = []
    if sched_events:
        all_ts.extend([t for t, *_ in sched_events])
    if gpu_samples:
        all_ts.extend([t for t, _ in gpu_samples])
    if sys_samples:
        all_ts.extend([t for t, _ in sys_samples])
    if kv_samples:
        all_ts.extend([t for t, _, _, _ in kv_samples])

    if not all_ts:
        return []

    min_t = min(all_ts)
    max_t = max(all_ts)
    num_windows = max(1, math.ceil((max_t - min_t) / bucket_sec))
    if num_windows > 100_000:
        raise ValueError(
            f"Time span ({max_t - min_t:.1f}s) creates {num_windows} windows, "
            "exceeding safety limit (100,000). Check source timestamps."
        )
    window_ms = int(round(bucket_sec * 1000))

    snapshots: list[dict[str, Any]] = []

    for idx in range(num_windows):
        w_start = min_t + idx * bucket_sec
        w_end = w_start + bucket_sec
        w_start_utc = _epoch_to_iso_utc(w_start)
        w_end_utc = _epoch_to_iso_utc(w_end)

        # 1. System section
        w_sys = [rec for t, rec in sys_samples if w_start <= t < w_end]
        if w_sys:
            mems = [rec.get("mem_available_mb") for rec in w_sys if rec.get("mem_available_mb") is not None]
            mem_avail = round(sum(mems) / len(mems), 3) if mems else None
            disk_r = [rec.get("disk_read_bytes_per_s") for rec in w_sys if rec.get("disk_read_bytes_per_s") is not None]
            disk_r_bytes = round(sum(disk_r) / len(disk_r), 1) if disk_r else None
            sys_src = "proc"
        else:
            mem_avail = None
            disk_r_bytes = None
            sys_src = "unavailable"

        system_section: dict[str, Any] = {
            "mem_available_mb": mem_avail,
            "disk_read_p95_ms": None,
            "disk_read_bytes_per_s": disk_r_bytes,
        }
        if w_sys:
            latest_sys = w_sys[-1]
            for ext_k in ("cpu", "memory", "psi", "io", "target_process"):
                if ext_k in latest_sys and latest_sys[ext_k] is not None:
                    system_section[ext_k] = latest_sys[ext_k]

        # 2. GPU section
        w_gpu = [norm for t, norm in gpu_samples if w_start <= t < w_end]
        if w_gpu:
            m_used = [norm.get("memory_used_mb") for norm in w_gpu if norm.get("memory_used_mb") is not None]
            gpu_mem = round(sum(m_used) / len(m_used), 2) if m_used else None
            gpu_src = w_gpu[-1].get("source", "nvidia-smi")
        else:
            gpu_mem = None
            gpu_src = "unavailable"

        gpu_section: dict[str, Any] = {"memory_used_mb": gpu_mem}
        if w_gpu:
            latest_gpu = w_gpu[-1]
            for ext_g in (
                "memory_total_mb",
                "memory_free_mb",
                "memory_used_pct",
                "utilization_pct",
                "utilization_memory_pct",
                "temperature_c",
                "power_w",
                "pstate",
                "sm_clock_mhz",
                "mem_clock_mhz",
                "pcie_tx_kb_per_s",
                "pcie_rx_kb_per_s",
            ):
                if ext_g in latest_gpu and latest_gpu[ext_g] is not None:
                    gpu_section[ext_g] = latest_gpu[ext_g]

        # 3. KV section
        w_kvs = [kv for kv in kv_samples if w_start <= kv[0] < w_end]
        if w_kvs:
            usage_vals = [kv[1] for kv in w_kvs if kv[1] is not None]
            prefix_vals = [kv[2] for kv in w_kvs if kv[2] is not None]
            req_vals = [kv[3] for kv in w_kvs if kv[3] is not None]

            kv_usage = round(sum(usage_vals) / len(usage_vals), 4) if usage_vals else None
            kv_prefix = round(sum(prefix_vals) / len(prefix_vals), 4) if prefix_vals else None
            kv_running = int(round(sum(req_vals) / len(req_vals))) if req_vals else None
            kv_src = "vllm"
        else:
            kv_usage = None
            kv_prefix = None
            kv_running = None
            kv_src = "unavailable"

        kv_section: dict[str, Any] = {
            "usage_ratio": kv_usage,
            "prefix_hit_ratio": kv_prefix,
            "running_requests": kv_running,
        }

        # 4. Sched section (windowed - zero copying from global summary!)
        w_sched = [lat for t, lat, _ in sched_events if w_start <= t < w_end]
        w_sched.sort()
        w_target = [lat for t, lat, is_tgt in sched_events if w_start <= t < w_end and is_tgt]
        w_target.sort()

        if has_target_events:
            sched_section: dict[str, Any] = {
                "scope": "target_process",
                "event_count": len(w_target),
                "p50_us": _percentile(w_target, 50),
                "p95_us": _percentile(w_target, 95),
                "p99_us": _percentile(w_target, 99),
                "system_baseline": {
                    "event_count": len(w_sched),
                    "p50_us": _percentile(w_sched, 50),
                    "p95_us": _percentile(w_sched, 95),
                    "p99_us": _percentile(w_sched, 99),
                },
            }
        else:
            sched_section: dict[str, Any] = {
                "scope": "system",
                "event_count": len(w_sched),
                "p50_us": _percentile(w_sched, 50),
                "p95_us": _percentile(w_sched, 95),
                "p99_us": _percentile(w_sched, 99),
            }

        # Sources dictionary
        sources = {
            "system": f"{sys_src}+{sched_source}" if sched_source != "unavailable" else sys_src,
            "gpu": gpu_src,
            "kv": kv_src,
        }

        snapshot: dict[str, Any] = {
            "schema_version": "0.1",
            "run_id": run_id,
            "timestamp": w_end_utc,
            "window_start_utc": w_start_utc,
            "window_end_utc": w_end_utc,
            "window_ms": window_ms,
            "system": system_section,
            "gpu": gpu_section,
            "kv": kv_section,
            "sources": sources,
            "sched": sched_section,
        }

        # Strict validation
        validated = validate_snapshot(snapshot)
        snapshots.append(validated)

    return snapshots


def write_aligned_snapshots(snapshots: list[dict[str, Any]], output_path: str | Path) -> int:
    """Write list of snapshots to an aligned_snapshots.jsonl file."""
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        for snap in snapshots:
            f.write(json.dumps(snap, ensure_ascii=False) + "\n")
    return len(snapshots)


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


def compute_lagged_correlation(
    x: list[float],
    y: list[float],
    max_lag: int = 3,
) -> dict[str, Any]:
    """Compute cross-correlation across temporal lags tau in [-max_lag, +max_lag].

    A positive lag tau > 0 with high positive r indicates series x leads series y
    by tau time windows (temporal precedence / potential causal driver).
    A negative lag tau < 0 indicates series y leads series x.
    """
    if len(x) != len(y) or len(x) < (max_lag + 2):
        return {
            "lags": [],
            "correlations": [],
            "peak_lag": 0,
            "peak_correlation": None,
            "causal_direction": "insufficient_data",
            "interpretation": "Insufficient sample points for lagged cross-correlation.",
        }

    lags = list(range(-max_lag, max_lag + 1))
    corrs: list[float | None] = []
    n = len(x)

    for lag in lags:
        if lag == 0:
            r = compute_correlation(x, y)
        elif lag > 0:
            r = compute_correlation(x[:n - lag], y[lag:])
        else:
            k = -lag
            r = compute_correlation(x[k:], y[:n - k])
        corrs.append(r)

    valid_pairs = [(lag, r) for lag, r in zip(lags, corrs) if r is not None]
    if not valid_pairs:
        return {
            "lags": lags,
            "correlations": corrs,
            "peak_lag": 0,
            "peak_correlation": None,
            "causal_direction": "undefined",
            "interpretation": "Correlation could not be computed (zero variance).",
        }

    peak_lag, peak_r = max(valid_pairs, key=lambda pair: abs(pair[1]))

    if abs(peak_r) < 0.3:
        direction = "no_correlation"
        interp = f"Weak or no correlation (|r| = {abs(peak_r):.2f} < 0.3)."
    elif peak_lag > 0:
        direction = "x_leads_y"
        interp = f"Series X leads Series Y by {peak_lag} window(s) (r={peak_r:.3f}), indicating temporal precedence."
    elif peak_lag < 0:
        direction = "y_leads_x"
        interp = f"Series Y leads Series X by {-peak_lag} window(s) (r={peak_r:.3f})."
    else:
        direction = "synchronous"
        interp = f"Synchronous co-movement (r={peak_r:.3f} at lag 0)."

    return {
        "lags": lags,
        "correlations": corrs,
        "peak_lag": peak_lag,
        "peak_correlation": peak_r,
        "causal_direction": direction,
        "interpretation": interp,
    }


def analyze_kv_pcie_transfers(
    kv_transfers_path: Path | str,
    gpu_samples_path: Path | str,
    sched_summary_path: Path | str | None = None,
    pcie_theoretical_bandwidth_mb_per_s: float = 31500.0,
) -> dict[str, Any]:
    """Correlate software KV Cache CPU-GPU offload events with hardware PCIe telemetry.

    Parameters
    ----------
    kv_transfers_path:
        Path to `kv_transfers.jsonl` produced by `tools/tracekv_offload.py`.
    gpu_samples_path:
        Path to `gpu_samples.jsonl` containing NVML PCIe metrics (`pcie_tx_kb_per_s`, `pcie_rx_kb_per_s`).
    sched_summary_path:
        Optional summary JSON with `start_wall_ns` and `start_monotonic_ns` anchors.
    pcie_theoretical_bandwidth_mb_per_s:
        Theoretical unidirectional link bandwidth in MB/s (default PCIe 4.0 x16 = ~31,500 MB/s).

    Returns
    -------
    dict[str, Any]
        Physical throughput validation, bus saturation %, and bottleneck assessment.
    """
    transfers_p = Path(kv_transfers_path)
    if not transfers_p.exists():
        return {
            "available": False,
            "reason": f"{kv_transfers_path} not found",
            "bottleneck": "NO_TRANSFERS",
        }

    transfers: list[dict[str, Any]] = []
    with open(transfers_p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("status") == "completed":
                    transfers.append(rec)
            except Exception:
                continue

    if not transfers:
        return {
            "available": True,
            "total_transfers": 0,
            "gpu_to_cpu_transfers": 0,
            "cpu_to_gpu_transfers": 0,
            "total_gpu_blocks": 0,
            "bottleneck": "NO_TRANSFERS",
            "summary": "No completed KV Cache transfers found.",
        }

    # Ingest GPU PCIe samples
    gpu_p = Path(gpu_samples_path)
    gpu_pcie_samples: list[tuple[float, float, float]] = []  # (epoch_sec, tx_mb_s, rx_mb_s)
    if gpu_p.exists():
        with open(gpu_p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    norm = parse_legacy_gpu_sample(line)
                    t_str = norm.get("timestamp")
                    tx_kb = norm.get("pcie_tx_kb_per_s")
                    rx_kb = norm.get("pcie_rx_kb_per_s")
                    if t_str:
                        sec = _iso_to_epoch_sec(t_str)
                        tx_mb = (tx_kb / 1000.0) if tx_kb is not None else 0.0
                        rx_mb = (rx_kb / 1000.0) if rx_kb is not None else 0.0
                        gpu_pcie_samples.append((sec, tx_mb, rx_mb))
                except Exception:
                    continue

    gpu_to_cpu = [t for t in transfers if t.get("direction") == "gpu_to_cpu"]
    cpu_to_gpu = [t for t in transfers if t.get("direction") == "cpu_to_gpu"]
    total_blocks = sum(t.get("gpu_blocks", 0) for t in transfers)
    elapsed_ms_list = sorted([t.get("elapsed_ms", 0.0) for t in transfers if t.get("elapsed_ms") is not None])

    peak_tx_mb_s = max([s[1] for s in gpu_pcie_samples], default=0.0)
    peak_rx_mb_s = max([s[2] for s in gpu_pcie_samples], default=0.0)
    avg_tx_mb_s = round(sum([s[1] for s in gpu_pcie_samples]) / len(gpu_pcie_samples), 2) if gpu_pcie_samples else 0.0
    avg_rx_mb_s = round(sum([s[2] for s in gpu_pcie_samples]) / len(gpu_pcie_samples), 2) if gpu_pcie_samples else 0.0

    peak_throughput_mb_s = max(peak_tx_mb_s, peak_rx_mb_s)
    bus_saturation_pct = round(
        (peak_throughput_mb_s / pcie_theoretical_bandwidth_mb_per_s) * 100.0, 2
    ) if pcie_theoretical_bandwidth_mb_per_s > 0 else 0.0

    elapsed_p50 = _percentile(elapsed_ms_list, 50)
    elapsed_p95 = _percentile(elapsed_ms_list, 95)
    elapsed_p99 = _percentile(elapsed_ms_list, 99)
    elapsed_mean = round(sum(elapsed_ms_list) / len(elapsed_ms_list), 2) if elapsed_ms_list else None

    if bus_saturation_pct >= 75.0:
        bottleneck = "PCIE_BUS_SATURATION"
        diagnostic = f"PCIe bus saturated ({bus_saturation_pct}% of theoretical link capacity). KV transfer rate bounded by physical interconnect."
    elif (elapsed_p95 is not None and elapsed_p95 >= 10.0) and bus_saturation_pct < 25.0:
        bottleneck = "HOST_QUEUE_OR_SCHEDULING_STALL"
        diagnostic = f"KV transfer latency high (P95={elapsed_p95}ms) but physical PCIe saturation is low ({bus_saturation_pct}%). Delay was dominated by host thread dispatch, polling, or CPU scheduling."
    else:
        bottleneck = "BALANCED_TRANSFER"
        diagnostic = f"KV transfers operating normally with {bus_saturation_pct}% peak PCIe utilization."

    return {
        "available": True,
        "total_transfers": len(transfers),
        "gpu_to_cpu_transfers": len(gpu_to_cpu),
        "cpu_to_gpu_transfers": len(cpu_to_gpu),
        "total_gpu_blocks": total_blocks,
        "elapsed_ms": {
            "p50": elapsed_p50,
            "p95": elapsed_p95,
            "p99": elapsed_p99,
            "mean": elapsed_mean,
        },
        "pcie_physical": {
            "theoretical_max_mb_per_s": pcie_theoretical_bandwidth_mb_per_s,
            "peak_tx_mb_per_s": round(peak_tx_mb_s, 2),
            "peak_rx_mb_per_s": round(peak_rx_mb_s, 2),
            "avg_tx_mb_per_s": avg_tx_mb_s,
            "avg_rx_mb_per_s": avg_rx_mb_s,
            "bus_saturation_pct": bus_saturation_pct,
        },
        "bottleneck": bottleneck,
        "diagnostic": diagnostic,
    }


def diagnose_system_bottlenecks(
    snapshots: list[dict[str, Any]],
    lagged_corr: dict[str, Any] | None = None,
    kv_pcie: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Automated root-cause bottleneck attribution across CPU, GPU, memory, and PCIe.

    Evaluates OS kernel scheduling delays, hardware interconnect saturation,
    Linux PSI pressure, and GPU compute starvation to pinpoint the primary
    performance limiter for the LLM inference experiment.
    """
    if not snapshots:
        return {
            "primary_bottleneck": "INSUFFICIENT_DATA",
            "severity": "INFO",
            "summary": "No aligned snapshots provided for bottleneck diagnosis.",
            "recommendations": ["Collect at least 2 aligned snapshots."],
        }

    run_id = snapshots[0].get("run_id", "unknown")

    sched_p99_list = [
        s.get("sched", {}).get("p99_us")
        for s in snapshots
        if s.get("sched", {}).get("p99_us") is not None
    ]
    sched_scope = snapshots[0].get("sched", {}).get("scope", "system")
    max_sched_p99_us = max(sched_p99_list, default=0.0)

    psi_cpu_list = []
    psi_mem_list = []
    psi_io_list = []
    for s in snapshots:
        psi = s.get("system", {}).get("psi") or {}
        if "psi_cpu_some_avg10" in psi and psi["psi_cpu_some_avg10"] is not None:
            psi_cpu_list.append(psi["psi_cpu_some_avg10"])
        if "psi_mem_some_avg10" in psi and psi["psi_mem_some_avg10"] is not None:
            psi_mem_list.append(psi["psi_mem_some_avg10"])
        if "psi_io_some_avg10" in psi and psi["psi_io_some_avg10"] is not None:
            psi_io_list.append(psi["psi_io_some_avg10"])

    max_psi_cpu = max(psi_cpu_list, default=0.0)
    max_psi_mem = max(psi_mem_list, default=0.0)
    max_psi_io = max(psi_io_list, default=0.0)

    gpu_util_list = [
        s.get("gpu", {}).get("utilization_pct")
        for s in snapshots
        if s.get("gpu", {}).get("utilization_pct") is not None
    ]
    avg_gpu_util = round(sum(gpu_util_list) / len(gpu_util_list), 1) if gpu_util_list else 0.0

    pcie_sat = (kv_pcie or {}).get("pcie_physical", {}).get("bus_saturation_pct", 0.0)
    kv_bot = (kv_pcie or {}).get("bottleneck")

    recommendations: list[str] = []

    if max_sched_p99_us >= 5000.0 or max_psi_cpu >= 25.0:
        primary = "HOST_SCHEDULING_CONTENTION"
        severity = "CRITICAL" if max_sched_p99_us >= 10000.0 else "WARNING"
        summary = (
            f"Host CPU scheduling contention detected: target thread queue delay P99 reached "
            f"{max_sched_p99_us / 1000.0:.2f}ms (PSI CPU avg10={max_psi_cpu:.1f}%). "
            f"Inference workers were delayed entering runqueue."
        )
        recommendations.append("Pin vLLM engine threads to dedicated physical cores using taskset / sched_setaffinity.")
        recommendations.append("Increase process scheduling priority or set SCHED_FIFO for time-critical token dispatch.")
        if lagged_corr and lagged_corr.get("causal_direction") == "x_leads_y":
            summary += f" Lagged cross-correlation confirmed CPU latency precedes GPU utilization drop by {lagged_corr['peak_lag']} window(s)."

    elif kv_bot == "PCIE_BUS_SATURATION" or pcie_sat >= 75.0:
        primary = "PCIE_BUS_SATURATION"
        severity = "WARNING"
        summary = f"Physical PCIe interconnect saturated ({pcie_sat}% utilization). CPU-GPU KV transfers are bounded by bus bandwidth."
        recommendations.append("Reduce KV Cache eviction frequency by allocating more GPU VRAM or increasing block size.")
        recommendations.append("Batch KV transfer requests to avoid concurrent bus contention.")

    elif max_psi_mem >= 20.0 or max_psi_io >= 20.0:
        primary = "SYSTEM_MEMORY_IO_PRESSURE"
        severity = "CRITICAL"
        summary = f"Host memory / IO pressure elevated (PSI Mem={max_psi_mem}%, PSI IO={max_psi_io}%). System is paging or under heavy disk load."
        recommendations.append("Reduce CPU offload memory buffer capacity to prevent host OS page thrashing.")
        recommendations.append("Isolate inference workload from concurrent disk I/O intensive processes.")

    elif avg_gpu_util >= 85.0 and max_sched_p99_us < 2000.0:
        primary = "GPU_COMPUTE_BOUND"
        severity = "INFO"
        summary = f"Inference is compute bound on GPU (average utilization={avg_gpu_util}%, CPU queue latency healthy < 2ms)."
        recommendations.append("System and scheduling baseline is healthy; consider tensor parallelism for higher throughput.")

    else:
        primary = "BALANCED"
        severity = "INFO"
        summary = "No dominant system bottleneck detected across scheduling, PCIe, memory, or GPU."
        recommendations.append("Workload operating within normal resource bounds.")

    return {
        "run_id": run_id,
        "primary_bottleneck": primary,
        "severity": severity,
        "summary": summary,
        "metrics_summary": {
            "sched_scope": sched_scope,
            "max_sched_p99_us": max_sched_p99_us,
            "max_psi_cpu_avg10": max_psi_cpu,
            "max_psi_mem_avg10": max_psi_mem,
            "max_psi_io_avg10": max_psi_io,
            "avg_gpu_utilization_pct": avg_gpu_util,
            "peak_pcie_bus_saturation_pct": pcie_sat,
        },
        "lagged_correlation": lagged_corr,
        "kv_pcie_transfer_analysis": kv_pcie,
        "recommendations": recommendations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TraceKV Cross-Layer Alignment & Diagnosis Engine (v0.2)")
    parser.add_argument("--run-id", required=True, help="Experiment run ID")
    parser.add_argument("--dir", type=Path, default=None, help="Directory containing experiment artifacts")
    parser.add_argument("--out", type=Path, default=None, help="Output aligned_snapshots.jsonl path")
    parser.add_argument("--bucket-sec", type=float, default=1.0, help="Window duration in seconds")
    parser.add_argument("--kv-transfers", type=Path, default=None, help="Path to kv_transfers.jsonl")
    parser.add_argument("--diagnose", action="store_true", help="Run automated bottleneck root cause diagnosis")
    parser.add_argument("--diagnosis-out", type=Path, default=None, help="Path for bottleneck_diagnosis.json output")
    args = parser.parse_args()

    base_dir = args.dir or Path(f"results/{args.run_id}")
    out_file = args.out or base_dir / "aligned_snapshots.jsonl"

    sched_events = base_dir / "sched_events.jsonl"
    sched_summary = base_dir / "sched_summary.json"
    gpu_samples = base_dir / "gpu_samples.jsonl"
    system_samples = base_dir / "system_samples.jsonl"
    kv_samples = base_dir / "kv_samples.jsonl"
    kv_transfers = args.kv_transfers or base_dir / "kv_transfers.jsonl"

    print(f"Aligning experiment artifacts from {base_dir} -> {out_file}...")
    snapshots = generate_aligned_snapshots(
        run_id=args.run_id,
        sched_events_path=sched_events if sched_events.exists() else None,
        sched_summary_path=sched_summary if sched_summary.exists() else None,
        gpu_samples_path=gpu_samples if gpu_samples.exists() else None,
        system_samples_path=system_samples if system_samples.exists() else None,
        kv_samples_path=kv_samples if kv_samples.exists() else None,
        bucket_sec=args.bucket_sec,
    )
    count = write_aligned_snapshots(snapshots, out_file)
    print(f"Successfully generated {count} aligned snapshots in {out_file}.")

    if args.diagnose:
        # 1. Cross-correlation between sched latency and GPU utilization
        sched_series = [s.get("sched", {}).get("p50_us") or 0.0 for s in snapshots]
        gpu_series = [s.get("gpu", {}).get("utilization_pct") or 0.0 for s in snapshots]
        lagged = compute_lagged_correlation(sched_series, gpu_series, max_lag=3)

        # 2. KV PCIe transfer analysis
        kv_pcie = None
        if kv_transfers.exists() and gpu_samples.exists():
            kv_pcie = analyze_kv_pcie_transfers(
                kv_transfers_path=kv_transfers,
                gpu_samples_path=gpu_samples,
                sched_summary_path=sched_summary if sched_summary.exists() else None,
            )

        # 3. Diagnosis
        diag = diagnose_system_bottlenecks(snapshots, lagged_corr=lagged, kv_pcie=kv_pcie)
        diag_out = args.diagnosis_out or base_dir / "bottleneck_diagnosis.json"
        diag_out.parent.mkdir(parents=True, exist_ok=True)
        with open(diag_out, "w", encoding="utf-8") as f:
            json.dump(diag, f, indent=2, ensure_ascii=False)
        print(f"Bottleneck diagnosis saved -> {diag_out}")
        print(f"  Primary Bottleneck: [{diag['severity']}] {diag['primary_bottleneck']}")
        print(f"  Summary: {diag['summary']}")


if __name__ == "__main__":
    main()
