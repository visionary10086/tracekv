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
    _check_bucket(bucket_sec)
    # 1. Load time anchor for eBPF clock calibration
    wall_anchor_ns: int | None = None
    mono_anchor_ns: int | None = None

    if sched_summary_path and Path(sched_summary_path).exists():
        with open(sched_summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
            wall_anchor_ns = summary.get("start_wall_ns")
            mono_anchor_ns = summary.get("start_monotonic_ns")
            if wall_anchor_ns is None and "kernel_clock_anchor" in summary:
                wall_anchor_ns = summary["kernel_clock_anchor"].get("wall_ns")
                mono_anchor_ns = summary["kernel_clock_anchor"].get("monotonic_ns")

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
                    lat_ns = record.get("value")
                    comm = record.get("comm", "")

                    if _finite(raw_mono_ns) and raw_mono_ns >= 0 and _finite(lat_ns) and lat_ns >= 0:
                        if has_calibrated_anchor:
                            # Formula per v0.2 spec: wall_ns = start_wall_ns + event_mono_ns - start_mono_ns
                            wall_ns = wall_anchor_ns + (raw_mono_ns - mono_anchor_ns)
                            epoch_sec = wall_ns / 1e9
                        else:
                            # Monotonic time is not UTC; omit uncalibrated events.
                            continue
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
    num_buckets = max(1, math.floor((max_t - min_t) / bucket_sec) + 1)
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
    _check_bucket(bucket_sec)
    summary = {}
    # 1. Ingest calibrated anchors
    wall_anchor_ns: int | None = None
    mono_anchor_ns: int | None = None
    if sched_summary_path and Path(sched_summary_path).exists():
        with open(sched_summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
            wall_anchor_ns = summary.get("start_wall_ns")
            mono_anchor_ns = summary.get("start_monotonic_ns")
            if wall_anchor_ns is None and "kernel_clock_anchor" in summary:
                wall_anchor_ns = summary["kernel_clock_anchor"].get("wall_ns")
                mono_anchor_ns = summary["kernel_clock_anchor"].get("monotonic_ns")

    has_anchor = wall_anchor_ns is not None and mono_anchor_ns is not None

    # 2. Ingest scheduler and kernel memory stall events
    sched_events: list[tuple[float, float, bool]] = []   # (epoch_sec, latency_us, is_target)
    runwait_events: list[tuple[float, float, bool]] = [] # (epoch_sec, wait_us, is_target)
    oncpu_events: list[tuple[float, float, bool]] = []   # (epoch_sec, dur_us, is_target)
    reclaim_events: list[tuple[float, float, bool]] = [] # (epoch_sec, stall_ms, is_target)
    sched_source = "unavailable"
    has_target_events = summary.get("scope") in ("target_process", "target_threads", "comm_match")
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
                        val_ns = record.get("value")
                        metric = record.get("metric", "wakeup_to_run_latency")
                        is_tgt = bool(record.get("is_target", False))
                        if "is_target" in record:
                            has_target_events = True
                        if _finite(mono_ns) and mono_ns >= 0 and _finite(val_ns) and val_ns >= 0:
                            wall_ns = wall_anchor_ns + (mono_ns - mono_anchor_ns)
                            epoch_sec = wall_ns / 1e9
                            if metric == "runnable_wait":
                                runwait_events.append((epoch_sec, val_ns / 1000.0, is_tgt))
                            elif metric == "on_cpu":
                                oncpu_events.append((epoch_sec, val_ns / 1000.0, is_tgt))
                            elif metric == "direct_reclaim":
                                reclaim_events.append((epoch_sec, val_ns / 1e6, is_tgt))
                            else:
                                sched_events.append((epoch_sec, val_ns / 1000.0, is_tgt))
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

    # Ratios refer to each sample's actual counter-difference interval.
    kv_samples = []
    if kv_samples_path and Path(kv_samples_path).exists():
        for line in Path(kv_samples_path).read_text().splitlines():
            try:
                rec = json.loads(line)
                kv_samples.append((_iso_to_epoch_sec(rec["timestamp"]), rec))
            except (ValueError, KeyError, TypeError):
                continue
    for samples in (gpu_samples, sys_samples, kv_samples):
        samples.sort(key=lambda item: item[0])

    # Collect time bounds across available sources
    all_ts: list[float] = []
    if sched_events:
        all_ts.extend([t for t, *_ in sched_events])
    if runwait_events:
        all_ts.extend([t for t, *_ in runwait_events])
    if oncpu_events:
        all_ts.extend([t for t, *_ in oncpu_events])
    if reclaim_events:
        all_ts.extend([t for t, *_ in reclaim_events])
    if gpu_samples:
        all_ts.extend([t for t, _ in gpu_samples])
    if sys_samples:
        all_ts.extend([t for t, _ in sys_samples])
    if kv_samples:
        all_ts.extend([t for t, _ in kv_samples])

    if not all_ts:
        return []

    min_t = min(all_ts)
    max_t = max(all_ts)
    num_windows = max(1, math.floor((max_t - min_t) / bucket_sec) + 1)
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

        # Last gauge, while retaining every counter-ratio interval unchanged.
        w_kvs = [rec for t, rec in kv_samples if w_start <= t < w_end]
        latest_kv = w_kvs[-1] if w_kvs else {}
        kv_src = "vllm" if w_kvs else "unavailable"
        kv_section = {
            "usage_ratio": latest_kv.get("usage_ratio"),
            "prefix_hit_ratio": None,
            "running_requests": latest_kv.get("running_requests"),
        }
        kv_intervals = [
            {"ended_at_utc": rec["timestamp"], "window_ms": rec.get("window_ms"),
             "prefix_hit_ratio": rec.get("prefix_hit_ratio"),
             "counter_deltas": rec.get("counter_deltas")}
            for rec in w_kvs
        ]

        # 4. Sched & stall sections (windowed)
        w_sched = [lat for t, lat, _ in sched_events if w_start <= t < w_end]
        w_sched.sort()
        w_target = [lat for t, lat, is_tgt in sched_events if w_start <= t < w_end and is_tgt]
        w_target.sort()

        # Runnable wait (preemption stall)
        w_rw = [lat for t, lat, _ in runwait_events if w_start <= t < w_end]
        w_rw.sort()
        w_rw_tgt = [lat for t, lat, is_tgt in runwait_events if w_start <= t < w_end and is_tgt]
        w_rw_tgt.sort()

        # On-CPU execution slice
        w_oncpu = [lat for t, lat, _ in oncpu_events if w_start <= t < w_end]
        w_oncpu.sort()
        w_oncpu_tgt = [lat for t, lat, is_tgt in oncpu_events if w_start <= t < w_end and is_tgt]
        w_oncpu_tgt.sort()

        # Direct memory reclaim stall (ms)
        w_reclaim = [ms for t, ms, _ in reclaim_events if w_start <= t < w_end]
        reclaim_stall_ms = round(sum(w_reclaim), 2) if w_reclaim else None
        if reclaim_stall_ms is not None:
            system_section["direct_reclaim_stall_ms"] = reclaim_stall_ms

        rw_tgt_p99 = _percentile(w_rw_tgt, 99) if w_rw_tgt else (_percentile(w_rw, 99) if w_rw else None)
        oncpu_tgt_p99 = _percentile(w_oncpu_tgt, 99) if w_oncpu_tgt else (_percentile(w_oncpu, 99) if w_oncpu else None)

        if has_target_events:
            sched_section: dict[str, Any] = {
                "scope": summary.get("scope", "target_threads"),
                "event_count": len(w_target),
                "p50_us": _percentile(w_target, 50),
                "p95_us": _percentile(w_target, 95),
                "p99_us": _percentile(w_target, 99),
                "runwait_p99_us": rw_tgt_p99,
                "on_cpu_p99_us": oncpu_tgt_p99,
                "system_baseline": {
                    "event_count": len(w_sched),
                    "p50_us": _percentile(w_sched, 50),
                    "p95_us": _percentile(w_sched, 95),
                    "p99_us": _percentile(w_sched, 99),
                    "runwait_p99_us": _percentile(w_rw, 99) if w_rw else None,
                    "on_cpu_p99_us": _percentile(w_oncpu, 99) if w_oncpu else None,
                },
            }
        else:
            sched_section: dict[str, Any] = {
                "scope": "system",
                "event_count": len(w_sched),
                "p50_us": _percentile(w_sched, 50),
                "p95_us": _percentile(w_sched, 95),
                "p99_us": _percentile(w_sched, 99),
                "runwait_p99_us": rw_tgt_p99,
                "on_cpu_p99_us": oncpu_tgt_p99,
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
            "aggregation": "mean system/GPU memory; last extended gauges; KV ratios kept as intervals",
            "system": system_section,
            "gpu": gpu_section,
            "kv": kv_section,
            "sources": sources,
            "sched": sched_section,
            "kv_intervals": kv_intervals,
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


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _check_bucket(bucket_sec):
    if not _finite(bucket_sec) or bucket_sec < 0.001:
        raise ValueError("bucket_sec must be finite and at least 0.001 seconds")


def compute_correlation(x: list[float], y: list[float]) -> float | None:
    """Pearson r for paired finite observations; undefined for constant data."""
    if len(x) != len(y):
        return None
    pairs = [(a, b) for a, b in zip(x, y) if _finite(a) and _finite(b)]
    if len(pairs) < 2:
        return None
    xs, ys = zip(*pairs)
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    cov = sum((a - mx) * (b - my) for a, b in pairs)
    vx = sum((a - mx) ** 2 for a in xs)
    vy = sum((b - my) ** 2 for b in ys)
    return round(cov / math.sqrt(vx * vy), 4) if vx > 0 and vy > 0 else None

def compute_lagged_correlation(x, y, max_lag=3):
    """Explore temporal association; lag and correlation do not prove causality.

    Missing windows retain their positions and are omitted only after pairing at
    each lag. At least three valid pairs are required for a lag.
    """
    if type(max_lag) is not int or max_lag < 0:
        raise ValueError("max_lag must be a nonnegative integer")
    result = {"lags": [], "correlations": [], "pair_counts": [], "peak_lag": None,
              "peak_correlation": None, "temporal_direction": "insufficient_data",
              "causality_established": False,
              "interpretation": "Exploratory association only; insufficient paired observations."}
    if len(x) != len(y) or len(x) < max_lag + 3:
        return result
    for lag in range(-max_lag, max_lag + 1):
        if lag > 0:
            xs, ys = x[:-lag], y[lag:]
        elif lag < 0:
            xs, ys = x[-lag:], y[:lag]
        else:
            xs, ys = x, y
        count = sum(_finite(a) and _finite(b) for a, b in zip(xs, ys))
        result["lags"].append(lag)
        result["pair_counts"].append(count)
        result["correlations"].append(compute_correlation(xs, ys) if count >= 3 else None)
    candidates = [(lag, r) for lag, r in zip(result["lags"], result["correlations"]) if r is not None]
    if not candidates:
        result["temporal_direction"] = "undefined"
        result["interpretation"] = "Insufficient variation or valid pairs; causality is not established."
        return result
    lag, r = max(candidates, key=lambda pair: (abs(pair[1]), -abs(pair[0])))
    direction = "no_correlation" if abs(r) < 0.3 else (
        "x_leads_y" if lag > 0 else "y_leads_x" if lag < 0 else "synchronous")
    result.update(peak_lag=lag, peak_correlation=r, temporal_direction=direction,
                  interpretation=f"Temporal association: {direction}, lag={lag}, r={r}. "
                  "Exploratory threshold, no causal conclusion or significance test.")
    return result

def analyze_kv_pcie_transfers(kv_transfers_path, gpu_samples_path,
                              sched_summary_path=None,
                              pcie_theoretical_bandwidth_mb_per_s=None):
    """Summarize co-observed transfers and sparse, device-wide PCIe samples.

    An explicit measured/configured link capacity is required for a capacity
    ratio. NVML samples describe a short hardware interval, not all bytes moved
    between polls. They cannot identify an individual transfer's bottleneck.
    sched_summary_path is retained for call compatibility; no temporal matching
    is claimed here.
    """
    capacity = pcie_theoretical_bandwidth_mb_per_s
    if capacity is not None and (not _finite(capacity) or capacity <= 0):
        raise ValueError("PCIe reference capacity must be a positive finite MB/s value")
    path = Path(kv_transfers_path)
    if not path.exists():
        return {"available": False, "bottleneck": "NO_TRANSFERS",
                "assessment_kind": "observational", "reason": f"{path} not found"}
    transfers = []
    for line in path.read_text().splitlines():
        try:
            rec = json.loads(line)
            if rec.get("status") == "completed":
                transfers.append(rec)
        except (ValueError, AttributeError):
            continue
    tx_values, rx_values = [], []
    gpu_path = Path(gpu_samples_path)
    if gpu_path.exists():
        for line in gpu_path.read_text().splitlines():
            try:
                sample = parse_legacy_gpu_sample(line)
                for key, values in (("pcie_tx_kb_per_s", tx_values), ("pcie_rx_kb_per_s", rx_values)):
                    value = sample.get(key)
                    if _finite(value) and value >= 0:
                        values.append(value / 1000.0)
            except (ValueError, AttributeError):
                continue
    peak_tx = max(tx_values, default=None)
    peak_rx = max(rx_values, default=None)
    peaks = [value for value in (peak_tx, peak_rx) if value is not None]
    peak = max(peaks, default=None)
    ratio = round(peak / capacity * 100, 2) if capacity is not None and peak is not None else None
    elapsed = sorted(t["elapsed_ms"] for t in transfers if _finite(t.get("elapsed_ms")) and t["elapsed_ms"] >= 0)
    p95 = _percentile(elapsed, 95)
    if not transfers:
        category, message = "NO_TRANSFERS", "No completed transfer records."
    elif not peaks:
        category, message = "NO_PCIE_TELEMETRY", "PCIe telemetry unavailable; no throughput or capacity ratio can be inferred."
    elif ratio is not None and ratio >= 75:
        category, message = "HIGH_PCIE_UTILIZATION", "Sampled device traffic is high relative to the supplied reference; transfer saturation is not proven."
    elif p95 is not None and p95 >= 10:
        category, message = "TRANSFER_DELAY_REQUIRES_INVESTIGATION", "Submission-to-observation P95 is elevated; inspect queueing, polling and device timing separately."
    else:
        category, message = "NO_RULE_TRIGGERED", "No configured observation threshold was crossed; this does not establish balanced operation."
    return {
        "available": True, "total_transfers": len(transfers),
        "gpu_to_cpu_transfers": sum(t.get("direction") == "gpu_to_cpu" for t in transfers),
        "cpu_to_gpu_transfers": sum(t.get("direction") == "cpu_to_gpu" for t in transfers),
        "total_gpu_blocks": sum(t.get("gpu_blocks", 0) for t in transfers),
        "elapsed_ms": {"p50": _percentile(elapsed, 50), "p95": p95,
                       "p99": _percentile(elapsed, 99),
                       "mean": round(sum(elapsed) / len(elapsed), 2) if elapsed else None},
        "pcie_physical": {
            "theoretical_max_mb_per_s": capacity,
            "peak_tx_mb_per_s": round(peak_tx, 2) if peak_tx is not None else None,
            "peak_rx_mb_per_s": round(peak_rx, 2) if peak_rx is not None else None,
            "avg_tx_mb_per_s": round(sum(tx_values) / len(tx_values), 2) if tx_values else None,
            "avg_rx_mb_per_s": round(sum(rx_values) / len(rx_values), 2) if rx_values else None,
            "tx_sample_count": len(tx_values), "rx_sample_count": len(rx_values),
            "bus_saturation_pct": ratio},
        "bottleneck": category, "diagnostic": message,
        "assessment_kind": "observational", "causality_established": False,
        "temporal_alignment": "not_established",
        "limitations": [
            "Whole-device NVML PCIe samples cover short intervals; sparse polling can miss bursts.",
            "Capacity ratio uses an explicitly supplied reference, not a detected link capacity.",
            "Transfer elapsed time includes queueing and completion observation, not pure DMA time.",
            "No per-transfer traffic attribution or precise byte accounting is established."]}

def diagnose_system_bottlenecks(snapshots, lagged_corr=None, kv_pcie=None):
    """Return threshold-based investigation hints, never root-cause proof."""
    def values(section, key):
        return [s.get(section, {}).get(key) for s in snapshots
                if _finite(s.get(section, {}).get(key))]
    def psi_values(key):
        return [s.get("system", {}).get("psi", {}).get(key) for s in snapshots
                if _finite((s.get("system", {}).get("psi") or {}).get(key))]
    def high(value, threshold):
        return value is not None and value >= threshold
    sched = values("sched", "p99_us")
    runwait = values("sched", "runwait_p99_us")
    reclaim = values("system", "direct_reclaim_stall_ms")
    cpu, mem, io = (psi_values("psi_" + name + "_some_avg10") for name in ("cpu", "mem", "io"))
    gpu = values("gpu", "utilization_pct")
    max_sched, max_rw, max_rec, max_cpu, max_mem, max_io = (
        max(v, default=None) for v in (sched, runwait, reclaim, cpu, mem, io)
    )
    gpu_mean = round(sum(gpu) / len(gpu), 1) if gpu else None
    pcie = (kv_pcie or {}).get("pcie_physical", {}).get("bus_saturation_pct")
    coverage = {"sched": len(sched), "runwait": len(runwait), "direct_reclaim": len(reclaim),
                "psi_cpu": len(cpu), "psi_memory": len(mem), "psi_io": len(io), "gpu": len(gpu)}

    if high(max_rec, 10.0):
        category = "DIRECT_RECLAIM_MEMORY_STALL"
        severity = "CRITICAL"
        message = (f"Linux kernel direct memory reclaim stall detected (peak {max_rec} ms). "
                   "Allocating threads were synchronously blocked on page scanning/compaction.")
        recommendations = ["Inspect memory PSI and swap activity; increase free memory headroom or adjust KV cache allocation thresholds."]
    elif high(max_rw, 8000) and (max_sched is None or max_rw > max_sched * 1.5):
        category = "CPU_PREEMPTION_CONTENTION"
        severity = "CRITICAL" if high(max_rw, 15000) else "WARNING"
        message = (f"Excessive CPU time-slice preemption detected (runwait P99 {max_rw} us). "
                   "Runnable threads are frequently preempted and stalled in the runqueue.")
        recommendations = ["Check task priorities (nice values), CPU affinity (taskset), and reduce competing background threads."]
    elif high(max_sched, 5000) or high(max_cpu, 25):
        category = "HOST_SCHEDULING_CONTENTION"
        severity = "CRITICAL" if high(max_sched, 10000) else "WARNING"
        message = "Scheduling latency or CPU PSI crossed an exploratory threshold. Inspect the recorded scope before attributing this to inference workers."
        recommendations = ["Compare the same workload with and without controlled CPU contention; record worker identity and collector loss."]
    elif high(pcie, 75):
        category, severity = "HIGH_PCIE_UTILIZATION", "WARNING"
        message = "Sampled device traffic is high relative to the supplied link reference; a KV transfer bottleneck is not established."
        recommendations = ["Verify the actual link capacity and record device-timed transfers before changing cache budgets."]
    elif high(max_mem, 20) or high(max_io, 20):
        category, severity = "SYSTEM_MEMORY_IO_PRESSURE", "CRITICAL"
        message = "Host PSI indicates memory or I/O pressure. These host-wide observations do not establish paging or a KV-specific cause."
        recommendations = ["Inspect memory and I/O counters, then repeat the same workload with background activity controlled."]
    elif high(gpu_mean, 85):
        category, severity = "GPU_HIGH_UTILIZATION", "INFO"
        message = "Average sampled GPU activity is high. Utilization alone cannot distinguish compute, memory-bandwidth or other device bottlenecks."
        recommendations = ["Inspect GPU kernel and memory timing before choosing a compute optimization."]
    elif not any(coverage.values()):
        category, severity = "INSUFFICIENT_DATA", "INFO"
        message = "No usable scheduling, PSI or GPU observations; missing metrics do not establish healthy operation."
        recommendations = ["Collect usable observations and inspect source availability."]
    else:
        category, severity = "NO_RULE_TRIGGERED", "INFO"
        message = "No configured threshold was crossed in available observations. Missing domains remain unknown."
        recommendations = ["Check source coverage and repeat a controlled comparison before drawing conclusions."]
    return {
        "run_id": snapshots[0].get("run_id") if snapshots else None,
        "primary_bottleneck": category, "severity": severity, "summary": message,
        "assessment_kind": "heuristic", "causality_established": False,
        "source_coverage": coverage,
        "metrics_summary": {
            "sched_scope": sorted({s.get("sched", {}).get("scope", "unknown") for s in snapshots}),
            "max_sched_p99_us": max_sched, "max_psi_cpu_avg10": max_cpu,
            "max_psi_mem_avg10": max_mem, "max_psi_io_avg10": max_io,
            "avg_gpu_utilization_pct": gpu_mean, "peak_pcie_bus_saturation_pct": pcie},
        "lagged_correlation": lagged_corr, "kv_pcie_transfer_analysis": kv_pcie,
        "recommendations": recommendations,
        "limitations": ["Thresholds are exploratory, not calibrated bottleneck classifiers.",
                         "Lagged correlation does not prove causality.",
                         "Host/device metrics do not establish per-request or per-process causes."]}

def main() -> None:
    parser = argparse.ArgumentParser(description="TraceKV Cross-Layer Alignment & Diagnosis Engine (v0.2)")
    parser.add_argument("--run-id", required=True, help="Experiment run ID")
    parser.add_argument("--dir", type=Path, default=None, help="Directory containing experiment artifacts")
    parser.add_argument("--out", type=Path, default=None, help="Output aligned_snapshots.jsonl path")
    parser.add_argument("--bucket-sec", type=float, default=1.0, help="Window duration in seconds")
    parser.add_argument("--kv-transfers", type=Path, default=None, help="Path to kv_transfers.jsonl")
    parser.add_argument("--diagnose", action="store_true", help="Generate exploratory bottleneck investigation hints")
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
        sched_series = [s.get("sched", {}).get("p50_us") for s in snapshots]
        gpu_series = [s.get("gpu", {}).get("utilization_pct") for s in snapshots]
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
