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
    sched_events: list[tuple[float, float]] = []  # (epoch_sec, latency_us)
    sched_source = "unavailable"
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
                        if mono_ns is not None:
                            wall_ns = wall_anchor_ns + (mono_ns - mono_anchor_ns)
                            sched_events.append((wall_ns / 1e9, lat_ns / 1000.0))
                    except Exception:
                        continue
            sched_source = "ebpf_sched"

    # 3. Ingest GPU samples
    gpu_samples: list[tuple[float, float | None]] = []  # (epoch_sec, memory_used_mb)
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
                        gpu_samples.append((sec, norm.get("memory_used_mb")))
                except Exception:
                    continue

    # 4. Ingest system samples
    sys_samples: list[tuple[float, float | None]] = []  # (epoch_sec, mem_available_mb)
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
                        sys_samples.append((_iso_to_epoch_sec(t_str), float(rec["mem_available_mb"])))
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
        all_ts.extend([t for t, _ in sched_events])
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
        w_sys_mems = [m for t, m in sys_samples if w_start <= t < w_end and m is not None]
        if w_sys_mems:
            mem_avail = round(sum(w_sys_mems) / len(w_sys_mems), 3)
            sys_src = "proc"
        else:
            mem_avail = None
            sys_src = "unavailable"

        system_section: dict[str, Any] = {
            "mem_available_mb": mem_avail,
            "disk_read_p95_ms": None,
            "disk_read_bytes_per_s": None,
        }

        # 2. GPU section
        w_gpu_mems = [m for t, m in gpu_samples if w_start <= t < w_end and m is not None]
        if w_gpu_mems:
            gpu_mem = round(sum(w_gpu_mems) / len(w_gpu_mems), 2)
            gpu_src = "nvidia-smi"
        else:
            gpu_mem = None
            gpu_src = "unavailable"

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
        w_sched = [lat for t, lat in sched_events if w_start <= t < w_end]
        w_sched.sort()

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
            "gpu": {"memory_used_mb": gpu_mem},
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


def main() -> None:
    parser = argparse.ArgumentParser(description="TraceKV Cross-Layer Alignment Engine (v0.2)")
    parser.add_argument("--run-id", required=True, help="Experiment run ID")
    parser.add_argument("--dir", type=Path, default=None, help="Directory containing experiment artifacts")
    parser.add_argument("--out", type=Path, default=None, help="Output aligned_snapshots.jsonl path")
    parser.add_argument("--bucket-sec", type=float, default=1.0, help="Window duration in seconds")
    args = parser.parse_args()

    base_dir = args.dir or Path(f"results/{args.run_id}")
    out_file = args.out or base_dir / "aligned_snapshots.jsonl"

    sched_events = base_dir / "sched_events.jsonl"
    sched_summary = base_dir / "sched_summary.json"
    gpu_samples = base_dir / "gpu_samples.jsonl"
    system_samples = base_dir / "system_samples.jsonl"
    kv_samples = base_dir / "kv_samples.jsonl"

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


if __name__ == "__main__":
    main()
