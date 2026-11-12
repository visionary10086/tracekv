"""Unified metrics snapshot builder and multi-source aggregator.

Aggregates system memory, eBPF CPU scheduler latency, GPU memory/utilization,
and vLLM Prometheus metrics into a unified snapshot conforming to
`contracts/schema.py:validate_snapshot`.

Preserves 100% backward compatibility with existing MVP v0.1 contracts while
enriching the snapshot with microsecond-level CPU scheduler metrics and real GPU telemetry.

Author: 邓智文 (DAGER)
Project: TraceKV
"""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4
from typing import Any

from collector.demo import collect_system_metrics
from collector.gpu import collect_gpu_metrics
from contracts.schema import validate_snapshot, utc_now
from engine.vllm_adapter import VllmAdapter


def build_unified_snapshot(
    run_id: str | None = None,
    vllm_adapter: VllmAdapter | None = None,
    sched_summary: dict | None = None,
    gpu_metrics: dict | None = None,
    system_metrics: dict | None = None,
    window_ms: int | None = None,
) -> dict[str, Any]:
    """Build a unified, cross-layer MetricsSnapshot.

    Parameters
    ----------
    run_id:
        Experiment or session identifier. Defaults to generated UUID.
    vllm_adapter:
        Optional VllmAdapter instance for sampling live KV cache metrics.
    sched_summary:
        Optional summary dict from `collector.cpu_scheduler.CollectorResult`.
        If present, CPU scheduler latency metrics (P50, P95, P99, preemption)
        are injected into `system` (preserving contract) and `sched`.
    gpu_metrics:
        Optional pre-collected GPU metrics dict. If None, queries live via `collector.gpu`.
    system_metrics:
        Optional pre-collected system metrics dict. If None, queries via `collector.demo`.
    window_ms:
        Sampling window duration in milliseconds. Defaults to adapter window or 1000.

    Returns
    -------
    dict
        Validated snapshot dictionary strictly passing `contracts.schema.validate_snapshot`.
    """
    effective_run_id = run_id or f"snapshot-{uuid4().hex[:12]}"

    # 1. System metrics (/proc/meminfo)
    sys_raw = system_metrics or collect_system_metrics()
    mem_available = sys_raw.get("mem_available_mb")
    disk_read_p95 = sys_raw.get("disk_read_p95_ms")
    disk_read_bytes = sys_raw.get("disk_read_bytes_per_s")
    sys_source = sys_raw.get("source", "proc")

    # 2. GPU telemetry (nvidia-smi)
    gpu_raw = gpu_metrics or collect_gpu_metrics()
    gpu_mem = gpu_raw.get("memory_used_mb")
    gpu_source = gpu_raw.get("source", "unavailable")

    # 3. vLLM KV Cache semantics
    kv_data = {"usage_ratio": None, "prefix_hit_ratio": None, "running_requests": None}
    kv_source = "unavailable"
    eff_window_ms = window_ms or 1000

    if vllm_adapter is not None:
        try:
            kv_data = vllm_adapter.sample()
            kv_source = "vllm"
            if vllm_adapter.last_window_ms:
                eff_window_ms = vllm_adapter.last_window_ms
        except Exception:
            kv_source = "unavailable"

    # Base contract snapshot
    system_section: dict[str, Any] = {
        "mem_available_mb": mem_available,
        "disk_read_p95_ms": disk_read_p95,
        "disk_read_bytes_per_s": disk_read_bytes,
    }

    # 4. Integrate eBPF CPU Scheduler Latency (Extension fields)
    sched_section: dict[str, Any] | None = None
    if sched_summary is not None:
        p50_ns = sched_summary.get("p50_ns")
        p95_ns = sched_summary.get("p95_ns")
        p99_ns = sched_summary.get("p99_ns")
        mean_ns = sched_summary.get("mean_ns")
        unmatched = sched_summary.get("unmatched_switch_count", 0)
        ev_count = sched_summary.get("event_count", 0)

        # Convert ns to us for human and policy friendliness
        p50_us = round(p50_ns / 1000.0, 2) if p50_ns is not None else None
        p95_us = round(p95_ns / 1000.0, 2) if p95_ns is not None else None
        p99_us = round(p99_ns / 1000.0, 2) if p99_ns is not None else None
        mean_us = round(mean_ns / 1000.0, 2) if mean_ns is not None else None

        # Extension fields inside system object (preserved by validate_snapshot)
        system_section["sched_p50_us"] = p50_us
        system_section["sched_p95_us"] = p95_us
        system_section["sched_p99_us"] = p99_us
        system_section["sched_preempt_count"] = unmatched

        sched_section = {
            "p50_us": p50_us,
            "p95_us": p95_us,
            "p99_us": p99_us,
            "mean_us": mean_us,
            "preempt_count": unmatched,
            "event_count": ev_count,
            "dropped_count": sched_summary.get("dropped_events_count", 0),
        }
        sys_source = f"{sys_source}+ebpf_sched"

    snapshot: dict[str, Any] = {
        "schema_version": "0.1",
        "run_id": effective_run_id,
        "timestamp": sys_raw.get("timestamp") or utc_now(),
        "window_ms": eff_window_ms,
        "system": system_section,
        "gpu": {"memory_used_mb": gpu_mem},
        "kv": kv_data,
        "sources": {
            "system": sys_source,
            "gpu": gpu_source,
            "kv": kv_source,
        },
    }

    if sched_section is not None:
        snapshot["sched"] = sched_section

    # Validation against shared contract (raises ValueError if contract violated)
    return validate_snapshot(snapshot)
