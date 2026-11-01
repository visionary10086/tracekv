"""MVP v0.1 API: one current snapshot and one policy evaluation endpoint."""
import os
from uuid import uuid4

from fastapi import FastAPI, HTTPException

from collector.demo import collect_system_metrics
from contracts.schema import validate_snapshot
from engine.vllm_adapter import VllmAdapter
from policy.demo import recommend_policy


app = FastAPI(title="TraceKV MVP", version="0.1")
_metrics_url = os.environ.get("TRACEKV_VLLM_METRICS_URL")
_vllm = VllmAdapter(_metrics_url) if _metrics_url else None


def build_snapshot(run_id=None):
    system = collect_system_metrics()
    kv = {"usage_ratio": None, "prefix_hit_ratio": None, "running_requests": None}
    kv_source = "unavailable"
    if _vllm is not None:
        try:
            kv = _vllm.sample()
            kv_source = "vllm"
        except (OSError, ValueError, UnicodeError):
            pass
    snapshot = {
        "schema_version": "0.1",
        "run_id": run_id or "snapshot-" + uuid4().hex,
        "timestamp": system["timestamp"],
        "window_ms": _vllm.last_window_ms if _vllm and _vllm.last_window_ms else 1000,
        "system": {key: system[key] for key in (
            "mem_available_mb", "disk_read_p95_ms", "disk_read_bytes_per_s")},
        "gpu": {"memory_used_mb": None},
        "kv": kv,
        "sources": {"system": system["source"], "gpu": "unavailable", "kv": kv_source},
    }
    return validate_snapshot(snapshot)


@app.get("/api/v1/snapshot")
def get_snapshot():
    return build_snapshot()


@app.post("/api/v1/policy/evaluate")
def evaluate_policy(snapshot: dict):
    try:
        return recommend_policy(snapshot)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
