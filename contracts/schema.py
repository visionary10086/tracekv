"""Validate the fields shared by the collector, engine, policy and API."""
from datetime import datetime, timezone
import math


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _number(value, name, minimum=None, maximum=None):
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number or null")
    if minimum is not None and value < minimum or maximum is not None and value > maximum:
        raise ValueError(f"{name} is outside its allowed range")


def validate_snapshot(snapshot):
    """Return a validated snapshot; unknown extension keys are preserved."""
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != "0.1":
        raise ValueError("schema_version must be 0.1")
    if not isinstance(snapshot.get("run_id"), str) or not snapshot["run_id"].strip():
        raise ValueError("run_id must be a nonempty string")
    stamp = snapshot.get("timestamp")
    if not isinstance(stamp, str):
        raise ValueError("timestamp must be ISO 8601 UTC")
    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be ISO 8601 UTC") from exc
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError("timestamp must be UTC")
    window = snapshot.get("window_ms")
    if isinstance(window, bool) or not isinstance(window, int) or window <= 0:
        raise ValueError("window_ms must be a positive integer")
    for section in ("system", "gpu", "kv", "sources"):
        if not isinstance(snapshot.get(section), dict):
            raise ValueError(f"{section} must be an object")
    system, gpu, kv = snapshot["system"], snapshot["gpu"], snapshot["kv"]
    for field in ("mem_available_mb", "disk_read_p95_ms", "disk_read_bytes_per_s"):
        _number(system.get(field), f"system.{field}", 0)
    _number(gpu.get("memory_used_mb"), "gpu.memory_used_mb", 0)
    for field in ("usage_ratio", "prefix_hit_ratio"):
        _number(kv.get(field), f"kv.{field}", 0, 1)
    running = kv.get("running_requests")
    if running is not None and (isinstance(running, bool) or not isinstance(running, int) or running < 0):
        raise ValueError("kv.running_requests must be a nonnegative integer or null")
    for section in ("system", "gpu", "kv"):
        source = snapshot["sources"].get(section)
        if not isinstance(source, str) or not source:
            raise ValueError(f"sources.{section} must identify its data source")
    return snapshot
