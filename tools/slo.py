"""Explicit client SLO accounting; chunk-derived TPOT is an estimate, not token ITL."""
import math


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def evaluate(records, seconds, config):
    definitions = [
        ("ttft", "ttft_ms", config.get("slo_ttft_ms")),
        ("e2e", "e2e_ms", config.get("slo_e2e_ms")),
        ("tpot_estimate", "client_tpot_estimate_ms", config.get("slo_tpot_ms")),
    ]
    requested = [(name, key, limit) for name, key, limit in definitions if limit is not None]
    rows = []
    for record in records:
        status = "not_configured"
        checks = {}
        if requested:
            if not record.get("success"):
                status = "failed_request"
            else:
                for name, key, limit in requested:
                    value = record.get(key)
                    checks[name] = value <= limit if finite(value) else None
                status = "violation" if False in checks.values() else (
                    "unknown" if None in checks.values() else "passed")
        rows.append({"request_id": record.get("request_id"), "status": status, "checks": checks})
    counts = {name: sum(r["status"] == name for r in rows)
              for name in ("passed", "violation", "unknown", "failed_request", "not_configured")}
    passed = counts["passed"]
    planned = config.get("requests", len(rows))
    return {
        "configured": bool(requested), "thresholds_ms": {name: limit for name, _, limit in requested},
        "planned_requests": planned, "recorded_requests": len(rows),
        "unrecorded_planned_requests": max(0, planned - len(rows)), **counts,
        "attainment_per_recorded_request": passed / len(rows) if requested and rows else None,
        "attainment_per_planned_request": passed / planned if requested and planned else None,
        "goodput_requests_per_second": passed / seconds if requested and seconds > 0 else None,
        "request_results": rows,
        "tpot_semantics": "Client first-to-last visible-content interval / (usage output tokens - 1); chunk-granularity estimate.",
    }
