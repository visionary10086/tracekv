"""Validated append-only batch ingestion; immutable batches make retries idempotent."""
import hashlib
import json
import math
import re
from pathlib import Path
import shutil
import threading
from contracts.platform import EbpfBatch, CacheBatch, StrategyRequest, StrategyAdvice
from contracts.schema import utc_now

_lock = threading.RLock()
MODELS = {"ebpf": EbpfBatch, "cache": CacheBatch}


def contracts():
    return {
        "interface_version": "1.0",
        "endpoints": {
            "strategies": "GET /api/v1/strategies",
            "recommendation": "POST /api/v1/strategies/evaluate",
            "ebpf": "POST /api/v1/runs/{run_id}/telemetry/ebpf",
            "cache": "POST /api/v1/runs/{run_id}/telemetry/cache",
            "summary": "GET /api/v1/runs/{run_id}/telemetry",
            "batch": "GET /api/v1/runs/{run_id}/telemetry/{source}/batches/{batch_id}",
            "reanalyze": "POST /api/v1/runs/{run_id}/analyze",
        },
        "guide_url": "/api/v1/interfaces/guide",
        "example_urls": {name: "/api/v1/interfaces/examples/" + name for name in ("ebpf", "cache", "strategy-request", "strategy-response")},
        "schemas": {name: model.model_json_schema() for name, model in
                    (("strategy_request", StrategyRequest), ("ebpf_batch", EbpfBatch), ("cache_batch", CacheBatch), ("strategy_advice", StrategyAdvice))},
        "rules": [
            "一个 batch_id 在同一 run/source 下不可变；同内容重试不重复计数，不同内容返回 409。",
            "事件时钟为 monotonic ns，UTC = clock.wall_ns + timestamp_ns - clock.monotonic_ns。",
            "统计按 collector_id、scope、target_tids/comm 分组，范围不同不混合。",
            "null 表示未知，实测零值保留；synthetic=true 不进入真实观测统计。",
            "接收数据不代表自动启动内核采集器，历史数据也不代表当前正在采集。",
        ],
    }


def capabilities():
    return {"interface_version": "1.0", "cache": {"ingest_ready": True, "source": "vLLM/cache module"},
            "ebpf": {"ingest_ready": True, "bpftrace_found": shutil.which("bpftrace") is not None,
                     "automatic_start": False, "status": "external_collector_required",
                     "message": "接收接口已就绪；需外部采集器提交真实事件。"},
            "contracts_url": "/api/v1/interfaces", "docs_url": "/docs"}


def _folder(run_dir, source):
    root = Path(run_dir).resolve()
    folder = root / "telemetry" / source
    if source not in MODELS or not folder.resolve().is_relative_to(root):
        raise ValueError("Invalid telemetry path")
    return folder


def accept(run_dir, source, packet):
    if Path(run_dir).name != packet.run_id:
        raise ValueError("Body run_id does not match URL run_id")
    payload = packet.model_dump()
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False)
    digest = hashlib.sha256(serialized.encode()).hexdigest()
    folder = _folder(run_dir, source)
    with _lock:
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / (packet.batch_id + ".json")
        if target.exists():
            receipt = json.loads(target.read_text())
            if receipt["sha256"] != digest:
                raise FileExistsError("batch_id already exists with different content")
            return {key: receipt[key] for key in ("batch_id", "received_at_utc", "event_count")} | {"duplicate": True}
        if packet.status == "ok":
            incoming_start = packet.clock.wall_ns + packet.window_start_ns - packet.clock.monotonic_ns
            incoming_end = packet.clock.wall_ns + packet.window_end_ns - packet.clock.monotonic_ns
            for existing in read_batches(run_dir, source):
                before = existing["payload"]
                same_stream = all(before.get(key) == payload.get(key) for key in
                                  ("collector_id", "scope", "target_tids", "target_comm", "synthetic"))
                if same_stream and before["status"] == "ok":
                    anchor = before["clock"]
                    left = anchor["wall_ns"] + before["window_start_ns"] - anchor["monotonic_ns"]
                    right = anchor["wall_ns"] + before["window_end_ns"] - anchor["monotonic_ns"]
                    if incoming_start < right and left < incoming_end:
                        raise FileExistsError("Observation windows overlap for the same collector and scope")
        receipt = {"batch_id": packet.batch_id, "received_at_utc": utc_now(),
                   "event_count": len(packet.events), "sha256": digest, "payload": payload}
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(receipt, ensure_ascii=False, allow_nan=False))
        temporary.replace(target)
        return {key: receipt[key] for key in ("batch_id", "received_at_utc", "event_count")} | {"duplicate": False}


def read_batches(run_dir, source):
    with _lock:
        return [json.loads(p.read_text()) for p in sorted(_folder(run_dir, source).glob("*.json"))]


def summarize(run_dir):
    result = {"interface_version": "1.0", "run_id": Path(run_dir).name, "sources": {}}
    revisions = []
    for source in MODELS:
        receipts = read_batches(run_dir, source)
        real = [r for r in receipts if not r["payload"]["synthetic"]]
        revisions.extend((source, r["batch_id"], r["sha256"]) for r in real)
        groups = {}
        for receipt in real:
            packet = receipt["payload"]
            identity = (packet["collector_id"], packet.get("scope"), tuple(sorted(packet.get("target_tids", []))), packet.get("target_comm"))
            group = groups.setdefault(identity, {"collector_id": identity[0], "scope": identity[1],
                "target_tids": list(identity[2]), "target_comm": identity[3], "event_count": 0,
                "metrics": {}, "dropped_events": 0, "loss_known": True})
            loss = packet["dropped_events"]
            group["loss_known"] &= loss is not None
            if loss is not None:
                group["dropped_events"] += loss
            for event in packet["events"]:
                group["event_count"] += 1
                name = event["metric"] if source == "ebpf" else ":".join(str(event.get(key) or "-") for key in ("metric", "tier", "direction", "status"))
                metric = group["metrics"].setdefault(name, {"count": 0, "durations_ns": [], "bytes": 0, "bytes_known": True})
                metric["count"] += 1
                duration = event.get("value") if source == "ebpf" else event.get("elapsed_ns")
                if duration is not None:
                    metric["durations_ns"].append(duration)
                byte_count = event.get("bytes")
                if byte_count is None:
                    metric["bytes_known"] = False
                else:
                    metric["bytes"] += byte_count
        for group in groups.values():
            if not group.pop("loss_known"):
                group["dropped_events"] = None
            for metric in group["metrics"].values():
                values = sorted(metric.pop("durations_ns"))
                metric["p95_us"] = values[max(0, math.ceil(len(values) * .95)-1)] / 1000 if values else None
                metric["total_duration_ns"] = sum(values) if values else None
                if not metric.pop("bytes_known"):
                    metric["bytes"] = None
        result["sources"][source] = {
            "status": "received" if real else "unavailable", "batch_count": len(real),
            "synthetic_batch_count": len(receipts) - len(real),
            "event_count": sum(len(r["payload"]["events"]) for r in real),
            "last_received_at_utc": max((r["received_at_utc"] for r in real), default=None),
            "collector_statuses": sorted({r["payload"]["status"] for r in real}),
            "errors": [r["payload"]["error"] for r in real if r["payload"]["error"]],
            "groups": list(groups.values()), "live_collection_verified": False,
        }
    result["revision"] = hashlib.sha256(json.dumps(revisions, sort_keys=True).encode()).hexdigest()
    return result


def read_batch(run_dir, source, batch_id):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", batch_id):
        raise ValueError("Invalid batch_id")
    path = _folder(run_dir, source) / (batch_id + ".json")
    if path.is_symlink():
        raise ValueError("Invalid batch path")
    return json.loads(path.read_text())
