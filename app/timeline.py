"""Run-scoped wall-clock timeline. Association is explicit; missing IDs stay unknown."""
from datetime import datetime, timezone
import json
from pathlib import Path


def records(path):
    if not path.is_file():
        return
    with path.open() as handle:
        for line in handle:
            if line.strip():
                try:
                    yield json.loads(line)
                except ValueError:
                    yield {"_invalid_line": True}


def epoch(stamp):
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timeline timestamps must include timezone")
    return parsed.timestamp()


def stamp(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace("+00:00", "Z")


def calibrate(ns, anchor):
    return (anchor["wall_ns"] + ns - anchor["monotonic_ns"]) / 1e9


def build(out, start=None, end=None, limit=500, cursor=None):
    from app.integrations import read_batches
    out = Path(out)
    manifest = json.loads((out / "manifest.json").read_text())
    events, counts, invalid = [], {}, 0
    left = epoch(start) if start else None
    right = epoch(end) if end else None
    if left is not None and right is not None and right <= left:
        raise ValueError("end must exceed start")
    def add(source, seconds, metric, data, ended=None):
        counts[source] = counts.get(source, 0) + 1
        if left is not None and (ended <= left if ended is not None else seconds < left):
            return
        if right is not None and seconds >= right:
            return
        events.append({"source": source, "timestamp": stamp(seconds), "epoch_s": seconds,
                       "metric": metric, "end_timestamp": stamp(ended) if ended is not None else None,
                       "data": data})
    for row in records(out / "requests.jsonl"):
        try:
            if row.get("run_id",out.name)!=out.name:
                raise ValueError("Request run_id mismatch")
            a, b = epoch(row["started_at_utc"]), epoch(row["finished_at_utc"])
            if b<a:
                raise ValueError("Negative request interval")
            data = {key: row.get(key) for key in ("request_id","case_id","success","ttft_ms","e2e_ms","client_tpot_estimate_ms","completion_tokens","error")}
            add("request", a, "request", data, b)
        except (KeyError, TypeError, ValueError):
            invalid += 1
    for row in records(out / "snapshots.jsonl"):
        try:
            if row.get("run_id", out.name) != out.name:
                invalid += 1
                continue
            add("snapshot", epoch(row["timestamp"]), "resource_snapshot", row)
        except (KeyError, TypeError, ValueError):
            invalid += 1
    external_ebpf = False
    for source in ("ebpf","cache"):
        for receipt in read_batches(out,source):
            packet = receipt["payload"]
            if packet.get("synthetic"):
                continue
            if source == "ebpf":
                external_ebpf = True
            for row in packet["events"]:
                try:
                    data = dict(row, collector_id=packet["collector_id"], scope=packet.get("scope"),
                                target_tids=packet.get("target_tids"), target_comm=packet.get("target_comm"),
                                dropped_events=packet.get("dropped_events"), batch_id=packet["batch_id"])
                    add(source, calibrate(row["timestamp_ns"],packet["clock"]), row["metric"],data)
                except (KeyError,TypeError,ValueError):
                    invalid += 1
    anchor = manifest.get("clock_anchor")
    # Existing worker hooks and managed collectors are real owned-job records.
    if anchor:
        for row in records(out / "kv_transfers.jsonl"):
            try:
                if row.get("run_id",out.name)!=out.name:
                    raise ValueError("Cache run_id mismatch")
                ns = row.get("observed_ns") or row.get("submitted_ns")
                add("cache_hook",calibrate(ns,anchor),"transfer",dict(row,scope="owned_vllm_worker",request_id=row.get("request_id")))
            except (KeyError,TypeError,ValueError):
                invalid += 1
    summary_path = out / "sched_summary.json"
    if not external_ebpf and summary_path.exists():
        summary = json.loads(summary_path.read_text())
        anchor = {"wall_ns":summary["start_wall_ns"],"monotonic_ns":summary["start_monotonic_ns"]}
        for row in records(out / "sched_events.jsonl"):
            try:
                if row.get("is_target"):
                    add("ebpf",calibrate(row["timestamp_ns"],anchor),row["metric"],dict(row,scope=summary.get("scope"),collector_id="managed-scheduler-v1"))
            except (KeyError,TypeError,ValueError):
                invalid += 1
    events.sort(key=lambda item:(item["epoch_s"],item["source"],str(item["data"].get("request_id",""))))
    total = len(events)
    offset = max(0,total-limit) if cursor is None else min(cursor,total)
    page = events[offset:offset+limit]
    snapshots = [item["data"] for item in events if item["source"]=="snapshot"]
    return {"run_id":out.name,"status":manifest["status"],"phase":manifest.get("phase"),
            "events":page,"total_events":total,"cursor":offset,
            "next_cursor":offset+len(page) if offset+len(page)<total else None,
            "previous_cursor":max(0,offset-limit) if offset else None,
            "source_event_counts":counts,"source_count_scope":"all valid records before time-window filtering","invalid_records":invalid,
            "bounds":{"start":events[0]["timestamp"],"end":stamp(max(epoch(e["end_timestamp"]) if e["end_timestamp"] else e["epoch_s"] for e in events))} if events else None,
            "samples":snapshots[-180:],"source_status":manifest.get("sources",{}),
            "clock_anchor":manifest.get("clock_anchor"),
            "limitations":["Thread/host/device events are temporal observations; request attribution requires an explicit request_id.",
                           "Events with unknown request_id remain unassigned.",
                           "Sparse GPU/PCIe samples cannot resolve each short transfer.",
                           "Cumulative histogram deltas retain their actual intervals and model/engine labels."]}
