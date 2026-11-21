"""Label-preserving Prometheus parsing and reset-aware cumulative histogram deltas."""
import json
import math
import re

LINE = re.compile(r'^vllm:([A-Za-z0-9_]+)(?:\{(.*)\})?\s+([^\s]+)(?:\s+\d+)?$')
LABEL = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"')
NAMES = {"request_queue_time_seconds", "request_prefill_time_seconds",
         "request_decode_time_seconds", "time_to_first_token_seconds",
         "request_time_per_output_token_seconds", "inter_token_latency_seconds",
         "time_per_output_token_seconds", "e2e_request_latency_seconds"}


def series(body):
    rows = []
    for line in body.splitlines():
        match = LINE.match(line)
        if not match:
            continue
        try:
            value = float(match[3])
            if not math.isfinite(value):
                continue
            labels = {m[1]: json.loads('"' + m[2] + '"') for m in LABEL.finditer(match[2] or "")}
            rows.append({"name": match[1], "labels": labels, "value": value})
        except (ValueError, TypeError):
            continue
    return rows


def histograms(body):
    groups = {}
    for row in series(body):
        name = row["name"]
        suffix = next((s for s in ("_bucket", "_count", "_sum") if name.endswith(s)), None)
        base = name[:-len(suffix)] if suffix else ""
        if base not in NAMES:
            continue
        labels = {k: v for k, v in row["labels"].items() if k != "le"}
        key = json.dumps([base, labels], sort_keys=True)
        group = groups.setdefault(key, {"name": base, "labels": labels, "buckets": {}, "count": None, "sum": None})
        if suffix == "_bucket" and "le" in row["labels"]:
            group["buckets"][row["labels"]["le"]] = row["value"]
        elif suffix in ("_count", "_sum"):
            group[suffix[1:]] = row["value"]
    return groups


def deltas(current, previous):
    result = []
    for key, now in current.items():
        before = (previous or {}).get(key)
        row = {"name": now["name"], "labels": now["labels"], "cumulative": now, "delta": None, "reset": False}
        if before and now["count"] is not None and before["count"] is not None:
            reset = now["count"] < before["count"]
            reset |= any(value < before["buckets"].get(bound, value)
                         for bound, value in now["buckets"].items())
            if now["sum"] is not None and before["sum"] is not None:
                reset |= now["sum"] < before["sum"]
            row["reset"] = reset
            if not reset and set(now["buckets"]) == set(before["buckets"]):
                row["delta"] = {
                    "count": now["count"] - before["count"],
                    "sum": now["sum"] - before["sum"] if None not in (now["sum"], before["sum"]) else None,
                    "buckets": {bound: value - before["buckets"][bound] for bound, value in now["buckets"].items()},
                }
        result.append(row)
    return result


def summarize_samples(samples):
    groups = {}
    for sample in samples:
        for row in sample.get("kv",{}).get("stage_histograms",[]):
            key=json.dumps([row["name"],row["labels"]],sort_keys=True)
            group=groups.setdefault(key,{"name":row["name"],"labels":row["labels"],"valid_intervals":0,
                "reset_intervals":0,"count":0,"sum_seconds":0.0,"sum_complete":True,"buckets":{}})
            if row.get("reset"):
                group["reset_intervals"]+=1
            delta=row.get("delta")
            if delta is None:
                continue
            group["valid_intervals"]+=1
            group["count"]+=delta["count"]
            if delta["sum"] is not None:
                group["sum_seconds"]+=delta["sum"]
            else:
                group["sum_complete"]=False
            for bound,value in delta["buckets"].items():
                group["buckets"][bound]=group["buckets"].get(bound,0)+value
    for group in groups.values():
        group["mean_ms"]=group["sum_seconds"]/group["count"]*1000 if group["count"]>0 and group["sum_complete"] else None
        group["p95_estimate_ms"]=None
        group["p95_lower_bound_ms"]=None
        if group["count"]>0:
            rank=.95*group["count"]
            previous_bound,previous_count=0.0,0
            for boundary,count in sorted(group["buckets"].items(),key=lambda pair:float(pair[0])):
                bound=float(boundary)
                if count>=rank:
                    if math.isfinite(bound) and count>previous_count:
                        group["p95_estimate_ms"]=(previous_bound+(bound-previous_bound)*(rank-previous_count)/(count-previous_count))*1000
                    else:
                        group["p95_lower_bound_ms"]=previous_bound*1000
                    break
                previous_bound,previous_count=bound,count
        group["quantile_method"]="Linear estimate within cumulative histogram bucket; model/engine labels preserved."
    return list(groups.values())
