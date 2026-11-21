"""Persist advice provenance and assess follow-up runs without claiming causal gains."""
import hashlib
import json
from pathlib import Path
import re
from uuid import uuid4
from tools.run_experiment import write_json, load_jsonl


def register(jobs, advice, snapshot, current_config=None):
    key = "advice-"+uuid4().hex
    source = None
    try:
        out = jobs.resolve(snapshot["run_id"])
        manifest = json.loads((out/"manifest.json").read_text())
        matches_config=current_config is None or all(manifest["config"].get(k)==v for k,v in current_config.items())
        if matches_config and any(row==snapshot for row in load_jsonl(out/"snapshots.jsonl")):
            source={"run_id":out.name,"config":manifest["config"],
                    "snapshot_sha256":hashlib.sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()}
    except (OSError,ValueError,KeyError):
        pass
    record={"recommendation_id":key,"advice":advice,"source":source,"snapshot":snapshot}
    folder=jobs.root/".recommendations"
    folder.mkdir(parents=True,exist_ok=True)
    write_json(folder/(key+".json"),record)
    return dict(advice,recommendation_id=key,source_run_verified=source is not None)


def resolve(jobs,key,config):
    if not isinstance(key,str) or not re.fullmatch(r"advice-[a-f0-9]{32}",key):
        raise ValueError("Invalid recommendation_id")
    record=json.loads((jobs.root/".recommendations"/(key+".json")).read_text())
    if any(config.get(name)!=value for name,value in record["advice"]["config_patch"].items()):
        raise ValueError("Run configuration does not match the selected recommendation")
    return record


def assess(out,manifest,summary):
    record=manifest.get("recommendation")
    if not record:
        return None
    result={"recommendation_id":record["recommendation_id"],"source_run_id":(record.get("source") or {}).get("run_id"),
            "status":"insufficient_data","causality_established":False}
    if not record.get("source"):
        result["reason"]="Source snapshot was not verified against saved run records."
        return result
    try:
        source=Path(out).parent/record["source"]["run_id"]
        old_manifest=json.loads((source/"manifest.json").read_text())
        baseline=json.loads((source/"request_summary.json").read_text())
        keys=("model_path","workload_path","requests","warmup","concurrency","max_tokens","seed","gpu_kv_blocks","trace_profile","slo_ttft_ms","slo_e2e_ms","slo_tpot_ms")
        same=all(manifest["config"].get(k)==old_manifest["config"].get(k) for k in keys)
        identity_keys=("model_weights_digest","workload_sha256","config.json_sha256","tokenizer_config.json_sha256","git_commit")
        result["identity_checks"]={k:bool(manifest.get(k)) and manifest.get(k)==old_manifest.get(k) for k in identity_keys}
        result["identity_checks"]["clean_worktree"]=manifest.get("git_worktree_dirty") is False and old_manifest.get("git_worktree_dirty") is False
        same &= all(result["identity_checks"].values())
        result["comparable_configuration"]=bool(same)
        for metric in ("client_ttft_ms_p95","client_e2e_ms_p95","successful_output_tokens_per_second"):
            a,b=baseline.get(metric),summary.get(metric)
            result[metric]={"baseline":a,"candidate":b,"relative_change_pct":(b-a)/a*100 if isinstance(a,(int,float)) and a and isinstance(b,(int,float)) else None}
        if manifest.get("status")!="completed" or old_manifest.get("status")!="completed":
            result["reason"]="Both source and follow-up experiments must complete before assessing benefit."
        elif not same:
            result["status"]="incomparable_configuration"
        elif min(baseline.get("successful_requests",0),summary.get("successful_requests",0))<100:
            result["reason"]="Short functional runs cannot establish a performance benefit; use repeated comparisons."
        else:
            result["status"]="paired_observation_requires_repeats"
            result["reason"]="Paired observations saved; repeated randomized comparisons are required for a decision."
    except (OSError,ValueError,KeyError) as exc:
        result["reason"]="Baseline evidence unavailable: "+str(exc)
    return result
