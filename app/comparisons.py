"""Serial, randomized repeated comparisons with explicit instrumentation scope."""
import itertools
import json
import math
from pathlib import Path
import random
import statistics
import threading
from uuid import uuid4
from contracts.schema import utc_now
from app.runtime import identity, alive
from app.collectors import capability
from tools.run_experiment import validate_config, write_json, load_jsonl, analyze


def validate_request(data):
    allowed = {"base_config","policies","trace_profiles","repetitions","cpu_kv_blocks","order_seed","effect_threshold_pct"}
    if not isinstance(data,dict) or set(data)-allowed:
        raise ValueError("Unknown comparison fields")
    base = validate_config(data.get("base_config",{}))
    policies = data.get("policies",["gpu_only","cpu_lru"])
    traces = data.get("trace_profiles",["off","sampling"])
    for values, valid, name in ((policies,{"gpu_only","cpu_lru"},"policies"),(traces,{"off","sampling","ebpf"},"trace_profiles")):
        if not isinstance(values,list) or not values or len(set(values))!=len(values) or any(v not in valid for v in values):
            raise ValueError("Invalid unique "+name)
    repetitions = data.get("repetitions",3)
    cpu = data.get("cpu_kv_blocks",72)
    seed = data.get("order_seed",42)
    threshold = data.get("effect_threshold_pct",5)
    if type(repetitions) is not int or not 1<=repetitions<=10:
        raise ValueError("repetitions must be 1..10")
    if type(cpu) is not int or not 1<=cpu<=100000:
        raise ValueError("cpu_kv_blocks must be positive")
    if type(seed) is not int or not 0<=seed<=2147483647:
        raise ValueError("order_seed must be a nonnegative 32-bit integer")
    if isinstance(threshold,bool) or not isinstance(threshold,(int,float)) or not math.isfinite(threshold) or not 0<=threshold<=100:
        raise ValueError("effect_threshold_pct must be finite in [0,100]")
    if "ebpf" in traces and not capability()["can_attempt"]:
        raise ValueError("Requested eBPF profile unavailable: "+str(capability()["reason"]))
    return dict(base_config=base,policies=policies,trace_profiles=traces,repetitions=repetitions,
                cpu_kv_blocks=cpu,order_seed=seed,effect_threshold_pct=threshold)


def schedule(config):
    rng = random.Random(config["order_seed"])
    result = []
    for repeat in range(config["repetitions"]):
        cells = list(itertools.product(config["policies"],config["trace_profiles"]))
        rng.shuffle(cells)
        for policy,trace in cells:
            result.append({"repeat":repeat+1,"policy":policy,"trace_profile":trace,"status":"pending","run_id":None})
    return result


def summarize_comparison(folder, result_root):
    manifest = json.loads((folder/"manifest.json").read_text())
    cells, outputs, run_info = {}, {}, []
    for member in manifest["members"]:
        key = member["policy"]+"/"+member["trace_profile"]
        cell = cells.setdefault(key,{"policy":member["policy"],"trace_profile":member["trace_profile"],"members":[],"completed":0,"failed_or_interrupted":0,"pending":0})
        item = dict(member)
        if member["run_id"]:
            out = Path(result_root)/member["run_id"]
            try:
                run_manifest = json.loads((out/"manifest.json").read_text())
                item["status"] = run_manifest["status"]
                item["summary"] = json.loads((out/"request_summary.json").read_text()) if (out/"request_summary.json").exists() else None
                item["source_status"] = run_manifest.get("sources",{})
                item["identities"] = {k:run_manifest.get(k) for k in ("workload_sha256","config.json_sha256","tokenizer_config.json_sha256","model_weights_digest","model_revision","git_commit","git_worktree_dirty")}
                records = load_jsonl(out/"requests.jsonl")
                outputs[(member["repeat"],key)] = {(r.get("request_id"),r.get("case_id")):r.get("output_text") for r in records if r.get("success")}
                run_info.append(item)
            except (OSError,ValueError) as exc:
                item["error"]=str(exc)
                item["status"]="failed"
        if item["status"]=="completed":
            cell["completed"]+=1
        elif item["status"] in ("pending","running"):
            cell["pending"]+=1
        else:
            cell["failed_or_interrupted"]+=1
        cell["members"].append(item)
    keys = ("client_ttft_ms_p95","client_e2e_ms_p95","client_tpot_estimate_ms_p95","successful_output_tokens_per_second","successful_requests_per_second")
    for key,cell in cells.items():
        completed = [m["summary"] for m in cell["members"] if m["status"]=="completed" and m.get("summary")]
        cell["descriptive_statistics"]={}
        for metric in keys:
            values=[s[metric] for s in completed if isinstance(s.get(metric),(int,float))]
            cell["descriptive_statistics"][metric]={"mean":statistics.mean(values) if values else None,"min":min(values) if values else None,"max":max(values) if values else None,"rounds":len(values)}
        values=[s.get("slo",{}).get("goodput_requests_per_second") for s in completed]
        values=[v for v in values if isinstance(v,(int,float))]
        cell["slo_goodput_mean"]=statistics.mean(values) if values else None
        cell["successful_requests"]=sum(s["successful_requests"] for s in completed)
        cell["recorded_requests_in_completed_rounds"]=sum(s["requests"] for s in completed)
        cell["comparison_result"]="baseline" if cell["policy"]=="gpu_only" else "insufficient_data"
    for key,cell in cells.items():
        if cell["policy"]=="cpu_lru":
            baseline=cells.get("gpu_only/"+cell["trace_profile"])
            consistency=[]
            for m in cell["members"]:
                ours=outputs.get((m["repeat"],key))
                theirs=outputs.get((m["repeat"],"gpu_only/"+cell["trace_profile"]))
                if ours is not None and theirs is not None:
                    consistency.append(bool(ours) and ours==theirs)
            cell["output_consistency"]=all(consistency) if consistency else None
            paired_complete=bool(baseline and cell["completed"]>=3 and baseline["completed"]>=3
                and not cell["failed_or_interrupted"] and not baseline["failed_or_interrupted"]
                and not cell["pending"] and not baseline["pending"])
            enough=paired_complete and all(m.get("summary",{}).get("successful_requests",0)>=100 for m in cell["members"]+baseline["members"])
            if enough and cell["output_consistency"]:
                ours=cell["descriptive_statistics"]["client_e2e_ms_p95"]["mean"]
                theirs=baseline["descriptive_statistics"]["client_e2e_ms_p95"]["mean"]
                gain=(theirs-ours)/theirs*100 if theirs else None
                cell["e2e_p95_improvement_pct"]=gain
                threshold=manifest["config"]["effect_threshold_pct"]
                if gain is not None:
                    cell["comparison_result"]="observed_improvement" if gain>threshold else "observed_regression" if gain<-threshold else "no_clear_gain"
            elif consistency and not all(consistency):
                cell["comparison_result"]="output_mismatch"
    source_sets={}
    for name in ("workload_sha256","config.json_sha256","tokenizer_config.json_sha256","model_weights_digest","git_commit"):
        values={m["identities"].get(name) for m in run_info}
        source_sets[name]={"consistent":len(values)==1 and None not in values,"values":sorted(v for v in values if v is not None)}
    overhead = []
    for key,cell in cells.items():
        if cell["trace_profile"]=="off":
            continue
        baseline=cells.get(cell["policy"]+"/off")
        if baseline:
            ours=cell["descriptive_statistics"]["client_e2e_ms_p95"]["mean"]
            theirs=baseline["descriptive_statistics"]["client_e2e_ms_p95"]["mean"]
            overhead.append({"policy":cell["policy"],"profile":cell["trace_profile"],
                             "e2e_p95_relative_change_pct":(ours-theirs)/theirs*100 if ours is not None and theirs else None,
                             "completed_rounds":cell["completed"],"baseline_rounds":baseline["completed"],
                             "interpretation":"Descriptive instrumentation comparison; negative values may reflect noise or workload variability."})
    summary={"comparison_id":manifest["comparison_id"],"status":manifest["status"],"config":manifest["config"],
             "cells":cells,"identity_checks":source_sets,"instrumentation_comparisons":overhead,
             "evaluation_rule":{"minimum_completed_rounds":3,"minimum_successful_requests_per_round":100,
                                "effect_threshold_pct":manifest["config"]["effect_threshold_pct"],
                                "primary_observation":"client E2E P95; also inspect SLO goodput, TTFT, failures and resource cost",
                                "causality_established":False},
             "limitations":["Results below the sample/round gate remain insufficient_data.",
                            "Per-round P95 values are compared descriptively, not averaged to claim a pooled request P95.",
                            "off disables platform sampling/eBPF; request logging and offload event hooks remain common instrumentation.",
                            "CPU LRU uses additional CPU capacity; fixed GPU budget does not mean equal total memory resources.",
                            "No significance test or causal attribution is implied.",
                            "SLO goodput and TTFT are reported separately; the decision label uses the declared E2E-P95 observation rule."]}
    source_sets["clean_worktree"]={"consistent":bool(run_info) and all(m["identities"].get("git_worktree_dirty") is False for m in run_info)}
    if any(not check["consistent"] for check in source_sets.values()):
        for cell in cells.values():
            if cell["policy"]=="cpu_lru":
                cell["comparison_result"]="incomparable_or_unverified_identity"
    write_json(folder/"summary.json",summary)
    lines=["# TraceKV 多轮对照报告","",f"- comparison_id: {manifest['comparison_id']}",f"- 状态: {manifest['status']}","",
           "| 策略/采集范围 | 完成/失败或中断 | 成功请求 | 观察结论 |","| --- | --- | --- | --- |"]
    for key,cell in cells.items():
        lines.append(f"| {key} | {cell['completed']}/{cell['failed_or_interrupted']} | {cell['successful_requests']} | {cell['comparison_result']} |")
    lines+=["","## 各组合描述统计","",
            "| 组合 | TTFT P95 均值 ms | E2E P95 均值 ms | 客户端 TPOT 估计 P95 均值 ms | SLO 达标吞吐均值 请求/s |",
            "| --- | --- | --- | --- | --- |"]
    for key,cell in cells.items():
        stats=cell["descriptive_statistics"]
        lines.append(f"| {key} | {stats['client_ttft_ms_p95']['mean']} | {stats['client_e2e_ms_p95']['mean']} | {stats['client_tpot_estimate_ms_p95']['mean']} | {cell['slo_goodput_mean']} |")
    lines+=["","## 采集开销对照（描述性结果）",""]+["- "+json.dumps(row,ensure_ascii=False) for row in overhead]
    lines+=["","## 可比性核验",""]+["- "+key+": "+json.dumps(value,ensure_ascii=False) for key,value in source_sets.items()]
    lines+=["","## 执行配置","",json.dumps(manifest["config"],ensure_ascii=False,indent=2)]
    lines+=["","## 原始实验",""]+["- 第 "+str(m["repeat"])+" 轮 "+m["policy"]+"/"+m["trace_profile"]+" "+str(m.get("run_id") or "未启动")+"："+m["status"]+("；"+m["error"] if m.get("error") else "") for m in manifest["members"]]
    lines+=["","## 评价规则与限制",""]+["- "+s for s in summary["limitations"]]
    (folder/"report.md").write_text("\n".join(lines)+"\n")
    return summary


class Comparisons:
    def __init__(self,jobs):
        self.jobs=jobs
        self.root=jobs.root/".comparisons"
        self.thread=None
        self.stop_event=threading.Event()
        self.active_id=None
        self.lock=threading.RLock()

    def resolve(self,name):
        if not name or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in name):
            raise ValueError("Invalid comparison_id")
        folder=self.root/name
        if folder.is_symlink() or folder.resolve().parent!=self.root.resolve() or not folder.is_dir():
            raise FileNotFoundError(name)
        return folder

    def start(self,data):
        config=validate_request(data)
        if str(Path(config["base_config"]["model_path"]).resolve()) not in self.jobs.models():
            raise ValueError("Select a configured model")
        with self.lock,self.jobs.lock:
            if self.thread and self.thread.is_alive() or self.jobs.reservation is not None or any(r["status"]=="running" for r in self.jobs.list()):
                raise RuntimeError("An experiment/comparison is already active")
            name="cmp-"+utc_now().replace(":","").replace("-","").split(".")[0].replace("Z","")+"-"+uuid4().hex[:8]
            self.root.mkdir(parents=True,exist_ok=True)
            folder=self.root/name
            folder.mkdir()
            manifest={"comparison_id":name,"status":"running","created_at_utc":utc_now(),"owner_identity":identity(),
                      "config":config,"members":schedule(config)}
            write_json(folder/"manifest.json",manifest)
            self.jobs.reservation=name
            self.active_id=name
            self.stop_event.clear()
            self.thread=threading.Thread(target=self._run,args=(folder,manifest),daemon=True,name="tracekv-comparison")
            self.thread.start()
            return manifest

    def _run(self,folder,manifest):
        try:
            for member in manifest["members"]:
                if self.stop_event.is_set():
                    break
                cfg=dict(manifest["config"]["base_config"],policy=member["policy"],trace_profile=member["trace_profile"],
                         cpu_kv_blocks=manifest["config"]["cpu_kv_blocks"] if member["policy"]=="cpu_lru" else 0)
                try:
                    run=self.jobs.start(cfg,comparison_id=manifest["comparison_id"],reservation=manifest["comparison_id"])
                    member.update(run_id=run["run_id"],status="running")
                    write_json(folder/"manifest.json",manifest)
                    proc=self.jobs.process
                    sent=False
                    while proc.poll() is None:
                        if self.stop_event.wait(.2) and not sent:
                            try:
                                self.jobs.stop(run["run_id"])
                            except ValueError:
                                if proc.poll() is None:
                                    raise
                            sent=True
                    proc.wait()
                    out=self.jobs.resolve(run["run_id"])
                    data=json.loads((out/"manifest.json").read_text())
                    if data["status"]=="running":
                        data.update(status="failed",phase="finished",error="Runner exited without final status",finished_at_utc=utc_now())
                        write_json(out/"manifest.json",data)
                        analyze(out)
                    member["status"]=data["status"]
                except Exception as exc:
                    member.update(status="failed",error=str(exc))
                write_json(folder/"manifest.json",manifest)
                summarize_comparison(folder,self.jobs.root)
            manifest["status"]="interrupted" if self.stop_event.is_set() else (
                "completed" if all(m["status"]=="completed" for m in manifest["members"]) else "failed")
        except Exception as exc:
            manifest.update(status="failed",error=str(exc))
        finally:
            manifest["finished_at_utc"]=utc_now()
            try:
                write_json(folder/"manifest.json",manifest)
                summarize_comparison(folder,self.jobs.root)
            finally:
                with self.jobs.lock:
                    if self.jobs.reservation==manifest["comparison_id"]:
                        self.jobs.reservation=None

    def stop(self,name):
        if name!=self.active_id or not self.thread or not self.thread.is_alive():
            raise ValueError("This controller does not own a live comparison")
        self.stop_event.set()
        return {"comparison_id":name,"status":"stopping"}

    def get(self,name):
        folder=self.resolve(name)
        manifest=json.loads((folder/"manifest.json").read_text())
        if manifest["status"]=="running" and not (name==self.active_id and self.thread and self.thread.is_alive()):
            manifest["status"]="external_running" if alive(manifest.get("owner_identity")) else "interrupted"
            manifest["error"]="Controller ownership changed; member runs retain their individual process state."
        result={"manifest":manifest}
        if (folder/"summary.json").exists():
            result["summary"]=json.loads((folder/"summary.json").read_text())
        return result

    def list(self):
        if not self.root.exists():
            return []
        return [self.get(p.name)["manifest"] for p in sorted(self.root.iterdir(),reverse=True) if p.is_dir() and not p.is_symlink()][:100]

    def close(self):
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=45)
