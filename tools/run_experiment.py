"""Managed Linux inference experiment. Raw records survive failure and cancellation."""
import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
from uuid import uuid4

from app.monitor import Monitor
from app.runtime import identity, PortLease
from app.collectors import capability, SchedulerSession
from tools.slo import evaluate as evaluate_slo
from collector.correlator import diagnose_system_bottlenecks
from contracts.schema import utc_now
from tools.benchmark import get_resource, load_cases, request_one, summarize

ROOT = Path(__file__).resolve().parents[1]


def write_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(path)


def append_json(path, value):
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n")


def load_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def run_id(policy):
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + policy.replace("_", "-") + "-" + uuid4().hex[:10]


def validate_config(config):
    if not isinstance(config,dict):
        raise ValueError("Experiment configuration must be an object")
    cfg = {
        "model_path": "/opt/tracekv/models/qwen2.5-0.5b-instruct",
        "served_name": "tracekv-model", "vllm_version": "0.11.2",
        "workload_path": "workloads/smoke.jsonl", "policy": "gpu_only",
        "requests": 6, "warmup": 1, "concurrency": 1, "max_tokens": 32,
        "gpu_kv_blocks": 64, "cpu_kv_blocks": 0,
        "max_model_len": 1024, "max_num_seqs": 4, "port": 8000, "sample_interval_ms": 1000,
        "timeout": 120, "startup_timeout": 600,
        "trace_profile": "sampling", "seed": 0,
        "slo_ttft_ms": None, "slo_e2e_ms": None, "slo_tpot_ms": None,
    }
    unknown = set(config) - set(cfg)
    if unknown:
        raise ValueError("Unknown configuration fields: " + ", ".join(sorted(unknown)))
    cfg.update(config)
    ranges = {"requests": (1, 10000), "warmup": (0, 100), "concurrency": (1, 4),
              "max_tokens": (1, 512), "gpu_kv_blocks": (1, 100000),
              "cpu_kv_blocks": (0, 100000), "max_model_len": (128, 32768),
              "max_num_seqs": (1, 16), "port": (1024, 65535), "sample_interval_ms": (250, 10000),
              "timeout": (1, 120), "startup_timeout": (10, 900)}
    for key, (lo, hi) in ranges.items():
        val = cfg[key]
        if type(val) is not int or not lo <= val <= hi:
            raise ValueError(f"{key} must be an integer in [{lo}, {hi}]")
    if cfg["trace_profile"] not in ("off", "sampling", "ebpf"):
        raise ValueError("trace_profile must be off, sampling or ebpf")
    if type(cfg["seed"]) is not int or not 0 <= cfg["seed"] <= 2147483647:
        raise ValueError("seed must be an integer in [0, 2147483647]")
    for key in ("slo_ttft_ms", "slo_e2e_ms", "slo_tpot_ms"):
        value = cfg[key]
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int,float)) or not math.isfinite(value) or value <= 0):
            raise ValueError(key + " must be a positive finite number or null")
    if cfg["policy"] not in ("gpu_only", "cpu_lru"):
        raise ValueError("policy must be gpu_only or cpu_lru")
    if (cfg["policy"] == "gpu_only") != (cfg["cpu_kv_blocks"] == 0):
        raise ValueError("gpu_only requires zero CPU blocks; cpu_lru requires positive CPU blocks")
    for key in ("served_name", "vllm_version", "model_path", "workload_path"):
        if not isinstance(cfg[key], str) or not cfg[key].strip():
            raise ValueError(key + " must be a nonempty string")
    if cfg["vllm_version"] != "0.11.2":
        raise ValueError("This runner is verified for vLLM 0.11.2")
    if not Path(cfg["model_path"]).is_dir():
        raise ValueError("Local model directory does not exist")
    workload = Path(cfg["workload_path"])
    if not workload.is_absolute():
        workload = ROOT / workload
    cfg["workload_path"] = str(workload.resolve())
    load_cases(workload)
    return cfg



def create_run(config, result_root, origin="cli", comparison_id=None, recommendation=None):
    """Create one validated, isolated experiment, shared by CLI and web API."""
    cfg = validate_config(config)
    root = Path(result_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    name = run_id(cfg["policy"])
    out = root / name
    out.mkdir(exist_ok=False)
    (out / "logs").mkdir()
    manifest = {
        "schema_version": 1, "run_id": name, "comparison_id": comparison_id,
        "origin": origin, "recommendation": recommendation,
        "status": "running", "phase": "queued", "created_at_utc": utc_now(),
        "config": cfg, "recorded_requests": 0,
        "sources": {"sched": {"status": "unavailable",
                              "error": "Not requested or collector not started"}},
    }
    write_json(out / "manifest.json", manifest)
    return out, manifest

def stop_server(server):
    if server is None:
        return
    try:
        os.killpg(server.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        server.poll()  # reap the group leader
        try:
            os.killpg(server.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        try:
            os.killpg(server.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    server.wait(timeout=5)


def analyze(out):
    from app.integrations import summarize as summarize_telemetry
    manifest = json.loads((out / "manifest.json").read_text())
    telemetry = summarize_telemetry(out)
    write_json(out / "telemetry_summary.json", telemetry)
    records = load_jsonl(out / "requests.jsonl")
    samples = load_jsonl(out / "snapshots.jsonl")
    started = manifest.get("measurement_started_at_utc")
    ended = manifest.get("measurement_finished_at_utc")
    duration = manifest.get("measurement_seconds", 0)
    summary = summarize(records, duration)
    slo = evaluate_slo(records, duration, manifest["config"])
    summary["slo"] = slo
    write_json(out / "request_summary.json", summary)
    aligned = out / "aligned_snapshots.jsonl"
    aligned.write_text("", encoding="utf-8")
    limits = [
        "GPU metrics describe device index 0; system metrics describe the whole host.",
        "External eBPF/cache batches are summarized separately by collector and scope; they are not injected into sampled gauges.",
        "No automatic causal attribution or strategy performance claim is made.",
        "KV ratios retain actual sampling intervals; they are not averaged into fixed buckets.",
    ]
    if started and ended:
        epoch = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        start_s, end_s = epoch(started), epoch(ended)
        width = manifest["config"]["sample_interval_ms"] / 1000
        for i in range(max(1, math.ceil((end_s - start_s) / width))):
            left, right = start_s + i * width, min(end_s, start_s + (i + 1) * width)
            items = [s for s in samples if left <= epoch(s["timestamp"]) < right]
            row = {
                "schema_version": "0.1", "run_id": manifest["run_id"],
                "timestamp": datetime.fromtimestamp(right, timezone.utc).isoformat().replace("+00:00", "Z"),
                "window_start_utc": datetime.fromtimestamp(left, timezone.utc).isoformat().replace("+00:00", "Z"),
                "window_end_utc": datetime.fromtimestamp(right, timezone.utc).isoformat().replace("+00:00", "Z"),
                "window_ms": max(1, round((right - left) * 1000)),
                "system": {"mem_available_mb": None, "disk_read_p95_ms": None, "disk_read_bytes_per_s": None},
                "gpu": {"memory_used_mb": None},
                "kv": {"usage_ratio": None, "prefix_hit_ratio": None, "running_requests": None},
                "sources": {"system": "unavailable", "gpu": "unavailable", "kv": "unavailable"},
                "sample_count": len(items), "aggregation": "last gauge in window; KV interval retained separately",
                "requests_started": sum(left <= epoch(r["started_at_utc"]) < right for r in records),
                "requests_finished": sum(left <= epoch(r["finished_at_utc"]) < right for r in records),
            }
            if items:
                last = items[-1]
                row["system"], row["gpu"], row["sources"] = last["system"], last["gpu"], last["sources"]
                row["kv"] = dict(last["kv"], prefix_hit_ratio=None)
                row["kv_interval"] = {"ended_at_utc": last["timestamp"], "window_ms": last["window_ms"],
                                      "prefix_hit_ratio": last["kv"]["prefix_hit_ratio"]}
            append_json(aligned, row)
    from engine.histograms import summarize_samples
    stage_summary = summarize_samples(samples)
    write_json(out / "stage_summary.json", stage_summary)
    from app.recommendations import assess
    assessment = assess(out, manifest, summary)
    analysis = {"run_id": manifest["run_id"], "status": manifest["status"],
                "summary": summary, "source_status": manifest.get("sources", {}),
                "limitations": limits, "error": manifest.get("error"),
                "telemetry": telemetry, "telemetry_revision": telemetry["revision"], "slo": slo,
                "trace_profile": manifest["config"].get("trace_profile", "sampling"),
                "stage_histograms": stage_summary, "recommendation_assessment": assessment}
    diagnosis = diagnose_system_bottlenecks(samples)
    diagnosis["run_id"] = manifest["run_id"]
    analysis["diagnosis"] = diagnosis
    write_json(out / "bottleneck_diagnosis.json", diagnosis)
    write_json(out / "analysis.json", analysis)
    write_json(out / "summary.json", {
        "run_id": manifest["run_id"], "status": manifest["status"],
        "config": manifest["config"], "requests": summary,
        "source_status": manifest.get("sources", {}),
        "diagnosis": diagnosis, "limitations": limits, "telemetry": telemetry,
        "stage_histograms": stage_summary, "recommendation_assessment": assessment,
    })
    lines = ["# TraceKV 实验报告", "", f"- run_id: {manifest['run_id']}",
             f"- 状态: {manifest['status']}", f"- 策略: {manifest['config']['policy']}",
             "- 缓存准备: 新服务启动后执行固定工作负载预热；未逐请求重置缓存。",
             f"- 成功/已记录请求: {summary['successful_requests']}/{summary['requests']}",
             f"- 计划请求数: {manifest['config']['requests']}",
             f"- TTFT P95 (ms): {summary['client_ttft_ms_p95']}",
             f"- E2E P95 (ms): {summary['client_e2e_ms_p95']}",
             f"- 输出吞吐 (token/s): {summary['successful_output_tokens_per_second']}",
             f"- 采集范围: {manifest['config'].get('trace_profile', 'sampling')}",
             f"- SLO 达标请求: {slo['passed']}；未知: {slo['unknown']}；未配置: {not slo['configured']}",
             f"- SLO 达标吞吐 (请求/s): {slo['goodput_requests_per_second']}",
             "- 客户端 TPOT 为内容块粒度估计；内容块间隔不解释为逐 token ITL。",
             f"- 错误: {manifest.get('error') or '无'}",
             "- 短负载结果仅用于链路验收；小样本分位数不能作为性能收益结论。", "", "## 数据与限制", ""]
    lines += ["- " + item for item in limits]
    lines += ["", "## 调查提示", "",
              "- 提示类型: " + diagnosis["primary_bottleneck"],
              "- " + diagnosis["summary"],
              "- 以上为阈值调查提示，未建立因果关系；缺失指标不能说明系统正常。",
              "- 规则阈值未经过瓶颈分类准确率校准。",
              "- 有效采样数: " + json.dumps(diagnosis["source_coverage"], ensure_ascii=False),
              "", "## 可追溯文件", "",
              "- 配置与进程记录: manifest.json",
              "- 请求与统计: requests.jsonl、request_summary.json",
              "- 原始采样与窗口结果: snapshots.jsonl、aligned_snapshots.jsonl",
              "- 调查依据: bottleneck_diagnosis.json",
              "- 统一结果清单: summary.json",
              "- 外部缓存与 eBPF 接入统计: telemetry_summary.json", "", "## 外部接入数据", "",
              "- 缓存事件数: " + str(telemetry["sources"]["cache"]["event_count"]),
              "- eBPF 事件数: " + str(telemetry["sources"]["ebpf"]["event_count"]),
              "- 缺失与采集失败、目标范围和丢失统计见 telemetry_summary.json。",
              "- 外部批次不混入实时快照调查规则，也不构成逐请求因果证明。",
              "", "## 阶段指标与建议复核", "",
              "- 排队/prefill/decode 等带标签直方图见 stage_summary.json。",
              "- 建议复核: " + (assessment["status"] if assessment else "本次未关联策略建议"),
              "- 平台不会根据单次短负载宣称建议有效。"]

    (out / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return analysis


def execute(out, cfg):
    manifest = json.loads((out / "manifest.json").read_text())
    stop = threading.Event()
    server = None
    collector = None
    lease = PortLease(cfg["port"])
    sampling_enabled = cfg["trace_profile"] != "off"

    def cancel(*_):
        stop.set()
        if server is not None:
            try:
                os.killpg(server.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, cancel)
    manifest["runner_identity"] = identity()
    manifest["runner_ready"] = True
    write_json(out / "manifest.json", manifest)
    if (out / "cancel.flag").exists():
        stop.set()
    sampling_thread = None
    sampler_stop = threading.Event()
    monitor = Monitor(f"http://127.0.0.1:{cfg['port']}/metrics", cfg["sample_interval_ms"] / 1000)
    monitor.run_id = out.name
    records = []
    manifest.setdefault("sources", {})
    for source in ("gpu", "system", "kv"):
        manifest["sources"].setdefault(source, {"status": "unavailable", "error": "No sample collected"})
    if not sampling_enabled:
        for source in ("gpu", "system", "kv", "sched"):
            manifest["sources"][source] = {"status": "disabled", "error": "Disabled by trace_profile=off"}
    manifest["instrumentation_note"] = "Profile controls platform GPU/system/vLLM sampling and optional eBPF; request logging and CPU offload event hooks are common to all profiles."
    coverage = {}
    measurement_start = None
    log = None
    try:
        lease.acquire(out.name)
        if cfg["trace_profile"] == "ebpf" and not capability()["can_attempt"]:
            raise RuntimeError("eBPF required: " + capability()["reason"])
        if stop.is_set():
            raise InterruptedError("Experiment cancelled before startup")
        if platform.system() != "Linux":
            raise RuntimeError("Managed inference experiments require Linux")
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", cfg["port"])) == 0:
                raise RuntimeError("vLLM port occupied; this runner will not stop an existing server")
        cases = load_cases(Path(cfg["workload_path"]))
        manifest["workload_sha256"] = hashlib.sha256(Path(cfg["workload_path"]).read_bytes()).hexdigest()
        manifest["cache_preparation"] = "fresh server, deterministic cyclic warmup; no per-request reset"
        manifest["python"] = sys.version
        manifest["kernel"] = platform.release()
        manifest["clock_anchor"] = {"wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}
        version = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
        manifest["git_commit"] = version.stdout.strip() or None
        diff = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True)
        manifest["git_worktree_dirty"] = bool(diff.stdout.strip())
        model = Path(cfg["model_path"])
        for filename in ("config.json", "tokenizer_config.json"):
            file = model / filename
            if file.exists():
                manifest[filename + "_sha256"] = hashlib.sha256(file.read_bytes()).hexdigest()
        weight_hashes = []
        for file in sorted(list(model.glob("*.safetensors")) + list(model.glob("*.bin"))):
            digest = hashlib.sha256()
            with file.open("rb") as weight_file:
                for chunk in iter(lambda: weight_file.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
            weight_hashes.append({"name":file.name,"bytes":file.stat().st_size,"sha256":digest.hexdigest()})
        manifest["model_weight_files"] = weight_hashes
        manifest["model_weights_digest"] = hashlib.sha256(json.dumps(weight_hashes,sort_keys=True).encode()).hexdigest() if weight_hashes else None
        manifest["model_revision"] = None
        manifest["model_identity_note"] = "Local weight files and configs are hashed; upstream weight revision is not independently verified."
        command = [sys.executable, str(ROOT / "tools/launch_vllm.py"),
                   "--model", cfg["model_path"], "--expected-version", cfg["vllm_version"],
                   "--served-name", cfg["served_name"], "--port", str(cfg["port"]),
                   "--max-model-len", str(cfg["max_model_len"]), "--max-num-seqs", str(cfg["max_num_seqs"]),
                   "--num-gpu-blocks-override", str(cfg["gpu_kv_blocks"]), "--enforce-eager",
                   "--out", str(out / "server")]
        if cfg["policy"] == "cpu_lru":
            command += ["--cpu-kv-blocks", str(cfg["cpu_kv_blocks"])]
        manifest["server_command"] = command
        env = dict(os.environ, HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                   VLLM_NO_USAGE_STATS="1", DO_NOT_TRACK="1", OMP_NUM_THREADS="4", TRACEKV_RUN_ID=out.name,
                   TOKENIZERS_PARALLELISM="false")
        log = (out / "logs/server-launcher.log").open("w")
        cache_root = Path("/opt/tracekv/cache")
        if cache_root.is_dir():
            env.update(XDG_CACHE_HOME=str(cache_root), HF_HOME=str(cache_root / "huggingface"),
                       TRITON_CACHE_DIR=str(cache_root / "triton"), VLLM_CACHE_ROOT=str(cache_root / "vllm"),
                       CUDA_CACHE_PATH=str(cache_root / "cuda"))
        manifest["runtime_environment"] = {key: env[key] for key in (
            "OMP_NUM_THREADS", "HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE",
            "XDG_CACHE_HOME", "HF_HOME", "TRITON_CACHE_DIR", "VLLM_CACHE_ROOT", "CUDA_CACHE_PATH") if key in env}
        server = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                  start_new_session=True, env=env)
        manifest["server_pid"] = server.pid
        manifest["server_identity"] = identity(server.pid)
        manifest["phase"] = "starting_server"
        write_json(out / "manifest.json", manifest)
        base = f"http://127.0.0.1:{cfg['port']}"
        deadline = time.monotonic() + cfg["startup_timeout"]
        while True:
            if stop.is_set():
                raise InterruptedError("Experiment cancelled")
            if server.poll() is not None:
                raise RuntimeError("vLLM exited; inspect logs/server-launcher.log and server/server.log")
            try:
                get_resource(base + "/health", 1)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("vLLM startup timed out")
                stop.wait(1)
        manifest["phase"] = "warmup"
        write_json(out / "manifest.json", manifest)
        for i in range(cfg["warmup"]):
            if stop.is_set():
                raise InterruptedError("Experiment cancelled")
            result = request_one(base, cfg["served_name"], cases[i % len(cases)], i, cfg["max_tokens"], cfg["timeout"], seed=cfg["seed"])
            append_json(out / "warmup.jsonl", dict(result, run_id=out.name))
            if not result["success"]:
                raise RuntimeError("Warmup request failed; inspect warmup.jsonl")

        if cfg["trace_profile"] == "ebpf":
            collector = SchedulerSession(out, server.pid).start()
            manifest["sources"]["sched"] = collector.state()

        def sample():
            value = monitor.sample()
            append_json(out / "snapshots.jsonl", value)
            for domain, filename in (("kv", "kv_samples.jsonl"), ("gpu", "gpu_samples.jsonl"), ("system", "system_samples.jsonl")):
                append_json(out / filename, dict(value[domain], run_id=out.name, timestamp=value["timestamp"],
                                                window_ms=value["window_ms"], source=value["sources"][domain]))
            for source, state in value["source_status"].items():
                if source == "sched" and collector is not None:
                    continue
                count = coverage.setdefault(source, {"sample_count": 0, "failed_sample_count": 0,
                    "first_sample_utc": None, "last_sample_utc": None})
                if state["status"] == "ok":
                    count["sample_count"] += 1
                    count["first_sample_utc"] = count["first_sample_utc"] or value["timestamp"]
                    count["last_sample_utc"] = value["timestamp"]
                elif state["status"] == "failed":
                    count["failed_sample_count"] += 1
                manifest["sources"][source] = dict(state, **count)

        if sampling_enabled:
            sample()  # counter baseline before the formal measurement
        manifest["measurement_started_at_utc"] = utc_now()
        measurement_start = time.monotonic()
        manifest["phase"] = "measuring"
        write_json(out / "manifest.json", manifest)

        def sampling():
            while not sampler_stop.wait(cfg["sample_interval_ms"] / 1000):
                try:
                    sample()
                except Exception as exc:
                    manifest["sampling_error"] = str(exc)

        if sampling_enabled:
            sample()
            sampling_thread = threading.Thread(target=sampling, daemon=True)
            sampling_thread.start()

        def request(i):
            item = request_one(base, cfg["served_name"], cases[i % len(cases)], i, cfg["max_tokens"], cfg["timeout"], seed=cfg["seed"])
            item.update(run_id=out.name)
            item["started_at_utc"] = item["started_at_utc"].replace("+00:00", "Z")
            return item

        pool = ThreadPoolExecutor(max_workers=cfg["concurrency"])
        pending = {}
        next_index = 0
        try:
            while next_index < cfg["requests"] or pending:
                if stop.is_set():
                    stop_server(server)
                    server = None
                    raise InterruptedError("Experiment cancelled")
                if server.poll() is not None:
                    raise RuntimeError("vLLM exited during measurement")
                if collector is not None and collector.error:
                    raise RuntimeError("Required collector failed: " + collector.error)
                while next_index < cfg["requests"] and len(pending) < cfg["concurrency"]:
                    future = pool.submit(request, next_index)
                    pending[future] = next_index
                    next_index += 1
                done, _ = wait(pending, timeout=0.2, return_when=FIRST_COMPLETED)
                for future in done:
                    pending.pop(future)
                    record = future.result()
                    records.append(record)
                    append_json(out / "requests.jsonl", record)
                manifest["recorded_requests"] = len(records)
                write_json(out / "manifest.json", manifest)
        finally:
            if stop.is_set() or pending:
                stop_server(server)
                server = None
            pool.shutdown(wait=True, cancel_futures=True)
            for future in pending:
                if not future.cancelled():
                    try:
                        record = future.result()
                        records.append(record)
                        append_json(out / "requests.jsonl", record)
                    except Exception:
                        pass
        manifest["measurement_finished_at_utc"] = utc_now()
        manifest["measurement_seconds"] = time.monotonic() - measurement_start
        manifest["status"] = "completed" if all(r["success"] for r in records) else "failed"
        if manifest["status"] == "failed":
            manifest["error"] = "One or more requests failed"
    except Exception as exc:
        manifest["status"] = "interrupted" if stop.is_set() else "failed"
        manifest["error"] = str(exc)
    finally:
        sampler_stop.set()
        if sampling_thread:
            sampling_thread.join(timeout=8)
        if measurement_start and "measurement_finished_at_utc" not in manifest:
            manifest["measurement_finished_at_utc"] = utc_now()
            manifest["measurement_seconds"] = time.monotonic() - measurement_start
        if collector is not None:
            try:
                manifest["sources"]["sched"] = collector.stop()
            except Exception as exc:
                collector.error = str(exc)
                manifest["sources"]["sched"] = {"status":"failed","error":str(exc)}
            if collector.error and manifest.get("status") == "completed":
                manifest["status"] = "failed"
                manifest["error"] = "Required collector failed: " + collector.error
        if sampling_enabled and server and server.poll() is None and measurement_start:
            try:
                sample()
            except Exception as exc:
                manifest["final_sample_error"] = str(exc)
        # Normalize worker-owned event files after writers have stopped.
        try:
            stop_server(server)
        except Exception as exc:
            manifest.update(status="failed",error="Server cleanup failed: "+str(exc))
        if log:
            log.close()
        for filename in ("kv_transfers.jsonl", "kv_cpu_occupancy.jsonl"):
            source = out / "server" / filename
            if source.exists():
                try:
                    for item in load_jsonl(source):
                        append_json(out / filename, dict(item, run_id=out.name))
                except (OSError,ValueError) as exc:
                    manifest["normalization_error"]=str(exc)
                    manifest["status"]="failed"
        manifest["recorded_requests"] = len(records)
        manifest["finished_at_utc"] = utc_now()
        manifest["phase"] = "finished"
        write_json(out / "manifest.json", manifest)
        try:
            analyze(out)
        finally:
            lease.release()
    return 0 if manifest["status"] == "completed" else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    entry = parser.add_mutually_exclusive_group(required=True)
    entry.add_argument("--run-dir", type=Path, help="Existing run directory")
    entry.add_argument("--config", type=Path, help="JSON experiment configuration; creates a new run_id")
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results")
    parser.add_argument("--analyze-only", action="store_true")
    args = parser.parse_args()
    if args.analyze_only:
        if args.config:
            parser.error("--analyze-only requires --run-dir")
        analyze(args.run_dir)
        return 0
    if args.config:
        try:
            config = json.loads(args.config.read_text(encoding="utf-8"))
            if not isinstance(config, dict):
                raise ValueError("Configuration must be a JSON object")
            args.run_dir, manifest = create_run(config, args.results_dir)
        except (OSError, ValueError) as exc:
            parser.error(str(exc))
        print(json.dumps({"run_id": manifest["run_id"], "result_dir": str(args.run_dir)},
                         ensure_ascii=False), flush=True)
    data = json.loads((args.run_dir / "manifest.json").read_text())
    return execute(args.run_dir, validate_config(data["config"]))


if __name__ == "__main__":
    raise SystemExit(main())
