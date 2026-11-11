"""Compare GPU-only prefix caching with traced CPU LRU on real vLLM requests."""

import argparse
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from benchmark import get_resource, percentile, request_one


VERSION = "0.11.2"
MODEL_NAME = "tracekv-model"
GPU_BLOCKS = 33
HIT_COUNTERS = (
    "prefix_cache_queries_total", "prefix_cache_hits_total",
    "external_prefix_cache_queries_total", "external_prefix_cache_hits_total",
)
GPU_USAGE = "kv_cache_usage_perc"
DIRECTIONS = ("gpu_to_cpu", "cpu_to_gpu")


def workload():
    def case(name, word, tokens):
        prompt = (f"Reference for {word}." + f" {word}" * tokens
                  + "\nWrite the first twelve positive integers separated by commas.")
        return {"id": name, "messages": [{"role": "user", "content": prompt}]}

    seed = case("seed", "apple", 220)
    return [case("older-prefix", "lemon", 330), seed,
            case("pressure-1", "tiger", 330), case("pressure-2", "river", 330),
            {**seed, "id": "revisit"}]


def parse_metrics(body):
    values = {}
    for line in body.splitlines():
        if not line.startswith("vllm:"):
            continue
        metric, value = line.rsplit(maxsplit=1)
        name = metric.removeprefix("vllm:").split("{", 1)[0]
        if name in (*HIT_COUNTERS, GPU_USAGE):
            values[name] = values.get(name, 0) + float(value)
    return values


def counter_deltas(before, after):
    return {
        name: int(after[name] - before[name])
        if name in before and name in after and 0 <= before[name] <= after[name] < float("inf")
        else None
        for name in HIT_COUNTERS
    }


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def append_jsonl(path, value):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")


@contextmanager
def running_server(args, out, cpu_blocks):
    with socket.socket() as sock:
        if sock.connect_ex(("127.0.0.1", args.port)) == 0:
            raise RuntimeError("Port already occupied; choose another --port")

    command = [sys.executable, str(Path(__file__).with_name("launch_vllm.py")),
               "--model", str(args.model), "--expected-version", VERSION,
               "--served-name", MODEL_NAME, "--port", str(args.port),
               "--max-model-len", "512", "--max-num-seqs", "1",
               "--num-gpu-blocks-override", str(GPU_BLOCKS), "--enforce-eager",
               "--out", str(out / "server")]
    if cpu_blocks:
        command += ["--cpu-kv-blocks", str(cpu_blocks)]
    env = {**os.environ, "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
           "VLLM_NO_USAGE_STATS": "1", "DO_NOT_TRACK": "1",
           "OMP_NUM_THREADS": "4", "TOKENIZERS_PARALLELISM": "false"}
    with (out / "launcher.log").open("w", encoding="utf-8") as log:
        server = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                  env=env, start_new_session=True)
        try:
            base = f"http://127.0.0.1:{args.port}"
            deadline = time.monotonic() + 600
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise RuntimeError(f"Server exited with code {server.returncode}; inspect {out}")
                try:
                    get_resource(base + "/health", 2)
                except OSError:
                    time.sleep(1)
                else:
                    break
            else:
                raise TimeoutError(f"Server did not become healthy; inspect {out}")
            yield base
        finally:
            try:
                os.killpg(server.pid, signal.SIGTERM)
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    server.poll()
                    os.killpg(server.pid, 0)
                    time.sleep(0.1)
                os.killpg(server.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            server.wait()


def snapshot(base, path=None):
    raw = get_resource(base + "/metrics", 5)
    if path is not None:
        path.write_bytes(raw)
    return parse_metrics(raw.decode("utf-8"))


def run_arm(args, cases, name, cpu_blocks):
    out = args.out / name
    out.mkdir()
    records, usage_samples = [], []
    print(f"{name}: starting a fresh vLLM server", flush=True)
    with running_server(args, out, cpu_blocks) as base, ThreadPoolExecutor(max_workers=1) as pool:
        print(f"{name}: running {len(cases)} requests", flush=True)
        before = previous = snapshot(base, out / "metrics_before.txt")
        for index, case in enumerate(cases):
            started = time.monotonic_ns()
            request = pool.submit(request_one, base, MODEL_NAME, case, index, 24, 120)
            while True:
                try:
                    record = request.result(timeout=0.025)
                    break
                except TimeoutError:
                    usage = snapshot(base).get(GPU_USAGE)
                    if usage is not None:
                        usage_samples.append(usage)
                        append_jsonl(out / "gpu_kv_samples.jsonl", {
                            "case_id": case["id"], "monotonic_ns": time.monotonic_ns(),
                            "in_use_ratio": usage,
                        })
            record.update(started_ns=started, finished_ns=time.monotonic_ns())
            current = snapshot(base, out / f"metrics_after_{case['id']}.txt")
            record["hit_tokens"] = counter_deltas(previous, current)
            previous = current
            records.append(record)
            append_jsonl(out / "requests.jsonl", record)
            if not record["success"]:
                raise RuntimeError(f"{name}/{case['id']} failed: {record.get('error')}")

    transfers = read_jsonl(out / "server" / "kv_transfers.jsonl")
    occupancy = read_jsonl(out / "server" / "kv_cpu_occupancy.jsonl")
    completed = [job for job in transfers if job["status"] == "completed"]
    summary = {
        "hit_tokens": counter_deltas(before, previous),
        "transfers": {},
        "reported_transfer_failures": len(transfers) - len(completed),
        "cpu_kv_capacity_blocks": cpu_blocks,
        "cpu_kv_occupied_blocks_peak": max((sample["occupied_blocks"] for sample in occupancy), default=0)
        if occupancy or not cpu_blocks else None,
        "cpu_kv_evicted_blocks": sum(sample["evicted_blocks"] for sample in occupancy)
        if occupancy or not cpu_blocks else None,
        "gpu_kv_in_use_ratio_sampled_peak": max(usage_samples, default=None),
        "gpu_kv_in_use_samples": len(usage_samples),
    }
    for direction in DIRECTIONS:
        jobs = [job for job in completed if job["direction"] == direction]
        durations = [job["elapsed_ms"] for job in jobs]
        summary["transfers"][direction] = {
            "completed_jobs": len(jobs),
            "gpu_blocks": sum(job["gpu_blocks"] for job in jobs),
            "submit_to_observation_ms_p50": percentile(durations, 50),
            "submit_to_observation_ms_p95": percentile(durations, 95),
        }
    write_json(out / "summary.json", summary)
    return records, completed, summary


def verify_replay(baseline, traced, jobs):
    revisit = traced[-1]
    external = "external_prefix_cache_hits_total"
    return {
        "all_outputs_match": [record["output_text"] for record in baseline]
        == [record["output_text"] for record in traced],
        "baseline_revisit_has_no_external_hit": baseline[-1]["hit_tokens"][external] == 0,
        "traced_revisit_has_external_hit": (revisit["hit_tokens"][external] or 0) > 0,
        "cpu_store_completed_before_revisit": any(
            job["direction"] == "gpu_to_cpu" and job["observed_ns"] <= revisit["started_ns"]
            for job in jobs
        ),
        "cpu_reload_completed_during_revisit": any(
            job["direction"] == "cpu_to_gpu"
            and revisit["started_ns"] <= job["submitted_ns"] <= job["observed_ns"] <= revisit["finished_ns"]
            for job in jobs
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, type=Path, help="Local Qwen2.5-0.5B-Instruct model directory")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--cpu-kv-blocks", type=int, default=72)
    args = parser.parse_args()
    if sys.platform != "linux" or not args.model.is_dir():
        parser.error("Run on Linux with a local model directory")
    if not 1 <= args.port <= 65535 or args.cpu_kv_blocks <= 0:
        parser.error("Use a valid port and positive CPU KV block budget")

    args.out.mkdir(parents=True, exist_ok=False)
    proxy_exceptions = os.environ.get("no_proxy", os.environ.get("NO_PROXY", ""))
    if proxy_exceptions != "*":
        os.environ["no_proxy"] = ",".join(filter(None, (proxy_exceptions, "127.0.0.1")))
    cases = workload()
    for case in cases:
        append_jsonl(args.out / "workload.jsonl", case)
    baseline, _, baseline_summary = run_arm(args, cases, "gpu-only", 0)
    traced, jobs, traced_summary = run_arm(args, cases, "cpu-lru", args.cpu_kv_blocks)
    checks = verify_replay(baseline, traced, jobs)
    checks["no_reported_transfer_failures"] = traced_summary["reported_transfer_failures"] == 0
    checks["cpu_kv_occupancy_observed"] = (traced_summary["cpu_kv_occupied_blocks_peak"] or 0) > 0
    checks["cpu_lru_eviction_observed"] = (traced_summary["cpu_kv_evicted_blocks"] or 0) > 0
    report = {"run_id": args.out.name, "passed": all(checks.values()), "checks": checks,
              "gpu_kv_blocks": GPU_BLOCKS, "gpu_only": baseline_summary, "cpu_lru": traced_summary}
    write_json(args.out / "comparison.json", report)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
