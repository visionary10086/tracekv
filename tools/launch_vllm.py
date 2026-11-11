"""Version-checked Linux vLLM launcher. Does not install or upgrade anything."""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
from datetime import datetime, timezone


def cpu_lru_config(cpu_blocks, output):
    return {
        "kv_connector": "OffloadingConnector",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "num_cpu_blocks": cpu_blocks,
            "eviction_policy": "lru",
            "spec_name": "TraceKVCPUOffloadingSpec",
            "spec_module_path": "tracekv_offload",
            "trace_dir": str(output.resolve()),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="Local model directory or explicit Hugging Face model ID")
    parser.add_argument("--revision", help="Pin a remote model to its commit SHA")
    parser.add_argument("--expected-version", required=True, help="Exact installed vLLM distribution version")
    parser.add_argument("--served-name", default="tracekv-model")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--max-model-len", type=int, default=1024)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    parser.add_argument("--max-num-seqs", type=int, default=4)
    parser.add_argument("--num-gpu-blocks-override", type=int, help="Fix the GPU KV block budget for an experiment")
    parser.add_argument("--cpu-kv-blocks", type=int, help="Enable traced CPU KV offloading with this LRU block budget")
    parser.add_argument("--enforce-eager", action="store_true", help="Avoid CUDA graph capture for initial low-memory checks")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("Run the inference server in a verified Linux environment; Windows inventory alone is insufficient")
    if not 0 < args.gpu_memory_utilization < 1 or args.max_model_len <= 0 or args.max_num_seqs <= 0 or not 1 <= args.port <= 65535:
        parser.error("Invalid capacity/port setting")
    if any(blocks is not None and blocks <= 0 for blocks in
           (args.num_gpu_blocks_override, args.cpu_kv_blocks)):
        parser.error("KV block budgets must be positive")
    if args.cpu_kv_blocks is not None and args.expected_version != "0.11.2":
        parser.error("Traced CPU LRU requires vLLM 0.11.2")
    if not Path(args.model).is_dir() and not args.revision:
        parser.error("Remote model requires --revision; alternatively provide a local model directory")
    version = importlib.metadata.version("vllm")
    if version != args.expected_version:
        parser.error(f"Installed vLLM {version} differs from requested {args.expected_version}")
    command = [sys.executable, "-m", "vllm.entrypoints.openai.api_server",
               "--model", args.model, "--served-model-name", args.served_name,
               "--host", "127.0.0.1", "--port", str(args.port),
               "--max-model-len", str(args.max_model_len),
               "--gpu-memory-utilization", str(args.gpu_memory_utilization),
               "--max-num-seqs", str(args.max_num_seqs)]
    if args.enforce_eager:
        command += ["--enforce-eager"]
    if args.revision:
        command += ["--revision", args.revision]
    if args.num_gpu_blocks_override is not None:
        command += ["--num-gpu-blocks-override", str(args.num_gpu_blocks_override)]
    env = None
    if args.cpu_kv_blocks is not None:
        command += ["--enable-prefix-caching", "--kv-transfer-config",
                    json.dumps(cpu_lru_config(args.cpu_kv_blocks, args.out))]
        env = os.environ.copy()
        python_paths = [str(Path(__file__).resolve().parent)]
        if env.get("PYTHONPATH"):
            python_paths.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(python_paths)
    args.out.mkdir(parents=True, exist_ok=False)
    manifest = {"started_at_utc": datetime.now(timezone.utc).isoformat(),
                "vllm_version": version, "python": sys.version, "command": command,
                "note": "Conservative smoke-test settings, not a performance-optimized configuration. "
                        "For a local model, separately record its source revision/checksums."}
    (args.out / "server_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    freeze = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True)
    (args.out / "requirements_actual.txt").write_text(freeze.stdout, encoding="utf-8")
    if freeze.returncode:
        (args.out / "requirements_error.txt").write_text(freeze.stderr, encoding="utf-8")
    print("Starting foreground server on 127.0.0.1; logs:", args.out / "server.log", flush=True)
    with (args.out / "server.log").open("w", encoding="utf-8") as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=env)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
