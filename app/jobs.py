"""Single-process job supervisor. Every API run owns its inference process."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading

from contracts.schema import utc_now
from tools.run_experiment import ROOT, run_id, validate_config, write_json

ARTIFACTS = {
    "manifest.json", "requests.jsonl", "request_summary.json", "analysis.json", "warmup.jsonl",
    "report.md", "snapshots.jsonl", "aligned_snapshots.jsonl", "kv_samples.jsonl",
    "gpu_samples.jsonl", "system_samples.jsonl", "kv_transfers.jsonl", "kv_cpu_occupancy.jsonl",
    "logs/runner.log", "logs/server-launcher.log", "server/server.log",
}


class Jobs:
    def __init__(self, result_root):
        self.root = Path(result_root).resolve()
        self.lock = threading.Lock()
        self.process = None
        self.active_id = None

    def resolve(self, name):
        if not name or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in name):
            raise ValueError("Invalid run_id")
        target = self.root / name
        if target.is_symlink() or target.resolve().parent != self.root or not target.is_dir():
            raise FileNotFoundError(name)
        return target

    def models(self):
        root = Path(os.environ.get("TRACEKV_MODEL_ROOT", "/opt/tracekv/models")).resolve()
        return [str(p) for p in sorted(root.iterdir()) if p.is_dir() and (p / "config.json").is_file()] if root.is_dir() else []

    def start(self, config):
        cfg = validate_config(config)
        if str(Path(cfg["model_path"]).resolve()) not in self.models():
            raise ValueError("Select a model from the configured model root")
        workload_root = (ROOT / "workloads").resolve()
        if not Path(cfg["workload_path"]).is_relative_to(workload_root):
            raise ValueError("Workload must be inside the repository workloads directory")
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                raise RuntimeError("An experiment is already running")
            if sys.platform != "linux":
                raise ValueError("Start experiments from Linux")
            name = run_id(cfg["policy"])
            self.root.mkdir(parents=True, exist_ok=True)
            out = self.root / name
            out.mkdir(exist_ok=False)
            (out / "logs").mkdir()
            manifest = {
                "schema_version": 1, "run_id": name, "comparison_id": None,
                "status": "running", "phase": "queued", "created_at_utc": utc_now(),
                "config": cfg, "recorded_requests": 0,
                "sources": {"sched": {"status": "unavailable", "error": "Not connected in this milestone"}},
            }
            write_json(out / "manifest.json", manifest)
            try:
                with (out / "logs/runner.log").open("w") as log:
                    self.process = subprocess.Popen(
                        [sys.executable, "-m", "tools.run_experiment", "--run-dir", str(out)],
                        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            except OSError as exc:
                manifest.update(status="failed", phase="finished", error=str(exc), finished_at_utc=utc_now())
                write_json(out / "manifest.json", manifest)
                raise
            self.active_id = name
            return manifest

    def stop(self, name):
        with self.lock:
            if self.active_id != name or self.process is None or self.process.poll() is not None:
                raise ValueError("This server does not own a running experiment with that run_id")
            out = self.resolve(name)
            (out / "cancel.flag").touch()
            manifest = json.loads((out / "manifest.json").read_text())
            if manifest.get("runner_ready"):
                self.process.send_signal(signal.SIGTERM)
            return {"run_id": name, "status": "stopping"}

    def list(self):
        runs = []
        if not self.root.exists():
            return runs
        for folder in sorted(self.root.iterdir(), reverse=True):
            if folder.is_symlink() or not folder.is_dir():
                continue
            try:
                manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
                if "config" in manifest and "policy" in manifest["config"]:
                    if manifest.get("status") == "running":
                        owned = self.active_id == folder.name and self.process and self.process.poll() is None
                        if not owned:
                            manifest["status"] = "orphaned"
                            manifest["error"] = "Supervisor no longer owns this job; inspect saved process and logs."
                    runs.append(manifest)
            except (OSError, ValueError):
                continue
        return runs[:100]

    def close(self):
        with self.lock:
            process = self.process
            if process and process.poll() is None:
                process.send_signal(signal.SIGTERM)
        if process:
            try:
                process.wait(timeout=40)
            except subprocess.TimeoutExpired:
                try:
                    manifest = json.loads((self.root / self.active_id / "manifest.json").read_text())
                    server_pid = manifest.get("server_pid")
                    if server_pid:
                        os.killpg(server_pid, signal.SIGKILL)
                except (OSError, ValueError):
                    pass
                process.kill()
                process.wait(timeout=5)
