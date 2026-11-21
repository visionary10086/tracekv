"""Single-process job supervisor. Every API run owns its inference process."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading

from contracts.schema import utc_now
from app.runtime import alive, signal_verified, port_busy
from tools.run_experiment import ROOT, create_run, validate_config, write_json

ARTIFACTS = {
    "manifest.json", "requests.jsonl", "request_summary.json", "analysis.json", "warmup.jsonl",
    "report.md", "snapshots.jsonl", "aligned_snapshots.jsonl", "kv_samples.jsonl",
    "gpu_samples.jsonl", "system_samples.jsonl", "kv_transfers.jsonl", "kv_cpu_occupancy.jsonl",
    "summary.json", "bottleneck_diagnosis.json", "telemetry_summary.json", "sched_events.jsonl", "sched_summary.json", "stage_summary.json",
    "logs/runner.log", "logs/server-launcher.log", "server/server.log",
}


class Jobs:
    def __init__(self, result_root):
        self.root = Path(result_root).resolve()
        self.lock = threading.Lock()
        self.process = None
        self.active_id = None
        self.reservation = None

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

    def start(self, config, comparison_id=None, reservation=None, recommendation=None):
        cfg = validate_config(config)
        if str(Path(cfg["model_path"]).resolve()) not in self.models():
            raise ValueError("Select a model from the configured model root")
        workload_root = (ROOT / "workloads").resolve()
        if not Path(cfg["workload_path"]).is_relative_to(workload_root):
            raise ValueError("Workload must be inside the repository workloads directory")
        with self.lock:
            if self.reservation is not None and self.reservation != reservation:
                raise RuntimeError("An automatic comparison owns the experiment queue")
            if self.process is not None and self.process.poll() is None:
                raise RuntimeError("An experiment is already running")
            if any(run.get("status") == "running" for run in self.list()) or port_busy(cfg["port"]):
                raise RuntimeError("A verified experiment already owns the inference resource")
            if sys.platform != "linux":
                raise ValueError("Start experiments from Linux")
            out, manifest = create_run(cfg, self.root, origin="platform", comparison_id=comparison_id, recommendation=recommendation)
            name = manifest["run_id"]
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
            try:
                out = self.resolve(name)
                manifest = json.loads((out / "manifest.json").read_text())
            except (OSError, ValueError) as exc:
                raise ValueError("Experiment not found") from exc
            if manifest.get("status") != "running":
                raise ValueError("Experiment has already ended")
            owned = self.active_id == name and self.process is not None and self.process.poll() is None
            external = alive(manifest.get("runner_identity"))
            if not owned and not external:
                raise ValueError("No verified running process belongs to this run_id")
            (out / "cancel.flag").touch()
            if owned and manifest.get("runner_ready"):
                self.process.send_signal(signal.SIGTERM)
            elif external:
                try:
                    signal_verified(manifest["runner_identity"])
                except (OSError, RuntimeError) as exc:
                    raise ValueError(str(exc)) from exc
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
                    owned = self.active_id == folder.name and self.process is not None and self.process.poll() is None
                    external = alive(manifest.get("runner_identity"))
                    manifest["can_stop"] = bool(manifest.get("status") == "running" and (owned or external))
                    manifest["process_status"] = "verified" if owned or external else "not_running_or_unknown"
                    if manifest.get("status") == "running" and not owned and not external:
                        manifest["status"] = "orphaned"
                        manifest["error"] = "No matching process identity; inspect saved logs. No PID was adopted."
                    if manifest.get("status")=="running" and (folder/"cancel.flag").exists():
                        manifest["phase"]="stopping"
                        manifest["can_stop"]=False
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
                    if server_pid and alive(manifest.get("server_identity")):
                        os.killpg(server_pid, signal.SIGKILL)
                except (OSError, ValueError):
                    pass
                process.kill()
                process.wait(timeout=5)
