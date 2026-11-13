"""Linux monitoring UI and managed experiment API. Run one Uvicorn worker."""
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.jobs import ARTIFACTS, Jobs
from app.monitor import Monitor
from policy.demo import recommend_policy

STATIC = Path(__file__).parent / "static"
monitor = Monitor(os.environ.get("TRACEKV_VLLM_METRICS_URL", "http://127.0.0.1:8000/metrics"))
jobs = Jobs(os.environ.get("TRACEKV_RESULTS_DIR", str(Path(__file__).resolve().parents[1] / "results")))


@asynccontextmanager
async def lifespan(app):
    monitor.start()
    try:
        yield
    finally:
        jobs.close()
        monitor.close()


app = FastAPI(title="TraceKV Linux Monitor", version="0.2", lifespan=lifespan)


@app.middleware("http")
async def local_writes(request: Request, call_next):
    if request.method in ("POST", "PUT", "DELETE"):
        origin = request.headers.get("origin")
        if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
            from fastapi.responses import JSONResponse
            return JSONResponse({"detail": "Cross-origin writes are not allowed"}, status_code=403)
    return await call_next(request)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/v1/snapshot")
def get_snapshot():
    history = monitor.get_history()
    if not history:
        raise HTTPException(503, "Waiting for the first real sample")
    return history[-1]


@app.get("/api/v1/history")
def get_history():
    return {"samples": monitor.get_history(), "interval_ms": int(monitor.interval * 1000)}


@app.get("/api/v1/environment")
def environment():
    root = Path(__file__).resolve().parents[1]
    return {
        "platform": os.uname().sysname if hasattr(os, "uname") else os.name,
        "models": jobs.models(),
        "workloads": [str(p.relative_to(root)) for p in sorted((root / "workloads").glob("*.jsonl"))],
        "metrics_url": os.environ.get("TRACEKV_VLLM_METRICS_URL", "http://127.0.0.1:8000/metrics"),
        "limitations": ["eBPF 尚未接入本版页面", "整卡 GPU / 整机内存指标", "单进程单实验运行"],
    }


@app.post("/api/v1/policy/evaluate")
def evaluate_policy(snapshot: dict):
    try:
        return recommend_policy(snapshot)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/api/v1/runs")
def list_runs():
    return {"runs": jobs.list()}


@app.post("/api/v1/runs", status_code=202)
def start_run(config: dict):
    try:
        return jobs.start(config)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    except (ValueError, OSError) as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/api/v1/runs/{run_id}/stop", status_code=202)
def stop_run(run_id: str):
    try:
        return jobs.stop(run_id)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/v1/runs/{run_id}")
def get_run(run_id: str):
    try:
        out = jobs.resolve(run_id)
        manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
        result = {"manifest": manifest, "artifacts": [name for name in sorted(ARTIFACTS) if (out / name).is_file()]}
        for key, filename in (("analysis", "analysis.json"), ("summary", "request_summary.json")):
            if (out / filename).is_file():
                result[key] = json.loads((out / filename).read_text(encoding="utf-8"))
        return result
    except (ValueError, OSError) as exc:
        raise HTTPException(404, "Experiment not found") from exc


@app.get("/api/v1/runs/{run_id}/files/{filename:path}")
def get_artifact(run_id: str, filename: str):
    if filename not in ARTIFACTS:
        raise HTTPException(404, "Unknown artifact")
    try:
        folder = jobs.resolve(run_id)
        path = folder / filename
        if not path.resolve().is_relative_to(folder) or not path.is_file():
            raise FileNotFoundError(filename)
        return FileResponse(path, filename=Path(filename).name, media_type="text/plain; charset=utf-8")
    except (ValueError, OSError) as exc:
        raise HTTPException(404, "Artifact not found") from exc


app.mount("/static", StaticFiles(directory=STATIC), name="static")
