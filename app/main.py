"""Linux monitoring UI and managed experiment API. Run one Uvicorn worker."""
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.jobs import ARTIFACTS, Jobs
from app.comparisons import Comparisons
from app import integrations
from contracts.platform import EbpfBatch, CacheBatch, StrategyRequest, StrategyAdvice
from policy.interface import STRATEGIES, evaluate
from app.monitor import Monitor
from policy.demo import recommend_policy

STATIC = Path(__file__).parent / "static"
monitor = Monitor(os.environ.get("TRACEKV_VLLM_METRICS_URL", "http://127.0.0.1:8000/metrics"))
jobs = Jobs(os.environ.get("TRACEKV_RESULTS_DIR", str(Path(__file__).resolve().parents[1] / "results")))
comparisons = Comparisons(jobs)
# Keep background host sampling out of every managed comparison profile.
from app.runtime import port_busy
monitor.paused_when = lambda: jobs.reservation is not None or port_busy(8000)


@asynccontextmanager
async def lifespan(app):
    monitor.start()
    try:
        yield
    finally:
        comparisons.close()
        jobs.close()
        monitor.close()


app = FastAPI(title="TraceKV Linux Monitor", version="0.3", lifespan=lifespan)


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
    return RedirectResponse("/monitor", status_code=307)


@app.get("/monitor", include_in_schema=False)
def monitor_page():
    return FileResponse(STATIC / "monitor.html")


@app.get("/experiments", include_in_schema=False)
def experiments_page():
    return FileResponse(STATIC / "experiments.html")


@app.get("/runs", include_in_schema=False)
def runs_page():
    return FileResponse(STATIC / "runs.html")


@app.get("/api/v1/interfaces", tags=["Integration contracts"])
def interface_contracts():
    return integrations.contracts()


@app.get("/api/v1/interfaces/guide", include_in_schema=False)
def interface_guide():
    return FileResponse(STATIC.parents[1] / "docs/interfaces.md", media_type="text/plain; charset=utf-8")


@app.get("/api/v1/interfaces/examples/{name}", tags=["Integration contracts"])
def interface_example(name: str):
    filenames = {"ebpf": "ebpf-batch-v1.json", "cache": "cache-batch-v1.json",
                 "strategy-request": "strategy-request-v1.json",
                 "strategy-response": "strategy-response-v1.json"}
    if name not in filenames:
        raise HTTPException(404, "Unknown example")
    return FileResponse(STATIC.parents[1] / "contracts/examples" / filenames[name], media_type="application/json")


@app.get("/api/v1/integrations", tags=["Integration contracts"])
def integration_capabilities():
    return integrations.capabilities()


@app.get("/api/v1/strategies", tags=["Cache strategy"])
def strategies():
    return {"interface_version": "1.0", "apply_modes": ["next_run"], "strategies": STRATEGIES}


@app.post("/api/v1/strategies/evaluate", tags=["Cache strategy"], response_model=StrategyAdvice)
def strategy_recommendation(request: StrategyRequest):
    try:
        from app.recommendations import register
        return register(jobs,evaluate(request),request.snapshot,request.current_config.model_dump())
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


def ingest(run_id, source, packet):
    try:
        folder = jobs.resolve(run_id)
        return integrations.accept(folder, source, packet)
    except FileExistsError as exc:
        raise HTTPException(409, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, "Experiment not found") from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/api/v1/runs/{run_id}/telemetry/ebpf", tags=["eBPF integration"], status_code=201)
def ingest_ebpf(run_id: str, packet: EbpfBatch):
    return ingest(run_id, "ebpf", packet)


@app.post("/api/v1/runs/{run_id}/telemetry/cache", tags=["Cache strategy"], status_code=201)
def ingest_cache(run_id: str, packet: CacheBatch):
    return ingest(run_id, "cache", packet)


@app.get("/api/v1/runs/{run_id}/telemetry", tags=["Integration contracts"])
def telemetry_summary(run_id: str):
    try:
        return integrations.summarize(jobs.resolve(run_id))
    except (OSError, ValueError) as exc:
        raise HTTPException(404, "Experiment not found") from exc


@app.get("/api/v1/runs/{run_id}/telemetry/{source}/batches/{batch_id}", tags=["Integration contracts"])
def telemetry_batch(run_id: str, source: str, batch_id: str):
    try:
        return integrations.read_batch(jobs.resolve(run_id), source, batch_id)
    except (OSError, ValueError) as exc:
        raise HTTPException(404, "Batch not found") from exc


@app.post("/api/v1/runs/{run_id}/analyze", tags=["Integration contracts"])
def reanalyze(run_id: str):
    from tools.run_experiment import analyze
    try:
        out = jobs.resolve(run_id)
        manifest = json.loads((out / "manifest.json").read_text())
        if manifest.get("status") == "running":
            raise HTTPException(409, "Stop or finish the experiment before offline analysis")
        return analyze(out)
    except (OSError, ValueError) as exc:
        raise HTTPException(404, "Experiment data not found") from exc


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
        "limitations": ["eBPF 自动采集取决于本机 bpftrace 与权限；能力检查不代表探针已挂载", "整卡 GPU / 整机内存指标", "单进程单实验运行"],
    }


@app.post("/api/v1/policy/evaluate")
def evaluate_policy(snapshot: dict):
    try:
        return recommend_policy(snapshot)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/api/v1/comparisons")
def list_comparisons():
    return {"comparisons":comparisons.list()}


@app.post("/api/v1/comparisons",status_code=202)
def start_comparison(config: dict):
    try:
        return comparisons.start(config)
    except RuntimeError as exc:
        raise HTTPException(409,str(exc)) from exc
    except (ValueError,OSError,TypeError) as exc:
        raise HTTPException(422,str(exc)) from exc


@app.get("/api/v1/comparisons/{comparison_id}")
def get_comparison(comparison_id: str):
    try:
        return comparisons.get(comparison_id)
    except (ValueError,OSError) as exc:
        raise HTTPException(404,"Comparison not found") from exc


@app.post("/api/v1/comparisons/{comparison_id}/stop",status_code=202)
def stop_comparison(comparison_id: str):
    try:
        return comparisons.stop(comparison_id)
    except ValueError as exc:
        raise HTTPException(409,str(exc)) from exc


@app.get("/api/v1/comparisons/{comparison_id}/files/{filename}")
def comparison_file(comparison_id: str,filename: str):
    if filename not in ("manifest.json","summary.json","report.md"):
        raise HTTPException(404,"Unknown artifact")
    try:
        path=comparisons.resolve(comparison_id)/filename
        if not path.is_file():
            raise FileNotFoundError(filename)
        return FileResponse(path,filename=filename,media_type="text/plain; charset=utf-8")
    except (ValueError,OSError) as exc:
        raise HTTPException(404,"Artifact not found") from exc


@app.get("/api/v1/runs")
def list_runs():
    return {"runs": jobs.list()}


@app.post("/api/v1/runs", status_code=202)
def start_run(config: dict):
    try:
        from app.recommendations import resolve
        config = dict(config)
        recommendation_id = config.pop("recommendation_id",None)
        recommendation = resolve(jobs,recommendation_id,config) if recommendation_id else None
        return jobs.start(config,recommendation=recommendation)
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
        for key, filename in (("analysis", "analysis.json"), ("summary", "request_summary.json"), ("diagnosis", "bottleneck_diagnosis.json")):
            if (out / filename).is_file():
                result[key] = json.loads((out / filename).read_text(encoding="utf-8"))
        result["telemetry"] = integrations.summarize(out)
        has_external = any(s["batch_count"] for s in result["telemetry"]["sources"].values())
        result["report_outdated"] = has_external and result.get("analysis", {}).get("telemetry_revision") != result["telemetry"]["revision"]
        return result
    except (ValueError, OSError) as exc:
        raise HTTPException(404, "Experiment not found") from exc


@app.get("/api/v1/runs/{run_id}/timeline")
def run_timeline(run_id: str, start: str | None = None, end: str | None = None,
                 limit: int = Query(500,ge=1,le=2000), cursor: int | None = Query(None,ge=0)):
    from app.timeline import build
    try:
        return build(jobs.resolve(run_id),start,end,limit,cursor)
    except FileNotFoundError as exc:
        raise HTTPException(404,"Experiment not found") from exc
    except ValueError as exc:
        raise HTTPException(422,str(exc)) from exc


@app.get("/api/v1/runs/{run_id}/history")
def run_history(run_id: str):
    from app.timeline import records
    from collections import deque
    try:
        out = jobs.resolve(run_id)
        manifest = json.loads((out / "manifest.json").read_text())
        rows = list(deque((r for r in records(out / "snapshots.jsonl") if not r.get("_invalid_line")),maxlen=180))
        return {"run_id":run_id,"samples":rows,"interval_ms":manifest["config"]["sample_interval_ms"],
                "status":manifest["status"],"source_status":manifest.get("sources",{})}
    except (OSError,ValueError) as exc:
        raise HTTPException(404,"Experiment not found") from exc


@app.get("/api/v1/runs/{run_id}/snapshot")
def run_snapshot(run_id: str):
    rows = run_history(run_id)["samples"]
    if not rows:
        raise HTTPException(503,"This experiment has no sampled observations; inspect trace profile and source status")
    return rows[-1]


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
