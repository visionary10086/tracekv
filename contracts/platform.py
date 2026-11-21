"""Versioned, strict contracts for cache-policy and collector integration."""
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, model_validator

Count = Annotated[StrictInt, Field(ge=0)]
Positive = Annotated[StrictInt, Field(gt=0)]
Identifier = Annotated[StrictStr, Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class StrategyConfig(Contract):
    policy: Literal["gpu_only", "cpu_lru"] = "gpu_only"
    gpu_kv_blocks: Annotated[StrictInt, Field(ge=1, le=100000)] = 64
    cpu_kv_blocks: Annotated[StrictInt, Field(ge=0, le=100000)] = 0

    @model_validator(mode="after")
    def budgets(self):
        if (self.policy == "gpu_only") != (self.cpu_kv_blocks == 0):
            raise ValueError("gpu_only requires CPU blocks=0; cpu_lru requires CPU blocks>0")
        return self


class StrategyRequest(Contract):
    interface_version: Literal["1.0"] = "1.0"
    snapshot: dict
    current_config: StrategyConfig
    candidate_cpu_kv_blocks: Annotated[StrictInt, Field(ge=1, le=100000)] = 72


class ClockAnchor(Contract):
    wall_ns: Positive
    monotonic_ns: Count


class EbpfEvent(Contract):
    metric: Literal["wakeup_to_run_latency", "runnable_wait", "on_cpu", "blocked",
                    "direct_reclaim", "major_fault", "minor_fault"]
    timestamp_ns: Count
    tid: Positive
    pid: Positive | None = None
    comm: Annotated[StrictStr, Field(max_length=64)] = ""
    cpu: Count | None = None
    request_id: Annotated[StrictStr, Field(max_length=200)] | None = None
    value: Count | None = None
    unit: Literal["ns"] = "ns"

    @model_validator(mode="after")
    def duration(self):
        if self.metric not in ("major_fault", "minor_fault") and self.value is None:
            raise ValueError("Duration metrics require value in ns")
        return self


class CacheEvent(Contract):
    metric: Literal["lookup", "hit", "evict", "transfer"]
    timestamp_ns: Count
    worker_id: Annotated[StrictStr, Field(min_length=1, max_length=100)]
    pid: Positive | None = None
    request_id: Annotated[StrictStr, Field(max_length=200)] | None = None
    cache_key: Annotated[StrictStr, Field(max_length=256)] | None = None
    tier: Literal["gpu", "cpu"]
    tokens: Count | None = None
    blocks: Count | None = None
    bytes: Count | None = None
    job_id: Annotated[StrictStr, Field(max_length=100)] | None = None
    direction: Literal["gpu_to_cpu", "cpu_to_gpu"] | None = None
    status: Literal["submitted", "completed", "failed"] | None = None
    elapsed_ns: Count | None = None

    @model_validator(mode="after")
    def identity(self):
        if self.metric in ("hit", "evict") and not self.cache_key:
            raise ValueError("hit/evict require a stable logical cache_key")
        if self.metric == "transfer" and (not self.job_id or not self.direction or not self.status):
            raise ValueError("transfer requires worker_id, job_id, direction and status")
        return self


class Batch(Contract):
    interface_version: Literal["1.0"] = "1.0"
    run_id: Identifier
    batch_id: Identifier
    collector_id: Annotated[StrictStr, Field(min_length=1, max_length=100)]
    clock: ClockAnchor
    window_start_ns: Count
    window_end_ns: Positive
    status: Literal["ok", "failed", "unavailable"] = "ok"
    error: Annotated[StrictStr, Field(max_length=2000)] | None = None
    dropped_events: Count | None = None
    synthetic: StrictBool = False

    @model_validator(mode="after")
    def window(self):
        if self.window_end_ns <= self.window_start_ns:
            raise ValueError("window_end_ns must exceed window_start_ns")
        if self.status != "ok" and not self.error:
            raise ValueError("failed/unavailable status requires error")
        for event in getattr(self, "events", []):
            if not self.window_start_ns <= event.timestamp_ns < self.window_end_ns:
                raise ValueError("Event timestamp must be in [window_start_ns, window_end_ns)")
        if self.status != "ok" and getattr(self, "events", []):
            raise ValueError("Failed/unavailable batches cannot contain observations")
        return self


class EbpfBatch(Batch):
    source: Literal["ebpf_sched"] = "ebpf_sched"
    scope: Literal["system", "target_threads", "comm_match"]
    target_tids: Annotated[list[Positive], Field(max_length=4096)] = []
    target_comm: Annotated[StrictStr, Field(max_length=64)] | None = None
    events: Annotated[list[EbpfEvent], Field(max_length=2000)]

    @model_validator(mode="after")
    def target(self):
        self.target_tids = sorted(set(self.target_tids))
        if self.scope == "target_threads":
            if not self.target_tids:
                raise ValueError("target_threads requires target_tids")
            if any(event.tid not in self.target_tids for event in self.events):
                raise ValueError("Event TID is outside declared target_tids")
        if self.scope == "comm_match":
            if not self.target_comm or any(event.comm != self.target_comm for event in self.events):
                raise ValueError("comm_match requires matching target_comm; it is not process identity")
        return self


class CacheBatch(Batch):
    source: Literal["cache"] = "cache"
    events: Annotated[list[CacheEvent], Field(max_length=2000)]


class StrategyAdvice(Contract):
    interface_version: Literal["1.0"]
    run_id: StrictStr
    created_at_utc: StrictStr
    apply_mode: Literal["next_run"]
    strategy_id: Literal["gpu_only", "cpu_lru"]
    config_patch: StrategyConfig
    reason: StrictStr
    reason_codes: list[StrictStr]
    evidence: dict
    requires_comparison: Literal[True]
    causality_established: Literal[False]
    limitations: list[StrictStr]
