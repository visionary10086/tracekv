"""Observe the vLLM 0.11.2 CPU LRU manager and GPU/CPU transfer handler."""

import json
from pathlib import Path
from time import monotonic_ns

from vllm.v1.kv_offload.cpu import CPUOffloadingSpec
from vllm.v1.kv_offload.mediums import GPULoadStoreSpec
from vllm.v1.kv_offload.worker.worker import OffloadingHandler


def append_event(path, **event):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event) + "\n")


class TracedTransfer(OffloadingHandler):
    def __init__(self, handler, path):
        self.handler = handler
        self.path = path
        self.pending = {}

    def transfer_async(self, job_id, spec):
        source, target = spec
        is_store = isinstance(source, GPULoadStoreSpec)
        gpu_spec = source if is_store else target
        job = {
            "job_id": job_id,
            "direction": "gpu_to_cpu" if is_store else "cpu_to_gpu",
            "gpu_blocks": len(gpu_spec.block_ids),
            "submitted_ns": monotonic_ns(),
        }
        if not self.handler.transfer_async(job_id, spec):
            append_event(self.path, **job, status="submission_failed")
            return False
        self.pending[job_id] = job
        return True

    def get_finished(self):
        results = self.handler.get_finished()
        observed_ns = monotonic_ns()
        for job_id, success in results:
            job = self.pending.pop(job_id)
            append_event(self.path, **job, observed_ns=observed_ns,
                         elapsed_ms=(observed_ns - job["submitted_ns"]) / 1_000_000,
                         status="completed" if success else "failed")
        return results


class TracedLRU:
    def __init__(self, manager, capacity, path):
        self.manager = manager
        self.capacity = capacity
        self.path = path
        self.record("initialize")

    def __getattr__(self, name):
        return getattr(self.manager, name)

    def record(self, operation, evicted=0):
        append_event(self.path, monotonic_ns=monotonic_ns(), operation=operation,
                     capacity_blocks=self.capacity, occupied_blocks=len(self.manager.blocks),
                     evicted_blocks=evicted)

    def prepare_store(self, block_hashes):
        result = self.manager.prepare_store(block_hashes)
        if result is not None and result.block_hashes_to_store:
            self.record("reserve_store", len(result.block_hashes_evicted))
        return result

    def complete_store(self, block_hashes, success=True):
        self.manager.complete_store(block_hashes, success)
        if not success:
            self.record("rollback_store")


class TraceKVCPUOffloadingSpec(CPUOffloadingSpec):
    def __init__(self, vllm_config):
        super().__init__(vllm_config)
        if self.eviction_policy != "lru":
            raise ValueError("TraceKV CPU offloading requires LRU")
        self.trace_dir = Path(self.extra_config["trace_dir"])
        self.traced_manager = None
        self.traced_handler = None

    def get_manager(self):
        if self.traced_manager is None:
            self.traced_manager = TracedLRU(
                super().get_manager(), self.num_cpu_blocks, self.trace_dir / "kv_cpu_occupancy.jsonl"
            )
        return self.traced_manager

    def get_handlers(self, kv_caches):
        for source, target, handler in super().get_handlers(kv_caches):
            # Both directions share one handler and its completion queue.
            if self.traced_handler is None:
                self.traced_handler = TracedTransfer(handler, self.trace_dir / "kv_transfers.jsonl")
            yield source, target, self.traced_handler
