"""One sampler shared by all browser clients; no synthetic measurements."""
import copy
import threading
import time
from collections import deque
from uuid import uuid4

from collector.demo import collect_system_metrics
from collector.gpu import collect_gpu_metrics
from contracts.schema import utc_now, validate_snapshot
from engine.vllm_adapter import VllmAdapter


class Monitor:
    def __init__(self, metrics_url=None, interval=2):
        self.adapter = VllmAdapter(metrics_url) if metrics_url else None
        self.interval = interval
        self.run_id = "monitor-" + uuid4().hex[:12]
        self.history = deque(maxlen=180)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None

    def sample(self):
        system = collect_system_metrics()
        gpu = collect_gpu_metrics()
        kv = dict(usage_ratio=None, prefix_hit_ratio=None, running_requests=None)
        statuses = {
            "system": {"status": "ok" if system["source"] != "unavailable" else "unavailable",
                       "error": None if system["source"] != "unavailable" else "/proc/meminfo unavailable"},
            "gpu": {"status": "ok" if gpu["source"] != "unavailable" else "unavailable",
                    "error": None if gpu["source"] != "unavailable" else "nvidia-smi unavailable or unsupported query"},
            "kv": {"status": "unavailable", "error": "vLLM metrics URL not configured"},
            "sched": {"status": "unavailable", "error": "eBPF live collection not connected in this milestone"},
        }
        if self.adapter:
            try:
                kv = self.adapter.sample()
                kv["counters"] = self.adapter.last_counters
                kv["counter_deltas"] = self.adapter.last_deltas
                statuses["kv"] = {"status": "ok", "error": None} if any(
                    kv[key] is not None for key in ("usage_ratio", "prefix_hit_ratio", "running_requests")
                ) else {"status": "unavailable", "error": "Endpoint returned no supported KV measurements"}
            except (OSError, ValueError, UnicodeError) as exc:
                statuses["kv"] = {"status": "failed", "error": str(exc)}
                self.adapter.previous = None
                self.adapter.last_sample_monotonic = None
                self.adapter.last_window_ms = None
        result = {
            "schema_version": "0.1", "run_id": self.run_id,
            "timestamp": utc_now(),
            "window_ms": self.adapter.last_window_ms if self.adapter and self.adapter.last_window_ms else int(self.interval * 1000),
            "system": {key: system[key] for key in ("mem_available_mb", "disk_read_p95_ms", "disk_read_bytes_per_s")},
            "gpu": gpu, "kv": kv,
            "sources": {"system": system["source"], "gpu": gpu["source"],
                        "kv": "vllm" if statuses["kv"]["status"] == "ok" else "unavailable"},
            "source_status": statuses,
            "scope": {"gpu": "device_index_0", "system": "whole_host"},
        }
        return validate_snapshot(result)

    def _loop(self):
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                sample = self.sample()
                with self.lock:
                    self.history.append(sample)
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
            self.stop_event.wait(max(0.05, self.interval - (time.monotonic() - started)))

    def start(self):
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="tracekv-monitor")
        self.thread.start()

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=8)

    def get_history(self):
        with self.lock:
            return copy.deepcopy(list(self.history))
