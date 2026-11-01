"""Read vLLM Prometheus counters and derive one sampling-window KV snapshot."""
import re
import time
import urllib.request


METRIC = re.compile(r'^vllm:([a-z_]+)(?:\{[^}]*\})?\s+([0-9.eE+-]+)$')


def parse_metrics(body):
    values = {}
    for line in body.splitlines():
        match = METRIC.match(line)
        if match:
            values[match.group(1)] = values.get(match.group(1), 0.0) + float(match.group(2))
    return values


class VllmAdapter:
    def __init__(self, url="http://127.0.0.1:8000/metrics"):
        self.url = url
        self.previous = None
        self.last_sample_monotonic = None
        self.last_window_ms = None

    def sample(self):
        with urllib.request.urlopen(self.url, timeout=2) as response:
            current = parse_metrics(response.read().decode("utf-8"))
        sampled_at = time.monotonic()
        if self.last_sample_monotonic is not None:
            self.last_window_ms = max(1, round((sampled_at - self.last_sample_monotonic) * 1000))
        self.last_sample_monotonic = sampled_at
        usage = current.get("kv_cache_usage_perc")
        running = current.get("num_requests_running")
        ratio = None
        if self.previous is not None:
            query = current.get("prefix_cache_queries_total")
            hits = current.get("prefix_cache_hits_total")
            old_query = self.previous.get("prefix_cache_queries_total")
            old_hits = self.previous.get("prefix_cache_hits_total")
            if None not in (query, hits, old_query, old_hits):
                dq, dh = query - old_query, hits - old_hits
                if dq > 0 and 0 <= dh <= dq:
                    ratio = dh / dq
        self.previous = current
        return {"usage_ratio": usage if usage is not None and 0 <= usage <= 1 else None,
                "prefix_hit_ratio": ratio,
                "running_requests": int(running) if running is not None and running >= 0 and running.is_integer() else None}
