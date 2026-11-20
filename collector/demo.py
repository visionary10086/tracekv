"""Real Linux available-memory sample; eBPF disk fields await collector integration."""
import json
from collector.system import collect_system_sample


def collect_system_metrics(meminfo_path="/proc/meminfo"):
    """Return one sample; MB means decimal megabytes (1,000,000 bytes)."""
    s = collect_system_sample(run_id="", meminfo_path=meminfo_path)
    return {
        "timestamp": s["timestamp"],
        "mem_available_mb": s["mem_available_mb"],
        "disk_read_p95_ms": s["disk_read_p95_ms"],
        "disk_read_bytes_per_s": s["disk_read_bytes_per_s"],
        "source": s["source"],
    }


if __name__ == "__main__":
    print(json.dumps(collect_system_metrics(), ensure_ascii=False))
