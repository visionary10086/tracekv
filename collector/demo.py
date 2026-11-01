"""Real Linux available-memory sample; eBPF disk fields await collector integration."""
import json
from pathlib import Path

from contracts.schema import utc_now


def collect_system_metrics(meminfo_path="/proc/meminfo"):
    """Return one sample; MB means decimal megabytes (1,000,000 bytes)."""
    available = None
    source = "unavailable"
    try:
        for line in Path(meminfo_path).read_text().splitlines():
            if line.startswith("MemAvailable:"):
                available = round(int(line.split()[1]) * 1024 / 1_000_000, 3)
                source = "proc"
                break
    except (OSError, ValueError, IndexError):
        pass
    return {
        "timestamp": utc_now(),
        "mem_available_mb": available,
        "disk_read_p95_ms": None,
        "disk_read_bytes_per_s": None,
        "source": source,
    }


if __name__ == "__main__":
    print(json.dumps(collect_system_metrics(), ensure_ascii=False))
