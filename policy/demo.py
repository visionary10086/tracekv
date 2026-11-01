"""Transparent starter rules; recommendations apply to a future experiment only."""
import json
import sys

from contracts.schema import validate_snapshot


def recommend_policy(snapshot):
    validate_snapshot(snapshot)
    system, kv = snapshot["system"], snapshot["kv"]
    if system.get("disk_read_p95_ms") is not None and system["disk_read_p95_ms"] >= 10:
        policy, bottleneck, codes = "os_aware", "io_pressure", ["DISK_READ_P95_HIGH"]
        reason = "系统级磁盘读取 P95 较高；建议在下一轮对比系统感知策略，尚不能归因于 KV 缓存。"
    elif system.get("mem_available_mb") is not None and system["mem_available_mb"] < 1024:
        policy, bottleneck, codes = "os_aware", "memory_pressure", ["MEM_AVAILABLE_LOW"]
        reason = "Linux 可用内存较低；建议下一轮对比系统感知策略。"
    elif kv.get("prefix_hit_ratio") is not None and kv["prefix_hit_ratio"] < 0.2:
        policy, bottleneck, codes = "lru", "cache_miss", ["PREFIX_HIT_RATIO_LOW"]
        reason = "前缀命中率较低；先保留 LRU 基线并检查负载复用模式。"
    else:
        policy, bottleneck, codes = "lru", "unknown", []
        reason = "现有指标不足以定位明确瓶颈；保留 LRU 基线。"
    return {"run_id": snapshot["run_id"], "policy": policy,
            "bottleneck": bottleneck, "reason": reason,
            "reason_codes": codes, "apply_mode": "next_run"}


if __name__ == "__main__":
    payload = json.load(sys.stdin)
    print(json.dumps(recommend_policy(payload), ensure_ascii=False))
