"""Executable next-run recommendations, preserving GPU capacity and evidence."""
from contracts.platform import StrategyRequest
from contracts.schema import utc_now, validate_snapshot

STRATEGIES = [
    {"id": "gpu_only", "name": "GPU-only", "description": "KV 保留在 GPU；CPU 块预算为 0。", "available": True},
    {"id": "cpu_lru", "name": "CPU LRU", "description": "使用 vLLM 原生 CPU 卸载与 LRU；需要正 CPU 块预算。", "available": True},
]


def evaluate(request: StrategyRequest):
    snap = validate_snapshot(request.snapshot)
    current = request.current_config.model_dump()
    candidate = dict(current)
    memory = snap["system"].get("mem_available_mb")
    usage, hit = snap["kv"].get("usage_ratio"), snap["kv"].get("prefix_hit_ratio")
    evidence = {"mem_available_mb": memory, "kv_usage_ratio": usage, "prefix_hit_ratio": hit,
                "scope": snap.get("scope", {"system": "whole_host"})}
    codes = ["KEEP_BASELINE"]
    reason = "当前观测不足以证明更换缓存策略有益，保留当前配置。"
    if current["policy"] == "cpu_lru" and memory is not None and memory < 1024:
        candidate.update(policy="gpu_only", cpu_kv_blocks=0)
        codes = ["HOST_MEMORY_LOW", "COMPARE_WITHOUT_CPU_OFFLOAD"]
        reason = "整机可用内存低于 1024 MB，建议下一轮对照关闭 CPU 卸载；尚不能归因于 KV。"
    elif current["policy"] == "gpu_only" and memory is not None and memory >= 4096 and usage is not None and usage >= .85:
        candidate.update(policy="cpu_lru", cpu_kv_blocks=request.candidate_cpu_kv_blocks)
        codes = ["GPU_KV_PRESSURE", "TRIAL_CPU_OFFLOAD"]
        reason = "GPU KV 使用率达到 85%，且整机可用内存不少于 4096 MB；可试验 CPU LRU，并核查实际复用与恢复成本。"
    return {
        "interface_version": "1.0", "run_id": snap["run_id"], "created_at_utc": utc_now(),
        "apply_mode": "next_run", "strategy_id": candidate["policy"],
        "config_patch": candidate, "reason": reason, "reason_codes": codes,
        "evidence": evidence, "requires_comparison": True, "causality_established": False,
        "limitations": ["规则阈值未经分类准确率校准，建议不保证收益。",
                        "保留 GPU 块预算；CPU 块数是实验参数，不是按内存容量推导的安全上限。",
                        "只返回下一轮候选配置，不修改运行中的服务。"],
    }
