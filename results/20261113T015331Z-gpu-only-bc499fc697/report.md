# TraceKV 实验报告

- run_id: 20261113T015331Z-gpu-only-bc499fc697
- 状态: interrupted
- 策略: gpu_only
- 缓存准备: 新服务启动后执行固定工作负载预热；未逐请求重置缓存。
- 成功/已记录请求: 0/0
- 计划请求数: 6
- TTFT P95 (ms): None
- E2E P95 (ms): None
- 输出吞吐 (token/s): None
- 错误: Experiment cancelled

## 数据与限制

- GPU metrics describe device index 0; system metrics describe the whole host.
- eBPF scheduling events are not connected in this milestone.
- No automatic causal attribution or strategy performance claim is made.
- KV ratios retain actual sampling intervals; they are not averaged into fixed buckets.

原始请求见 requests.jsonl；实时快照见 snapshots.jsonl；窗口结果见 aligned_snapshots.jsonl。
