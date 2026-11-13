# TraceKV 实验报告

- run_id: 20261113T015413Z-gpu-only-2bacd4d505
- 状态: completed
- 策略: gpu_only
- 缓存准备: 新服务启动后执行固定工作负载预热；未逐请求重置缓存。
- 成功/已记录请求: 3/3
- 计划请求数: 3
- TTFT P95 (ms): 57.84226959995067
- E2E P95 (ms): 583.1584371999156
- 输出吞吐 (token/s): 30.624878304787064
- 错误: 无
- 短负载结果仅用于链路验收；小样本分位数不能作为性能收益结论。

## 数据与限制

- GPU metrics describe device index 0; system metrics describe the whole host.
- eBPF scheduling events are not connected in this milestone.
- No automatic causal attribution or strategy performance claim is made.
- KV ratios retain actual sampling intervals; they are not averaged into fixed buckets.

原始请求见 requests.jsonl；实时快照见 snapshots.jsonl；窗口结果见 aligned_snapshots.jsonl。
