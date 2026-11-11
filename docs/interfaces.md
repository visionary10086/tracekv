# TraceKV MVP v0.1 数据与模块接口

状态：第一周联调协议。模块负责人可讨论修改；公共字段的变更须同步修改 `contracts/schema.py`、样例和测试。

## 目标与边界

首周跑通 **采集系统数据 → 统一 JSON → 规则诊断 → HTTP 返回建议**。`contracts/sample_snapshot.json` 是模拟数据，不能用于性能结论。真实 vLLM 基线已单独保存在 `results/`；当前建议仅供下一轮实验选择，不改变运行中的服务。eBPF 读取 Linux 内核事件；KV 命中等语义指标来自 vLLM `/metrics`，不能由 eBPF 单独推断。

| 模块 | 负责人 | v0.1 交付 |
| --- | --- | --- |
| `app/`、`engine/`、`contracts/` | 队长 | API、统一字段、vLLM 指标适配和联调 |
| `collector/` | 邓智文 | `collect_system_metrics()`；当前 `/proc` 可用内存已实现，继续加入至少一项 eBPF 指标 |
| `policy/` | 侯振宇 | `recommend_policy(snapshot)`；当前演示规则已实现，继续验证 LRU 与 OS-Aware 策略 |
| `tools/`、`workloads/` | 队长主导 | 固定负载与实验结果；策略对照在下一阶段 |

## B → A：系统采样

`collector.demo.collect_system_metrics()` 返回含 `timestamp`、`mem_available_mb`、`disk_read_p95_ms`、`disk_read_bytes_per_s`、`source` 的字典。时间为 ISO 8601 UTC；`source` 为 `proc`、`ebpf`、`mock` 或 `unavailable`。`/proc/meminfo` 的 `MemAvailable` 已作为真实数据接入。未接入的磁盘字段为 `null`。磁盘采样默认是系统级，不能直接归因于 vLLM。

## A → C：MetricsSnapshot

JSON 样例见 [`contracts/sample_snapshot.json`](../contracts/sample_snapshot.json)，运行时校验见 `contracts/schema.py`。必须包含：

| 字段 | 单位 / 含义 |
| --- | --- |
| `schema_version` | 固定 `"0.1"` |
| `run_id` | 非空实验编号；请求、采集、结果须共用同一编号 |
| `timestamp` | ISO 8601 UTC |
| `window_ms` | 正整数；未连接 vLLM 时为约定默认值 1000，连接后记录相邻两次实际采样间隔（首样本仍为默认值） |
| `system.mem_available_mb` | 十进制 MB，1 MB = 1,000,000 B |
| `system.disk_read_p95_ms` | 采样窗口读取延迟 P95，ms |
| `system.disk_read_bytes_per_s` | B/s |
| `gpu.memory_used_mb` | 当前整卡占用，十进制 MB；尚未接入时为 `null` |
| `kv.usage_ratio` | KV 占用比例，0–1 |
| `kv.prefix_hit_ratio` | **相邻窗口**命中与查询计数增量的比值，0–1；首个样本或计数重置时为 `null` |
| `kv.running_requests` | 当前运行请求数，非负整数 |
| `sources.system/gpu/kv` | 各域数据来源，常用 `proc`、`ebpf`、`vllm`、`mock`、`unavailable` |

没有采到的数值使用 `null`，不能伪造为 0。`mock` 必须显式标注。`source=unavailable` 表示该域没有真实采样。真实 vLLM 前缀计数有可能一个窗口内无查询，此时命中比例也为 `null`。

## C → A：PolicyRecommendation

`policy.demo.recommend_policy(snapshot)` 返回 `run_id`、`policy`（`lru` / `os_aware`）、`bottleneck`（`memory_pressure` / `io_pressure` / `cache_miss` / `unknown`）、`reason`、`reason_codes` 和固定 `apply_mode=next_run`。当前阈值是**演示规则**：磁盘读取 P95 ≥ 10 ms、可用内存 < 1024 MB、前缀命中比 < 0.2，按此优先级评估。系统级高磁盘延迟只触发待验证的建议，不证明 KV 缓存造成 I/O 压力。真实策略执行和对照实验未实现。

## HTTP 接口与独立演示

第一周只提供：

- `GET /api/v1/snapshot`：返回当前统一快照。设置 `TRACEKV_VLLM_METRICS_URL=http://127.0.0.1:8000/metrics` 时读取真实 vLLM KV 指标；否则 KV 字段为 `null`。首个 vLLM 样本的窗口命中比例为 `null`。
- `POST /api/v1/policy/evaluate`：提交完整快照并返回策略建议；非法数据返回 422。

```bash
python -m collector.demo
python -m policy.demo < contracts/sample_snapshot.json
python -m uvicorn app.main:app --host 127.0.0.1 --port 8010
```

启动后可用 `http://127.0.0.1:8010/docs` 测接口。下一阶段才增加创建实验与读取报告的 HTTP 路由；实验配置至少包含 `model_id`、`scenario`、`policy`、`concurrency`、`duration_s`、`seed`。

团队从 `main` 创建功能分支，通过 PR 合并。邓智文负责证明事件、采集窗口与丢失率；侯振宇负责策略正确性和固定访问序列测试；队长负责统一 `run_id`、同负载对照和报告。首周联合验收是同一份模拟快照经 HTTP 获得建议，并单独证明至少一项真实 Linux 采样。
