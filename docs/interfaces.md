# TraceKV MVP v0.1 数据与模块接口

状态：前半部分记录首周 MVP v0.1 历史协议；现行接入见下文 v1.0 和 [实验闭环](platform-workflow.md)。下一轮统一实验的数据交接见 [联调接口规范 v0.2](联调接口规范_v0.2.md)；公共字段的变更须同步修改 `contracts/schema.py`、样例和测试。

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

`policy.demo.recommend_policy(snapshot)` 返回 `run_id`、`policy`（`lru` / `os_aware`）、`bottleneck`（`memory_pressure` / `io_pressure` / `cache_miss` / `unknown`）、`reason`、`reason_codes` 和固定 `apply_mode=next_run`。当前阈值是**演示规则**：磁盘读取 P95 ≥ 10 ms、可用内存 < 1024 MB、前缀命中比 < 0.2，按此优先级评估。系统级高磁盘延迟只触发待验证的建议，不证明 KV 缓存造成 I/O 压力。这是旧演示接口；现行 GPU-only / CPU LRU 执行与自动对照使用下文平台接口。

## HTTP 接口与独立演示

第一周只提供：

- `GET /api/v1/snapshot`：返回当前统一快照。设置 `TRACEKV_VLLM_METRICS_URL=http://127.0.0.1:8000/metrics` 时读取真实 vLLM KV 指标；否则 KV 字段为 `null`。首个 vLLM 样本的窗口命中比例为 `null`。
- `POST /api/v1/policy/evaluate`：提交完整快照并返回策略建议；非法数据返回 422。

```bash
python -m collector.demo
python -m policy.demo < contracts/sample_snapshot.json
python -m uvicorn app.main:app --host 127.0.0.1 --port 8010
```

启动后可用 `http://127.0.0.1:8010/docs` 测接口。现行版本已提供创建实验与读取报告的 HTTP 路由，字段见下文与实验闭环文档。首周曾计划的配置字段为 `model_id`、`scenario`、`policy`、`concurrency`、`duration_s`、`seed`。

团队从 `main` 创建功能分支，通过 PR 合并。邓智文负责证明事件、采集窗口与丢失率；侯振宇负责策略正确性和固定访问序列测试；队长负责统一 `run_id`、同负载对照和报告。首周联合验收是同一份模拟快照经 HTTP 获得建议，并单独证明至少一项真实 Linux 采样。

## 平台与模块接入接口 v1.0（2026-11-21）

此版本新增模块边界，已有快照 schema_version=0.1 和旧策略接口保持兼容。网页使用新接口，策略 ID 与实验入口一致。

### 三个独立页面

| 页面 | 地址 | 内容 |
| --- | --- | --- |
| 实时监测 | /monitor | GPU、系统、KV 曲线与接入状态 |
| 实验工作台 | /experiments | 配置、策略建议、启动停止、模块接口入口 |
| 运行记录 | /runs | 筛选实验、查看结果与原始文件、接入统计、离线重算 |

根地址跳转 /monitor。详情可直接使用 /runs?run_id=具体ID。页面不再用锚点滚动到同一文档的不同板块。

### 缓存策略负责人接口

| 方法与路径 | 输入 | 返回 |
| --- | --- | --- |
| GET /api/v1/strategies | 无 | 可执行策略 gpu_only、cpu_lru，及 apply_modes |
| POST /api/v1/strategies/evaluate | interface_version、snapshot、current_config、candidate_cpu_kv_blocks | strategy_id、config_patch、reason_codes、evidence、limitations、apply_mode=next_run |
| POST /api/v1/runs | 实验配置 | 新 run_id 与实际配置清单 |
| POST /api/v1/runs/{run_id}/telemetry/cache | CacheBatch | 批次回执 |
| GET /api/v1/runs/{run_id}/telemetry | 无 | 分采集器与范围的接入统计 |

snapshot 采用现有 v0.1 快照：run_id、UTC timestamp、正 window_ms、system/gpu/kv/sources。current_config 只包含 policy、gpu_kv_blocks、cpu_kv_blocks；新接口拒绝未知字段、布尔预算和不合法组合。

config_patch 可以覆盖下一轮实验配置中的三个策略字段。gpu_only 要求 CPU 块为 0，cpu_lru 要求正 CPU 块。建议不修改当前服务，不自动执行，不声称性能收益；网页须点击“填入下一轮配置”，再点击“启动实验”。

CacheEvent 支持 lookup/hit/evict/transfer。命中与淘汰需要稳定逻辑 cache_key；键由模型修订、格式与前缀等兼容信息派生，不只使用可复用的物理块号，也不上传原始提示词。transfer 需要 worker_id+job_id、方向和 submitted/completed/failed 状态。bytes/tokens/blocks 未知填 null，不能按块数无依据地推算 bytes。不同方向、层和阶段分开统计，提交与完成不重复合成搬运总量。

elapsed_ns 的边界由生产者记录，本接口不把它解释成纯 GPU DMA 时间；request_id/pid 无可靠映射时为 null。

### eBPF 负责人接口

| 方法与路径 | 输入 | 返回 |
| --- | --- | --- |
| GET /api/v1/integrations | 无 | 接收端就绪、本机工具与权限、can_attempt，以及 automatic_start=when_requested_and_capable |
| POST /api/v1/runs/{run_id}/telemetry/ebpf | EbpfBatch | batch_id、received_at_utc、event_count、duplicate |
| GET /api/v1/runs/{run_id}/telemetry/{source}/batches/{batch_id} | source=ebpf/cache | 原始声明、接收时间与校验摘要 |
| POST /api/v1/runs/{run_id}/analyze | 无 | 离线重算结果，仅允许任务已结束 |

EbpfBatch 必填 run_id、batch_id、collector_id、clock、window_start_ns/window_end_ns、scope、events。事件 metric 为 wakeup_to_run_latency、runnable_wait、on_cpu、blocked、direct_reclaim、major_fault 或 minor_fault。duration 指标 value 单位为 ns；缺页计数事件可不提供耗时。

scope=target_threads 时 target_tids 必填，事件 TID 必须属于该集合；scope=comm_match 时声明 target_comm，但这不证明进程身份；system 表示系统范围。pid 是真实 TGID，未知填 null，不能用 tid 顶替。线程创建退出及目标集合更新由采集器负责，变化后的集合单独分组。

时间约定：所有事件 timestamp_ns 使用 CLOCK_MONOTONIC；UTC ns = clock.wall_ns + timestamp_ns - clock.monotonic_ns。窗口左闭右开，事件必须落在声明窗口内。该约定只校准时间，不能建立逐请求因果。

dropped_events=null 表示未知；0 必须有核对证据。这个数描述采集器丢失核对，不能直接解释为每个目标线程的真实丢失数。failed/unavailable 必须带 error，且 events 为空。接收批次不启动 root 进程，也不自动安装或授予采集权限。

### 传输、重试与报告

- 每批最多 2000 个事件。按 run_id/source/batch_id 保存不可变 JSON。
- 同 ID 同内容重试返回 duplicate=true；同 ID 不同内容为 409。同一采集器与范围的观测窗口不允许重叠，避免换 batch_id 后重复计数。
- 合法新批次为 201；参数/目标/窗口/版本错误为 422；实验或批次不存在为 404；冲突或运行中重算为 409。
- source、collector_id、scope、目标集合不同的统计分开；缓存事件另按层、方向和状态分开。缺失值保持 null。
- synthetic=true 的示例/合成批次不进入真实事件统计。真实采集器才可标记 false。接收端仅能校验声明，不能证明生产者提交的是真实内核数据。
- 产物保存在 results/<run_id>/telemetry/<source>/<batch_id>.json。统计可在运行记录中查询；已结束任务收到新数据后页面提示报告过期，可点击重算。
- 重算生成 telemetry_summary.json，并纳入 analysis.json、summary.json 和 report.md。外部事件按声明范围单独展示，不把整段汇总复制到每个采样窗口。
- 这些入口面向本机受信任协作进程，与现有平台同样只监听 127.0.0.1，不是公网多用户服务。

### 可交接文件

- 类型与运行时验证：contracts/platform.py。
- 机器可读契约：contracts/platform-interface-v1.json；在线 GET /api/v1/interfaces。
- 可直接阅读的样例：contracts/examples/strategy-request-v1.json、strategy-response-v1.json、ebpf-batch-v1.json、cache-batch-v1.json。
- 交互式请求/响应文档：/docs。
- 样例包含 synthetic=true；不要把样例改名后当成实测证据。

在线交接入口：/api/v1/interfaces/guide 提供本文；/api/v1/interfaces/examples/ebpf、cache、strategy-request、strategy-response 提供对应 JSON 样例，均无需外部 CDN。

## 实验闭环更新

共享实验上下文、采集档位、SLO、时间线、多轮对照与建议追溯见 [实验平台闭环](platform-workflow.md)。
