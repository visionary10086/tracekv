# CPU Scheduler Latency Collector

状态：第一周原型。bpftrace 追踪脚本已通过静态审查；动态挂载需要管理员权限。Python 用户态解析器和单元测试不依赖内核追踪。

负责人：邓智文

## 目标

测量 Linux 内核中任务从被唤醒（`sched_wakeup` / `sched_wakeup_new`）到实际获得 CPU 执行（`sched_switch`）之间的排队等待延迟（Wakeup-to-Run Latency）。

这个延迟反映了：

- CPU 运行队列的负载压力。
- 调度器的公平性与响应时间。
- 与 vLLM 推理进程的性能关联（通过 TID 或进程名过滤后分析）。

## 架构

```text
┌─────────────────────────────────────────────────────────────┐
│  Kernel                                                     │
│                                                             │
│  tracepoint:sched:sched_wakeup ──┐                          │
│  tracepoint:sched:sched_wakeup_new ──┤                      │
│                                      │                      │
│                              BPF Map (TID → wakeup_ns)      │
│                                      │                      │
│  tracepoint:sched:sched_switch ──────┘                      │
│           ↓ match next_pid                                  │
│           ↓ compute latency                                 │
│           ↓ printf to pipe                                  │
└────────────────┬────────────────────────────────────────────┘
                 │ stdout (pipe-delimited CSV)
                 ↓
┌─────────────────────────────────────────────────────────────┐
│  Python  collector/cpu_scheduler.py                         │
│                                                             │
│  parse_event() → SchedEvent                                 │
│  process_stream() → CollectorResult                         │
│  compute_stats() → P50/P95/P99/min/max/mean                │
│  event_to_jsonl() → JSONL output                            │
└─────────────────────────────────────────────────────────────┘
```

## 文件结构

| 文件 | 用途 |
| --- | --- |
| `collector/ebpf/sched_latency.bt` | bpftrace 追踪脚本，在内核中配对 wakeup/switch 事件 |
| `collector/cpu_scheduler.py` | Python 用户态采集器，解析事件、统计指标、管理子进程 |
| `tests/test_collector.py` | 单元测试，使用模拟数据流验证解析与统计逻辑 |
| `docs/collector-scheduler.md` | 本文档 |

## Tracepoint 字段参考

基于当前 WSL2 内核 `6.18.54.1-microsoft-standard-WSL2` 的 BTF 实际解析结果：

### sched_wakeup / sched_wakeup_new

`struct trace_event_raw_sched_wakeup_template` (36 bytes):

| 偏移 | 字段名 | 类型 | 说明 |
| ---: | --- | --- | --- |
| 0 | ent.pid | int | common_pid：唤醒者 TID（不是目标） |
| 8 | comm | char[16] | 目标任务名 |
| 24 | pid | pid_t | **目标任务 TID** |
| 28 | prio | int | 目标任务调度优先级 |
| 32 | target_cpu | int | 调度器拟分配的目标 CPU |

### sched_switch

`struct trace_event_raw_sched_switch` (64 bytes):

| 偏移 | 字段名 | 类型 | 说明 |
| ---: | --- | --- | --- |
| 0 | ent.pid | int | common_pid：当前 CPU 上执行的 TID |
| 8 | prev_comm | char[16] | 被切出任务名 |
| 24 | prev_pid | pid_t | 被切出任务 TID |
| 32 | prev_state | long | 被切出任务状态 |
| 40 | next_comm | char[16] | **切入任务名** |
| 56 | next_pid | pid_t | **切入任务 TID** |
| 60 | next_prio | int | 切入任务优先级 |

## 事件配对算法

1. `sched_wakeup` 触发时，使用 `args.pid`（目标任务 TID）作为 Map Key，记录 `nsecs`（内核单调时钟）。如果该 Key 已存在则不覆盖，递增 `dup_wakeup` 计数。

2. `sched_switch` 触发时，使用 `args.next_pid`（即将上 CPU 的任务 TID）查找 Map。找到则计算 `nsecs - wakeup_ts` 得到延迟并输出，然后删除 Key。未找到则递增 `unmatched_switch`（追踪器启动前已在就绪队列中的任务，或 idle task）。

3. `sched_process_exit` 触发时，如果退出任务有未消费的 wakeup 记录则清理，递增 `exit_cleanup`。

## 输出格式

### 实时事件流（bpftrace stdout）

```text
SCHED|<switch_ns>|<wakeup_ns>|<tid>|<cpu>|<latency_ns>|<comm>
```

管道符分隔，紧凑高效，避免 JSON 字符串拼接开销。

### JSONL 事件记录

```json
{
  "schema_version": 1,
  "run_id": "tracekv-001",
  "source": "ebpf_sched",
  "metric": "wakeup_to_run_latency",
  "clock": "monotonic",
  "timestamp_ns": 123456789000,
  "wakeup_timestamp_ns": 123456704000,
  "pid": null,
  "tid": 1234,
  "comm": "vllm_worker",
  "cpu": 2,
  "request_id": null,
  "value": 85000,
  "unit": "ns"
}
```

- `pid` 暂为 `null`（bpftrace 下获取 TGID 不可靠，不猜测）。
- `request_id` 待 vLLM 请求关联后填充。
- `timestamp_ns` 和 `wakeup_timestamp_ns` 均为内核单调时钟纳秒值。
- `value` 是延迟值，单位纳秒。所有统计基于纳秒原始值计算。

## 百分位数计算方法

使用 **nearest-rank** 方法：

```
rank = ceil(p / 100 × N)
result = sorted_values[rank - 1]
```

其中 `p` 为百分位数（如 50、95、99），`N` 为样本数。

例如 20 个值取 P95：`ceil(0.95 × 20) = 19`，即排序后第 19 个值。

## 时间校准

采集器在启动和结束时记录一对校准锚点：

- `time.time_ns()`：系统 UTC 墙钟时间
- `time.monotonic_ns()`：Python 进程的单调时钟

内核事件的 `nsecs` 使用的是内核单调时钟（`bpf_ktime_get_ns()`），与 Python 的 `time.monotonic_ns()` 基于同一 `CLOCK_MONOTONIC` 源。通过锚点可将任何内核事件时间戳近似转换为 UTC。

**调度延迟的计算始终使用同一内核单调时钟的差值，不受 NTP 跳变影响。**

## 权限要求

| 操作 | 所需权限 |
| --- | --- |
| 导入 Python 模块、运行测试 | 普通用户 |
| 启动 bpftrace 并 attach tracepoint | root 或 CAP_BPF + CAP_PERFMON |
| 读取 `/sys/kernel/tracing/events/` | root（mode 0700） |

当前环境 `unprivileged_bpf_disabled=2`，普通用户无法 attach。

## 使用方法

```bash
# 查看帮助
python3 -m collector.cpu_scheduler --help

# 采集 10 秒（需要 sudo）
sudo python3 -m collector.cpu_scheduler \
    --run-id tracekv-001 \
    --out /tmp/sched_events.jsonl \
    --summary /tmp/sched_summary.json \
    --duration 10
```

## 已知限制

1. bpftrace 的 `printf` 通过 perf event buffer 传输到用户态，高事件频率下可能丢失事件。丢失事件由 bpftrace 运行时报告但无法在脚本内精确计数。
2. `pid`（TGID）字段暂为 `null`。后续版本可在 `sched_wakeup` 中通过 `curtask->tgid` 获取，但需要额外验证。
3. Map 容量受 bpftrace 默认限制（通常 4096 或更高），极端场景下新唤醒可能无法插入。
4. 目标进程过滤（按 PID 或 comm）未在首版实现，采集全系统事件。后续版本增加过滤参数。
