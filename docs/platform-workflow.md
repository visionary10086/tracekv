# 实验平台闭环

本平台从 Ubuntu Linux 启动，浏览器访问 http://127.0.0.1:8010。服务使用单个 Uvicorn worker。

## 三页使用方式

- 实时监测：选择整机或某次实验，查看资源曲线、请求与缓存/eBPF 时间线、阶段直方图与来源质量。
- 实验工作台：创建、停止单轮实验；配置 SLO 与采集范围；按保存快照获取并应用下一轮建议；启动自动多轮对照。
- 运行记录：按 run_id 查询原始文件、报告、SLO、建议复核，以及多轮对照的各轮产物。

页首选择通过 URL run_id 和本标签页 sessionStorage 保持；显式 run_id=host 表示整机。新实验配置独立保存在本标签页，切换查看对象不会替换草稿。未采样实验显示空白，不回退到整机数据。

## 进程与采集

平台和 CLI 共用 tools.run_experiment。manifest 记录来源、PID、进程启动 ticks 与 boot_id。平台只操作身份可核对的 CLI 进程，使用 pidfd 发信号；找不到对应进程的旧记录标为 orphaned。每个推理端口通过 flock 互斥，端口占用不会终止已有服务。

trace_profile 有三档：

| 档位 | 范围 |
| --- | --- |
| off | 关闭平台 GPU、系统、vLLM 周期采样和 eBPF |
| sampling | GPU、系统、vLLM 周期采样 |
| ebpf | 普通采样加已有 wakeup_to_run_latency 内核采集器 |

所有档位保留请求日志与已有 CPU offload 迁移钩子。因此 off 不是零插桩。默认端口 8000 的受管实验期间暂停后台整机采样；自动对照整批期间暂停。使用自定义端口的外部 CLI 需单独关闭后台监测再评估采集开销。

eBPF 启动前检查 bpftrace 与当前权限，不自动提权。满足能力检查仅表示可以尝试，实际挂载或中途失败会使要求 eBPF 的实验失败并保留报告。当前机器缺少 bpftrace，尚未验证真实内核挂载。

受管采集器在预热后获取推理进程树的 TID 快照，实验结束请求停止、等待回收、保存原始记录并转成外部接入批次。后续新增线程不会自动纳入；同名 comm 不代表进程身份。新增 runnable、阻塞、回收、缺页等探针由 eBPF 成员通过既有批次契约接入。

## 数据接口

| 方法和路径 | 用途 |
| --- | --- |
| GET /api/v1/runs/{id}/history | 本轮最后 180 个真实快照 |
| GET /api/v1/runs/{id}/snapshot | 本轮最后快照，无数据 503 |
| GET /api/v1/runs/{id}/timeline | UTC 对齐事件，start/end 为含时区 ISO 时间；limit 1..2000，cursor 为偏移 |
| GET/POST /api/v1/comparisons | 查询/启动自动对照 |
| GET /api/v1/comparisons/{id} | 对照配置、成员、统计、身份核验 |
| POST /api/v1/comparisons/{id}/stop | 停止后保留已开始/未开始成员 |
| GET /api/v1/comparisons/{id}/files/{name} | manifest.json / summary.json / report.md |

timeline 默认最后一页；传 cursor=0 从最早开始。source_event_counts 是筛选前全部有效记录数，total_events 是当前时间窗口数。单调时间通过各采集批次 wall_ns / monotonic_ns 锚点换算 UTC；请求 ID 缺失保持未知。时间接近不证明请求归属或因果关系。分页基于当前文件，运行中追加新记录后可刷新最新窗口。

## SLO 与阶段指标

请求日志保存首个可见内容、最后可见内容、DONE 与完成时间，以及内容块间隔。client_tpot_estimate_ms 是首末可见内容时间差除以 usage 输出 token 数减一；缺少 usage、只有一个内容块或失败时为 null。内容块不是 token，不能据此宣称测得真实逐 token ITL。

配置可选 slo_ttft_ms / slo_e2e_ms / slo_tpot_ms，均为正有限值或 null。SLO 分为达标、违反、未知、失败、未配置；分别报告按已记录与计划请求数计算的达标率。goodput 是达标请求数除以正式测量时长。未知值不补零。

vLLM 阶段直方图保留模型/引擎标签，计算累计桶、count、sum 的相邻采样差分。首次采样、标签变化与重置间隔不计算有效差分。stage_summary.json 的 P95 是桶内线性估计；落在 +Inf 桶时只给下界。仪表盘标量要求同一指标只有一个标签组，不把不同引擎比例相加。

## 自动对照与建议复核

POST comparisons 请求示例：

    {
      "base_config": {"requests": 100, "concurrency": 1, "warmup": 1,
                      "max_tokens": 32, "slo_ttft_ms": 1000, "slo_e2e_ms": 5000},
      "policies": ["gpu_only", "cpu_lru"],
      "trace_profiles": ["off", "sampling"],
      "repetitions": 3, "cpu_kv_blocks": 72, "order_seed": 42,
      "effect_threshold_pct": 5
    }

每轮包含所有组合，按固定种子打乱，串行加载新模型服务，保存固定负载、预热、生成种子、GPU 预算和模型权重文件摘要。CPU LRU 另有 CPU 容量，不能宣称总内存相等。单轮与其他自动批次不能插入被预约的队列。失败与中断保留，控制器重启不盲目接管或重新执行旧队列。

报告同时提供 TTFT / E2E / 客户端 TPOT 估计 / 吞吐 / SLO goodput、输出一致性、采集开销描述值及各轮原始文件。P95 按轮描述，不称为合并请求的 P95。性能标签至少要求每组合 3 个完整轮次且每轮 100 次成功请求、输出一致、模型/负载/代码摘要一致、工作树干净。该门槛是保守的工程规则，不是统计显著性证明。

默认主要观察量为各轮 E2E P95 的均值，相对改善超过 effect_threshold_pct 标为 observed_improvement，反向超过阈值为 observed_regression，阈值内为 no_clear_gain。不满足门槛为 insufficient_data；身份未知/变化为 incomparable_or_unverified_identity。所有结论均不建立因果关系，TTFT、goodput、失败率和资源代价仍需独立检查。

策略建议返回 recommendation_id 与 source_run_verified。只有与本轮保存快照完全一致的来源才标记已核对。POST runs 可带 recommendation_id，配置必须匹配建议补丁；manifest 保留原始证据和建议。报告给出与来源实验的配置/身份核验、成对变化及是否样本不足；单次跟随建议的实验不会自动宣称建议有效，应进一步执行自动多轮对照。
