# TraceKV Linux 监测平台 v0.2

本版将真实 GPU / 系统 / vLLM 指标、实验启动停止和结果查看接入同一个本地页面。
运行目标是 Linux（含 Ubuntu 24.04 WSL），页面不依赖外部 CDN 或 Node 构建。

## 启动

在仓库根目录执行：

    /opt/tracekv/venv/bin/python -m pip install -r requirements-mvp.in
    /opt/tracekv/venv/bin/python -m tools.serve_platform

打开 http://localhost:8010 。Linux 内监听 127.0.0.1；WSL 可通过 Windows localhost 访问。
必须使用单 worker，避免重复采样和并发实验调度。当前是本机科研平台，尚无多用户鉴权。
前台运行时 Ctrl+C 会先停止本平台拥有的实验，再停止监测。

可选环境变量：

- TRACEKV_MODEL_ROOT：可选模型根目录，默认 /opt/tracekv/models；列出包含 config.json 的一级子目录。
- TRACEKV_RESULTS_DIR：实验结果根目录，默认仓库 results。
- TRACEKV_VLLM_METRICS_URL：监测页面的 vLLM 指标地址，默认 http://127.0.0.1:8000/metrics。

## 页面操作

1. 实时监测显示整机可用内存、GPU 0 显存、KV 占用、前缀命中率和历史曲线。
2. 选择本地模型、工作负载、策略和资源预算，点击启动实验。
3. 平台生成 UTC 时间 + 策略 + 随机后缀的 run_id，独立创建目录。
4. 实验启动自己的模型服务，进行固定负载预热，执行正式请求并持续采样。
5. 查看实验详情，下载报告、JSON 汇总、逐请求记录和采样文件。
6. 点击停止实验可取消加载、预热或正式测量；保留已落盘数据，生成中断报告。

GPU-only 要求 CPU 块数为 0；CPU LRU 要求正整数 CPU 块数。
两组对照须固定模型、工作负载、并发、生成参数和 GPU 块预算。本版逐次启动，
尚未提供自动多轮 comparison_id 批次管理。短 smoke 负载只验证链路，不能证明 LRU 性能收益。

默认实验使用 8000 端口。如端口已被其他服务占用，实验失败并留存日志，不关闭该服务。
平台实时监测也连接 8000；仅监测外部服务时可通过环境变量指定其他指标地址。

## 文件与重算

每次实验保存 manifest、请求、采样、离线快照、analysis.json、report.md 及服务日志。
manifest 记录实际配置、工作负载哈希、Git commit、Python / 内核、启动命令、
模型配置哈希及各源状态。精确模型权重修订尚未自动验证，manifest 明确说明这一限制。

离线重算，不重新运行推理：

    /opt/tracekv/venv/bin/python -m tools.run_experiment --run-dir results/<run_id> --analyze-only

快照沿用 schema_version "0.1"；manifest 的 schema_version 为整数 1。
空窗口指标为 null。GPU / 内存取窗口内最后一个样本，不向空窗口延续旧值。
KV 命中率按实际计数器差分窗口保留在 kv_interval，不能解释为固定窗口内精确命中率。
请求开始数和完成数分别统计；TTFT、E2E 和 token 吞吐从真实请求记录计算。
失败和中断也生成报告；计划请求数与实际已记录数分开呈现。

## 当前边界

- 已接入：真实请求、vLLM 指标、GPU 0 遥测、Linux 可用内存、CPU LRU 迁移原始文件。
- 未接入页面：eBPF 调度采集、SSD 迁移、自动策略应用、实验批次性能比较。
- 没有调度数据就不生成调度分位数；整机指标不能视为单请求因果证据。
- 模型服务结束后 KV 实时连接不可用是正常状态；历史实验文件仍可查看。
- 平台异常退出后旧任务标为需检查，不自动接管或杀死旧 PID。
- 本轮修改位于 Linux 原生仓库 /home/visionary/tracekv 的 feat/linux-monitor-platform 分支。

## 检查

    /opt/tracekv/venv/bin/python -m unittest discover -s tests -q

人工验收：页面启动真实模型，正式请求成功，查看报告；再验证停止实验后
模型服务退出、数据保留且下一次实验可启动。

## 2026-11-13 实机验收

- Ubuntu 24.04 WSL，RTX 5070 Laptop GPU，vLLM 0.11.2。
- 90 项单元测试通过。
- CPU LRU：20261113T015131Z-cpu-lru-b7d86da14e，3/3 正式请求成功。
- GPU-only：20261113T015413Z-gpu-only-2bacd4d505，3/3 正式请求成功。
- 页面停止：20261113T015331Z-gpu-only-bc499fc697，中断报告生成，8000 端口释放。
- 两种实验的 aligned_snapshots 均通过现有快照契约校验。
- 证据汇总：evidence/20261113-linux-monitor.json。
- 初次预热超时和中断记录保留在 results 中；采用既有成功基线的缓存环境及服务容量后复测通过。
- 以上是短负载功能验收，不构成策略性能收益结论。
