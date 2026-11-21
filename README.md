# TraceKV：LLM KV Cache 性能追踪与策略优化（开发中）

团队当前的交付节奏、三人分工、验收测试和 Git 协作命令见 [两周完整 Demo 计划（2026-11-12）](docs/两周完整Demo计划与Git协作_2026-11-12.md)。

面向 Linux 上的大模型推理，建立从请求到系统指标、KV 缓存事件和性能报告的可复现实验链路。仓库提供 vLLM 小模型基线和 CPU LRU 缓存对照工具，可保存逐请求、指标和迁移任务记录。

## 当前实测

- 环境：WSL 2 Ubuntu 24.04、RTX 5070 Laptop GPU、vLLM 0.11.2、PyTorch 2.9.0+cu128。
- 模型：Qwen2.5-0.5B-Instruct，修订 `7ae557604adf67be50417f59c2c2f167def9a775`；模型权重不包含在仓库中。
- 并发 1、2、4 各三轮，每轮 30 次正式请求，共 270/270 成功。原始结果见 `results/20261101-wsl-02/`，解释和限制见 [阶段测试报告](docs/队长阶段测试报告_2026-11-01.md)。
- 这些是短提示小模型基线，缓存状态未重置；没有验证 eBPF 开销、真实 KV 迁移或长上下文性能。

## Linux 可视化监测平台

在 Linux 仓库根目录运行：

    /opt/tracekv/venv/bin/python -m tools.serve_platform

打开 http://localhost:8010/monitor ，查看 GPU / CPU / PSI / I/O / KV 实时指标与曲线，启动与停止真实推理实验，
检查逐请求记录、原始采样及报告。页面离线可用，无需 Node 构建。
本版尚未接入 eBPF 页面和自动对照批次；具体范围见 [平台使用说明](docs/Linux监测平台_使用与验收.md)。

## 统一实验命令

页面与命令行共用同一套配置校验、run_id 和目录创建逻辑。在 Linux 仓库根目录：

    /opt/tracekv/venv/bin/python -m tools.run_experiment --config configs/smoke-gpu-only.json
    /opt/tracekv/venv/bin/python -m tools.run_experiment --config configs/smoke-cpu-lru.json

启动时打印 run_id 与结果目录；可用 --results-dir 指定结果根目录。示例使用本机默认模型路径，其他机器需在 JSON 中设置 model_path。失败与停止也保留清单和报告。

结果包含 manifest.json、summary.json、逐请求与原始采样、aligned_snapshots.jsonl、bottleneck_diagnosis.json 和 report.md。调查提示为可解释的静态阈值，尚未校准分类准确率，不能当作因果证明或策略收益结论。

## MVP v0.1 联调接口

已提供统一快照、Linux `/proc` 可用内存采样、vLLM `/metrics` 的 KV 窗口指标适配、演示规则和 FastAPI 两个接口。当前可运行接口见 [MVP v0.1 接口文档](docs/interfaces.md)；下一轮三人联调的数据、目录、时间和验收约定见 [联调接口规范 v0.2](docs/联调接口规范_v0.2.md)（实施协议，部分接口尚未实现）。[模拟快照](contracts/sample_snapshot.json)只用于接口测试。

```bash
python -m pip install -r requirements-mvp.in
python -m collector.demo
python -m policy.demo < contracts/sample_snapshot.json
python -m uvicorn app.main:app --host 127.0.0.1 --port 8010
```

打开 `http://127.0.0.1:8010/docs` 测试 `GET /api/v1/snapshot` 和 `POST /api/v1/policy/evaluate`。若本地 vLLM 服务正在运行，启动 API 前设置 `TRACEKV_VLLM_METRICS_URL=http://127.0.0.1:8000/metrics`。未采到的指标明确为 `null`，不会冒充真实 KV 或 GPU 数据。

## 目录

| 路径 | 内容 |
| --- | --- |
| `tools/` | 环境检查、vLLM 启动、流式基线客户端、WSL 测试批次 |
| `app/`、`collector/`、`engine/`、`policy/`、`contracts/` | MVP 联调 API、采集、指标适配、规则与数据契约 |
| `workloads/` | 固定短提示冒烟负载 |
| `tests/` | 客户端与统计逻辑测试 |
| `results/` | 成功批次的逐请求记录、指标、服务日志和 GPU 采样 |
| `evidence/` | CUDA、环境和模型文件校验记录 |
| `docs/` | 设计、分工与阶段测试报告 |

## 本机复现

在 Ubuntu 24.04 WSL 中准备 NVIDIA GPU 可用的 Python 3.12 环境，安装 `requirements-linux.lock` 中的固定包版本，并取得上述模型修订。具体依赖以 `results/20261101-wsl-02/server/requirements_actual.txt` 为准。不要在 WSL 中另装 Linux 显示驱动。

```bash
python -m unittest discover -s tests -v
python tools/preflight.py --out /tmp/tracekv-environment.json
python tools/run_wsl_baselines.py \
  --model /path/to/Qwen2.5-0.5B-Instruct \
  --version 0.11.2 \
  --out results/<new-run-id>
```

测试入口仅监听本机 `127.0.0.1`，执行 1 次冒烟和 9 轮基线，最后关闭其启动的服务。输出目录必须是新目录。GPU 采样来自整卡 `nvidia-smi`，包含其他进程占用；客户端 TTFT 包含网络和客户端开销。详见阶段报告。

## CPU LRU 对照

在同一 Linux 环境下准备上述本地模型。脚本分别冷启动 GPU-only 和 CPU LRU 两组服务，两组使用相同的 33 个 GPU KV 块预算。CPU LRU 组通过 vLLM 0.11.2 的 `OffloadingConnector` 启用原生 LRU，默认使用 72 个 CPU KV 块。两组都先填入较早的前缀，再执行种子请求、两条不同前缀的压力请求和种子重访。

```bash
python tools/run_kv_lru.py \
  --model /path/to/Qwen2.5-0.5B-Instruct \
  --out results/<new-run-id>
```

`comparison.json` 中的验收条件包括观察到 CPU 淘汰、重访时 CPU 外部前缀命中、重访前完成的 GPU→CPU 作业、重访期间完成的 CPU→GPU 作业和两组输出一致。命中与查询以 token 计数增量记录。迁移次数按方向统计完成的作业，耗时为 worker 提交到观察到完成的间隔，包含等待和轮询。CPU 占用是已占用块位，包含写入中的预留。GPU 占用是在用比例的采样峰值。每组目录保存逐请求记录、原始指标快照和服务配置，CPU LRU 组另存迁移与占用事件。该对照验证 CPU 层是否改变真实请求的缓存行为。评估淘汰算法差异时需要固定 CPU 容量。

WSL 上的 RTX 4060 实测中，两组各完成 5 次请求，输出一致。CPU LRU 组在目标重访中命中 240 个外部前缀 token，完成 15 个 GPU KV 块的回读。逐请求、迁移和占用事件见 [实测证据](evidence/20261109-cpu-lru-02.json)。

## 下一步

统一实验入口、run_id、框架 KV 与系统指标已接入；继续接入 eBPF 页面和自动多轮对照，用长上下文、共享及非共享前缀负载比较原生、开启追踪和启用缓存策略三组结果。参赛提交还需补充完整过程记录与演示视频链接。

## 参考

- [vLLM 0.11.2 GPU 安装文档](https://docs.vllm.ai/en/v0.11.2/getting_started/installation/gpu/)
- [Qwen2.5-0.5B-Instruct 模型卡](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct) 

## 页面与模块接口

- 实时监测：http://localhost:8010/monitor
- 实验工作台：http://localhost:8010/experiments
- 运行记录：http://localhost:8010/runs
- 缓存策略与 eBPF 明确接口：[接口文档](docs/interfaces.md)、[机器可读契约](contracts/platform-interface-v1.json)。
- 在线交互文档：http://localhost:8010/docs；接口版本 1.0。

三块内容是独立页面。eBPF 接收与报告接口已提供，本机内核采集器仍需独立准备；收到数据不代表当前正在实时采集。

## 实验闭环更新

共享实验上下文、采集档位、SLO、时间线、多轮对照与建议追溯见 [实验平台闭环](docs/platform-workflow.md)。
