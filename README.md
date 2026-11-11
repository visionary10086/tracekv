# TraceKV：LLM KV Cache 性能追踪与策略优化（开发中）

面向 Linux 上的大模型推理，建立从请求到系统指标、KV 缓存事件和性能报告的可复现实验链路。仓库提供 vLLM 小模型基线和 CPU LRU 缓存对照工具，可保存逐请求、指标和迁移任务记录。

## 当前实测

- 环境：WSL 2 Ubuntu 24.04、RTX 5070 Laptop GPU、vLLM 0.11.2、PyTorch 2.9.0+cu128。
- 模型：Qwen2.5-0.5B-Instruct，修订 `7ae557604adf67be50417f59c2c2f167def9a775`；模型权重不包含在仓库中。
- 并发 1、2、4 各三轮，每轮 30 次正式请求，共 270/270 成功。原始结果见 `results/20261101-wsl-02/`，解释和限制见 [阶段测试报告](docs/队长阶段测试报告_2026-11-01.md)。
- 这些是短提示小模型基线，缓存状态未重置；没有验证 eBPF 开销、真实 KV 迁移或长上下文性能。

## 目录

| 路径 | 内容 |
| --- | --- |
| `tools/` | 环境检查、vLLM 启动、流式基线客户端、WSL 测试批次 |
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

统一 `run_id` 与时间字段，接入 eBPF CPU/内存事件和框架 KV 指标；用长上下文、共享及非共享前缀负载，比较原生、开启追踪、启用缓存策略三组结果。参赛提交最终还需补充完整实现、过程记录与演示视频链接。

## 参考

- [vLLM 0.11.2 GPU 安装文档](https://docs.vllm.ai/en/v0.11.2/getting_started/installation/gpu/)
- [Qwen2.5-0.5B-Instruct 模型卡](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct) 
