# TraceKV：LLM KV Cache 性能追踪与策略优化（开发中）

面向 Linux 上的大模型推理，建立从请求到系统指标、KV 缓存事件和性能报告的可复现实验链路。当前仓库是第一阶段成果：可启动 vLLM 小模型、执行固定请求基线并保存原始记录。eBPF 采集器和缓存策略模块尚在联调，不能将现有数据解释为策略优化收益。

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

## 下一步

统一 `run_id` 与时间字段，接入 eBPF CPU/内存事件和框架 KV 指标；用长上下文、共享及非共享前缀负载，比较原生、开启追踪、启用缓存策略三组结果。参赛提交最终还需补充完整实现、过程记录与演示视频链接。

## 参考

- [vLLM 0.11.2 GPU 安装文档](https://docs.vllm.ai/en/v0.11.2/getting_started/installation/gpu/)
- [Qwen2.5-0.5B-Instruct 模型卡](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct) 
