# Scripts README

`scripts/` 目录只放可直接执行的入口脚本。

这些脚本按用途分成 4 类：

## 1. Runtime Entry

### [`run_cycle.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/run_cycle.py)

这是当前推荐的调度入口。

用途：

- 用当前本地时间自动生成一轮盘中运行
- 自动将时点收敛到 5 分钟槽位
- 自动计算该交易日的 `round_seq`
- 正式写入 SQLite

适合：

- Cronicle
- cron
- 盘中定时任务

### [`run_once.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/run_once.py)

这是当前推荐的手工运行入口。

用途：

- 按明确的 `trade_date / snapshot_time / round_seq` 运行一轮
- 可 `--dry-run`
- 可指定 SQLite 输出路径

适合：

- 手工 review
- 精确复现实验时点
- 联调

## 2. Runtime Diagnostics

### [`check_runtime.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/check_runtime.py)

检查 `.env`、数据库连接和基础 source health。

### [`smoke_sources.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/smoke_sources.py)

检查某个时点 source adapters 是否能取到数据。

### [`profile_m0_runtime.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/profile_m0_runtime.py)

对 `M0` 构建做阶段级 profile。

注意：

- 这个脚本用于看热点和相对耗时
- 不要把它的 cold profile 直接等同于真实调度 wall time

### [`check_intraday_sources.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/check_intraday_sources.py)

固定检查某个时点的盘中源状态，重点看：

- QMT / DCF 是否有数据
- `ths_cli_hot_concept` 当天是否持续刷新
- `ths_app_hot_concept_trade` 是否明显滞后，是否还能作为生产评分源

### [`raw_market_judgement.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/raw_market_judgement.py)

不依赖 runtime `market_understanding` 输出，直接基于原始源和当前配置做一版独立盘面判断。

适合：

- 人工复核当天大风格
- 看主线候选是否被 runtime 说偏
- 快速判断“是数据问题还是方法问题”

### [`compare_runtime_vs_raw.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/compare_runtime_vs_raw.py)

同一时点同时跑：

- runtime 正式输出
- raw 独立判断

然后直接比较两者差异。

适合：

- 复核为什么 runtime 和盘感不一致
- 定位差异更偏“数据源问题”还是“方法层问题”

## 3. Replay / Evaluation

### [`replay_intraday.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/replay_intraday.py)

按一组盘中时点顺序回放。

### [`replay_day.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/replay_day.py)

查看某个交易日的 replay 摘要。

### [`evaluate_day.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/evaluate_day.py)

输出某天的评估摘要，可带后验收益。

### [`compare_v1_v2_snapshot.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/compare_v1_v2_snapshot.py)

对比同一时点的 V1 durable snapshot 和 V2 run。

## 4. Debug Helpers

### [`debug_catchup.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/debug_catchup.py)

解释个股为什么进入或没进入 `catchup / new_long`。

### [`debug_risk.py`](/home/yh/.openclaw/workspace/projects/awin/scripts/debug_risk.py)

解释风险侧判断。

## 5. Current Scheduling Recommendation

当前推荐让调度系统调用：

```bash
cd /home/yh/.openclaw/workspace/projects/awin
conda run -n awin-py312 python scripts/run_cycle.py
```

而不是直接调：

```bash
python scripts/run_once.py
```

原因：

- `run_once.py` 适合手工精确指定参数
- `run_cycle.py` 才负责把“当前时间 -> 盘中槽位 -> round_seq -> 正式落库”这套调度逻辑收口

## 6. Recommended Diagnostics

如果你要快速复核某个时点，推荐依次运行：

```bash
cd /home/yh/.openclaw/workspace/projects/awin
conda run -n awin-py312 python scripts/check_intraday_sources.py --trade-date 2026-04-20 --snapshot-time 14:50:00
conda run -n awin-py312 python scripts/raw_market_judgement.py --trade-date 2026-04-20 --snapshot-time 14:50:00
conda run -n awin-py312 python scripts/compare_runtime_vs_raw.py --trade-date 2026-04-20 --snapshot-time 14:50:00 --round-seq 1
```

这三步分别回答：

- 当前源数据是否可靠
- 如果完全按原始数据判断，盘面应该怎么理解
- runtime 和 raw 判断到底差在哪里
