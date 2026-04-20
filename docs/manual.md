# Manual

这份文档集中记录 `awin / A视界` 当前可手工执行的常用命令，便于后续联调、review 和日常排查时直接参考。

默认环境：

```bash
cd /home/yh/.openclaw/workspace/projects/awin
conda activate awin-py312
```

## 1. 环境与数据源检查

检查 `.env`、数据库驱动和 source health：

```bash
python scripts/check_runtime.py
```

检查 source adapters 当前是否能读到指定时点的数据：

```bash
python scripts/smoke_sources.py \
  --trade-date 2026-04-16 \
  --snapshot-time 10:35:00 \
  --analysis-snapshot-ts 2026-04-16T10:35:00
```

检查某个时点的盘中源是否真的可用，并直接看 `ths_app_hot_concept_trade` 是否滞后：

```bash
python scripts/check_intraday_sources.py \
  --trade-date 2026-04-20 \
  --snapshot-time 14:50:00
```

## 1.1 原始盘面判断与 runtime 对照

不依赖 runtime 的 `market_understanding` 输出，直接基于原始数据做一版独立判断：

```bash
python scripts/raw_market_judgement.py \
  --trade-date 2026-04-20 \
  --snapshot-time 14:50:00
```

对比同一时点 runtime 正式输出和 raw 独立判断：

```bash
python scripts/compare_runtime_vs_raw.py \
  --trade-date 2026-04-20 \
  --snapshot-time 14:50:00 \
  --round-seq 1
```

## 2. 生成最新结果

### 2.1 只看当前结果，不落库

使用当前机器时间生成一轮结果，只打印输出，不写 SQLite：

```bash
python scripts/run_once.py --dry-run
```

指定交易日和时点生成一轮结果，只打印输出，不写 SQLite：

```bash
python scripts/run_once.py \
  --trade-date 2026-04-18 \
  --snapshot-time 10:35:00 \
  --round-seq 1 \
  --dry-run
```

### 2.2 正式生成并落库

按指定交易日和时点生成一轮结果，并写入 SQLite：

```bash
python scripts/run_once.py \
  --trade-date 2026-04-18 \
  --snapshot-time 10:35:00 \
  --round-seq 1
```

自定义 SQLite 输出路径：

```bash
python scripts/run_once.py \
  --trade-date 2026-04-18 \
  --snapshot-time 10:35:00 \
  --round-seq 1 \
  --db-path /tmp/awin_manual.db
```

### 2.3 手工连续跑多轮

更接近真实盘中状态机的做法，是按递增时间手工跑多轮：

```bash
python scripts/run_once.py --trade-date 2026-04-18 --snapshot-time 10:25:00 --round-seq 1
python scripts/run_once.py --trade-date 2026-04-18 --snapshot-time 10:30:00 --round-seq 2
python scripts/run_once.py --trade-date 2026-04-18 --snapshot-time 10:35:00 --round-seq 3
```

### 2.4 按调度方式跑一轮

如果是模拟真正的盘中调度，而不是手工指定 `round_seq`，推荐用：

```bash
python scripts/run_cycle.py
```

它会自动：

- 取当前上海时区时间
- 向下收敛到 5 分钟槽位
- 计算该交易日当前槽位的 `round_seq`
- 正式写入 SQLite

只想看它会跑哪个槽位，不落库：

```bash
python scripts/run_cycle.py --dry-run
```

当前会直接打印 1 页盘中业务摘要，固定包含：

- 标题行
- 时间
- `结论与证据`
- `顺风看多观察`
- `潜在补涨观察`
- `偏空 / 过热预警`

并且每个股票 section 的第一只股票会单独展开解释，不只是给一句模糊理由，而会补当前可量化的排序证据，例如：

- 主线排序
- 模块强度分
- 内部排序分
- 模块拆解
- 资金节奏
- 日内位置
- 量比 / 成交额 / 主力净流入
- 补涨分或相对主题偏离

摘要尾部还会固定附带 `评分说明`，解释这些字段的含义和范围，避免只看到分数却不知道是高还是低。

其中资金口径目前分两类：

- `主线资金`
  来自 `ts_moneyflow_cnt_ths` 的概念板块历史资金流，原始 `net_amount` 口径按 `万元` 理解，摘要里会自动显示为 `万 / 亿`。
- `市场资金`
  来自 `ts_moneyflow_mkt_dc` 的市场级资金流，摘要里按 `亿` 展示。

### 2.4.1 如何读顺风首选的分数

顺风首选现在会同时输出三层分数，请区分阅读：

- `模块强度分`
  含义：把 6 个模块原始和按理论上限标准化到 10 分后的业务展示分。
- `内部排序分`
  含义：在模块强度之外，再叠加新晋加分、主概念加分等排序项后的内部排序分。
- `模块拆解`
  含义：把模块原始和拆成 6 项，并显示为 `当前值/上限`。

6 个模块分别是：

- `主线一致性 alignment`
  含义：越高越说明股票与当前强主线、强概念越一致。
- `双重支撑 dual_support`
  含义：越高越说明有多概念、多主题或风格标签共同支持。
- `温度 temperature`
  含义：越高越说明不过热；越低说明短线涨幅、换手或振幅已偏拥挤。
- `研究覆盖 research`
  含义：越高越说明 onepage、公司卡、情报覆盖越充分。
- `盘口节奏 tape`
  含义：越高越说明资金节奏、日内位置、量比和主力流向更健康。
  注意：满分只代表“健康”，不代表“全市场最强”。
- `风格画像 profile`
  含义：越高越说明慢变量风格与历史资金画像更支持当前机会。

常见的两个排序加分：

- `新晋加分`
  含义：当前轮新进入重点视野时给予的排序加分，当前规则默认 `+0.60`，仅用于排序。
- `主概念加分`
  含义：命中当前主线核心概念时给予的排序加分，当前规则默认 `+0.15`，仅用于排序。

### 2.4.2 如何读补涨首选的分数

补涨首选当前不直接展示 10 分制，而是保留 `补涨原始分`，因为 catchup 公式本身没有天然固定上限。

- `补涨原始分`
  含义：只在 catchup 候选内部排序时使用，分数越高，说明这只票既有盘口转强和资金承接，又没有明显透支。

- `补涨拆解`
  默认拆成：
  - `盘口转强`
  - `位置强度`
  - `研究质量`
  - `成交承接`
  - `资金承接`
  - `相对滞涨`
  - `焦点奖励`
  - `新发现奖励`
  - `惩罚`

其中：

- `盘口转强`
  来自资金节奏 `money_pace_ratio`
- `位置强度`
  来自日内位置 `range_position`
- `研究质量`
  来自公司卡质量
- `成交承接`
  来自成交额归一化
- `资金承接`
  来自主力净流入归一化
- `相对滞涨`
  来自近 3 日没有涨得太透支
- `焦点奖励`
  来自命中当前补涨焦点概念
- `新发现奖励`
  来自“新发现但盘口明显转强”的补涨机会
- `惩罚`
  来自资金过弱、深跌反抽、负资金流或跟踪禁用等条件

### 2.4.3 如何读风险首选的资金描述

风险首选不再只写“连续流出几天”这种纯定性描述，而是固定优先给数字。

- `近1日主力净额`
  含义：T-1 历史主力资金净额，使用自动单位展示，优先让你看到最近一天到底流入或流出多少。
- `近5日累计`
  含义：最近 5 个交易日累计主力净额；如果 5 日不可用，则退回 `近3日累计`。
- `已连续流出 N 天`
  含义：最近连续净流出的天数，用来表达“方向是否持续”，但必须和金额一起看。
- `呈现价强资弱`
  含义：价格仍偏强，但最近 1 日主力净额为负，说明价格与资金出现背离。

因此，风险段的资金文案要读成：

- 金额有多大
- 持续了几天
- 是否出现价量背离

指定交易日/时点做一次调度模拟：

```bash
python scripts/run_cycle.py \
  --trade-date 2026-04-18 \
  --snapshot-time 10:35:00 \
  --dry-run
```

## 3. Replay

### 3.1 重放一组盘中时点

把一组时点顺序 replay 到指定 SQLite：

```bash
python scripts/replay_intraday.py \
  --trade-date 2026-04-16 \
  --times 10:25,10:30,10:35 \
  --db-path /tmp/awin_manual_review.db
```

### 3.2 查看某天的 replay 摘要

输出某个交易日的 replay markdown 摘要：

```bash
python scripts/replay_day.py --trade-date 2026-04-16 --format markdown
```

输出 JSON：

```bash
python scripts/replay_day.py --trade-date 2026-04-16 --format json
```

## 4. Evaluation

输出某天的评估摘要：

```bash
python scripts/evaluate_day.py --trade-date 2026-04-16 --format markdown
```

带后验收益结果：

```bash
python scripts/evaluate_day.py --trade-date 2026-04-16 --format markdown --with-outcomes
```

## 5. V1 / V2 对比

对比同一交易日附近的 V1 durable snapshot 与 V2 replay run：

```bash
python scripts/compare_v1_v2_snapshot.py \
  --trade-date 2026-04-16 \
  --snapshot-time 10:35 \
  --db-path /tmp/awin_manual_review.db \
  --v2-run-id 2026-04-16-103500-r03
```

如果不传 `--v2-run-id`，脚本会自动在指定 SQLite 中寻找最接近目标时点的 run。

## 6. 单票调试

调试 `catchup / new_long` 的关键分项：

```bash
python scripts/debug_catchup.py \
  --db-path /tmp/awin_manual_review.db \
  --targets 688111.SH,688615.SH,300418.SZ,300058.SZ
```

调试风险侧：

```bash
python scripts/debug_risk.py
```

## 7. SQLite 直接查看结果

查看最新几轮 `monitor_run`：

```bash
sqlite3 /tmp/awin_manual_review.db "
select run_id, trade_date, snapshot_time, confirmed_style, latest_status, market_regime, top_attack_lines, summary_line
from monitor_run
order by trade_date desc, snapshot_time desc, round_seq desc
limit 5;
"
```

查看某一轮的 watchlist / warning：

```bash
sqlite3 /tmp/awin_manual_review.db "
select symbol, stock_name, display_bucket, confidence_score, best_meta_theme, best_concept, risk_tag
from stock_snapshot
where run_id = '2026-04-16-103500-r03'
  and (is_watchlist = 1 or is_warning = 1)
order by
  case display_bucket
    when 'core_anchor' then 1
    when 'new_long' then 2
    when 'catchup' then 3
    else 4
  end,
  confidence_score desc,
  symbol asc;
"
```

## 8. 常用手工 review 流程

### 场景 A：看当前最新结果

```bash
python scripts/check_runtime.py
python scripts/run_once.py --dry-run
```

### 场景 B：正式落一轮最新结果

```bash
python scripts/run_once.py \
  --trade-date 2026-04-18 \
  --snapshot-time 10:35:00 \
  --round-seq 1 \
  --db-path /tmp/awin_manual.db
```

### 场景 B2：按生产调度方式正式跑一轮

```bash
python scripts/run_cycle.py
```

### 场景 C：做一组 replay 并看 V1 / V2 差异

```bash
python scripts/replay_intraday.py \
  --trade-date 2026-04-16 \
  --times 10:25,10:30,10:35 \
  --db-path /tmp/awin_manual_review.db

python scripts/compare_v1_v2_snapshot.py \
  --trade-date 2026-04-16 \
  --snapshot-time 10:35 \
  --db-path /tmp/awin_manual_review.db \
  --v2-run-id 2026-04-16-103500-r03
```

### 场景 D：追某只票为什么上榜或没上榜

```bash
python scripts/debug_catchup.py \
  --db-path /tmp/awin_manual_review.db \
  --targets 688111.SH,688615.SH,300418.SZ,300058.SZ
```

## 9. 调度建议

### 9.1 当前推荐入口

当前推荐让 Cronicle 或 cron 调用：

```bash
cd /home/yh/.openclaw/workspace/projects/awin
conda run -n awin-py312 python scripts/run_cycle.py
```

而不是直接调用：

```bash
python scripts/run_once.py
```

原因：

- `run_once.py` 更适合手工精确指定时点和 `round_seq`
- `run_cycle.py` 才负责把“当前时间 -> 5 分钟槽位 -> round_seq -> 正式落库”收口成统一调度逻辑

### 9.2 与 V1 的区别

V1 当前是通过 Cronicle 调一个 shell 包装：

```bash
#!/usr/bin/env bash
set -euo pipefail
cd /home/yh/.openclaw/workspace/projects/investment-team
python3 scripts/run_intraday_style_monitor_cycle.py
```

AWIN 当前建议简化为直接调 Python 入口：

```bash
cd /home/yh/.openclaw/workspace/projects/awin
conda run -n awin-py312 python scripts/run_cycle.py
```

如果后续需要，也可以再补一层 shell wrapper，但当前没有必要。
