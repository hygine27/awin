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
