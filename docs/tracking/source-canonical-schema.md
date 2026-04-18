# Source Canonical Schema

## 1. 目的

这份文档定义 `awin` 第一批只读数据源的 canonical schema。

目标不是一次性把所有字段都接进来，而是先回答两个问题：

1. 第一阶段到底接哪些源
2. 接进来之后，统一成什么字段口径

## 2. 第一阶段数据源范围

### P0 必接

1. 主股票池 / 基础静态属性
2. QMT 5m / latest snapshot
3. DCF hq / zj
4. THS concept mapping / hot concept
5. onepage / 公司卡 / market intelligence

### P0 可选

1. THS market overview
2. V1 durable archive

## 3. canonical 层原则

### A. 代码统一

- `symbol` 保留完整后缀，例如 `300570.SZ`
- `stock_code` 保留无后缀 6 位，例如 `300570`

### B. 时间统一

- `trade_date` 使用 `YYYY-MM-DD`
- `snapshot_time` 使用 `HH:MM:SS`
- `analysis_snapshot_ts` 使用 `YYYY-MM-DDTHH:MM:SS`
- 上游批次时间额外保留 `vendor_batch_ts`

### C. 数值统一

- 比例字段统一用小数，不用带 `%` 字符串
- 金额字段统一用原始数值
- 布尔字段统一用 `0/1` 或 `True/False`

### D. 降级统一

每个 adapter 都要显式输出：

- `source_status`
- `freshness_seconds`
- `coverage_ratio`
- `fallback_used`

## 4. 各源 canonical 输出

### 4.1 主股票池 / 基础静态属性

最小字段：

- `symbol`
- `stock_code`
- `stock_name`
- `exchange`
- `market`
- `industry`
- `is_st`
- `is_listed`

### 4.2 QMT snapshot / latest

最小字段：

- `symbol`
- `stock_code`
- `trade_date`
- `snapshot_time`
- `last_price`
- `last_close`
- `open_price`
- `high_price`
- `low_price`
- `volume`
- `amount`
- `bid_price1`
- `ask_price1`
- `bid_volume1`
- `ask_volume1`

### 4.3 DCF hq / zj

最小字段：

- `symbol`
- `trade_date`
- `vendor_batch_ts`
- `turnover_rate`
- `volume_ratio`
- `amplitude`
- `float_mkt_cap`
- `total_mkt_cap`
- `ret_3d`
- `ret_5d`
- `ret_10d`
- `ret_20d`
- `main_net_inflow`
- `super_net`
- `large_net`
- `freshness_seconds`
- `coverage_ratio`
- `source_status`

### 4.4 THS concept mapping / hot concept

最小字段：

- `symbol`
- `stock_code`
- `concept_name`
- `meta_theme`
- `concept_rank`
- `concept_hot_score`
- `concept_rank_change`
- `concept_limit_up_count`

### 4.5 研究增强层

最小字段：

- `symbol`
- `onepage_path`
- `company_card_path`
- `recent_intel_mentions`
- `research_coverage_score`
- `research_hooks`

## 5. `stock_snapshot` 建议落库字段

### A. 主键与时间

- `run_id`
- `trade_date`
- `snapshot_time`
- `analysis_snapshot_ts`
- `symbol`
- `stock_code`

### B. 静态身份

- `stock_name`
- `exchange`
- `market`
- `industry`

### C. 行情与盘口

- `last_price`
- `last_close`
- `open_price`
- `high_price`
- `low_price`
- `volume`
- `amount`
- `bid_volume1`
- `ask_volume1`
- `pct_chg_prev_close`
- `open_ret`
- `range_position`

### D. 增强温度

- `turnover_rate`
- `volume_ratio`
- `amplitude`
- `float_mkt_cap`
- `total_mkt_cap`
- `money_pace_ratio`
- `main_net_inflow`
- `super_net`
- `large_net`
- `ret_3d`
- `ret_5d`
- `ret_10d`
- `ret_20d`

### E. 归属与结论

- `style_bucket`
- `best_meta_theme`
- `best_concept`
- `theme_names_json`
- `style_names_json`
- `signal_state`
- `display_bucket`
- `risk_tag`
- `confidence_score`

### F. 研究与数据质量

- `research_coverage_score`
- `research_hooks_json`
- `source_status`
- `fallback_used`
- `freshness_seconds`
- `coverage_ratio`
- `is_watchlist`
- `is_warning`

## 6. 当前代码落点

第一阶段 canonical schema 当前对应：

- `src/awin/adapters/contracts.py`
- `src/awin/storage/schema.py`
