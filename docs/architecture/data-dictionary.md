# AWIN Data Dictionary

## 1. Purpose

这份文档是 `awin` 当前版本的数据主字典。

它只回答 4 类问题，并作为这些问题的单一主入口：

1. 系统当前接了哪些真实数据源
2. 每个数据源是如何读取和标准化的
3. 系统内部用了哪些派生指标、评分因子、业务标签和画像
4. 关键输出字段是如何从上游一路派生出来的

这份文档不覆盖最终输出协议细节。最终输出对象的字段契约仍以
`[m0-output-contracts.md](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/m0-output-contracts.md)`
为准。

## 2. Terms

为了避免同一概念在不同模块里被混用，这里统一术语。


| 术语   | 英文             | 定义                                          | 典型例子                                              |
| ---- | -------------- | ------------------------------------------- | ------------------------------------------------- |
| 数据源  | source         | 真实输入接口，来自文件、数据库或上游服务落表                      | `qmt_ashare_snapshot_5m`, `ts_moneyflow_ths`      |
| 源字段  | source field   | 数据源原始输出字段，经最小清洗后保留                          | `last_price`, `turnover_rate`, `net_amount`       |
| 派生指标 | derived metric | 基于源字段计算的中间指标，本身不直接表达业务结论                    | `pct_chg_prev_close`, `range_position`, `ret_20d` |
| 因子   | factor         | 直接参与打分、排序、风格刻画或风险识别的指标                      | `quality_growth_score`, `money_pace_ratio`        |
| 业务标签 | label          | 供业务阅读的离散标签或分类结果                             | `dividend_style`, `risk_tag`, `display_bucket`    |
| 画像   | profile        | 同一对象的一组因子和标签的组合结果                           | `style_profile`, `fund_flow_profile`              |
| 输出字段 | output field   | 最终写入 `market_understanding`、候选股、风险股、提醒结果的字段 | `confirmed_style`, `summary_line`, `reason`       |


补充约定：

- `metric` 偏中间量
- `factor` 偏参与判断/排序的量
- `label` 偏可读业务分类
- `profile` 偏一组因子和标签的组合对象

## 3. Runtime Dataflow

当前 `M0` 的实际数据流如下：

```text
files / postgres sources
        |
        v
  source adapters
        |
        +--> canonical rows
        |
        +--> source_health
        |
        +--> derived source metrics
              |-- qmt_bar_1d_metrics
              |-- ts_style_daily_metrics
        |
        v
  derived profiles
        |-- style_profile
        |-- fund_flow_profile
        |
        v
  stock_facts
        |
        +--> market_understanding
        +--> opportunity_discovery
        +--> risk_surveillance
        |
        v
  alert_output / sqlite persistence / replay inputs
```

`M0` 当前真实入口位于
`[m0.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/builders/m0.py)`。

## 4. Source Registry

这一节只描述当前已经接入 `build_m0_snapshot_bundle()` 的真实数据源。

### 4.1 Direct Sources


| Source                   | Real Source                                 | Adapter                                                                                                               | Refresh             | Load Logic                               | Key Source Fields                                                                                                                                           | Main Consumers                                              |
| ------------------------ | ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `stock_master`           | 本地基础股票池文件                                   | `[stock_master.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/stock_master.py)`                     | 跟随本地主数据维护           | 读取全市场股票池，提供静态身份信息                        | `symbol`, `stock_code`, `stock_name`, `exchange`, `market`, `industry`, `is_st`, `is_listed`                                                                | `stock_facts`, `market_understanding`                       |
| `ths_concepts`           | 本地 THS 概念映射文件                               | `[ths_concept.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ths_concept.py)`                       | 跟随概念同步              | 读取股票到 THS 概念及元主题映射                       | `symbol`, `stock_code`, `concept_name`, `meta_theme`                                                                                                        | `stock_facts`, `market_understanding`                       |
| `research`               | 本地 onepage / 公司卡 / 情报笔记目录                   | `[research_coverage.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/research_coverage.py)`           | 跟随研究流水线             | 聚合研究覆盖、公司卡和近 90 天情报命中                    | `onepage_path`, `company_card_path`, `recent_intel_mentions`, `research_coverage_score`, `company_card_quality_score`, `research_hooks`                     | `stock_facts`, `opportunity_discovery`, `risk_surveillance` |
| `ths_market_overview`    | 本地 transient 市场概览文件                         | `[ths_market_overview.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ths_market_overview.py)`       | 盘中抓取                | 读取盘中市场概览，形成市场环境底座                        | 指数分时、涨跌分布、涨跌停结构、昨日涨停收益等                                                                                                                                     | `market_understanding`                                      |
| `qmt_ashare_snapshot_5m` | `qt.stg.qmt_ashare_snapshot_5m`             | `[qmt_ashare_snapshot_5m.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/qmt_ashare_snapshot_5m.py)` | 5 分钟                | 取分析时点之前每只股票最近一批可用快照                      | `last_price`, `last_close`, `open_price`, `high_price`, `low_price`, `volume`, `amount`, `bid_price1`, `ask_price1`, `bid_volume1`, `ask_volume1`           | `stock_facts`, `market_understanding`                       |
| `dcf_hq_zj_snapshot`     | `fin.stg.dcf_cli_hq` + `fin.stg.dcf_cli_zj` | `[dcf_hq_zj_snapshot.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/dcf_hq_zj_snapshot.py)`         | 盘中批次                | 选择分析时点之前最近且完整的一批，并对 `hq / zj` 配对         | `turnover_rate`, `volume_ratio`, `amplitude`, `float_mkt_cap`, `total_mkt_cap`, `ret_3d`, `ret_10d`, `ret_20d`, `main_net_inflow`, `super_net`, `large_net` | `stock_facts`, `market_understanding`, 风险与机会判断              |
| `ths_app_hot_concept`    | `fin.stg.ths_app_hot_concept_trade`         | `[ths_app_hot_concept.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ths_app_hot_concept.py)`       | 盘中批次                | 取当前时点之前最近一批 app 热概念榜，并按 overlay 规范化      | `concept_name`, `concept_rank`, `concept_hot_score`, `concept_rank_change`, `limit_up_tag`                                                                  | `market_understanding`                                      |
| `ths_cli_hot_concept`    | `fin.stg.ths_cli_hot_concept`               | `[ths_cli_hot_concept.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ths_cli_hot_concept.py)`       | 盘中批次                | 取当前时点之前最近 1-2 个 CLI 热概念批次，并按 overlay 规范化 | `concept_name`, `change_pct`, `speed_1min`, `main_net_amount`, `limit_up_count`, `rising_count`, `falling_count`, `leading_stock`                           | `market_understanding`                                      |
| `ts_stock_basic`         | `qt.stg.ts_stock_basic`                     | `[ts_stock_basic.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_stock_basic.py)`                 | 日更                  | 读取 T-1 基础属性快照                            | `ts_code`, `market`, `exchange`, `industry`, `act_ent_type`                                                                                                 | `style_profile`                                             |
| `ts_daily_basic`         | `qt.stg.ts_daily_basic`                     | `[ts_daily_basic.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_daily_basic.py)`                 | 日更                  | 读取 T-1 估值与市值快照                           | `free_share`, `circ_mv`, `total_mv`, `turnover_rate`, `dv_ratio`, `dv_ttm`, `pe_ttm`, `pb`, `ps_ttm`                                                        | `style_profile`                                             |
| `ts_index_member_all`    | `qt.stg.ts_index_member_all`                | `[ts_index_member_all.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_index_member_all.py)`       | 日更                  | 读取当前有效行业成分映射                             | `ts_code`, `l1_code`, `l1_name`, `l2_code`, `l2_name`, `l3_code`, `l3_name`, `in_date`, `out_date`                                                          | `style_profile`                                             |
| `ts_fina_indicator`      | `qt.stg.ts_fina_indicator`                  | `[ts_fina_indicator.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_fina_indicator.py)`           | 财报更新                | 读取最近财务质量与成长字段                            | `roe`, `roe_yearly`, `roic`, `debt_to_assets`, `ocf_to_or`, `tr_yoy`, `or_yoy`, `q_sales_yoy`, `netprofit_yoy`, `dt_netprofit_yoy`                          | `style_profile`                                             |
| `ts_moneyflow_ths`       | `qt.stg.ts_moneyflow_ths`                   | `[ts_moneyflow_ths.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_moneyflow_ths.py)`             | 日更，runtime 用 T-1 历史 | 读取个股历史资金流，当前已优化为数据库侧聚合优先                 | `net_amount`, `pct_change`, `net_d5_amount` 及聚合后窗口字段                                                                                                        | `fund_flow_profile`                                         |
| `ts_moneyflow_dc`        | `qt.stg.ts_moneyflow_dc`                    | `[ts_moneyflow_dc.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_moneyflow_dc.py)`               | 日更，runtime 用最近有效交易日 | 读取东财个股资金结构快照                             | `net_amount_rate`, `buy_elg_amount`, `buy_lg_amount`                                                                                                        | `fund_flow_profile`                                         |
| `ts_moneyflow_cnt_ths`   | `qt.stg.ts_moneyflow_cnt_ths`               | `[ts_moneyflow_cnt_ths.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_moneyflow_cnt_ths.py)`     | 日更                  | 读取概念板块历史资金流                              | `ts_code`, `name`, `trade_date`, `net_amount`, `pct_change`                                                                                                 | `fund_flow_profile`, `market_understanding`                 |
| `ts_moneyflow_ind_ths`   | `qt.stg.ts_moneyflow_ind_ths`               | `[ts_moneyflow_ind_ths.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_moneyflow_ind_ths.py)`     | 日更                  | 读取行业历史资金流                                | `ts_code`, `industry`, `trade_date`, `net_amount`, `pct_change`                                                                                             | `fund_flow_profile`, `market_understanding`                 |
| `ts_moneyflow_mkt_dc`    | `qt.stg.ts_moneyflow_mkt_dc`                | `[ts_moneyflow_mkt_dc.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_moneyflow_mkt_dc.py)`       | 日更                  | 读取市场级资金流                                 | `trade_date`, `net_amount`, `net_amount_rate`, `buy_elg_amount`, `buy_lg_amount`                                                                            | `fund_flow_profile`, `market_understanding`                 |


### 4.2 Derived Source Adapters

这类接口不直接对应一个外部业务源，而是对上游明细做数据库侧收口，专门服务 runtime。


| Source                   | Upstream Tables                            | Adapter                                                                                                               | Purpose                      | Output Fields                                                                                        | Main Consumers  |
| ------------------------ | ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------- | --------------- |
| `qmt_bar_1d_metrics`     | `qt.stg.qmt_bar_1d`                        | `[qmt_bar_1d_metrics.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/qmt_bar_1d_metrics.py)`         | 将日线明细压缩成每股一行的 runtime 指标     | `avg_amount_20d`, `close_3d_ago`, `close_5d_ago`, `close_10d_ago`, `close_20d_ago`                   | `stock_facts`   |
| `ts_style_daily_metrics` | `qt.stg.ts_daily` + `qt.stg.ts_adj_factor` | `[ts_style_daily_metrics.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/ts_style_daily_metrics.py)` | 将历史日线与复权因子在数据库侧聚合为风格画像所需最终指标 | `avg_amount_20d`, `ret_20d`, `ret_60d`, `vol_20d`, `vol_60d`, `max_drawdown_20d`, `max_drawdown_60d` | `style_profile` |


### 4.3 Source Health

所有 source adapter 当前都需要给出最小健康信息：

- `source_name`
- `source_status`
- `freshness_seconds`
- `coverage_ratio`
- `fallback_used`
- `detail`

这些字段聚合后进入：

- `build_result.source_health`
- SQLite `monitor_run.source_status`
- runtime 降级判断

## 5. Derived Metrics, Factors, Labels, Profiles

### 5.1 `style_profile`

`style_profile` 是股票慢变量画像，不回答“今天最热炒什么”，而回答“这只股票本身属于什么风格气质”。

定义位置：

- `[style_profile/engine.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/style_profile/engine.py)`
- `[style_profile_rules.yaml](/home/yh/.openclaw/workspace/projects/awin/configs/style_profile_rules.yaml)`

#### A. Core Identity Fields


| Field                                      | Type  | From                          | Meaning             |
| ------------------------------------------ | ----- | ----------------------------- | ------------------- |
| `market_type_label`                        | label | `ts_stock_basic.market`       | 市场层次标签              |
| `exchange_label`                           | label | `ts_stock_basic.exchange`     | 交易所标签               |
| `ownership_style`                          | label | `ts_stock_basic.act_ent_type` | 央国企 / 民企 / 混合 / 未识别 |
| `legacy_industry_label`                    | label | `ts_stock_basic.industry`     | 原始行业标签              |
| `sw_l1_name` / `sw_l2_name` / `sw_l3_name` | label | `ts_index_member_all`         | 申万行业层级标签            |


#### B. Capacity and Size Fields


| Field              | Type   | From                                    | Meaning             |
| ------------------ | ------ | --------------------------------------- | ------------------- |
| `free_float_share` | metric | `ts_daily_basic.free_share`             | 自由流通股本              |
| `float_mv`         | metric | `ts_daily_basic.circ_mv`                | 流通市值                |
| `total_mv`         | metric | `ts_daily_basic.total_mv`               | 总市值                 |
| `avg_amount_20d`   | metric | `ts_style_daily_metrics.avg_amount_20d` | 近 20 日平均成交额         |
| `size_bucket_pct`  | label  | `float_mv` 横截面分位                        | 分位口径大小盘             |
| `size_bucket_abs`  | label  | `float_mv` 固定阈值                         | 绝对口径大小盘             |
| `capacity_bucket`  | label  | `float_mv + avg_amount_20d`             | 机构容量 / 中小票 / 微盘弹性分层 |


#### C. Style Factors

这些字段属于慢变量评分因子，是后续业务标签和复合风格标签的基础。


| Field                     | Type   | Main Inputs                                                | Meaning |
| ------------------------- | ------ | ---------------------------------------------------------- | ------- |
| `dividend_value_score`    | factor | `dv_ttm`, `dv_ratio`, `pe_ttm`, `pb`, `ps_ttm`             | 红利价值评分  |
| `growth_valuation_score`  | factor | 估值 + 成长字段                                                  | 高估值成长评分 |
| `quality_growth_score`    | factor | `roe`, `roe_yearly`, `roic`, `debt_to_assets`, `ocf_to_or` | 质量成长评分  |
| `sales_growth_score`      | factor | `tr_yoy`, `or_yoy`, `q_sales_yoy`                          | 收入成长评分  |
| `profit_growth_score`     | factor | `netprofit_yoy`, `dt_netprofit_yoy`                        | 利润成长评分  |
| `low_vol_defensive_score` | factor | 波动率、回撤、股息等                                                 | 低波防御评分  |
| `high_beta_attack_score`  | factor | 波动率、回撤、小市值等                                                | 高弹性进攻评分 |


#### D. Business Labels


| Field                    | Type      | Meaning                                 |
| ------------------------ | --------- | --------------------------------------- |
| `dividend_style`         | label     | 红利核心 / 红利次优 / 中性 / 低股息                  |
| `valuation_style`        | label     | 低估值 / 中估值 / 高估值                         |
| `growth_style`           | label     | 高成长 / 中成长 / 低成长                         |
| `quality_style`          | label     | 高质量 / 中质量 / 低质量                         |
| `volatility_style`       | label     | 低波 / 中波 / 高弹性                           |
| `composite_style_labels` | label set | 例如 `科技成长`、`红利价值`、`顺周期资源`、`小盘题材` 等综合风格标签 |


### 5.2 `fund_flow_profile`

`fund_flow_profile` 是历史资金画像，不看单个盘中脉冲，而看个股/概念/行业/市场的资金持续性与结构。

定义位置：

- `[fund_flow_profile/engine.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/fund_flow_profile/engine.py)`

#### A. Stock Fund Flow Fields


| Field                        | Type   | Meaning            |
| ---------------------------- | ------ | ------------------ |
| `main_net_amount_1d`         | factor | 个股最近 1 日主力净流入      |
| `main_net_amount_3d_sum`     | factor | 个股近 3 日主力净流入累计     |
| `main_net_amount_5d_sum`     | factor | 个股近 5 日主力净流入累计     |
| `main_net_amount_10d_sum`    | factor | 个股近 10 日主力净流入累计    |
| `main_net_amount_rate_1d`    | factor | 个股最近 1 日净流入率       |
| `ths_net_d5_amount`          | metric | THS 口径的近 5 日净流入额   |
| `super_large_net_1d`         | factor | 超大单净额              |
| `large_order_net_1d`         | factor | 大单净额               |
| `inflow_streak_days`         | factor | 连续净流入天数            |
| `outflow_streak_days`        | factor | 连续净流出天数            |
| `flow_acceleration_3d`       | factor | 最近 3 日与前一段相比的资金加速度 |
| `price_flow_divergence_flag` | label  | 价格和资金方向是否背离        |


#### B. Theme / Industry / Market Flow Fields


| Layer    | Fields                                                                                                                         | Meaning         |
| -------- | ------------------------------------------------------------------------------------------------------------------------------ | --------------- |
| concept  | `net_amount_1d`, `net_amount_3d_sum`, `net_amount_5d_sum`, `pct_change_1d`, `flow_acceleration_3d`                             | 概念主线的价格与资金强度    |
| industry | `net_amount_1d`, `net_amount_3d_sum`, `net_amount_5d_sum`, `pct_change_1d`, `flow_acceleration_3d`                             | 大风格对应行业的价格与资金强度 |
| market   | `net_amount_1d`, `net_amount_rate_1d`, `super_large_net_1d`, `large_order_net_1d`, `inflow_streak_days`, `outflow_streak_days` | 市场层风险偏好与承接      |


### 5.3 `stock_facts`

`stock_facts` 是盘中单股事实层，是 `market_understanding / opportunity_discovery / risk_surveillance`
共同消费的统一底表。

定义位置：

- `[stock_facts.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/analysis/stock_facts.py)`

#### A. Intraday Derived Metrics


| Field                                       | Type   | From                                      | Meaning         |
| ------------------------------------------- | ------ | ----------------------------------------- | --------------- |
| `pct_chg_prev_close`                        | metric | `last_price / last_close - 1`             | 相对昨收涨跌幅         |
| `open_ret`                                  | metric | `open_price / last_close - 1`             | 开盘相对昨收涨跌幅       |
| `range_position`                            | metric | `last_price`, `high_price`, `low_price`   | 当前价格在日内区间中的位置   |
| `bid_ask_imbalance`                         | metric | `bid_volume1`, `ask_volume1`              | 一档委比代理          |
| `elapsed_ratio`                             | metric | `snapshot_time`                           | 当前时点占完整交易日的进度比例 |
| `money_pace_ratio`                          | factor | `amount / avg_amount_20d / elapsed_ratio` | 盘中成交节奏是否放大      |
| `flow_ratio`                                | factor | `main_net_inflow / amount`                | 主力净流入相对成交额占比    |
| `super_flow_ratio`                          | factor | `super_net / amount`                      | 超大单净额占比         |
| `large_flow_ratio`                          | factor | `large_net / amount`                      | 大单净额占比          |
| `ret_3d` / `ret_5d` / `ret_10d` / `ret_20d` | factor | DCF 或 `qmt_bar_1d_metrics`                | 盘中相对历史区间收益      |


#### B. Attached Profiles

`stock_facts` 会把两个画像层打平到单股事实层：

- 来自 `style_profile`
  - 七类慢变量评分
  - 五类业务标签
  - `ownership_style`
  - `capacity_bucket`
  - `composite_style_labels`
- 来自 `fund_flow_profile`
  - 个股历史资金持续性与结构字段

#### C. Theme / Research Fields


| Field                        | Type      | Meaning                 |
| ---------------------------- | --------- | ----------------------- |
| `meta_themes`                | label set | 股票命中的元主题集合              |
| `concepts`                   | label set | 股票命中的 THS 概念集合          |
| `style_names`                | label set | 股票命中的风格篮子               |
| `best_meta_theme`            | label     | 当前最优主元主题                |
| `best_concept`               | label     | 当前最优概念                  |
| `research_coverage_score`    | factor    | 研究覆盖得分                  |
| `company_card_quality_score` | factor    | 公司卡质量分                  |
| `recent_intel_mentions`      | factor    | 近 90 天情报命中次数            |
| `research_hooks`             | label set | onepage / intel / 行业钩子等 |


### 5.4 `market_understanding`

`market_understanding` 是市场层解释，不直接做股票排序。

主要使用：

- `stock_master`
- `qmt_ashare_snapshot_5m`
- `dcf_hq_zj_snapshot`
- `ths_concepts`
- `ths_app_hot_concept`
- `ths_cli_hot_concept`
- `ths_market_overview`
- `style_profile`
- `fund_flow_profile`

关键结果字段：


| Field                   | Type      | Meaning                  |
| ----------------------- | --------- | ------------------------ |
| `confirmed_style`       | label     | 当前确认的大风格                 |
| `latest_status`         | label     | 稳定 / 分化 / 弱化等状态          |
| `latest_dominant_style` | label     | 当前盘面最主导风格                |
| `market_regime`         | label     | 市场环境，如 `trend_expansion` |
| `top_styles`            | ranking   | 风格篮子横截面得分排名              |
| `top_meta_themes`       | ranking   | 元主题强度排名                  |
| `strongest_concepts`    | label set | 当前最强概念                   |
| `acceleration_concepts` | label set | 当前加速概念                   |
| `summary_line`          | output    | 业务摘要行                    |
| `evidence_lines`        | output    | 解释证据行                    |


### 5.5 `opportunity_discovery` and `risk_surveillance`

这两个模块不再直接读 source，而是主要消费：

- `stock_facts`
- `market_understanding`

关键业务标签：


| Module      | Field              | Meaning                              |
| ----------- | ------------------ | ------------------------------------ |
| opportunity | `display_bucket`   | `core_anchor`, `new_long`, `catchup` |
| opportunity | `confidence_score` | 候选强度分                                |
| opportunity | `reason`           | 一行原因                                 |
| risk        | `display_bucket`   | 当前统一为 `warning`                      |
| risk        | `risk_tag`         | `overheat`, `weak` 等风险标签             |
| risk        | `confidence_score` | 风险强度分                                |
| risk        | `reason`           | 一行风险原因                               |


## 6. Key Lineage

这一节只列关键字段的主血缘，不试图覆盖所有字段。

### 6.1 Style Lineage

```text
ts_stock_basic
ts_daily_basic
ts_index_member_all
ts_fina_indicator
ts_style_daily_metrics
      |
      v
build_style_profiles()
      |
      +--> style_profile.size_bucket_pct / size_bucket_abs / capacity_bucket
      +--> style_profile.dividend_value_score / quality_growth_score / high_beta_attack_score ...
      +--> style_profile.dividend_style / quality_style / volatility_style
      +--> style_profile.composite_style_labels
      |
      +--> stock_facts
              |
              +--> market_understanding
              +--> opportunity_discovery
              +--> risk_surveillance
```

### 6.2 Flow Lineage

```text
ts_moneyflow_ths
ts_moneyflow_dc
ts_moneyflow_cnt_ths
ts_moneyflow_ind_ths
ts_moneyflow_mkt_dc
      |
      v
build_fund_flow_snapshot()
      |
      +--> stock_profiles
      +--> concept_profiles
      +--> industry_profiles
      +--> market_profile
      |
      +--> stock_facts
      +--> market_understanding.evidence_lines
      +--> opportunity_discovery
      +--> risk_surveillance
```

### 6.3 Intraday Stock Lineage

```text
qmt_ashare_snapshot_5m
dcf_hq_zj_snapshot
qmt_bar_1d_metrics
stock_master
ths_concepts
research
style_profile
fund_flow_profile
      |
      v
build_stock_facts()
      |
      +--> pct_chg_prev_close
      +--> range_position
      +--> money_pace_ratio
      +--> flow_ratio / super_flow_ratio / large_flow_ratio
      +--> ret_3d / ret_5d / ret_10d / ret_20d
      +--> best_meta_theme / best_concept
      +--> research_coverage_score / company_card_quality_score
```

### 6.4 Market Output Lineage

```text
stock_facts + ths hot concepts + market tape + style_profile + fund_flow_profile
      |
      v
compute_market_understanding()
      |
      +--> confirmed_style
      +--> latest_status
      +--> latest_dominant_style
      +--> market_regime
      +--> top_styles
      +--> top_meta_themes
      +--> strongest_concepts
      +--> acceleration_concepts
      +--> summary_line
      +--> evidence_lines
```

## 7. Current Source of Truth

这份文档是数据字典主入口，但以下文件仍然是各自层级的实现真相源：

- source schema / row contract:
`[contracts.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/contracts.py)`
- runtime source wiring:
`[m0.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/builders/m0.py)`
- style factor and label logic:
`[style_profile/engine.py](/home/yh/.openclaw/workspace/projects/awin/src/awin/style_profile/engine.py)`
- style rules:
`[style_profile_rules.yaml](/home/yh/.openclaw/workspace/projects/awin/configs/style_profile_rules.yaml)`
- concept overlay and meta themes:
`[ths_concept_overlay.yaml](/home/yh/.openclaw/workspace/projects/awin/configs/ths_concept_overlay.yaml)`
- final output contract:
`[m0-output-contracts.md](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/m0-output-contracts.md)`

后续如果与代码不一致，应以代码与 active config 为准，并回写本字典。