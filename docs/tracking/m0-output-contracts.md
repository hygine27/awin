# M0 Output Contracts

## 1. 目的

这份文档把 `M0: V1 Core Parity` 的实现边界收成统一输出协议。

原因只有一个：

- 如果没有统一协议，后面 adapter、builder、market、opportunity、risk、alerting 会各自长出自己的字段
- 到真正做 V1 / V2 对照时，会发现“看起来功能差不多”，但其实无法逐项验证

因此，M0 先不追求复杂算法，而是先把 **必须输出什么** 固定下来。

## 2. M0 必须产出的 4 类结果

### A. `market_understanding`

必须覆盖：

- `confirmed_style`
- `latest_status`
- `latest_dominant_style`
- `market_regime`
- `top_styles`
- `top_meta_themes`
- `strongest_concepts`
- `acceleration_concepts`
- `summary_line`
- `evidence_lines`

这层负责承接 V1 的：

- 风格判断
- 风格状态机
- THS concept / meta theme 主线解释
- 市场温度层

### B. `opportunity_discovery`

必须覆盖：

- `core_anchor_watchlist`
- `new_long_watchlist`
- `catchup_watchlist`

每个候选至少要有：

- `symbol`
- `stock_name`
- `display_bucket`
- `confidence_score`
- `themes`
- `reason`
- `display_line`
- `best_meta_theme`
- `best_concept`

这层负责承接 V1 的：

- 顺风看多观察
- 核心锚定名单
- 潜在补涨观察
- 10 分制评分
- 一行理由

### C. `risk_surveillance`

必须覆盖：

- `short_watchlist`

每个候选至少要有：

- `symbol`
- `stock_name`
- `display_bucket`
- `confidence_score`
- `themes`
- `reason`
- `display_line`
- `risk_tag`

这层负责承接 V1 的：

- 偏空 / 过热预警
- weak / overheat 区分
- 风险型一行理由

### D. `alerting`

必须覆盖：

- `alert_material`
- `alert_changes`
- `alert_decision`
- `alert_body`

这层负责承接 V1 的：

- 只在材料变化时提醒
- `NO_UPDATE`
- alert diff

## 2.1 M0 新增的 Agent 证据对象

为避免 `awin` 退化成“规则引擎 + 文案润色器”，`M0` 当前开始补充两类结构化证据对象：

### E. `market_evidence_bundle`

至少覆盖：

- `confirmed_style`
- `latest_status`
- `latest_dominant_style`
- `market_regime`
- `top_styles`
- `top_meta_themes`
- `strongest_concepts`
- `acceleration_concepts`
- `t1_market_fund`
- `intraday_market_fund`
- `theme_evidence`
- `source_health`

这层不是替代 `market_understanding`，而是给 agent 一个可继续推理、追问、质疑的市场证据底座。

### F. `stock_evidence_bundle`

至少覆盖：

- `focus_stocks`

其中每个 `focus_stock` 至少要有：

- `symbol`
- `stock_name`
- `role`
- `display_bucket`
- `confidence_score`
- `best_meta_theme`
- `best_concept`
- `theme_rank`
- `concept_overlay_rank`
- `reason`
- `themes`
- `style_names`
- `composite_style_labels`
- `pct_chg_prev_close`
- `money_pace_ratio`
- `volume_ratio`
- `turnover_rate`
- `main_net_inflow`
- `main_net_amount_1d`
- `main_net_amount_5d_sum`
- `outflow_streak_days`
- `price_flow_divergence_flag`
- `research_coverage_score`
- `candidate_metadata`

这层的目标是：让 Agent 不需要回头扫原始数据库，也不只拿一句候选摘要，而是直接拿到面向“为什么该做/不该做”的个股证据对象。

## 3. M0 聚合结果

M0 每轮运行的统一聚合结果应该至少包含：

1. `run_context`
2. `market_understanding`
3. `opportunity_discovery`
4. `risk_surveillance`
5. `alert_output`
6. `market_evidence_bundle`
7. `stock_evidence_bundle`

后续 markdown note、daily brief、SQLite state summary、replay timeline 都应该从这份统一结果派生，而不是各模块各写一套。

## 4. M0 与 V1 对照关系

| V1 输出 | M0 对应输出 |
|---|---|
| `confirmed_style` / `latest_status` / `latest_dominant_style` | `market_understanding` |
| `top_meta_themes` | `market_understanding.top_meta_themes` |
| `core_anchor_watchlist` | `opportunity_discovery.core_anchor_watchlist` |
| `new_long_watchlist` | `opportunity_discovery.new_long_watchlist` |
| `catchup_watchlist` | `opportunity_discovery.catchup_watchlist` |
| `short_watchlist` | `risk_surveillance.short_watchlist` |
| `summary_line` | `market_understanding.summary_line` |
| `alert_decision` / `alert_material_changes` / `alert_body` | `alert_output` |

## 4.1 终端摘要输出

`scripts/run_cycle.py` 当前的终端摘要输出，不再只打印一行 `summary_line`，而是统一渲染成 1 页业务摘要，固定包含：

- 标题行
- 时间
- `结论与证据`
- `顺风看多观察`
- `潜在补涨观察`
- `偏空 / 过热预警`
- 每个 section 的首位股票解释
- `评分说明`

其中：

- `结论与证据` 来自 `market_understanding.summary_line`、`market_understanding.evidence_lines` 和关键数据源健康状态
- `顺风看多观察` 来自 `core_anchor_watchlist` + `new_long_watchlist`，并按候选强度统一重排；首位要补“为什么它是本节第一名”
- 顺风首位解释里，要明确区分：
  - 模块强度分：把 6 个模块原始和归一化到 10 分后的业务展示分
  - 内部排序分：额外叠加新晋 / 主概念等奖励后的内部排序分
- `潜在补涨观察` 来自 `catchup_watchlist`；首位要补“补涨原始分 / 近3日-10日位置 / 资金节奏”等解释
- 补涨首位解释里，要明确区分：
  - 补涨原始分：仅用于 catchup 候选内部排序，不是 10 分制
  - 补涨拆解：盘口转强、位置强度、研究质量、成交承接、资金承接、相对滞涨、奖励项、惩罚项
- `偏空 / 过热预警` 来自 `short_watchlist`；首位要补“相对主题偏离 / 近10日-20日涨幅 / 振幅 / 近1日与近5日主力金额 / 连续流出天数 / 价强资弱”解释
- `评分说明` 作为终端摘要固定尾注，解释 `alignment / dual_support / temperature / research / tape / profile / 新晋加分 / 主概念加分`

## 5. 当前代码落点

M0 输出协议当前落在：

- `src/awin/contracts/m0.py`
- `src/awin/alerting/diff.py`

这里的目标不是先把算法做完，而是先让：

- builder 有明确聚合目标
- market / opportunity / risk 有明确返回值
- alerting 有明确 diff 材料
- tests 可以对协议和 diff 逻辑做稳定校验
