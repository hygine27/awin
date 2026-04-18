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

## 3. M0 聚合结果

M0 每轮运行的统一聚合结果应该至少包含：

1. `run_context`
2. `market_understanding`
3. `opportunity_discovery`
4. `risk_surveillance`
5. `alert_output`

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

## 5. 当前代码落点

M0 输出协议当前落在：

- `src/awin/contracts/m0.py`
- `src/awin/alerting/diff.py`

这里的目标不是先把算法做完，而是先让：

- builder 有明确聚合目标
- market / opportunity / risk 有明确返回值
- alerting 有明确 diff 材料
- tests 可以对协议和 diff 逻辑做稳定校验
