# V1 / V2 Capability Matrix

## 1. 目的

这份文档回答两个问题：

1. `awin / A视界` 实现后，是否能承接现有 V1 已有功能
2. V2 相比 V1，明确新增了哪些能力，后续该怎么验收

如果这层不先说清楚，后面实现会出现两个问题：

- 做了很多新东西，但 V1 已有关键能力没有被承接
- 做完之后缺少统一验收口径，不知道“是不是已经可替代 / 可并行验证”

因此，V2 的实现与验收必须按下面这张矩阵推进。

## 2. 验收原则

V2 不以“代码重写”为成功标准，而以 3 个标准为准：

1. **V1 核心能力不丢**
   市场风格判断、主线解释、watchlist、warning、alert diff 这些核心输出必须能承接。

2. **V2 新能力明确增加**
   不能只是把 V1 搬到新目录里，必须新增 SQLite 主事实表、replay、evaluation、全市场 stock snapshot 等能力。

3. **按阶段验收，而不是一次到位**
   先完成 V1 核心承接，再逐步打开 V2 的扩展能力。

## 3. 分阶段标准

### M0: V1 Core Parity

目标：

- V2 至少能承接 V1 核心分析和核心输出
- 即使还没有所有增强能力，也不影响并行验证

当前收尾口径：

- 市场层必须对齐
- `core_anchor / new_long / short` 必须对齐
- `catchup` 允许保留最后 1 个末位排序差异
- 差异必须收敛为同类候选的末位波动，而不是框架性错位
- 必须有脚本化 parity compare 与回归测试护栏

### M1: V2 Structured Upgrade

目标：

- 在承接 V1 的基础上，升级为统一 SQLite + 状态系统 + replay 的结构化系统

### M2: V2 Business Expansion

目标：

- 在 V1 能力之上，新增持仓诊断、更多数据融合、更多后验评估与研究增强

## 4. V1 -> V2 承接矩阵

| 能力项 | V1 当前已有 | V2 目标 | 阶段 | 验收方式 |
|---|---|---|---|---|
| 交易时段运行控制 | `run_intraday_style_monitor_cycle.py` 只在交易窗口触发 | V2 需要有自己的 `run_cycle` 或等价调度入口 | M0 | 指定交易窗口内可运行，非交易时段返回 `NO_UPDATE` 或等价结果 |
| 市场风格判断 | 输出 `confirmed_style`、`latest_status`、`latest_dominant_style` | 必须保留，并升级成 `market_regime + style_state + dominant_direction` | M0 | 同一 snapshot 下可产出结构化风格结论 |
| 风格状态机 | `stable / observation / confirmation / backswitch` | 必须保留 | M0 | 状态字段完整，能重放状态变化 |
| 风格篮子横截面比较 | V1 以 style baskets 做相对强弱、扩散度、活跃度比较 | 必须保留，但实现可重构 | M0 | 同一批 snapshot 可生成 style ranking / spread summary |
| THS concept overlay | V1.1 引入概念热度、元主题、子概念解释 | 必须保留 | M0 | 可输出 top concepts / top meta themes / acceleration concepts |
| 主线解释 | V1.1+ 已能回答“现在市场在交易什么” | 必须保留，并继续增强 | M0 | 输出 strongest meta themes 和解释线索 |
| 市场温度层 | V1.4+ 已使用 `market_tape` / THS 市场概览 | 必须保留 | M0 | 输出 market regime / tape evidence |
| 顺风看多观察清单 | V1.2 有 `new_long_watchlist` | 必须保留 | M0 | 能输出候选清单、评分、理由 |
| 偏空 / 过热预警清单 | V1.2 有 `short_watchlist` | 必须保留 | M0 | 能输出 warning / overheat / weak 清单 |
| 潜在补涨观察 | V1.6 已引入 `catchup_watchlist` | 建议保留，视为 V1 后期核心能力 | M0 | 能输出 catch-up 候选 |
| 核心锚定名单 | V1.2 有 `core_anchor_watchlist` | 必须保留 | M0 | 能区分 core anchor 与新晋候选 |
| 10 分制评分 | V1.2 对 long / short / catchup 都有 `confidence_score` | 必须保留，但口径可升级 | M0 | 每个候选都有评分与解释 |
| 一行理由 | V1.2 有 `reason` / `display_line` | 必须保留 | M0 | 输出稳定、可读的一行理由 |
| 研究增强 | V1.2 已使用 onepage / 公司卡 / 近 90 天情报做辅助加分 | 必须保留，但第一阶段只作为增强层 | M0 | 候选结果中保留研究证据钩子 |
| DCF 增强盘口字段 | V1.4 使用 `dcf_cli_hq / dcf_cli_zj` 做盘中增强 | 必须保留 | M0 | 可读取 DCF 批次并通过 freshness / completeness guard |
| 数据降级机制 | V1.4 存在 stale / low coverage / fallback 处理 | 必须保留 | M0 | 数据异常时能标记 degraded 或 fallback |
| alert diff | V1.2 只在材料变化时提醒 | 必须保留 | M0 | 相同材料重复运行时输出 `NO_UPDATE` 或等价结果 |
| alert material 对比 | V1 比较风格、主题、watchlist 变化 | 必须保留 | M0 | 可比较上一轮与当前轮的 material change |
| markdown note 输出 | V1 产出盘中 note | 必须保留，格式可升级 | M0 | 每轮可生成业务摘要 |
| alert body 输出 | V1 产出独立 alert 文本 | 必须保留 | M0 | 有变化时产出提醒正文 |
| daily summary 输出 | V1 写日内摘要 | 建议保留 | M0 | 当日可汇总为 daily brief |
| transient state 输出 | V1 有 state / alert_state / repeat_state | 必须等价承接，但主存储改为 SQLite | M1 | 关键状态可从 SQLite 派生 |
| durable archive | V1 会归档每轮 state 到 durable 目录 | 必须保留，但以 SQLite 为主 | M1 | 可按交易日回看所有轮次 |
| anti-repeat 机制 | V1 对 long / short 有 repeat control | 必须保留 | M1 | 相邻轮次重复出现时有 repeat penalty / anchor 判定 |
| replay | V1 只有归档与 review 雏形，不是一级能力 | V2 必须升级为一级能力 | M1 | 可按交易日查看 run timeline、stock state path |
| evaluation | V1.3 有 daily / weekly review 雏形 | V2 必须升级为正式模块 | M1 | 支持次日 / 3日 / 5日后验评估 |
| Notion 同步 | V1 cycle 会调用 notion sync | V2 建议保留，但不作为第一阶段阻塞项 | M1 | 有独立 sync 层或导出层 |
| 股票池 / 持仓诊断 | V1 没有正式模块 | V2 新增 | M2 | 可接持仓池并输出 hold / watch / trim / exit |
| 全市场主事实表 | V1 以脚本中间 DataFrame 为主，没有统一 SQLite stock snapshot | V2 新增 | M1 | 每轮可落全市场 `stock_snapshot` |
| 统一 source adapter | V1 数据接入逻辑分散在脚本中 | V2 新增 | M1 | 数据源以 adapters 封装，字段口径统一 |
| 统一输出协议 | V1 输出散落在 state / note / alert / review | V2 新增 | M1 | 有明确的 market / opportunity / risk / alert 输出协议 |

## 5. V2 相比 V1 的新增功能

下面这些不是“可有可无的优化”，而是 V2 应明确增加的能力。

### A. 结构化新增

1. SQLite 主事实表
   - `monitor_run`
   - `stock_snapshot`

2. 全市场 snapshot 落库
   - 不再只保留最终名单
   - 而是保留 5000+ 股票在每轮 snapshot 的横截面事实和结论

3. 统一状态系统
   - `market_understanding`
   - `opportunity_discovery`
   - `risk_surveillance`
   - `alerting`

### B. 方法论新增

1. replay 成为一级能力
   - 不只是归档文件
   - 而是可按时间轴重放状态路径

2. evaluation 成为正式模块
   - 不只是事后 review 文档
   - 而是可系统比较方法 / 手段 / 参数

3. 数据质量 guard 显式化
   - freshness
   - completeness
   - fallback

### C. 业务能力新增

1. 持仓 / 股票池诊断
2. 更清晰的主线结构层
   - 龙头
   - 中军
   - 补涨
   - 掉队
3. 更完整的变化提醒层
   - 新增什么
   - 降级什么
   - 切换什么
4. 研究融合层正式模块化

## 6. 后续实现必须回答的 3 个验证问题

后续每推进一个实现里程碑，都必须回答下面 3 个问题：

### 问题 1

这一版是否已经承接了某个 V1 现有能力？

如果答案是否定的，就不能说“V2 已具备并行替换基础”。

### 问题 2

这一版新增了什么 V1 没有的能力？

如果答案是否定的，就只是“重写 V1”，不是“升级到 V2”。

### 问题 3

这一版如何验证？

必须给出明确验证方式，例如：

- 同一 trade_date / snapshot_time 与 V1 对比
- 相邻轮次 diff 是否正确
- `NO_UPDATE` 逻辑是否稳定
- replay 是否能复原状态路径
- evaluation 是否能产出后验结果

## 7. 当前建议的实施约束

从现在开始，`awin` 的实现必须遵守下面这条顺序：

1. 先补齐 V1 core parity 的能力定义
2. 再实现 V2 的主事实表和 adapters
3. 再落 market / opportunity / risk 的输出协议
4. 最后再做更重的持仓诊断和研究融合扩展

否则会很容易出现：

- 新架构已经搭起来
- 但 V1 最重要的 watchlist / warning / alert diff 没有先被承接

这会直接导致后面无法做并行验证。

## 8. 当前 M0 验收状态

截至当前版本，`M0` 可按“可并行验证”标准收尾。

当前结果：

| 项目 | 当前状态 |
|---|---|
| 市场层 | 已对齐 |
| `core_anchor` | 100% |
| `new_long` | 100% |
| `short` | 100% |
| `catchup` | 80% |
| 个股层平均重合度 | 95% |
| 全量测试 | `44 passed` |

当前剩余差异：

- 只剩 `catchup` 最后 1 个名额的末位排序波动
- 该差异当前不再视为 `M0` 阻塞项
- 后续进入 tracking，并在 `M1` 阶段通过更多交易日 replay 样本继续横向验证

当前验收状态详见：

- `docs/tracking/m0-parity-acceptance-status.md`
