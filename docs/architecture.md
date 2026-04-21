# 架构设计

## 1. 设计目标

A视界不是“一个主脚本做所有事情”，而是一个并行于 V1 的分层系统。

它的架构要同时满足 4 个要求：

1. `awin` 的代码、规则配置、SQLite 存储必须独立
2. 第一阶段允许通过 `.env` 只读接入外部输入资产，并允许后续持续扩源
3. 以可查询的主事实表承载盘中状态，而不是以 JSON 文案承载
4. 支持 replay 和 evaluation，形成方法迭代闭环

一句话概括：

```text
现有只读源 -> 适配与规范化 -> SQLite 主事实表 -> 领域引擎 -> Agent 判断层 -> 业务输出 / 回放 / 评估
```

当前阶段对“独立”的定义进一步收口为：

1. 代码独立
  `src/`、`scripts/`、`configs/`、`docs/` 全部归 `awin` 自己维护。
2. 配置独立
  风格篮子、概念 overlay、阈值等规则配置不再复用外部项目目录。
3. 存储独立
  SQLite、输出文件、运行状态均写入 `awin` 自己目录。
4. 输入资产暂外接
  主数据、知识文件、数据库、市场概览等暂时允许外接，但只能通过 `.env` 注入，不能在代码里写死外部项目路径。

## 2. 逻辑架构

```text
+----------------------------------------------------------------------------------+
|                                     A视界 / awin                                 |
+----------------------------------------------------------------------------------+

   +----------------------+   +----------------------+   +----------------------+
   | Market Data Domain   |   | Research Domain      |   | Entity Domain        |
   |----------------------|   |----------------------|   |----------------------|
   | QMT / THS / DCF      |   | onepage / reports    |   | stock master         |
   | Tushare / index      |   | intel / filings      |   | concept map          |
   | northbound / ETF     |   | zhishixingqiu        |   | company metadata     |
   +----------+-----------+   +----------+-----------+   +----------+-----------+
              \                         |                          /
               \                        |                         /
                \                       |                        /
                 v                      v                       v
               +--------------------------------------------------+
               |               Source Adapter Layer               |
               |   db adapters / file adapters / normalization    |
               +--------------------------+-----------------------+
                                          |
                                          v
               +--------------------------------------------------+
               |                 Build / Persist Layer             |
               |      monitor_run + stock_snapshot + sqlite        |
               +--------------------------+-----------------------+
                                          |
                 +------------------------+------------------------+
                 |                        |                        |
                 v                        v                        v
   +----------------------+   +----------------------+   +----------------------+
   | Market Understanding |   | Opportunity Engine   |   | Risk Engine          |
   |----------------------|   |----------------------|   |----------------------|
   | environment          |   | long candidates      |   | overheat             |
   | style                |   | confirmed names      |   | weak names           |
   | attack line          |   | catchup names        |   | avoid list           |
   +----------+-----------+   +----------+-----------+   +----------+-----------+
              \                         |                          /
               \                        |                         /
                \                       |                        /
                 +----------------------+-----------------------+
                                        |
                                        v
                         +-------------------------------+
                         | Portfolio Diagnosis Engine    |
                         |-------------------------------|
                         | holdings / watchlists         |
                         | hold / trim / watch / exit    |
                         +---------------+---------------+
                                         |
                                         v
                         +-------------------------------+
                         | Output Layer                  |
                         |-------------------------------|
                         | business brief                |
                         | alerts                        |
                         | replay                        |
                         | evaluation                    |
                         +-------------------------------+
```

## 2.1 从“脚本中心”升级到“Agent 中心”的分层

未来更合理的逻辑，不是让一个 Python 脚本先把结论全算完，再让 LLM 负责改写文案，而是把系统拆成两个互补平面：

```text
+----------------------------------------------------------------------------------+
|                                A视界 / awin                                      |
+----------------------------------------------------------------------------------+

  Deterministic Data Plane                                  Agent Decision Plane
  ------------------------                                  --------------------
  source adapters                                            market analyst agent
  feature / factor calculation                               opportunity analyst agent
  state machine / ranking                                    risk reviewer agent
  replay / evaluation                                        portfolio diagnosis agent
  sqlite persistence                                         replay evaluator agent
  audit trail                                                interactive Q&A / drill-down

            |                                                             ^
            v                                                             |
  +----------------------------+                             +----------------------------+
  | structured evidence base   |---------------------------->| agent tools + prompts      |
  | market / theme / stock     |<----------------------------| challenge / synthesis      |
  | signals / research / flows |                             | recommendation / critique  |
  +----------------------------+                             +----------------------------+
```

这里的核心原则是：

1. 确定性层不负责“像人一样判断”
  它负责把事实稳定、可审计、可回放地算出来。
2. Agent 层不负责“再造一个数据库”
  它负责围绕证据做综合判断、反方审查、交互解释和后验归因。
3. 两层之间必须通过结构化 evidence 交互
  不能让 agent 直接吃原始杂乱数据，更不能只吃一句摘要。

## 3. 架构原则

1. 状态优先于文案
  先有结构化状态，再生成业务摘要、提醒和汇报口径。
2. 主表优先于中间文件
  尽量从统一主表派生市场、风格、主线和个股结论，避免到处散落中间结果。
3. 并行演进，不干扰 V1
  V2 在自己的目录、自己的 SQLite、自己的输出协议中运行。第一阶段允许只读消费外部输入资产，但不把外部项目路径写死在代码中。
4. 能回放，才能演进
  每轮结果都要可追溯、可比较、可评估。
5. 确定性与主观判断分层
  指标计算、排序、阈值、状态机保持可审计；跨源综合、反证审查、路由建议由 agent 层承担。
6. 先证据，再判断，再行动建议
  任何面向业务的结论都应该能回溯到 evidence bundle，而不是只有一个黑箱总分。
7. Agent 必须可挑战
  不是一个 agent 直接给最终答案，而是要保留 review / critique 机制，避免“会说话但不可靠”。

## 4. 数据架构

### 4.1 总体形态

```text
Read-only existing sources
  |
  +-- DB tables
  |     - qmt snapshot 5m
  |     - qmt latest
  |     - ptrade 1d
  |     - ths hot concepts
  |     - dcf hq / zj
  |
  +-- Files
        - ashare master
        - concept map
        - company cards
        - onepage
        - market intelligence
        - ths market overview
        - optional v1 archive
  |
  v
+-----------------------------------+
| canonical adapters                |
| db + file normalization           |
+----------------+------------------+
                 |
                 v
+-----------------------------------+
| SQLite                            |
|-----------------------------------|
| monitor_run                       |
| stock_snapshot                    |
| alert_log(optional)               |
+----------------+------------------+
                 |
                 v
+-----------------------------------+
| derived views / sql aggregations  |
|-----------------------------------|
| market summary                    |
| style summary                     |
| attack line summary               |
| watchlists                        |
| state diffs                       |
| replay / evaluation               |
+-----------------------------------+
```

### 4.2 为什么主存储用 SQLite

V2 的核心问题不是“把一份结果存下来”，而是要持续回答下面这些问题：

- 某只股票第一次进入候选是什么时候
- 某状态持续了几轮、多长时间
- 某条主攻线何时开始加速、何时开始退潮
- 当前与上一轮相比，变化发生在哪一层
- 同一方法在不同交易日回放时，触发早晚和噪音差异如何

这些问题天然更适合关系型、可查询、可聚合、可回放的存储。

### 4.3 最小数据模型

第一阶段先控制在 2 张核心表 + 1 张可选表。

#### `monitor_run`

一轮监控一条记录，保存运行级状态。

建议包含：

- `run_id`
- `trade_date`
- `snapshot_time`
- `analysis_snapshot_ts`
- `round_seq`
- `market_regime`
- `style_state`
- `top_attack_lines`
- `has_update`
- `alert_level`

#### `stock_snapshot`

一轮 snapshot x 一只股票 = 一行。  
这是 V2 的主事实表。

建议同时保存两类信息：

1. 当轮特征
  - 涨跌幅
  - 开盘后收益
  - 日内位置
  - 成交额 / 放量 / 换手 / 量比
  - 振幅 / 盘口 / 资金
  - 近 3/5/10/20 日收益
  - 研究覆盖度与主题映射
2. 当轮结论
  - 所属大风格背景
  - 所属主攻线 / 元主题
  - `signal_state`
  - `display_bucket`
  - `confidence_score`
  - 风险标签 / 过热标签 / 观察标签

#### `alert_log`（可选）

只在确定需要持久化提醒摘要时保留。否则先由 `monitor_run` + `stock_snapshot` 派生提醒。

### 4.4 哪些结果先不物化

第一阶段不急着建很多 summary 表。下面这些优先作为查询结果或服务输出：

- market environment summary
- style summary
- attack line summary
- watchlist entries
- stock transition events

只有在回放性能或下游消费确实需要时，再补物化表。

## 4.5 Agent-First 证据交付

如果要真正发挥 LLM / Agent 的价值，确定性层不能只产出最终摘要，而要把“可推理、可追问、可复核”的证据按批次落地。

AWIN 后续采用三层交付：

1. `evidence.db`
  作为每轮 canonical evidence base，给 agent 和 replay 使用。
2. `manifest.yaml`
  作为 run 元数据和 artifact 索引，告诉 agent 这一轮有哪些内容、各自多大、先读什么。
3. `*.md`
  作为人和 agent 的 digest 层，用来快速阅读和形成判断，不替代底层事实表。

这里明确收口：

- SQLite 是主证据载体
- CSV 是可选导出物，不是唯一事实源
- JSON 只保留给轻量 debug、接口桥接或临时传输
- Markdown 负责解释，不负责承载唯一事实

建议的每轮目录如下：

```text
data/runs/YYYY-MM-DD/<run_id>/
├── manifest.yaml
├── evidence.db
├── market_brief.md
├── analyst_output.md
├── reviewer_output.md
└── exports/
    ├── focus_stocks.csv
    └── style_ranking.csv
```

## 4.6 Evidence Tables 设计

### 4.6.1 evidence.db 目标表

按 granularity 拆为 4 层：

1. run 级
  `run_context`, `source_health`, `market_change_vs_prev`, `run_artifact`
2. market / theme 级
  `market_overview_evidence`, `market_fund_t1`, `market_fund_intraday`,
  `style_ranking`, `theme_evidence`, `theme_concept_evidence`,
  `theme_leader_stocks`, `theme_laggard_stocks`, `theme_change_vs_prev`
3. stock 全量级
  `stock_snapshot_full`, `stock_style_profile`, `stock_fund_profile`,
  `stock_theme_mapping`
4. focus stock 级
  `focus_stock_evidence`, `focus_stock_score_breakdown`,
  `focus_stock_risk_breakdown`, `focus_stock_research_hooks`,
  `candidate_metadata_full`

### 4.6.2 表关系图

```text
run_context
   |
   +--> source_health
   +--> market_overview_evidence
   +--> market_fund_t1 / market_fund_intraday
   +--> style_ranking
   +--> market_change_vs_prev
   +--> theme_evidence
   |      |
   |      +--> theme_concept_evidence
   |      +--> theme_leader_stocks
   |      +--> theme_laggard_stocks
   |      +--> theme_change_vs_prev
   |
   +--> stock_snapshot_full
          |
          +--> stock_style_profile
          +--> stock_fund_profile
          +--> stock_theme_mapping
          +--> focus_stock_evidence
                 |
                 +--> focus_stock_score_breakdown
                 +--> focus_stock_risk_breakdown
                 +--> focus_stock_research_hooks
                 +--> candidate_metadata_full
```

### 4.6.3 表规模预估

| 层级 | 代表表 | 预估行数 | 说明 |
|---|---|---:|---|
| run | `run_context` | 1 | 当前轮主索引 |
| run | `source_health` | 10-30 | 每个数据源一行 |
| market | `style_ranking` | 6-12 | 大风格排序 |
| theme | `theme_evidence` | 10-30 | 元主题主线 |
| theme | `theme_concept_evidence` | 30-150 | 主题下概念支撑 |
| stock full | `stock_snapshot_full` | 5000+ | 全市场盘中事实 |
| stock full | `stock_theme_mapping` | 10000-50000 | 一股多主题映射 |
| focus | `focus_stock_evidence` | 10-50 | 本轮重点股票 |
| focus | `focus_stock_score_breakdown` | 60-400 | 分数模块拆解 |

按当前 AWIN 的字段密度估算，单轮 `evidence.db` 在 3 MB 到 10 MB 内是合理区间，可被 agent 完整消费。

## 5. Agent 读取协议

Agent 不应直接扫描原始上游库，也不应一上来就读全量 5000+ 股票明细。固定读取顺序如下：

1. 读 `manifest.yaml`
  了解本轮时间、表规模、建议入口、异常源。
2. 读 run / market 级表
  `run_context`、`source_health`、`market_overview_evidence`、`style_ranking`、`theme_evidence`。
3. 读 focus stock 级表
  `focus_stock_evidence` 及分数拆解、风险拆解、研究钩子。
4. 必要时下钻全市场
  `stock_snapshot_full`、`stock_style_profile`、`stock_fund_profile`、`stock_theme_mapping`。
5. 最后读 markdown digest
  用于理解解释链和对外话术，不反向覆盖事实表。

## 6. Agent 不该做什么

为了避免走偏，这里明确边界。

Agent 不应该直接负责：

1. 直接读取原始数据库后自由发挥
2. 直接替代因子计算和状态机
3. 直接给无法追溯的黑箱分数
4. 跳过 evidence 就输出交易判断
5. 每轮都把自己的结论直接写入长期 memory

否则系统会失去：

- 稳定性
- 可审计性
- 可回放性
- 可演进性

## 7. Replay 与 Memory Promotion

Replay 不是“历史展示”，而是方法迭代基础设施。

它解决 4 个问题：

1. 状态可追溯
2. 提醒可解释
3. 方法可比较
4. 参数可验证

闭环如下：

```text
定义方法 / 手段 / 参数
          |
          v
历史交易日回放
          |
          v
比较状态路径与触发结果
          |
          v
评估优劣与误差归因
          |
          +--> 规则修订
          +--> prompt 修订
          +--> memory 候选
          |
          v
再次回放验证
```

这里把“记忆”分成 4 层：

1. `run artifacts`
  每轮强制落地，包含证据、agent 结论、review 结论和评估结果。
2. `episodic memory`
  近几轮 / 近几日滚动上下文，可自动写入，但必须有窗口或 TTL。
3. `semantic memory`
  经过 replay / eval 验证的稳定规律，不能让普通 analyst agent 直接写入。
4. `policy memory`
  影响系统行为的提示词模板、路由策略、评估标准，必须版本化管理。

因此 AWIN 的原则不是“每轮都写 memory”，而是：

- 每轮都落 artifact
- 只有少数经验证结论才 promotion 到长期 memory

## 8. 系统目录结构

### 8.1 目标结构

```text
awin/
├── README.md
├── pyproject.toml
├── docs/
├── configs/
│   ├── project.yaml
│   ├── sources/
│   ├── models/
│   └── thresholds/
├── data/
│   ├── sqlite/
│   ├── runs/
│   ├── exports/
│   └── replay/
├── src/
│   └── awin/
│       ├── adapters/
│       ├── storage/
│       ├── builders/
│       ├── market_understanding/
│       ├── opportunity_discovery/
│       ├── risk_surveillance/
│       ├── portfolio_diagnosis/
│       ├── research_fusion/
│       ├── alerting/
│       ├── replay/
│       ├── evaluation/
│       └── utils/
├── scripts/
├── tests/
└── notebooks/
```

### 8.2 第一阶段最小目录

```text
awin/
├── docs/
├── configs/
├── data/
│   ├── sqlite/
│   └── runs/
├── src/awin/
│   ├── adapters/
│   ├── storage/
│   ├── builders/
│   ├── market_understanding/
│   ├── opportunity_discovery/
│   ├── risk_surveillance/
│   ├── alerting/
│   └── utils/
├── scripts/
└── tests/
```

这套结构的重点不是“目录多”，而是把源接入、证据构建、领域分析、agent 消费和回放评估拆开，便于后续持续扩展。
