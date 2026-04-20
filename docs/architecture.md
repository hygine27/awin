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

## 4.5 Agent 层需要的证据对象

如果要真正发挥 LLM / Agent 的价值，确定性层不能只产出最终摘要，还需要稳定产出结构化证据对象。

建议至少补 4 类 evidence bundle：

1. `market_evidence_bundle`
   - 市场环境
   - 大风格排序
   - 主线排序
   - T-1 市场资金
   - 当前盘中资金
   - 本轮变化摘要
2. `theme_evidence_bundle`
   - 主线强度
   - 主线扩散度
   - 梯队结构
   - 概念资金与加速情况
   - 代表股与掉队股
3. `stock_evidence_bundle`
   - 个股行情特征
   - 风格画像
   - 资金画像
   - 研究覆盖
   - 同主线相对位置
   - 候选角色与风险标签
4. `portfolio_evidence_bundle`
   - 持仓/观察池当前状态
   - 对市场和主线的暴露
   - 持仓个股的继续持有 / 减仓 / 观察证据

这些 bundle 的目的不是替代现有 `market_understanding / opportunity / risk`，而是让 agent 拿到可以推理、可以对比、可以追问的“证据底座”。

## 4.6 Agent 不该做什么

为了避免走偏，这里也明确边界。

Agent 不应该直接负责：

1. 直接读取原始数据库后自由发挥
2. 直接替代因子计算和状态机
3. 直接给无法追溯的黑箱分数
4. 跳过 evidence 就输出交易判断

否则系统会失去：

- 稳定性
- 可审计性
- 可回放性
- 可迭代性

## 5. Agent 协同闭环

更完整的闭环应收口为：

```text
raw sources
    |
    v
deterministic evidence build
    |
    +--> market / theme / stock / portfolio bundles
    |
    v
agent analysis
    |
    +--> analyst conclusion
    +--> reviewer challenge
    +--> revised recommendation
    |
    v
business output
    |
    v
replay / evaluation
    |
    v
method update / prompt update / rule update
```

在这个闭环里：

1. Python 规则不再是最终答案
   而是可验证的事实和初始假设提供者。
2. Agent 不再只是“写摘要”
   而是承担分析、反驳、交互和复盘角色。
3. Replay 不只验证规则
   也要验证 agent 的判断质量、误判类型和提示词稳定性。

## 6. Replay 与评估闭环

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
          v
调整方法 / 手段 / 参数
          |
          +------> 再次回放验证
```

对 V2 的实际含义是：

- replay 支撑日内复盘
- evaluation 支撑次日/3日/5日后验检验
- 两者共同支撑方法、手段和参数的持续迭代

## 6. 系统目录结构

### 6.1 目标结构

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

### 6.2 第一阶段最小目录

```text
awin/
├── docs/
├── configs/
├── data/sqlite/
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

这套结构的重点不是“目录多”，而是把源接入、状态构建、领域分析和业务输出拆开，便于后续持续扩展。
