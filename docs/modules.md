# 模块设计与演进

## 1. 模块总图

```text
A视界 / awin
├── market_understanding
├── opportunity_discovery
├── risk_surveillance
├── portfolio_diagnosis
├── research_fusion
├── alerting
├── replay
└── evaluation
```

模块划分的原则只有一句话：

**按业务问题拆模块，不按数据源或脚本拆模块。**

## 2. 模块定义

### `market_understanding`

职责：

- 判断市场环境
- 判断大风格
- 判断主攻线
- 判断主线结构

主要输出：

- `market_regime`
- `style_state`
- `top_attack_lines`
- `line_structure_summary`

这是 V1“风格监控”能力在 V2 中的正式承接模块。

### `opportunity_discovery`

职责：

- 从全市场股票中发现更值得买入或继续跟踪的标的
- 区分核心、顺风、补涨、观察等状态

主要输出：

- long candidates
- confirmed names
- catch-up names
- watchlist

这是 V2 相比 V1 最直接面向业务价值的一层。

### `risk_surveillance`

职责：

- 识别过热、掉队、拥挤和高风险股票
- 输出预警、回避和风险观察结果

主要输出：

- overheat list
- weak / laggard list
- warning list
- avoid list

它不是“做空模块”，而是风险识别模块。

### `portfolio_diagnosis`

职责：

- 接收持仓池、观察池或自定义股票池
- 输出继续持有、观察、减仓、退出等诊断结论

主要输出：

- holdings diagnosis
- pool review
- action tags

这是未来基金经理最可能直接消费的模块之一，但不作为第一阶段强依赖。

### `research_fusion`

职责：

- 融合公司卡、onepage、行业一页纸、财报、研报、知识星球、市场情报等证据
- 给市场、机会和风险判断提供解释增强

主要输出：

- research coverage tags
- evidence summary
- explanation hooks

第一阶段它主要用于“解释增强”和“排序校准”，不直接主导盘中状态判断。

### `alerting`

职责：

- 输出盘中提醒
- 输出分层变化原因
- 控制提醒级别和频率

主要输出：

- intraday alerts
- change diff
- alert severity

它负责把结构化状态转成业务可消费的提醒。

### `replay`

职责：

- 回放某个交易日从开盘到收盘的状态演进
- 追踪环境、风格、主线和个股状态的路径变化

主要输出：

- intraday timeline
- stock state path
- attack line path

它的定位不是展示层，而是方法迭代基础设施。

### `evaluation`

职责：

- 对机会发现、风险识别和状态迁移做后验评估
- 支持次日、3日、5日观察窗口

主要输出：

- forward return evaluation
- hit / miss review
- parameter comparison

`replay` 回答“当时发生了什么”，`evaluation` 回答“后来对不对”。

## 3. 模块关系

```text
market_understanding
        |
        +-------------------+
        |                   |
        v                   v
opportunity_discovery   risk_surveillance
        \                   /
         \                 /
          +-------+-------+
                  |
                  v
              alerting
                  |
                  v
        portfolio_diagnosis(optional input: holdings/watchlists)

research_fusion = cross-cutting support layer
replay          = time-axis replay layer
evaluation      = ex-post validation layer
```

关系上有 3 个重点：

1. `market_understanding` 是上游判断层
2. `opportunity_discovery` 和 `risk_surveillance` 是核心业务层
3. `research_fusion`、`replay`、`evaluation` 是增强层，但都不是装饰层

## 4. 第一阶段优先级

### P0

- `market_understanding`
- `opportunity_discovery`
- `risk_surveillance`
- `alerting`

### P1

- `replay`
- `evaluation`

### P2

- `portfolio_diagnosis`
- `research_fusion` 的深层版本

第一阶段的原则是：

- 先把市场理解、机会发现、风险识别和提醒做实
- 再把 replay / evaluation 做成迭代闭环
- 最后把持仓诊断和更重的研究融合接进来

## 5. 与 V1 的关系

```text
V1
  风格 + 主题 + watchlist + alert

V2
  market_understanding  <- 吸收 V1 的环境 / 风格 / 主线
  opportunity_discovery <- 升级 V1 的 long / catchup
  risk_surveillance     <- 升级 V1 的 short / warning
  alerting              <- 升级 V1 的 diff alert
  replay / evaluation   <- 新增
  portfolio_diagnosis   <- 新增
  research_fusion       <- 从辅助层升级为正式模块
```

所以 V2 不是简单把 V1 拆文件，而是在保留既有业务认知的基础上，补上 V1 没有正式承载的 4 类能力：

1. 机会发现
2. 风险识别
3. 回放与评估
4. 持仓诊断
