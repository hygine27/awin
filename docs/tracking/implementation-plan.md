# Implementation Plan

## 目标

把 `awin / A视界` 从“设计文档阶段”推进到“可开工、可落库、可迭代”的 MVP 实现阶段。

当前 implementation plan 只覆盖第一阶段，不覆盖完整 V2。

相关验收基线见：

- `docs/tracking/v1-v2-capability-matrix.md`

## 实施顺序

### Step 1. 建立项目骨架

目标：

- 项目可作为独立 Python 包运行
- 有自己的 `src/`、`scripts/`、`tests/`、`data/`、`configs/`
- 不依赖改动现有 `investment-team`

交付物：

- `pyproject.toml`
- `src/awin/`
- `scripts/`
- `tests/`

### Step 2. 固定 SQLite MVP 模型

目标：

- 把“主事实表”从概念变成可初始化的 schema
- 明确 `monitor_run` 和 `stock_snapshot` 的落库边界

交付物：

- `src/awin/storage/schema.py`
- `src/awin/storage/db.py`
- `data/sqlite/awin.db`

### Step 3. 打通 run once 骨架

目标：

- 有一个最小 `run once` 入口
- 能初始化 SQLite
- 能写入一条 `monitor_run`
- 为后续接 source adapters 留出接口

交付物：

- `src/awin/builders/run_once.py`
- `scripts/run_once.py`

### Step 4. 接入第一批只读数据源

目标：

- 先接最关键、最稳定的只读源
- 先跑通 snapshot 落库，再上复杂状态判断

优先顺序：

1. 主股票池 / 基础静态属性
2. QMT 5m / latest
3. DCF hq / zj
4. THS concept / hot concept
5. onepage / 公司卡 / market intelligence

交付物：

- 第一批 source adapters
- canonical field mapping
- freshness / completeness 校验

### Step 5. 构建第一版状态输出

目标：

- 从 `stock_snapshot` 派生出第一版业务状态
- 先做“能解释、能比较”，再追求复杂度

优先输出：

1. `market_regime`
2. `style_state`
3. `top_attack_lines`
4. `signal_state`
5. `watchlist`
6. `warning`

交付物：

- `market_understanding`
- `opportunity_discovery`
- `risk_surveillance`
- `alerting`

### Step 6. 补 replay / evaluation 闭环

目标：

- 支持按交易日回放
- 支持后验评估
- 让方法 / 手段 / 参数可以比较

交付物：

- `scripts/replay_day.py`
- `scripts/evaluate_day.py`
- replay / evaluation 查询或导出接口

## 当前建议的开工顺序

第一周建议只做下面 4 件事：

1. 初始化项目骨架
2. 初始化 SQLite schema
3. 写通 `run_once`
4. 补最小测试

只有这 4 件做完，后面接数据源和写状态引擎才不会继续飘在概念层。

## 阶段 gate

### M0: V1 Core Parity

必须至少承接：

- 市场风格判断
- 风格状态机
- THS concept / meta theme 主线解释
- long / short / catch-up / core anchor watchlist
- 10 分制评分与一行理由
- alert diff / `NO_UPDATE`
- note / alert / daily brief 输出

### M1: V2 Structured Upgrade

在 M0 基础上补齐：

- SQLite 主事实表
- 全市场 `stock_snapshot`
- anti-repeat
- replay
- evaluation
- source adapter 封装

### M2: V2 Business Expansion

在 M1 基础上补齐：

- portfolio diagnosis
- 深层 research fusion
- 更多增强型数据源
- 下游同步和更多业务出口

## 当前不做

- 不直接实现完整盘中分析逻辑
- 不直接接所有未来数据源
- 不直接做 UI / dashboard
- 不直接输出交易执行路由

## 开工完成标准

当下面 5 条全部满足时，说明项目已经进入真正实现阶段：

1. `python scripts/run_once.py --help` 可执行
2. SQLite 可初始化
3. `monitor_run` 可写入最小记录
4. 测试可验证 schema 初始化
5. action list 已经从“概念任务”转成“实现任务”
