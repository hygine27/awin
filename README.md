# awin

`awin` 是一个面向基金经理与投研团队的 A 股投资洞察平台项目。  
当前建议中文名：**A视界**

它不是现有“风格监控”脚本的就地升级，而是并行于 `investment-team` 的新项目，用于逐步承接：

1. 市场理解
2. 机会发现
3. 风险识别
4. 持仓诊断
5. 研究融合

## 项目边界

- 不 touch 现有 `investment-team` 的 V1 脚本
- 不改现有 V1 存储结构
- 只读接入现有数据源和现有运行产物
- 在自己的目录、自己的 SQLite、自己的输出协议中实现 V2

```text
investment-team/V1 = 现有生产监控
awin/V2            = 并行新项目，逐步验证和演进
```

## 文档分工

- [项目总览](./docs/overview.md)
  讲项目定位、目标、边界和阶段验收标准
- [架构设计](./docs/architecture.md)
  讲系统架构、数据架构、SQLite 主事实表和 replay 闭环
- [模块设计与演进](./docs/modules.md)
  讲模块职责、模块关系、分期优先级和与 V1 的承接关系
- [Manual](./docs/manual.md)
  汇总当前可手工执行的环境检查、生成结果、replay、evaluation、parity compare 和单票调试命令
- [Action List](./docs/tracking/action-list.md)
  跟踪当前待办、进行中事项和已完成事项
- [Implementation Plan](./docs/tracking/implementation-plan.md)
  说明当前从文档阶段推进到 MVP 实现阶段的开工顺序
- [V1 / V2 Capability Matrix](./docs/tracking/v1-v2-capability-matrix.md)
  说明 V2 必须承接哪些 V1 核心能力，以及新增能力和验收口径
- [M0 Parity Acceptance Status](./docs/tracking/m0-parity-acceptance-status.md)
  说明当前 M0 收尾口径、最新对齐结果、残余差异和收尾结论
- [M0 Output Contracts](./docs/tracking/m0-output-contracts.md)
  说明 M0 阶段各模块必须产出的结构化结果和代码落点
- [Source Canonical Schema](./docs/tracking/source-canonical-schema.md)
  说明第一阶段只读数据源的统一字段口径和 `stock_snapshot` 建议落库字段

## 当前目录边界

```text
awin/
├── docs/
├── configs/
├── data/
├── src/
├── scripts/
├── tests/
└── notebooks/
```

## 当前设计约束

1. 优先使用现有数据源，不把未来所有设想一次性塞进 MVP
2. 以结构化状态和可查询主表为核心，不以 markdown / json 作为主存储
3. 先建立状态系统、提醒机制和 replay 闭环，再扩展更复杂的交易路由
4. 先做并行验证，稳定后再讨论是否替换 V1 前台产出

## 当前可执行入口

## Conda 环境

建议单独使用一个 `conda` 环境运行 `awin`，不要复用 `base`。

```bash
cd /home/yh/.openclaw/workspace/projects/awin
conda env create -f environment.yml
conda activate awin-py312
```

如果环境已经存在，需要更新依赖：

```bash
cd /home/yh/.openclaw/workspace/projects/awin
conda env update -f environment.yml --prune
conda activate awin-py312
```

这套环境的目标很直接：

1. 固定 Python 版本为 `3.12`
2. 安装 `psycopg[binary]`，打通 `QMT / DCF` 的 PostgreSQL 只读接入
3. 用 `pip install -e .` 让 `src/awin` 以 editable 模式运行
4. 保留 `pytest` 和 `pandas` 作为当前开发与联调的基础工具

在当前版本里，`psycopg` 是数据库 adapters 的必要依赖；如果没有这个驱动，`check_runtime.py` 会明确报 `psycopg driver is not installed`。

## 当前可执行入口

- `python3 scripts/check_runtime.py`
  验证 `.env` 是否齐全、数据库配置是否被严格加载、`psycopg` 驱动是否存在、各 source adapter 当前 health
- `python3 scripts/run_once.py --dry-run`
  验证 SQLite 初始化与最小运行入口
- `python3 scripts/smoke_sources.py --trade-date 2026-04-16 --snapshot-time 10:35:00 --analysis-snapshot-ts 2026-04-16T10:35:00`
  验证本地 source adapters、数据库 adapters 当前 health，以及第一版 `market_understanding` 输出
- `python3 scripts/replay_day.py --trade-date 2026-04-16 --format markdown`
  输出当日轮次回放摘要，可直接用于 review 当前 replay 结果
- `python3 scripts/evaluate_day.py --trade-date 2026-04-16 --format markdown`
  输出当日活跃标的与最新状态摘要，作为 evaluation 的第一版业务口径
- `python3 scripts/evaluate_day.py --trade-date 2026-04-16 --format markdown --with-outcomes`
  在日内摘要之外，额外拉取 `stg.qmt_bar_1d` 做 `next_open / close+1d / close+3d / close+5d` 的后验收益汇总
- `python3 scripts/compare_v1_v2_snapshot.py --trade-date 2026-04-16 --snapshot-time 10:35`
  对比同一时段附近的 V1 durable snapshot 与 awin V2 SQLite run，量化市场层和 watchlist 层差异
