# Action List

用于跟踪 `awin / A视界` 项目的待办事项、进行中事项和已完成事项。

## In Progress

- 将 replay / evaluation 从 JSON 输出升级为 markdown 业务摘要
- 基于 `stg.qmt_bar_1d` 落地 post-trade evaluation 的行情来源与收益口径
- 将 `catchup` 末位排序差异从“单样本微调”转成“多交易日、多时点横向验证”
  目标：不再围绕单一样本做无限打磨，而是验证当前末位波动是否稳定且可接受

## Todo

### P0 立项与基线

- 产出第一版里程碑
  目标：将 P0 / P1 / P2 转成可跟踪的实施节奏
- 将 capability matrix 转成 milestone gate
  目标：明确 M0/M1/M2 每阶段的必达功能和验收标准

### P0 数据与存储

- 明确 `alert_log` 是否需要在第一阶段落表
  目标：决定提醒是先派生还是持久化
- 设计 schema 迁移策略
  目标：在已有 `awin.db` 存在时，如何安全升级 `monitor_run` / `stock_snapshot`

### P0 Source Adapter

- 盘点第一阶段只读接入的数据源
  目标：确认 QMT、DCF、THS、主股票池、onepage、公司卡、market intelligence、可选 V1 归档的接入优先级
- 明确 source freshness / completeness 校验规则
  目标：定义采集批次是否可用、何时降级、何时跳过
- 在真实数据库环境下验证 QMT / DCF 查询
  目标：确认 SQL、字段映射和 guard 逻辑与 V1 结果一致或可解释偏差

### P0 Builder / Persist

- 设计增量写入和幂等规则
  目标：同一 snapshot 重跑时可覆盖或去重，不产生脏重复

### P0 领域分析引擎

- 定义 V1 core parity 范围
  目标：明确 style / theme / watchlist / warning / alert diff 哪些必须第一批承接
- 收敛 `core_anchor / new_long / catchup / short` 的 V1 语义
  目标：不要只保留字段名，必须承接 V1 的业务行为含义
- 定义 `market_understanding` 第一版输出协议
  目标：明确 `market_regime`、`style_state`、`top_attack_lines`、`line_structure_summary` 的输出形式
- 定义 `opportunity_discovery` 第一版输出协议
  目标：明确 long candidates、confirmed、catch-up、watchlist 的状态口径
- 定义 `risk_surveillance` 第一版输出协议
  目标：明确 overheat、warning、avoid、laggard 的状态口径
- 定义股票状态机第一版
  目标：明确 observation、confirmed、warning、overheat 等状态及迁移条件

### P0 输出与提醒

- 定义业务摘要输出模板
  目标：统一盘中摘要的结构，不再依赖临时文案拼接
- 定义 diff alert 规则
  目标：明确哪些变化值得提醒、提醒级别如何分层
- 定义 watchlist / warning 输出格式
  目标：让后续 notebook、markdown、数据库查询的口径一致

### P1 Replay / Evaluation

- 设计 replay 最小能力
  目标：能按交易日查看每轮 `monitor_run` 与个股状态路径
- 设计 evaluation 最小能力
  目标：支持次日、3日、5日的后验检验
- 建立“方法 / 手段 / 参数 -> replay -> evaluation -> 调整”的迭代闭环
  目标：把 replay 从展示功能变成方法验证基础设施

### P1 项目骨架

- 补第一批 adapters 包结构
  目标：为 db / file 两类只读源建立明确入口
- 补 `market_understanding` / `opportunity_discovery` / `risk_surveillance` 包结构
  目标：让状态引擎有正式的代码落点

### P2 后续扩展

- 设计 `portfolio_diagnosis` 接口
  目标：支持持仓池、观察池、自定义股票池诊断
- 设计 `research_fusion` 深层接入路线
  目标：规划财报、研报、行业一页纸、知识星球等来源的接入顺序
- 设计更多增强型数据源的接入方案
  目标：规划北向、ETF 资金流、股指期货基差、题材催化事件流等增强层

## Done

- 确认新项目命名为 `awin`，中文名为 `A视界`
- 确认 V2 为并行新项目，不 touch 现有 `investment-team` V1 脚本和存储
- 确认 V2 主存储采用 SQLite，而不是 JSON
- 确认 V2 以 `monitor_run` + `stock_snapshot` 为最小主事实表思路
- 确认 replay 是一级能力，用于方法 / 手段 / 参数的迭代闭环
- 完成项目文档收口
  - `README.md`
  - `docs/overview.md`
  - `docs/architecture.md`
  - `docs/modules.md`
- 建立 `docs/tracking/` 目录和 action list 跟踪文件
- 产出 implementation plan
  - `docs/tracking/implementation-plan.md`
- 产出 V1 / V2 capability matrix
  - `docs/tracking/v1-v2-capability-matrix.md`
- 产出 M0 output contracts 文档
  - `docs/tracking/m0-output-contracts.md`
- 产出 source canonical schema 文档
  - `docs/tracking/source-canonical-schema.md`
- 初始化项目骨架
  - `pyproject.toml`
  - `configs/project.yaml`
  - `src/awin/`
  - `scripts/`
  - `tests/`
- 初始化 SQLite MVP schema
  - `monitor_run`
  - `stock_snapshot`
  - `alert_log`
- 实现最小 `run_once` 入口
  - `scripts/run_once.py`
  - `src/awin/builders/run_once.py`
- 建立 M0 结构化输出契约
  - `src/awin/contracts/m0.py`
- 建立 alert material / diff / NO_UPDATE 基础逻辑
  - `src/awin/alerting/diff.py`
- 建立第一批 source adapter contracts 和 placeholder 入口
  - `src/awin/adapters/contracts.py`
  - `src/awin/adapters/base.py`
  - `src/awin/adapters/master.py`
  - `src/awin/adapters/qmt.py`
  - `src/awin/adapters/dcf.py`
  - `src/awin/adapters/ths.py`
  - `src/awin/adapters/research.py`
- 收敛 `monitor_run` / `stock_snapshot` 字段版本
  - `src/awin/storage/schema.py`
- 补 `init_db` 的 legacy schema 迁移逻辑
  - `src/awin/storage/db.py`
- 实现真实 file-backed adapters
  - `StockMasterAdapter`
  - `ThsConceptAdapter`
  - `ResearchCoverageAdapter`
- 为 QMT / DCF adapter 补充 SQL contract 和无连接降级逻辑
  - `QmtSnapshotAdapter.build_query/load_rows`
  - `DcfSnapshotAdapter.build_query/load_rows`
  - `DcfSnapshotAdapter.evaluate_guard/load_rows_with_health`
- 实现 `market_understanding` 第一版单轮逻辑
  - `src/awin/market_understanding/engine.py`
- 增加 source smoke script
  - `scripts/smoke_sources.py`
- 增加运行时配置 / 驱动 / source health 检查脚本
  - `scripts/check_runtime.py`
- 预留 `replay` / `evaluation` CLI 入口
  - `scripts/replay_day.py`
  - `scripts/evaluate_day.py`
- 补最小测试并验证通过
  - `python3 -m unittest tests/test_schema.py`
- 补 M0 协议与 alert diff 测试并验证通过
  - `python3 -m unittest tests/test_schema.py tests/test_m0_contracts.py tests/test_alert_diff.py`
- 补 adapter contracts 测试
  - `tests/test_adapters_contracts.py`
- 补 schema migration 测试并验证通过
  - `python3 -m unittest tests/test_schema.py tests/test_m0_contracts.py tests/test_alert_diff.py tests/test_adapters_contracts.py`
- 验证旧版 `awin.db` 在 schema 收敛后仍可执行 `run_once --dry-run`
  - `python3 scripts/run_once.py --dry-run`
- 将 QMT adapter 升级为按 snapshot cutoff 回看最近可用快照
- 将 DCF adapter 升级为选批 + freshness / completeness guard + 降级输出
- 补 `market_understanding` 单轮逻辑测试并验证通过
  - `tests/test_market_understanding.py`
- 跑通 source smoke script
  - `python3 scripts/smoke_sources.py --trade-date 2026-04-16 --snapshot-time 10:35:00 --analysis-snapshot-ts 2026-04-16T10:35:00`
- 跑通 runtime check script
  - `python3 scripts/check_runtime.py`
- 将 replay / evaluation 升级为双输出协议
  - 保留 JSON
  - 新增 markdown 摘要
  - `scripts/replay_day.py --format markdown`
  - `scripts/evaluate_day.py --format markdown`
- 为 evaluation 增加显式后验收益开关
  - `scripts/evaluate_day.py --with-outcomes`
  - 使用 `stg.qmt_bar_1d`
  - 汇总 `next_open / close+1d / close+3d / close+5d`
- 增加 V1/V2 snapshot parity compare 工具
  - `scripts/compare_v1_v2_snapshot.py`
  - 对比市场层、主线、core/new/catchup/short 四类名单
- 完成 `M0` 第一轮 parity 收口
  - 市场层对齐
  - `core_anchor` 100%
  - `new_long` 100%
  - `short` 100%
  - `catchup` 收敛到最后 1 个末位排序差异
  - 当前验收状态见 `docs/tracking/m0-parity-acceptance-status.md`
- 完成 `opportunity_discovery` 第一轮业务语义收口
  - 主线骨干连续强势保留
  - 主概念优先
  - `catchup -> new_long` 升级守门
  - `catchup` 的深跌反弹惩罚 / 负主力净流入惩罚 / 负 `ret_3d` 惩罚
  - `catchup` 的新发现奖励与弱重复占位惩罚
- 补齐 parity 收口后的回归护栏
  - 当前全量测试 `44 passed`
  - `tests/test_opportunity_discovery.py` 已覆盖本轮主要排序与惩罚规则
- 验证 `run_once` 可初始化 SQLite 并写入最小 `monitor_run`
  - `python3 scripts/run_once.py --trade-date 2026-04-16 --snapshot-time 10:35:00 --round-seq 1`
- 验证 `run_once` dry-run 在引入 M0 协议代码后仍可执行
  - `python3 scripts/run_once.py --dry-run`
