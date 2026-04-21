# Action List

用于跟踪 `awin / A视界` 的当前实施主线。此版本已经将早期零散待办收口为统一的
`P0 / P1 / P2` 结构，并补入“配置文件命名去版本化”的治理动作。

## Current Focus

- 当前目标不是继续扩散设计讨论，而是把 V2 变成一个：
  - 保留并改进 V1 有效业务行为
  - 真正接入慢变量风格画像与历史资金画像
  - 能通过真实 `run_once --dry-run` 验收的版本
- 当前方向新增一条关键约束：
  - `awin` 不能停留在“规则引擎 + LLM 润色”的模式
  - 下一阶段要开始把 Agent 放进分析、质疑、交互和 replay 迭代闭环
- 当前新增的关键约束：
  - `qt.stg` 中历史资金表已具备，不再把“是否有数据”当作 blocker
  - `configs/` 规则文件不再使用 `.v1.yaml` / `.v1.json` 命名，统一收敛为无版本文件名

## In Progress

- P0.1 配置命名收口
  - 目标：去掉 `configs/*.v1.yaml` / `configs/*.v1.json` 后缀，统一改为无版本文件名
  - 当前状态：已完成文件重命名，并更新 `.env` 生效路径
  - 验收标准：
    - `configs/` 下 active 配置不再带 `.v1`
    - 代码、环境变量、文档引用不再出现旧文件名
- P0.2 Action list 收口
  - 目标：将旧的分散待办收敛成面向实现与验收的统一列表
  - 当前状态：已完成当前阶段收口
- P0.3 历史资金接入第一阶段
  - 当前状态：代码层已完成
  - 已完成内容：
    - 新增 `ts_moneyflow_ths / dc / cnt_ths / ind_ths / mkt_dc` adapters
    - 新增 `fund_flow_profile` 派生层
    - `build_m0_snapshot_bundle` 已接入历史资金源与 `source_health`
    - `market_understanding` 已开始输出主线/市场资金证据
    - `stock_facts / opportunity_discovery / risk_surveillance / stock_snapshot` 已开始消费关键风格画像与历史资金画像
    - 已完成真实库适配器健康检查：
      - `ts_moneyflow_ths`: `163,201` 行，`ready`
      - `ts_moneyflow_dc`: `189,744` 行，`ready`
      - `ts_moneyflow_cnt_ths`: `12,384` 行，`ready`
      - `ts_moneyflow_ind_ths`: `2,880` 行，`ready`
      - `ts_moneyflow_mkt_dc`: `32` 行，`ready`
  - 剩余内容：
    - 真实数据库环境下的 runtime 耗时与全链路 dry-run 验收
    - 将数据库 IO 慢点压到可盘中使用水平
    - 当前已完成一项明确优化：
      - `ts_moneyflow_dc` 已从“45 天历史明细拉取”收敛为“最近有效交易日快照拉取”
      - 真实库耗时从 `44.25s` 降到 `4.22s`
      - `ts_moneyflow_ths` 已从“45 天明细拉取”收敛为“数据库侧聚合后单股票一行”
      - 真实库耗时从 `33.61s` 降到 `7.16s`
      - `ts_daily` / `ts_adj_factor` 已去掉数据库排序，并将回看窗口从 `220` 天收敛到 `120` 天
      - 真实库耗时：
        - `ts_daily`: `123.71s -> 45.26s`
        - `ts_adj_factor`: `108.35s -> 39.57s`
      - 新增 `ts_style_daily_metrics`
        - 由 `ts_daily + ts_adj_factor` 在数据库侧聚合为每股一行
        - 真实库耗时 `7.94s`
        - 替代 runtime 中原来的 `ts_daily + ts_adj_factor` 明细拉取
      - `dcf_hq_zj_snapshot` 已从数据库内联 join 改为 `hq / zj` 分开拉取后本地 merge
        - 真实库耗时从 `40.51s` 降到 `4.57s`
      - 新增 `qmt_bar_1d_metrics`
        - 由 `qmt_bar_1d` 在数据库侧聚合为每股一行
        - 真实库耗时从 `27.80s` 降到 `9.26s`
        - 替代 runtime 中原来的 `qmt_bar_1d` 明细拉取
      - 运行时并行策略已做一轮收口
        - 验证结论：`source_load` 层不适合粗粒度并发
        - 原因：远端 PostgreSQL 在多源并发拉取下明显退化，曾出现 `145.47s` 的失败性 profile
      - 当前保留策略：
          - 数据源读取保持串行
          - 仅保留 `qmt_bar_1d_metrics` 与纯 Python 派生/分析阶段的安全并行
- P0.4 Agent 证据交付契约
  - 目标：把 `awin` 从“规则输出 + 文案摘要”推进到“SQLite 证据底座 + agent 分析层”
  - 当前状态：已完成第一阶段
  - 已完成内容：
    - `docs/architecture.md` 已收口为 `artifact / evidence.db / memory promotion` 架构
    - `docs/architecture/data-dictionary.md` 已新增 per-run evidence tables 定义、读取顺序和规模预算
    - SQLite schema 已新增 `run_artifact` 索引表，用于记录每轮产出的 artifact / table
  - 下一步：
    - 真实将 `market_evidence_bundle / stock_evidence_bundle` 落地到 `evidence.db`
    - 新增 `manifest.yaml`
    - 补 `market_brief.md / analyst_output.md / reviewer_output.md` 的标准落地流程

## P0 Must Do

### P0.1 历史资金接入

- 新增 `ts_moneyflow_ths` adapter
  - 目标：把个股历史资金持续性接入 runtime
  - 当前状态：已完成
  - 验收标准：`build_m0_snapshot_bundle` 中可见真实加载，`source_health` 中出现该源
- 新增 `ts_moneyflow_dc` adapter
  - 目标：把个股资金结构接入 runtime
  - 当前状态：已完成
  - 验收标准：可以读取 `net_amount / net_amount_rate / buy_elg_amount / buy_lg_amount` 等字段
- 新增 `ts_moneyflow_cnt_ths` adapter
  - 目标：把概念板块资金流接入主攻线确认层
  - 当前状态：已完成
  - 验收标准：运行结果中可见“主线价格强度 + 板块资金强度”的联合证据
- 新增 `ts_moneyflow_ind_ths` adapter
  - 目标：把行业资金流接入大风格确认层
  - 当前状态：已完成
  - 验收标准：大风格判断能引用行业资金证据
- 新增 `ts_moneyflow_mkt_dc` adapter
  - 目标：把市场级资金流接入环境增强层
  - 当前状态：已完成
  - 验收标准：市场环境输出可引用市场级净流入/大单承接

### P0.2 资金画像层

- 新增 `fund_flow_profile` 派生层
  - 目标：不要把 `ts_moneyflow_*` 原始字段直接散落在多个模块里
  - 当前状态：已完成第一阶段
  - 验收标准：形成统一的个股/概念/行业/市场四层资金画像
- 个股资金画像最小字段集
  - 目标：先补齐最有交易意义的短窗持续性与结构字段
  - 验收标准：至少包含：
    - `main_net_amount_1d`
    - `main_net_amount_3d_sum`
    - `main_net_amount_5d_sum`
    - `main_net_amount_rate_1d`
    - `super_large_net_1d`
    - `large_order_net_1d`
    - `inflow_streak_days`
    - `outflow_streak_days`
    - `flow_acceleration_3d`
    - `price_flow_divergence_flag`
- 概念/行业/市场资金画像最小字段集
  - 目标：支撑主攻线确认和大风格确认
  - 验收标准：至少包含：
    - `theme_net_amount_1d / 3d / 5d`
    - `industry_net_amount_1d / 5d`
    - `market_net_amount_1d`
    - `theme_flow_rank / industry_flow_rank`

### P0.3 风格解释输出升级

- 将新分数字段真正用于 `market_understanding`
  - 目标：从“判出来”升级到“解释清楚为什么”
  - 当前状态：已完成第一阶段，已开始输出主线/市场资金证据
  - 验收标准：输出能回答：
    - 为什么当前是 `科技成长 / 红利价值 / 顺周期资源`
    - 是大票主导还是小票扩散
    - 是高弹性进攻还是低波防御在占优
- 增加风格切换证据
  - 目标：让“切风格”具备可解释性
  - 验收标准：输出能明确说明切换来自：
    - 行业变化
    - 大小盘变化
    - 红利/成长变化
    - 央国企/民企变化
- 增加风格结构摘要
  - 目标：把“市场内部结构”讲清楚
  - 验收标准：至少能区分：
    - 机构容量票主导
    - 小票题材扩散
    - 防御收缩
    - 高弹性进攻

### P0.4 风格因子业务标签化

- 为 `style_profile` 的分数字段增加稳定标签
  - 目标：让分数从工程字段变成业务字段
  - 当前状态：已完成
  - 验收标准：至少产出：
    - `dividend_style`
    - `valuation_style`
    - `growth_style`
    - `quality_style`
    - `volatility_style`
- 收口标签分档规则
  - 目标：避免不同模块自己解释同一分数字段
  - 当前状态：已完成
  - 验收标准：分档逻辑集中在一处配置或规则模块中

### P0.5 候选排序与风险监控升级

- `opportunity_discovery` 接入 `style_profile + fund_flow_profile`
  - 目标：真正让候选排序吃到慢变量风格画像和历史资金确认
  - 当前状态：已完成第一阶段
  - 验收标准：
    - `core_anchor` 偏好容量、质量、持续资金确认
    - `new_long` 能识别刚增强但有主题和资金支持的标的
    - `catchup` 能识别回调后再启动，但避免纯情绪脉冲
- `risk_surveillance` 接入 `style_profile + fund_flow_profile`
  - 目标：让风险识别不只看当轮 DCF 和短期涨幅
  - 当前状态：已完成第一阶段
  - 验收标准：
    - 能识别“价格强但资金连续衰减”
    - 能识别“高弹性 + 高换手 + 高涨幅”的过热组合
    - 能识别“主题还热但个股承接转弱”的 warning
- `stock_snapshot` 扩展
  - 目标：把业务上真正需要 review 的风格/资金画像打平到快照层
  - 当前状态：已完成第一阶段
  - 验收标准：快照中可直接看到核心风格标签和核心资金标签

### P0.6 实盘验收

- 跑通真实 `run_once --dry-run`
  - 目标：不是只靠测试过关，而是真实源全链路跑完
  - 当前状态：已打通一轮真实 dry-run，剩余工作转为结果口径与验收收口
  - 已确认现象：
    - `ts_moneyflow_ths` 优化前约 `33.61s`，优化后约 `7.16s`
    - `ts_moneyflow_dc` 优化前约 `44.25s`，优化后约 `4.22s`
    - 并行压测两者时抬升到 `65.23s / 76.35s`
    - `ts_daily` 优化前约 `123.71s`，优化后约 `45.26s`
    - `ts_adj_factor` 优化前约 `108.35s`，优化后约 `39.57s`
    - `ts_style_daily_metrics` 实测约 `7.94s`
    - `dcf_hq_zj_snapshot` 优化前约 `40.51s`，优化后约 `4.57s`
    - `qmt_bar_1d_metrics` 实测约 `9.26s`
    - 全链路 `build_m0_snapshot_bundle`
      - 优化前：约 `87.32s`
      - 中间阶段：约 `56.78s`
      - 当前真实构建：约 `32.71s`
    - CLI 入口 `scripts/run_once.py --dry-run`
      - 已在 `2026-04-20` 真实跑通
      - 输出包含：
        - `market_understanding`
        - `opportunity_discovery`
        - `risk_surveillance`
        - `alert_output`
    - `profile_m0_runtime.py`
      - 当前 cold profile 约 `44.60s`
      - 口径说明：用于看阶段热点和相对占比，不直接等同于 warm-cache 下的真实构建 wall time
    - source-level 粗并行实验
      - 结果：已否决
      - 原因：18 个数据源按线程池并发后，数据库端出现明显争抢，profile 抬升到 `145.47s`
  - 当前判断：
    - blocker 主要在远端 PostgreSQL 历史资金读取，不在本地 Python 计算
    - 当前最大慢点已不再是单一源，而是多源累积 + 本地构建过程
    - 下一步应从“单源替换”切到“继续压单源 SQL + 业务输出验收”，而不是继续扩大 source-level 并发
  - 验收标准：dry-run 成功输出，且 `source_health` 与预期一致
- 跑通 per-run artifact 最小落地
  - 目标：不止打印 runtime bundle，而是按 run 落 `manifest + evidence.db + digest`
  - 当前状态：设计与 schema 骨架已完成，尚未落真正 artifact
  - 验收标准：
    - 每轮可看到标准目录 `data/runs/YYYY-MM-DD/<run_id>/`
    - `run_artifact` 可索引该轮文件和表
    - agent 可按 `manifest -> evidence.db -> markdown` 固定顺序读取
- 检查 SQLite 落地
  - 目标：验证 `style_profile` 和新增资金画像表/字段真实可用
  - 验收标准：字段非空率、值域、样本分布符合预期
- 核对 V1 业务保留情况
  - 目标：确保 V2 没有业务缩水
  - 验收标准：V1 已有且合理的市场层、主线层、候选层、风险层功能仍可运行，且解释更清晰
- 输出质量收口
  - 目标：把摘要从“字段堆叠”提升到“面向决策的判断链”
  - 当前状态：进行中
  - 验收标准：
    - 首屏能回答“市场能不能做 / 现在在做什么 / 资金是不是真的在做 / 最值得做什么 / 最不该做什么 / 相比上一轮变了什么”
    - 重要结论一律数值化，不再出现无单位或无量纲表达
    - `市场资金（T-1）` 与 `盘中资金（当前）` 分开表达
    - 首选股票同时给出结论、证据、动作路由、风险说明

## P1 Next

### P1.1 复合风格标签升级

- 将 `composite_style_labels` 从“规则命中”升级为“规则 + 因子约束”
  - 目标：同一行业内也能分出不同风格气质
  - 验收标准：风格标签不再只是行业映射结果

### P1.2 多交易日稳定性校验

- 做多交易日抽样验证
  - 目标：确认 T-1 口径、日期对齐和分布稳定性
  - 验收标准：无明显日期错位，关键标签和分数分布稳定
- 增加因子缺失率/覆盖率监控
  - 目标：提前暴露 `fina_indicator`、`moneyflow` 等缺失问题
  - 验收标准：形成定期可 review 的覆盖率结果

### P1.3 输出协议收口

- 统一 `market_understanding / stock_snapshot / alert_output` 字段口径
  - 目标：减少同一含义在多个对象中的重复和歧义
  - 验收标准：字段命名一致，业务解释一致

### P1.4 Agent 化第一阶段

- 定义 evidence bundles
  - 目标：不要让 agent 直接吃原始数据库，也不要只吃最终摘要
  - 验收标准：至少形成
    - `market_evidence_bundle`
    - `theme_evidence_bundle`
    - `stock_evidence_bundle`
    - `portfolio_evidence_bundle`
- 建立 `analyst -> reviewer` 双 Agent 闭环
  - 目标：避免单个 agent 直接给不可挑战的最终判断
  - 验收标准：
    - analyst 负责给出初始判断
    - reviewer 负责找反证、找漏洞、找未解释风险
    - 最终输出保留“结论 + 反证 + 修正后判断”
- 引入交互式 drill-down
  - 目标：让基金经理可以追问“为什么”
  - 验收标准：同一轮结果支持围绕主线、个股、持仓继续追问并返回结构化证据
- 将 replay 扩展到 agent 评估
  - 目标：不只验证规则，也验证 agent 判断质量
  - 验收标准：至少能比较
    - 初始判断
    - reviewer 修正意见
    - 后验表现
    - 误判归因

## P2 Later

### P2.1 风格底座补全

- 接入 `ts_index_classify`
  - 目标：增强行业树稳定性校验
- 接入 `ts_ths_index`
  - 目标：为更标准化的 THS 主题树做准备
- 接入 `ts_ths_member`
  - 目标：为更标准化的 THS 成分映射做准备

### P2.2 资金口径扩展

- 评估是否接入 `ts_moneyflow_ind_dc`
  - 目标：形成行业资金第二口径，用于交叉校验
  - 验收标准：能显式识别 THS 与 DC 行业口径是否一致

### P2.3 Replay / Evaluation 闭环升级

- 将风格因子和资金画像纳入 replay / evaluation
  - 目标：让方法、手段、参数的迭代有正式闭环
  - 验收标准：可以对不同规则与参数进行回放、对比和后验验证

### P2.4 持仓诊断

- 正式落地 `portfolio_diagnosis`
  - 目标：把当前市场/个股框架延伸到持仓池
  - 验收标准：能对持仓给出“继续持有 / 禁加仓 / 减仓观察”建议

## Config Governance

- 规则文件命名去版本化
  - 说明：`configs/ths_concept_overlay.yaml` 这类文件属于“当前生效配置”，不应通过文件名携带 `v1`
  - 原则：
    - 当前生效文件统一使用无版本文件名
    - 版本演进通过 git 历史、变更说明和 replay/evaluation 管理
    - 不再新增新的 `*.v1.yaml` / `*.v2.yaml` 命名
- JSON 规则文件清理
  - 说明：JSON 当前仅为遗留产物，不再作为主配置格式
  - 后续动作：在 YAML 配置稳定后删除 `configs/*.json`

## Done

- 确认项目命名为 `awin`，中文名为 `A视界`
- 确认 V2 以独立项目并行演进，不直接修改 V1 脚本
- 建立项目骨架、SQLite 主存储、M0 contracts、run_once、replay、evaluation 基础能力
- 完成第一轮 V1 / V2 parity 收口
  - 市场层对齐
  - `core_anchor` 对齐
  - `new_long` 对齐
  - `short` 对齐
  - `catchup` 收敛到末位差异
- 完成 `style_profile` 第一版落地
  - 大小盘 / 容量 / 产权 / 行业风格标签独立为慢变量层
  - 已补充业务标签化：
    - `dividend_style`
    - `valuation_style`
    - `growth_style`
    - `quality_style`
    - `volatility_style`
  - 已打通链路：
    - `style_profile -> stock_facts -> stock_snapshot`
- 完成 `style_profile` 的 Tushare 底座接入
  - `ts_stock_basic`
  - `ts_daily_basic`
  - `ts_index_member_all`
  - `ts_daily`
  - `ts_adj_factor`
  - `ts_fina_indicator`
- 完成 `ths_concept_overlay.yaml` 第一轮业务 review 收口
  - 元主题覆盖扩展到 20+ 条
  - 解决重复概念分配与边界混乱问题
- 完成规则文件去版本化第一步
  - `configs/*.v1.yaml` 已重命名为无版本文件名
  - `.env` 已同步更新生效路径
- 完成历史资金排序语义收口
  - 资金流时间序列排序改为应用侧保证，避免依赖数据库 `order by` 才能得到正确末值与窗口和
  - 全量单元测试 `55` 项通过
- 完成一轮 runtime 并行策略复盘
  - 否决了 `source_load` 粗并行方案
  - 保留 `derived_build / analysis_build / decision_build` 的安全并行
  - 最新真实 `build_m0_snapshot_bundle` 实测约 `32.71s`
