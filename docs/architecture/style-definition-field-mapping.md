# 风格定义字段映射

## 1. 目标

这份文档回答 3 个问题：

1. V2 要定义哪些“真正有业务意义”的风格维度
2. 每个维度基于哪些 `qt.stg.ts_*` 表和字段
3. 每个维度如何派生，最终落到哪个结构层

这里的“风格定义”指的是慢变量底座，不等同于盘中主线或主攻线。

主攻线、概念加速、板块热度仍然由 THS 概念和盘中行情层解决。

## 2. 当前可用底座

当前确认可接入 V2 的 Tushare 同步表：

- `stg.ts_stock_basic`
- `stg.ts_daily_basic`
- `stg.ts_daily`
- `stg.ts_adj_factor`
- `stg.ts_fina_indicator`
- `stg.ts_index_classify`
- `stg.ts_index_member_all`
- `stg.ts_ths_index`
- `stg.ts_ths_member`

其中：

- `ts_index_member_all` 不是标准 `index_code / con_code` 结构，而是行业链式字段：
  - `l1_code`, `l1_name`
  - `l2_code`, `l2_name`
  - `l3_code`, `l3_name`
  - `ts_code`
- `ts_ths_member` 在 replay 中先按“当前映射”使用，不强求精确历史时点还原。

## 3. 风格定义分层

V2 的风格定义建议分为 4 层：

1. 静态身份层
   - 市场层次
   - 交易所
   - 央国企 / 民企
   - 申万行业归属
2. 容量与估值层
   - 大盘 / 中盘 / 小盘 / 微盘
   - 红利价值
   - 高估值成长
3. 质量与弹性层
   - 质量成长
   - 低波防御
   - 高弹性进攻
4. 主题补充层
   - THS 概念
   - 元主题
   - 主线解释标签

最终不是只给股票贴 1 个风格，而是给出一组 `style_profile`。

## 4. 字段映射总表

| 风格维度 | 来源表 | 关键字段 | 派生方法 | 建议输出 |
|---|---|---|---|---|
| 市场层次 | `ts_stock_basic` | `market`, `exchange` | 直接映射主板 / 创业板 / 科创板 / 北交所 | `market_type_label` |
| 交易所 | `ts_stock_basic` | `exchange` | 直接映射 SSE / SZSE / BSE | `exchange_label` |
| 央国企 / 民企 | `ts_stock_basic` | `act_ent_type`, `act_name` | 先规则映射为 `soe / private / mixed / unknown` | `ownership_style` |
| 基础行业 | `ts_stock_basic` | `industry` | 当前粗分类兜底 | `legacy_industry_label` |
| 申万一级行业 | `ts_index_member_all` | `ts_code`, `l1_code`, `l1_name` | 按 `ts_code` 映射当前有效一级行业 | `sw_l1_name` |
| 申万二级行业 | `ts_index_member_all` | `ts_code`, `l2_code`, `l2_name` | 按 `ts_code` 映射当前有效二级行业 | `sw_l2_name` |
| 申万三级行业 | `ts_index_member_all` | `ts_code`, `l3_code`, `l3_name` | 按 `ts_code` 映射当前有效三级行业 | `sw_l3_name` |
| 行业体系元数据 | `ts_index_classify` | `index_code`, `industry_name`, `level`, `parent_code` | 用于校验行业树结构和层级完整性 | `industry_tree_ref` |
| 自由流通盘 | `ts_daily_basic` | `free_share`, `circ_mv`, `trade_date` | 取最新交易日快照 | `free_float_share`, `float_mv` |
| 总市值 | `ts_daily_basic` | `total_mv`, `trade_date` | 取最新交易日快照 | `total_mv` |
| 大中小盘分桶 | `ts_daily_basic` | `circ_mv`, `free_share` | 横截面分位或固定阈值分桶 | `size_bucket` |
| 容量票标签 | `ts_daily_basic` + `ts_daily` | `circ_mv`, `amount` | 结合流通市值和近 20 日成交额做机构容量分层 | `capacity_bucket` |
| 红利价值 | `ts_daily_basic` | `dv_ttm`, `dv_ratio`, `pe_ttm`, `pb`, `ps_ttm` | 高股息 + 中低估值联合打分 | `dividend_value_score` |
| 高估值成长 | `ts_daily_basic` + `ts_fina_indicator` | `pe_ttm`, `pb`, `ps_ttm`, `tr_yoy`, `or_yoy`, `netprofit_yoy` | 高估值不单独成风格，需配合成长性 | `growth_valuation_score` |
| 质量成长 | `ts_fina_indicator` | `roe`, `roe_yearly`, `roic`, `debt_to_assets`, `ocf_to_or` | ROE / ROIC / 杠杆 / 现金流综合评分 | `quality_growth_score` |
| 收入成长 | `ts_fina_indicator` | `tr_yoy`, `or_yoy`, `q_sales_yoy` | 关注收入扩张而非只看利润 | `sales_growth_score` |
| 利润成长 | `ts_fina_indicator` | `netprofit_yoy`, `dt_netprofit_yoy` | 看利润扩张和扣非质量 | `profit_growth_score` |
| 复权收益 | `ts_daily` + `ts_adj_factor` | `close`, `trade_date`, `adj_factor` | 生成复权收盘价序列 | `adj_close` |
| 中期动量 | `ts_daily` + `ts_adj_factor` | 同上 | 计算 20 / 60 / 120 日收益 | `ret_20d`, `ret_60d`, `ret_120d` |
| 波动率 | `ts_daily` + `ts_adj_factor` | 同上 | 计算 20 / 60 日收益波动率 | `vol_20d`, `vol_60d` |
| 低波防御 | `ts_daily` + `ts_adj_factor` + `ts_daily_basic` | 波动率, 回撤, 股息率 | 低波 + 稳定 + 分红 | `low_vol_defensive_score` |
| 高弹性进攻 | `ts_daily` + `ts_adj_factor` + `ts_daily_basic` | 波动率, 振幅替代, 小市值 | 高波动 + 小中盘 + 高换手 | `high_beta_attack_score` |
| THS 概念映射 | `ts_ths_member` | `ts_code`, `con_code`, `in_date`, `out_date` | 当前时点按有效成分关系映射概念 | `ths_concepts` |
| THS 概念主表 | `ts_ths_index` | `ts_code`, `name`, `type` | 概念代码到概念名称映射 | `ths_concept_name` |
| 主题元标签 | `ts_ths_index` + `ts_ths_member` + 本地 overlay | 概念名称 | 将 THS 概念再聚合为业务元主题 | `meta_themes` |

## 5. 建议派生的核心风格标签

下面这些不是直接取字段，而是 V2 应该明确产出的风格标签。

### 5.1 静态标签

- `market_type_label`
  - 主板
  - 创业板
  - 科创板
  - 北交所

- `ownership_style`
  - 央国企
  - 民企
  - 混合属性
  - 未识别

- `industry_primary_label`
  - 优先使用申万一级行业
  - 缺失时回退到 `ts_stock_basic.industry`

### 5.2 慢变量风格标签

- `size_bucket`
  - 超大盘
  - 大盘
  - 中盘
  - 小盘
  - 微盘

- `capacity_bucket`
  - 机构核心容量
  - 机构可做容量
  - 中小票
  - 微盘弹性

- `dividend_style`
  - 红利核心
  - 红利次优
  - 中性
  - 低股息

- `valuation_style`
  - 低估值
  - 中估值
  - 高估值

- `growth_style`
  - 高成长
  - 中成长
  - 低成长

- `quality_style`
  - 高质量
  - 中质量
  - 低质量

- `volatility_style`
  - 低波
  - 中波
  - 高弹性

### 5.3 综合风格标签

综合标签建议通过规则组合，而不是单字段硬编码。

候选综合标签：

- `科技成长`
  - 创业板 / 科创板优先
  - 高成长
  - 中高估值
  - 行业偏半导体 / 软件 / 通信 / IT

- `红利价值`
  - 红利高
  - 低估值
  - 低波
  - 行业偏银行 / 电力 / 煤炭 / 石油 / 港口 / 电信运营

- `顺周期资源`
  - 行业偏有色 / 钢铁 / 化工 / 建材 / 工程机械 / 煤炭 / 石油
  - 可叠加中低估值

- `消费防御`
  - 行业偏食品饮料 / 家电 / 商贸 / 旅游酒店
  - 低波和稳定现金流优先

- `医药防御`
  - 行业偏化学制药 / 中成药 / 生物制药 / 医疗保健
  - 质量优先于估值

- `金融地产链`
  - 行业偏银行 / 券商 / 保险 / 地产 / 建筑建材

- `小盘题材`
  - 小盘 / 微盘
  - 高弹性
  - THS 主题集中
  - 这个标签主要是风险偏好和情绪风格，不应只靠静态字段定义

## 6. 具体派生建议

### 6.1 大小盘分桶

建议优先用 `circ_mv`，按全市场横截面分位切分：

- top 10%: `mega_cap`
- 75%~90%: `large_cap`
- 40%~75%: `mid_cap`
- 10%~40%: `small_cap`
- bottom 10%: `micro_cap`

也可以保留一套固定阈值口径供业务解释：

- `>= 1000e8`
- `300e8 ~ 1000e8`
- `100e8 ~ 300e8`
- `30e8 ~ 100e8`
- `< 30e8`

V2 最好同时保留：

- `size_bucket_pct`
- `size_bucket_abs`

### 6.2 红利价值评分

建议用以下字段组合：

- `dv_ttm`
- `pe_ttm`
- `pb`
- `ps_ttm`

基本思路：

- `dv_ttm` 越高越加分
- `pe_ttm / pb / ps_ttm` 越低越加分
- 金融股与周期股要允许估值口径差异，不建议只靠单一 PE

### 6.3 质量成长评分

建议组合：

- `roe_yearly`
- `roic`
- `debt_to_assets`
- `ocf_to_or`
- `tr_yoy`
- `or_yoy`
- `netprofit_yoy`
- `dt_netprofit_yoy`

原则：

- 质量和成长要拆开算
- 不要把“高增长低质量”误当成高质量成长

### 6.4 低波 / 高弹性

建议基于复权价格序列计算：

- 20 日波动率
- 60 日波动率
- 20 / 60 日最大回撤
- 20 / 60 日涨跌幅

然后再和：

- `circ_mv`
- `turnover_rate_f`
- `dv_ttm`

一起组合。

低波防御：

- 波动低
- 回撤小
- 分红高或现金流稳

高弹性进攻：

- 波动高
- 市值偏小
- 换手高
- 主题集中

## 7. 建议落表位置

### 7.1 第一阶段

先不急着新建很多表，建议在统一派生层输出一张 `style_profile` 宽表或宽视图。

建议字段包括：

- `ts_code`
- `trade_date`
- `market_type_label`
- `exchange_label`
- `ownership_style`
- `legacy_industry_label`
- `sw_l1_name`
- `sw_l2_name`
- `sw_l3_name`
- `float_mv`
- `total_mv`
- `size_bucket_pct`
- `size_bucket_abs`
- `capacity_bucket`
- `dividend_value_score`
- `growth_valuation_score`
- `quality_growth_score`
- `sales_growth_score`
- `profit_growth_score`
- `low_vol_defensive_score`
- `high_beta_attack_score`
- `composite_style_labels_json`

### 7.2 与现有 `stock_snapshot` 的关系

建议不要把全部慢变量字段塞进当前 `stock_snapshot` 主表。

更合理的方式：

- `style_profile`
  负责跨日、慢变量、基础风格画像
- `stock_snapshot`
  负责盘中状态、当轮信号、主线归属和风险状态

然后在盘中引擎里按 `ts_code + trade_date` 或最近可用交易日把两者 join。

## 8. 当前实现优先级

建议按下面顺序落地：

1. 静态身份层
   - `market_type_label`
   - `ownership_style`
   - `sw_l1/l2/l3`
2. 容量与估值层
   - `size_bucket`
   - `capacity_bucket`
   - `dividend_value_score`
3. 质量与成长层
   - `quality_growth_score`
   - `sales_growth_score`
   - `profit_growth_score`
4. 波动与弹性层
   - `low_vol_defensive_score`
   - `high_beta_attack_score`
5. 综合风格标签
   - `科技成长`
   - `红利价值`
   - `顺周期资源`
   - `消费防御`
   - `医药防御`
   - `金融地产链`

## 9. 当前不建议过度设计的点

第一阶段先不要做：

- 只靠单个字段直接硬判综合风格
- 把 THS 概念直接当成慢变量大风格
- 把所有派生结果都塞进主快照表
- 在没有分钟级指数和分钟级股票底座前，过早定义“盘中风格回切”

先把慢变量风格底座搭实，再叠加盘中状态。
