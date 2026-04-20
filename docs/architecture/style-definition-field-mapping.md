# 风格定义方法说明

## 1. Status

这份文档已从“风格字段总字典”收口为“`style_profile` 方法说明”。

自
[`data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)
建立后，以下内容不再在本文件完整重复：

- 真实数据源目录
- active 字段清单
- 运行时下游消费者
- 全局字段血缘

这份文档只保留一件事：

- 解释 `style_profile` 为什么这样定义，以及各类风格因子背后的业务方法

## 2. Role of `style_profile`

`style_profile` 是慢变量画像层。

它回答的问题不是：

- 今天资金最热打什么

而是：

- 这只股票本身更像什么风格气质
- 它在容量、估值、红利、成长、质量、波动这些维度上处于什么位置

因此它主要服务于：

- 风格解释
- 候选排序校准
- 风险识别增强

而不是直接替代盘中主线判断。

## 3. Method Layers

`style_profile` 当前方法上分 4 层：

### A. 静态身份层

主要回答：

- 市场层次是什么
- 交易所是什么
- 央国企还是民企
- 当前申万行业归属是什么

这一层解决“股票是谁”的问题。

### B. 容量与估值层

主要回答：

- 这只股票是超大盘、大盘、中盘、小盘还是微盘
- 是机构容量票还是中小票弹性票
- 是更偏红利价值还是更偏高估值成长

这一层解决“股票能不能被大资金承接、估值气质偏哪边”的问题。

### C. 质量与弹性层

主要回答：

- 是高质量成长还是低质量高波动
- 是低波防御还是高弹性进攻

这一层解决“股票本身的基本面气质和波动气质是什么”的问题。

### D. 综合风格标签层

主要回答：

- 这只股票更接近 `科技成长`、`红利价值`、`顺周期资源`，还是 `小盘题材`

这一层不是简单行业映射，而是规则组合结果。

## 4. Method Principles

### A. 不把行业直接当风格

行业只是风格判断的一部分，不是全部。

例如：

- 同样是电子或计算机，可能既有机构容量核心票，也有小盘高弹性题材票
- 同样是电力链，可能既有红利防御，也有高弹性景气票

因此综合风格标签必须同时看：

- 行业
- 大小盘 / 容量
- 红利 / 成长
- 质量 / 波动

### B. 因子先打分，再转业务标签

当前逻辑不是直接写死：

- 高成长 = 某个字段大于多少

而是：

1. 先对原始指标做横截面打分
2. 再把分数映射为业务可读标签

这样做的目的，是把：

- 工程数值
- 业务标签

这两层明确拆开。

### C. 综合风格标签是规则组合，不是单因子胜负

例如 `科技成长` 不是只看：

- 行业 = 半导体 / 软件

还会叠加：

- 高成长
- 中高估值
- 中高弹性
- 容量与市场层次特征

因此 `composite_style_labels` 的定位是“风格气质集合”，不是唯一分类。

## 5. Current Active Factor Families

当前 active 的核心风格因子家族有：

- 红利价值
  - `dividend_value_score`
- 估值成长
  - `growth_valuation_score`
- 质量成长
  - `quality_growth_score`
- 收入成长
  - `sales_growth_score`
- 利润成长
  - `profit_growth_score`
- 低波防御
  - `low_vol_defensive_score`
- 高弹性进攻
  - `high_beta_attack_score`

对应的业务标签有：

- `dividend_style`
- `valuation_style`
- `growth_style`
- `quality_style`
- `volatility_style`

active 字段名和来源表请直接看：

- [`data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)
- [`style_profile/engine.py`](/home/yh/.openclaw/workspace/projects/awin/src/awin/style_profile/engine.py)
- [`style_profile_rules.yaml`](/home/yh/.openclaw/workspace/projects/awin/configs/style_profile_rules.yaml)

## 6. Design Caveats

这里保留方法层面的几个重要边界，避免后续把 `style_profile` 用歪。

### A. `style_profile` 不是主线引擎

盘中主线、概念加速、热点扩散仍然主要由：

- QMT 盘中快照
- DCF 盘中增强
- THS 热概念 overlay

来解决。

`style_profile` 主要是：

- 解释增强
- 排序校准
- 风险校准

### B. `style_profile` 不等于基本面模型

它吸收了财务质量、成长和波动信息，但并不试图成为完整的财务选股框架。

当前目标仍然是：

- 给盘中与跨日判断提供慢变量背景

而不是独立输出长期选股结论。

### C. 综合风格标签允许多标签并存

一只股票可能同时命中多个风格标签。

例如：

- `科技成长`
- `高弹性进攻`
- `小盘题材`

这不是错误，而是为了表达股票在多个维度上的风格气质。

## 7. Source of Truth

方法层真相源：

- [`style_profile/engine.py`](/home/yh/.openclaw/workspace/projects/awin/src/awin/style_profile/engine.py)
- [`style_profile_rules.yaml`](/home/yh/.openclaw/workspace/projects/awin/configs/style_profile_rules.yaml)
- [`market_style_baskets.yaml`](/home/yh/.openclaw/workspace/projects/awin/configs/market_style_baskets.yaml)

字段与来源真相源：

- [`data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)

## 8. Maintenance Rule

如果后续调整 `style_profile`：

1. 因子、标签、规则本身的业务方法变化，更新本文件
2. active 字段名、来源、下游用途变化，更新 `data-dictionary.md`
