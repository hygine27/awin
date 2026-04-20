# Source Canonical Schema

## 1. Status

这份文档已从“主数据说明文档”收口为“canonical 约束补充说明”。

自 `data-dictionary.md` 建立后，数据源目录、关键字段、下游消费者和字段血缘统一以
[`data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)
为主入口。

这份文档只保留两类内容：

1. adapter 层统一字段口径
2. row contract / source health 这类实现约束

## 2. What Lives Here

### A. Symbol Convention

- `symbol`
  - 保留完整后缀
  - 例如 `300570.SZ`
- `stock_code`
  - 保留无后缀 6 位代码
  - 例如 `300570`

### B. Time Convention

- `trade_date`
  - `YYYY-MM-DD`
- `snapshot_time`
  - `HH:MM:SS`
- `analysis_snapshot_ts`
  - `YYYY-MM-DDTHH:MM:SS`
- `vendor_batch_ts`
  - 保留上游原始批次时间

### C. Numeric Convention

- 比例字段统一用小数
- 金额字段统一用原始数值
- 不保留带 `%` 的字符串表示

### D. Source Health Convention

每个 adapter 当前都应能给出最小健康信息：

- `source_name`
- `source_status`
- `freshness_seconds`
- `coverage_ratio`
- `fallback_used`
- `detail`

## 3. Canonical Row Contracts

当前 canonical row contract 的代码真相源为：

- [`contracts.py`](/home/yh/.openclaw/workspace/projects/awin/src/awin/adapters/contracts.py)

其中定义了当前 adapter 层统一输出的最小对象：

- `StockMasterRow`
- `QmtSnapshotRow`
- `QmtBar1dRow`
- `DcfSnapshotRow`
- `ThsConceptRow`
- `ThsHotConceptRow`
- `ResearchCoverageRow`
- `SourceHealth`

## 4. Relation With Storage Schema

canonical row contract 不等于最终落库结构。

当前 SQLite 落库结构真相源为：

- [`schema.py`](/home/yh/.openclaw/workspace/projects/awin/src/awin/storage/schema.py)

理解顺序应为：

```text
source adapter row contract
        |
        v
runtime derived objects
        |
        v
sqlite persistence schema
```

## 5. What Does Not Live Here

以下内容不再在本文件维护：

- 当前接了哪些数据源
- 各数据源读取逻辑和下游消费者
- `style_profile / fund_flow_profile / stock_facts` 字段字典
- 关键业务字段血缘

这些内容统一迁移到：

- [`data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)

## 6. Maintenance Rule

如果后续新增 adapter 或调整 canonical row contract：

1. 先改代码中的 contract
2. 再回写本文件中的约束描述
3. 如果涉及新的 source registry 或字段用途变化，同时回写 `data-dictionary.md`
