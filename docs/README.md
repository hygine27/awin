# Docs README

`docs/` 目录的目标不是堆很多讨论稿，而是把 `awin` 当前有效文档收口成少数几个可长期维护的入口。

## Reading Order

建议按下面顺序阅读：

1. 项目总览
   - [`overview.md`](/home/yh/.openclaw/workspace/projects/awin/docs/overview.md)
   - 说明项目定位、边界、目标用户和第一阶段目标
2. 系统架构
   - [`architecture.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture.md)
   - 说明模块关系、系统分层、项目结构，以及“确定性数据面 + Agent 判断面”的分工
3. 数据主字典
   - [`architecture/data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)
   - 这是当前“数据从哪来、怎么变形、关键字段怎么被用”的主入口
4. 输出协议
   - [`tracking/m0-output-contracts.md`](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/m0-output-contracts.md)
   - 说明 `M0` 最终必须产出的对象和字段
5. 手工运行与联调
   - [`manual.md`](/home/yh/.openclaw/workspace/projects/awin/docs/manual.md)
   - 说明当前如何手工运行、回放、评估和排查

## Directory Split

### `architecture/`

放稳定设计说明，重点回答“系统是什么、数据如何组织、方法如何定义”。

当前主要文件：

- [`architecture.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture.md)
- [`architecture/data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)
- [`architecture/style-definition-field-mapping.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/style-definition-field-mapping.md)

边界：

- `data-dictionary.md` 是数据主字典
- `style-definition-field-mapping.md` 只保留 `style_profile` 方法说明，不再重复数据字典

### `tracking/`

放实施跟踪、阶段验收和输出协议，不承担主设计说明职责。

当前主要文件：

- [`tracking/action-list.md`](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/action-list.md)
- [`tracking/m0-output-contracts.md`](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/m0-output-contracts.md)
- [`tracking/source-canonical-schema.md`](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/source-canonical-schema.md)
- [`tracking/m0-parity-acceptance-status.md`](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/m0-parity-acceptance-status.md)

边界：

- `action-list.md` 只跟踪动作和状态
- `m0-output-contracts.md` 只定义输出协议
- `source-canonical-schema.md` 只保留 adapter canonical 约束

## Maintenance Rules

为了尽量保持 MECE，后续维护遵循下面原则：

1. 数据源、字段、派生指标、画像、关键血缘，统一更新到
   [`architecture/data-dictionary.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/data-dictionary.md)
2. `style_profile` 的方法和业务定义变化，更新到
   [`architecture/style-definition-field-mapping.md`](/home/yh/.openclaw/workspace/projects/awin/docs/architecture/style-definition-field-mapping.md)
3. 输出对象字段变化，更新到
   [`tracking/m0-output-contracts.md`](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/m0-output-contracts.md)
4. 实施进度和验收结论，更新到
   [`tracking/action-list.md`](/home/yh/.openclaw/workspace/projects/awin/docs/tracking/action-list.md)
