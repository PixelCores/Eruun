# Context Index (项目上下文索引)

仅在需要了解 Eruun 基本概念时阅读。当前目录、模块所有权与实现事实以目标仓库的 `docs/README.md`、相关代码和 Current 文档为准。

---

## Core Concepts (核心概念)

| 概念 | 说明 |
|------|------|
| **Trait** | 组件能力的可组合特性（存储/环境变量/探针/sidecar） |
| **Workflow** | 任务编排与执行流（Job/TaskSpec/状态机） |
| **Operator/Controller** | 资源生成与生命周期管理 |
| **DSL/Spec** | 声明式描述（JSON/YAML），驱动部署与运行 |

---

## Authoritative References (事实入口)

以下路径均相对于目标仓库根目录：

- `AGENTS.md`：项目约束、验证命令及其适用范围。
- `docs/README.md`：需求定位、目录职责、当前能力和专题文档状态。
- 相关代码、测试与 Current 专题文档：本次改动的行为与契约证据。
- `devlogs/README.md` 与相关决策记录：需要追溯非显然技术取舍时阅读；历史记录不能覆盖当前实现。

---

## Quick Links (快速链接)

- [SKILL 规范](../SKILL.md) - 开发规则与交付流程
- [Context Packet](context-packet.md) - 提需求模板
- `devlogs/README.md` - 技术决策记录索引与模板（仓库内）

---

## Glossary (术语)

| 术语 | 含义 |
|------|------|
| error contract | 错误契约（错误类型/码及调用方依赖方式） |
| operational usage | 运维使用方式（部署/升级/迁移/运行参数） |
| devlog | 仅为持久且非显然的技术决策记录原因与取舍；普通修复无需创建 |
