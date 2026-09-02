# Context Index (项目上下文索引)

Eruun 的上下文入口，帮助快速定位关键信息。

---

## Core Concepts (核心概念)

| 概念 | 说明 |
|------|------|
| **Trait** | 组件能力的可组合特性（存储/环境变量/探针/sidecar） |
| **Workflow** | 任务编排与执行流（Job/TaskSpec/状态机） |
| **Operator/Controller** | 资源生成与生命周期管理 |
| **DSL/Spec** | 声明式描述（JSON/YAML），驱动部署与运行 |

---

## Project Layout (目录结构)

| 目录 | 职责 |
|------|------|
| `cmd/main.go` | API Server 入口 |
| `pkg/apiserver/interfaces/api` | HTTP 路由、DTO 与 assembler |
| `pkg/apiserver/domain` | 模型、仓储契约和业务服务 |
| `pkg/apiserver/event/workflow` | 调度、执行与 Kubernetes Job |
| `pkg/apiserver/infrastructure` | MySQL、Redis、Kafka、Kubernetes 与观测适配 |
| `pkg/apiserver/workflow` | Traits 与资源命名 |
| `config/` | 默认配置 |
| `docs/` | 设计与使用文档 |
| `devlogs/` | 重要代码变更的技术选型与取舍记录 |
| `examples/` | API 请求示例 |

---

## Single Source of Truth (单一事实源)

| 类型 | 位置 |
|------|------|
| Constants | `pkg/apiserver/config/consts.go` |
| Spec/Schema | `pkg/apiserver/domain/spec`, `docs/` Current 文档 |
| Decision Records | `devlogs/` |
| Errors | `pkg/apiserver/utils/bcode`, `docs/api-error-response-contract.md` |

---

## Common Commands (常用命令)

```bash
# 单测
go test ./...

# 格式化
gofmt -w .

# 静态检查
go vet ./...
golangci-lint run
```

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
| devlog | 每 PR/主题一篇的技术决策记录，用于说明重要代码变更为什么这样实现 |
