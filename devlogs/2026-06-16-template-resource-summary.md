# Template Resource Summary

> Superseded note：本文记录 2026-06-16 当时的模板资源摘要决策。`ApplicationBase` 顶层资源字段已在 `devlogs/2026-06-22-application-resources-response.md` 调整为 `resources` 对象；当前 API shape 以 `docs/template-engine-status.md` 和 DTO 代码为准。

## 背景与需求

模板发现接口 `GET /api/v1/applications/templates` 原本只返回应用元信息。调用方需要在模板卡片或选择器中直接展示资源建议，因此列表项需要带上主组件的 CPU、内存和副本数摘要。

## 影响范围

- API: `ApplicationBase` 增加 `cpuReq`、`memReq`、`cpuLimit`、`memLimit`、`replicas` 字段，模板列表会填充这些字段；`traits.resources` 增加 `cpuLimit`、`memoryLimit`。
- Domain: 模板列表查询在分页应用列表后批量读取组件，用主组件 `traits.resources` 生成摘要；资源 trait 将 `cpu` / `memory` 作为 requests，`cpuLimit` / `memoryLimit` 作为 limits。
- DB: 无表结构变化。
- Cache: 模板列表缓存 key 从 `app:template:list:v2` 升级为 `app:template:list:v3`，避免旧缓存缺少新增字段。
- K8s: 生成 Pod 资源时可表达独立 requests/limits；旧配置仍生成 request 与 limit 同值。
- Workflow: 无变化。

## 技术选型与取舍

采用主组件推导，不新增模板实体。主组件按组件 `ID` 升序、`name` 升序选取第一个可部署 workload 组件，并跳过 `config`、`secret`、`cloudjob` 等辅助或非 Pod workload 组件。

资源 trait 保持旧字段兼容：`cpu` / `memory` 继续可独立工作，并在 limit 字段为空时作为 limits 的 fallback；新字段 `cpuLimit` / `memoryLimit` 只在需要 request/limit 分离时传入。没有从 `properties.env.temPodResource` 解析旧字符串 JSON。

## 实现摘要

- `ApplicationBase` 顶层新增资源摘要字段。
- `ListTemplateApplications` 在已有 workflow enrichment 后批量读取本页模板组件并填充资源摘要。
- `ResourceTraitsSpec` 新增 `cpuLimit` / `memoryLimit`，资源处理器、校验、K8s YAML 转换、模板 override、组件 resourceConfigs 同步支持。
- 测试内存 store 增加 `app_id IN` 过滤支持，以覆盖生产 SQL 批量查询路径。
- `docs/template-engine-status.md` 更新当前模板列表响应契约。

## 测试与验收

已执行：

- `GOCACHE=/private/tmp/eruun-go-cache go test ./pkg/apiserver/domain/service/application ./pkg/apiserver/interfaces/api`
- `GOCACHE=/private/tmp/eruun-go-cache go test ./pkg/apiserver/...`

验收口径：

- 模板列表响应包含 `cpuReq`、`memReq`、`cpuLimit`、`memLimit`、`replicas`。
- 有 config/secret 辅助组件时，资源摘要来自第一个可部署主组件。
- 多个 workload 组件时选择顺序稳定。
- 没有 workload 或没有 `traits.resources` 时返回字段零值，不报错。
- 旧 `traits.resources.cpu/memory` 继续同时生成 request 和 limit；显式 `cpuLimit/memoryLimit` 会覆盖 limit。

## 风险与后续

- `ApplicationBase` 被多个响应复用，非模板路径也会序列化新增字段的零值；当前保持结构复用以避免新增重复 DTO。
- `gpu` 仍沿用 request/limit 同值行为；如果未来要细分 GPU limits，需要单独评估 Kubernetes 设备资源约束。
