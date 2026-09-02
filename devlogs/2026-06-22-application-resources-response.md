# Application Resources Response Shape

## 背景与需求

`ApplicationBase` 在模板资源摘要改造后把 `cpuReq`、`memReq`、`cpuLimit`、`memLimit`、`replicas` 直接序列化在应用顶层。普通创建响应没有资源摘要来源，也会输出这些空值，导致应用基础字段和资源摘要混在一起。

## 影响范围

- API: `ApplicationBase` 将资源摘要移动到 `resources` 对象内，旧顶层资源字段不再输出。
- Domain: 创建响应和模板列表都按主组件推导资源摘要，并写入 `ApplicationBase.resources`。
- DB: 无表结构变化。
- Cache: 应用列表缓存 key 升级为 `app:list:v3`，模板列表缓存 key 升级为 `app:template:list:v4`，避免旧 flat JSON 被新 DTO 反序列化后丢失资源摘要。
- K8s: 无变化。
- Workflow: 无变化。

## 技术选型与取舍

采用一个嵌套 `ApplicationResources` DTO，不拆出新的应用响应实体。这样所有复用 `ApplicationBase` 的接口保持一致，代价是这是一次 breaking response shape change。

没有保留 legacy 顶层字段，避免同一语义在响应中出现两份来源。

## 实现摘要

- `ApplicationBase` 新增 `resources` 字段，承载资源 request、limit 和 replicas。
- 创建响应从本次解析后的组件列表推导 `resources`，模板列表从已落库组件推导同一结构。
- 应用列表和模板列表缓存 key 升级。
- 更新 API/domain 测试、当前行为文档和 create-and-exec 示例响应。

## 测试与验收

验收口径：

- `ApplicationBase` JSON 总是包含 `resources` 对象。
- `cpuReq`、`memReq`、`cpuLimit`、`memLimit`、`replicas` 不再出现在应用对象顶层。
- 创建响应会在主组件有 `traits.resources` 时返回资源摘要。
- 模板列表资源摘要仍来自主组件，并保留零值行为。

## 风险与后续

这是公开 API 响应形状调整，依赖旧顶层字段的客户端需要迁移到 `resources.*`。
