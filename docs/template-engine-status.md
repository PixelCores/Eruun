# Template Status

> 状态：Current。本文说明当前模板能力边界，避免引入不存在的独立模板引擎抽象。

## 当前状态

当前代码库没有独立的 `pkg/apiserver/utils/template` 模板引擎包，也没有单独的模板引擎扩展抽象文件。

模板相关能力主要体现在两类代码中：

- 应用模板克隆链路：`pkg/apiserver/domain/service/application_template_clone.go`
- 模板模型定义：`pkg/apiserver/domain/model/template.go`

其中，对外创建应用请求中的 `component[].tmp` 会被解析为 `TemplateRef`，进入 `CreateApplications` 后由 `resolveComponents` 与 `cloneComponentsFromTemplate` 展开为实际组件创建请求。

## 术语边界

- `component[].tmp.id` 是创建应用请求中的模板引用，指向一个已启用模板的应用。
- `templateEnabled` 是 API 字段，映射到 DB 列 `tmp_enable`，表示某个应用是否允许被其他请求作为模板引用。
- `tmpCreate` 是 storage trait 字段，用于控制持久化存储走 StatefulSet `volumeClaimTemplates` 风格还是 standalone PVC；它不是模板应用发现 API，也不表示 `templateEnabled`。

## 对外接口范围

- `POST /applications` 支持通过 `component[].tmp.id` 引用已启用模板的应用，并按模板组件克隆生成新应用组件。创建响应的 `resources` 会从本次解析后的第一个主组件 `traits.resources` 推导。
- `GET /applications/templates` 返回 `templateEnabled=true` 的应用列表，供客户端发现可引用模板。每个模板对象会在 `resources` 内带上主组件资源摘要：`cpuReq`、`memReq`、`cpuLimit`、`memLimit`、`replicas`。主组件按模板组件的 `ID` 升序、`name` 升序选择第一个可部署 workload 组件（`webservice`、`store`、`job`、`scheduledjob`），跳过 `config`、`secret`、`cloudjob` 等辅助或非 Pod workload 组件。
- `traits.resources.cpu` / `traits.resources.memory` 表示 Kubernetes Requests；`traits.resources.cpuLimit` / `traits.resources.memoryLimit` 表示 Limits。为兼容旧模板，limit 字段为空时会回退到对应 request 值，因此旧的 `cpu` / `memory` 配置仍会同时生成 request 和 limit；显式配置 limit 时必须大于等于对应 request。
- `POST /applications/import/namespace` 与 `POST /applications/import/namespace/try` 走 `NamespaceImportService` 路径，不依赖独立模板引擎。

## 维护约定

当前不新增独立模板引擎抽象。后续维护应优先保持现有应用模板克隆链路和模型定义清晰，避免在没有接线需求时增加预留实体。

若未来确实需要平台化模板渲染或扩展机制，应先明确对外 API、领域契约、存储模型和调用路径，再引入新的包或接口。
