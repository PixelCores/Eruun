# Programming Language CRUD

## 背景与需求


## 影响范围

- API: 新增 `/api/v1/programming-languages` CRUD。
- Domain: 新增独立编程语言服务与仓储。
- DB: 新增 `eruun_programming_languages`，通过现有 AutoMigrate 创建。
- Cache: 不新增缓存。
- K8s: 不生成或校验任何运行时资源，仅复用 Kubernetes quantity 格式校验。
- Workflow: 不参与 Workflow 创建、更新、执行或校验。

## 技术选型与取舍

本 PR 使用独立实体，原因是编程语言是管理员维护的资源配置，不等同于模板应用或系统设置 JSON。独立表可以提供单条 CRUD、`code + version` 唯一性和明确错误码。

未复用 `/settings`，因为 settings 当前按 type 存整块 JSON，缺少单条记录级别的 CRUD 语义。未复用模板应用，是因为本需求不需要生成应用模板、镜像或 Workflow 步骤。

## 实现摘要

新增 `ProgrammingLanguage` 模型，字段包括 `id`、`code`、`name`、`version`、`enabled`、`cpuReq`、`memReq` 和时间戳。`code` 由后端根据 `name` 做机械 slug/符号编码并只在响应中输出，不维护语言别名归并规则；符号使用十六进制 token 保留区分度，避免 `C`、`C#`、`C++` 生成同一个 `code`。创建后不可修改，`code + version` 通过唯一索引和服务层冲突检查共同保证唯一。API 使用 strict JSON，请求如果传入不允许的 `code` 会被拒绝。

## 测试与验收

需要覆盖创建、列表、查询、更新、删除、重复 `code + version`、资源规格格式错误、缺失记录和 API envelope 映射。接口示例位于 `examples/programming-language/`。

## 风险与后续

当前列表不返回分页 total，保持与现有轻量列表响应风格一致。若后台数据量增长，再补分页响应和查询条件。
