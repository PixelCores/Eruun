# Domain Service 目录重组

## 背景与需求

`pkg/apiserver/domain/service` 已按主题拆出多个 `application_*`、`validation_*` 文件，但仍处于单一 Go package。此次调整目标是把 apiserver domain/service 内部按服务类型组织到子目录，降低单目录文件密度，同时不改变 API、event、repository 等外部调用契约。

## 影响范围

- API: 保持 `service.ApplicationsService`、`service.ValidationService` 等父包类型不变。
- Domain: 将 service 实现拆到 `application`、`workflow`、`validation`、`conversion`、`namespaceimport`、`systemsetting` 子包。
- DB: 无表结构或仓储契约变化。
- Cache: 无缓存键或 Redis 契约变化。
- K8s: 无资源生成、等待或清理语义变化。
- Workflow: 保留父包导出的 workflow helper 兼容入口。

## 技术选型与取舍

采用父 `service` 包兼容门面：父包继续导出原接口、流类型、构造函数和少量跨层 helper，实际实现由子包承载。这样可以避免 API/event 层大面积 import 改动，PR 重点保持在 `domain/service` 内部。

共享逻辑按必要性收敛到 `service/internal` 或窄的子包导出函数。没有把全部 helper 都公开为长期 API，只为跨子包依赖保留最小桥接面。

## 实现摘要

- 新增 domain service 子包：application、workflow、validation、conversion、namespaceimport、systemsetting。
- 新增 internal helper 包承载调度锁、取消信号和 trait 写入校验等跨子包逻辑。
- 父 `service` 包保留原来的注入和调用面，`InitServiceBean` 只负责创建 service bean；配置仍由容器通过 `inject:""` 字段注入。
- application 操作的默认超时和删除轮询间隔收敛到 `config` 常量，包内仅保留实现细节常量。
- 测试随实现包迁移，并补齐子包测试夹具。

## 测试与验收

- `GOCACHE=/private/tmp/gocache-eruun go test ./pkg/apiserver/domain/service/...`
- `GOCACHE=/private/tmp/gocache-eruun go test ./pkg/apiserver/interfaces/api ./pkg/apiserver/event/workflow`
- `GOCACHE=/private/tmp/gocache-eruun go test ./...`

## 风险与后续

- 主要风险是子包间桥接函数增加后形成新的隐式依赖；后续若继续架构演进，应优先把 DTO 依赖从 domain service 中下沉到应用层或接口层。
- 目前不改变行为契约，因此未更新 `docs/` 对外行为文档。
