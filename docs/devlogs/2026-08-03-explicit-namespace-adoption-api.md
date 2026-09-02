# 显式 Namespace Adoption API 激活记录

> 状态：Implemented Reference。当前对外契约以 `docs/import-existing-namespace-api.md` 为准。

## 目标


## 决策

- 复用 `POST /api/v1/applications/import/namespace`，以 `managementMode: "adopted"` 作为明确授权。
- adopted 必须显式提交 `mode`，并且一次只允许一个 `applications[]` 映射。
- `applications` 与旧 `includeKinds` 互斥；只传 namespace 继续进入 observe 兼容路径。
- dry-run 是零写操作，返回 source identity、依赖角色、ownership、disposition 和签名 fingerprint。
- apply 重新扫描并验证 Kubernetes identity 与目标 DB/workflow 状态；漂移统一返回 HTTP 409。
- 应用、组件、workflow、snapshot、binding 与 Secret 密文使用同一事务提交。
- import apply 不直接修改 Kubernetes；Pod metadata 由 leader-only coordinator 在 binding 提交后处理。
- adopted 默认删除只 detach；资源删除必须经过 cleanup plan/fingerprint 两阶段接口。

## 安全边界

- HPA、冲突标签、ownerReference、source UID 冲突和不完整 identity 阻断 apply。
- PVC/PV、shared/external 和集群级 RBAC 不进入可删除所有权。
- Secret 明文不进入 DTO、日志、fingerprint 或 snapshot；数据库仅保存 AES-256-GCM 密文。
- 严格 JSON 会拒绝未知字段、任意嵌套层级的重复字段和多个 JSON value。
- 同一 apply 重试只在 source、snapshot、binding 和 fingerprint 完全一致时视为幂等 replay。

## 兼容性

- observe namespace import 保留原有请求形态与标签行为。
- native lifecycle 和无指纹资源清理保持兼容。

## 验证重点

- dependency closure 与 shared/data protection 传播；
- dry-run DB/Kubernetes 零写入；
- apply 指纹、UID、resourceVersion、spec digest 和目标状态漂移；
- 单事务 mutation 失败回滚；
- HPA、Pod 标签和 ownerReference 冲突；
- Secret AAD/key rotation；
- cleanup HTTP partial result 语义。
