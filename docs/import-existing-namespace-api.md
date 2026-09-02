# Namespace 存量资源导入与显式接管 API

> 状态：Current。主路由为 `POST /api/v1/applications/import/namespace`；`/try` 保留旧分组规则的只读预览能力。

## 两种管理模式

| 模式 | 进入方式 | 行为边界 |
| --- | --- | --- |
| `observe` | 旧的 `namespace/mode/includeKinds` 请求；只传 `namespace` 也走此兼容路径 | 创建只读应用。保留既有导入分组和打标行为，后续 lifecycle、workflow、版本、数据库重置和资源清理写操作被拒绝。 |
| `adopted` | 请求显式包含 `managementMode: "adopted"`、`mode` 和一个 workload 映射 | 按 source `apiVersion/kind/name/UID` 接管，并使用签名计划、snapshot 与安全调和。不存在 namespace-only 自动接管。 |

`ApplicationBase.managementMode` 会公开返回；source binding、adoption snapshot 与 Secret 密文只在服务端内部保存。

## 显式 adopted 请求

### dry-run

```json
{
  "namespace": "production",
  "mode": "dry-run",
  "managementMode": "adopted",
  "applications": [
    {
      "name": "payments",
      "alias": "payments",
      "components": [
        {
          "name": "api",
          "workload": {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "name": "payments-api"
          }
        },
        {
          "name": "database",
          "workload": {
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "name": "payments-db"
          }
        }
      ]
    }
  ]
}
```

约束：

- `namespace` 必填，且不能是 `default`。
- `mode` 必须显式为 `dry-run` 或 `apply`。
- `managementMode` 必须为 `adopted`。
- `applications` 必须且只能包含一个映射；`components` 中每个名称和 workload identity 必须唯一。
- 首版 root workload 只支持 `apps/v1` Deployment 与 StatefulSet。
- `targetAppId` 可选；用于替换一个同 namespace 的既有 observe/adopted 应用，不能指向 native 应用。
- `applications` 与旧字段 `includeKinds` 互斥。
- 请求及嵌套对象采用严格 JSON：未知字段、大小写折叠后重复的字段、多 JSON value 均被拒绝。

### dry-run 返回

dry-run 会扫描 root workload 的依赖闭包，但不写数据库，也不修改 Kubernetes。响应包含 HMAC `planFingerprint`，并为资源返回：

- `source`：`apiVersion/kind/namespace/name/uid/resourceVersion/specDigest`；
- `dependencyRole`：workload、service、ingress、config、secret、pvc、service-account、rbac、pdb、network-policy 等；
- `ownership`：`exclusive`、`shared`、`external` 或 `data-protected`；
- `disposition`：`managed`、`shared-preserved`、`data-protected`、`blocked` 或 `excluded`。

规划覆盖 Deployment、StatefulSet、Service、Ingress、ConfigMap、Secret、PVC/PV、ServiceAccount、Role/RoleBinding、关联的 ClusterRole/ClusterRoleBinding、PodDisruptionBudget 和 NetworkPolicy。Namespace、CRD 与自定义资源不在首版边界内。

以下情况会阻断接管：

- root 被 HPA 管理；
- source 缺少 UID/resourceVersion，或 UID 已绑定到其他组件；
- 资源 identity 已由同 namespace 的其他 adopted 应用的有效 snapshot 记录为 `exclusive/managed`；该检查不依赖旧 root workload 仍然存活，同 namespace 的其他 adopted 应用 snapshot 缺失或无效时也会 fail-closed；
- PodTemplate、ReplicaSet 或 Pod 已带有冲突/不完整的 Eruun 管理标签；
- 选中资源的 ownerReference 指向会被接管调和的资源；
- 一个依赖跨显式应用共享，或发现无法安全归属的外部使用；
- keyring 未配置。

PVC/PV 永远按数据资源保护；集群级 RBAC 和外部/共享资源只记录与保留，不取得删除所有权。

### apply

apply 必须提交与 dry-run 相同的映射，并携带返回的指纹：

```json
{
  "namespace": "production",
  "mode": "apply",
  "managementMode": "adopted",
  "planFingerprint": "v1:key-id:<dry-run-hmac>",
  "applications": [
    {
      "name": "payments",
      "components": [
        {
          "name": "api",
          "workload": {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "name": "payments-api"
          }
        }
      ]
    }
  ]
}
```

服务端会重新扫描并验证 source UID、resourceVersion、spec digest，以及目标应用、组件和 workflow 的规范化状态。资源替换、配置变化、目标 DB 状态变化或指纹不匹配返回 HTTP 409；调用方必须重新 dry-run。

adopted apply 会先获取按 namespace 归一化的分布式锁，再在锁内重新扫描并重建计划；锁覆盖 ownership 检查、指纹复核和数据库事务提交，避免并发导入在扫描与提交之间重复取得同一资源的 exclusive ownership。锁后端不可用或续租失败时 fail-closed；同 namespace 的并发 apply 等待前序请求释放后重新扫描。目标为既有应用时，事务还会取得应用级调度锁。成功 apply 在单一数据库事务中写入应用、组件、workflow、版本化 snapshot、source binding、resume replicas 与加密 Secret 数据。导入本身不 patch Kubernetes，不修改 PodTemplate，也不触发 rollout。相同 apply 可幂等重试。

Secret 内容使用 AES-256-GCM 保存，AAD 绑定应用/组件/source identity；plan 使用 keyring HMAC 签名。配置入口是 `ERUUN_IMPORT_SECRET_KEYRING` 或 `ERUUN_IMPORT_SECRET_KEYRING_FILE`，轮转时保留旧 key 用于验证，active key 用于新加密。

## adopted 生命周期与查询

adopted workflow 使用 snapshot 中的 dependency jobs，并在执行时按 source identity 校验：同 UID 才能更新/no-op，异 UID 返回冲突，缺失的非数据资源才允许安全重建。StatefulSet 不可变字段、PVC 数据和 Secret 重加密均使用专用保护规则。

stop/start/restart 在应用调度锁内执行 HPA 预检、UID 校验和真实副本恢复。database reset 对 adopted 禁用。状态、日志、容器、文件与 exec 通过 source-bound owner UID 链定位 Pod，不依赖可伪造的普通标签。

## 删除与显式资源清理

删除 adopted 应用默认只解除纳管并返回 `resourcesRetained: true`，不会删除源资源。

如确实需要删除 exclusive 非数据资源，先获取签名计划：

```http
POST /api/v1/applications/:appID/resources/cleanup-plan
```

再提交：

```http
DELETE /api/v1/applications/:appID/resources
Content-Type: application/json

{"planFingerprint":"v1:key-id:<cleanup-plan-hmac>"}
```

cleanup apply 会 quiesce root、重扫 ReplicaSet/ControllerRevision/Pod 等 runtime children，并且只按签名 identity 和 UID 删除 exclusive 非数据资源。PVC/PV、shared/external 资源永久保留。漂移、晚到子资源、finalizer 和部分失败均可重新获取计划后安全重试；产生部分结果时 HTTP 层返回完整 deleted/failed/retained 明细。

native 应用原有无请求体 `DELETE .../resources` 行为保持兼容；observe 仍拒绝资源清理。

## 兼容 observe 请求

未传 `managementMode`、`applications` 和 `planFingerprint` 时继续使用旧协议：

```json
{
  "namespace": "production",
  "mode": "dry-run",
  "includeKinds": ["deployments", "services"]
}
```

`mode` 仍默认 `dry-run`。只传 `namespace` 不构成 adopted 授权，不会自动接管。`POST /api/v1/applications/import/namespace/try` 继续用于旧分组、孤儿识别和 conversion 预览，不能生成 adopted 指纹。

## 脱敏示例

完整请求和响应位于 `examples/namespace-import/`：

- `01-dry-run-request.json`
- `02-apply-request.json`
- `03-cleanup-apply-request.json`
- `04-dry-run-response.json`

示例 UID、resourceVersion、digest 和 fingerprint 都是占位值；生产 apply 必须使用当前环境新生成的 dry-run 指纹。

