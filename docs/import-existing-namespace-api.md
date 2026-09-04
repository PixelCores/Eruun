# Resource Import：一次性扫描与纳管任务

> 状态：Current。推荐入口是 `POST /api/v1/resource-import/jobs/scan` 与 `POST /api/v1/resource-import/jobs/manage`。扫描和纳管都是持久化异步任务，客户端通过 `GET /api/v1/resource-import/jobs/:taskID` 查询结果；它们不是持续监听器。

`resourceimport` 是一个独立业务模块，表示“按用户规则发现已有 Kubernetes 资源，再由用户明确选择并纳入 Eruun 管理”的完整流程。`adoption` 不再作为包或模块名称；现有数据库与应用 API 中的 `managementMode: "adopted"` 仅保留为已发布的应用管理模式取值。

## 任务流程

1. 用户提交一次扫描任务，并定义本次扫描规则。
2. API 立即返回 HTTP 202 和 `taskId`；worker 先验证持久化的 workspace/namespace，再由 resource import 模块执行扫描。
3. 客户端轮询任务，直到扫描完成，再从候选结果中选择 root workload 并给出应用/组件映射。
4. 用户提交独立的纳管任务，引用已完成的 `scanTaskId`。
5. worker 在真正写入前重新规划并校验资源是否在扫描后发生漂移，然后完成纳管。

扫描和纳管所需时间都不由客户端预估，也不占用原 HTTP 请求。任务状态以数据库中的 `WorkflowQueue` / `JobInfo` 为事实源，进程恢复后仍可由现有 workflow worker 继续处理。两个提交接口都是 system-admin 操作；执行器只接受任务中已验证的 workspace namespace，集群级依赖也会按该 namespace 的引用关系过滤。

## 提交一次扫描任务

```http
POST /api/v1/resource-import/jobs/scan
Content-Type: application/json
```

```json
{
  "namespace": "production",
  "rules": [
    {
      "kinds": ["Deployment", "StatefulSet"],
      "nameRegex": "^payments-",
      "labelSelector": "team=payments"
    },
    {
      "kinds": ["Service"],
      "labelSelector": "import.eruun.io/enabled=true"
    }
  ]
}
```

规则约束：

- `namespace` 必须是当前 workspace 的 namespace，且不能是 `default`。
- `rules` 必须包含 1–32 条规则。
- 多条规则之间是 OR；同一条规则内的 `kinds`、`nameRegex`、`labelSelector` 是 AND。
- `nameRegex` 使用 Go RE2 语义，最长 512 字符；`labelSelector` 使用 Kubernetes label selector 语义。
- 单条规则至少设置一个筛选字段。省略 `kinds` 表示该规则覆盖模块支持的所有资源类型。

提交成功返回：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "taskId": "scan-task-id",
    "type": "resource_import_scan",
    "status": "waiting"
  }
}
```

扫描结果只保存候选资源的 `apiVersion/kind/namespace/name/uid/resourceVersion/specDigest`。它不保存完整 Kubernetes manifest，也不会返回或持久化 Secret 内容。

## 查询扫描或纳管任务

```http
GET /api/v1/resource-import/jobs/:taskID
```

完成的扫描任务示例：

```json
{
  "code": 0,
  "message": "",
  "data": {
    "taskId": "scan-task-id",
    "type": "resource_import_scan",
    "status": "completed",
    "result": {
      "namespace": "production",
      "resources": [
        {
          "kind": "Deployment",
          "namespace": "production",
          "name": "payments-api",
          "source": {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "namespace": "production",
            "name": "payments-api",
            "uid": "source-uid",
            "resourceVersion": "42",
            "specDigest": "sha256-digest"
          },
          "status": "candidate"
        }
      ]
    }
  }
}
```

执行中的任务没有 `result`；失败任务返回终态 `status` 和脱敏后的 `error`。

## 提交纳管任务

```http
POST /api/v1/resource-import/jobs/manage
Content-Type: application/json
```

```json
{
  "scanTaskId": "scan-task-id",
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
        }
      ]
    }
  ]
}
```

当前纳管任务要求：

- `scanTaskId` 必须指向当前 workspace 中已完成的扫描任务。
- `applications` 当前必须且只能包含一个应用映射。
- 用户直接选择的是扫描结果中的 root workload；首版 root workload 支持 `apps/v1` Deployment 与 StatefulSet。
- 模块会根据所选 root workload 计算 Service、ConfigMap、Secret、PVC、RBAC 等依赖闭包，并沿用已有 shared / external / data-protected 安全边界。
- 执行前会重新 dry-run，并比较所选 workload 的 UID、resourceVersion 与 spec digest。任一资源在扫描后变化，任务会 fail closed；用户需要重新扫描和选择。

提交成功同样返回 HTTP 202，任务类型为 `resource_import_manage`。纳管任务内部完成重新规划、签名指纹生成与 apply，客户端不需要在两个同步 HTTP 请求之间保存短期指纹。

## 兼容的同步导入接口

现有 `POST /api/v1/applications/import/namespace` 与 `/try` 仍保留，供旧客户端使用。新交互应使用上面的异步 resource import jobs。

### 两种底层应用管理模式

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

异步任务请求位于 `examples/resource-import-jobs/`：

- `01-scan-request.json`
- `02-manage-request.json`

兼容同步请求和响应位于 `examples/namespace-import/`：

- `01-dry-run-request.json`
- `02-apply-request.json`
- `03-cleanup-apply-request.json`
- `04-dry-run-response.json`

示例 UID、resourceVersion、digest 和 fingerprint 都是占位值；生产 apply 必须使用当前环境新生成的 dry-run 指纹。
