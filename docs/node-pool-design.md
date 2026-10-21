# 节点池设计

> 状态：Draft / Proposal。本文记录管理员定义节点池、组件按名字引用的设计，以及分 PR 实施边界。已实现行为以 Current API 文档和代码为准；后续权限收敛仍是提案。

## 1. 背景

原组件调度字段直接暴露节点标签，无法约束空间成员可选的节点范围。节点池把调度配置交给系统管理员，空间成员只选择名字。MySQL 是配置的唯一来源，不增加缓存，也不新增运行角色。

## 2. 目标与边界

组件顶层和独立 Job command 步骤使用 `traits.nodePool`。应用创建、更新、模板实例化和 Job 提交时校验名字；实际部署构建时再次从 MySQL 读取配置。init/sidecar 不接受节点池引用。配置更新影响下一次部署，不触发自动重新部署。

节点池只决定 Pod 的 `nodeSelector` 和 `tolerations`。空间安全检查、租约、fencing、CAS、Job 执行身份和共享资源保护保持现有契约。

## 3. 配置与执行流

```mermaid
flowchart TD
    Admin[系统管理员] --> Manage[/api/v1/admin/node-pools]
    Manage --> DB[(MySQL eruun_node_pools)]
    Member[空间成员] --> Options[/api/v1/node-pools 名字与描述]
    DB --> Options
    Member --> DSL[traits.nodePool]
    DSL --> Validate[写入时按名字校验]
    DB --> Validate
    DSL --> Build[部署构建时解析]
    DB --> Build
    Build --> Pool[*model.NodePool 显式传入]
    Pool --> Pod[nodeSelector 与 tolerations]
    Pod --> Policy[PrepareTask 空间检查]
```

## 4. 数据模型与校验

```go
type NodePool struct {
    Name        string              `json:"name" gorm:"primaryKey;column:name;size:63"`
    Description string              `json:"description" gorm:"column:description;type:text"`
    NodeSelector map[string]string  `json:"nodeSelector" gorm:"serializer:json;column:node_selector"`
    Tolerations []corev1.Toleration  `json:"tolerations" gorm:"serializer:json;column:tolerations"`
    BaseModel
}
```

表名为 `eruun_node_pools`，以名字为主键，不另设 ID。名字必须是 DNS-1123 label，最长 63 字符。`nodeSelector` 至少一项，键为合法 Kubernetes 标签键，值合法且非空。Toleration 必须显式选择 `Equal` 或 `Exists`；`Exists` 不带 value，`Equal` 必须带 key。effect 仅允许 Kubernetes 的三个效果或空值；只有 `NoExecute` 可设置 `tolerationSeconds`。

## 5. HTTP 与 gRPC

系统管理员管理接口为 `GET/POST /api/v1/admin/node-pools` 和 `GET/PUT/DELETE /api/v1/admin/node-pools/:name`。PUT 整体替换可变字段，路径上的名字不可改。成员接口 `GET /api/v1/node-pools` 只返回名字与描述，要求当前空间至少 viewer。删除不检查引用、不触发部署；已引用的名字被删除后，下次写入或部署必须失败。

```json
{
  "name": "gpu-a100",
  "description": "A100 推理节点",
  "nodeSelector": {"eruun.io/pool": "gpu-a100"},
  "tolerations": [{"key":"nvidia.com/gpu","operator":"Exists","effect":"NoSchedule"}]
}
```

创建成功返回 HTTP 201，data 为上述对象加 `createTime`、`updateTime`；成功与错误沿用统一 envelope。错误码保持 `ErrNodePoolInvalid` 35000、`ErrNodePoolExists` 35001、`ErrNodePoolNotFound` 35002。gRPC 使用 `NodePoolsService`，管理员 CRUD 与成员 options 同 HTTP 权限和字段语义。

## 6. DSL 与写入校验

```json
{"traits":{"nodePool":"gpu-a100"}}
```

`spec.Traits` 使用 `NodePool string`，JSON 名为 `nodePool`。应用创建/更新、模板实例化、Job command 提交均按名字查库；失败返回 `ErrCodeInvalidTraitConfig`，字段指向对应的 `.traits.nodePool`。init/sidecar 的嵌套引用返回相同错误码。模板克隆保留配置名，覆盖整体替换名字。评测组件规范化将 nodePool 计入非评测 traits，拒绝混用。

gRPC `AppSpecTraits.node_pool = 16`，JSON 名为 `nodePool`。已删除的字段号 6、10 与名字 `rbac`、`target_work_env` reserved，不复用。HTTP 和 gRPC 均严格拒绝删除的字段。

## 7. 部署时解析与渲染

解析结果以 `*model.NodePool` 显式传给 `ApplyTraits` 与 `GenerateWebService`、`GenerateStoreService`、`GenerateInstantJob`、`GenerateScheduledCronJob`；不增加隐式查库或全局配置。

`event/workflow.buildJobsForComponent` 和 `jobs` command 构建路径按名字从 MySQL 获取 `*model.NodePool`，查不到或 datastore 不可用立即使构建失败。组件引用不为空，但未传入同名节点池时，Trait 处理器返回错误，不能静默跳过。

仅生成对象身份、清理资源名、比较 StatefulSet 不可变字段、版本重启以及数据库重置时，可使用 `IdentityNodePool(component)`：其中只有节点池名字，不查库，不得用于部署。没有节点池引用的内部生成调用传空 Profiles。

渲染把 nodeSelector 与 tolerations 合并进 Pod 模板，`podspec_compare` 将 tolerations 纳入变更比较。真实渲染结果继续经过 `PrepareTask`，空间策略将 token 自动挂载设为 false。

## 8. 为什么不提供组件级 RBAC

空间安全模型有三条明确限制：

1. `workspace/transport.go` 的 `validatePodSecurity` 只允许空值或 `default` ServiceAccount，并拒绝 token 挂载。
2. `securePodMap` 强制把 `automountServiceAccountToken` 设为 false。
3. `jobexec.PrepareTask` 白名单不接受 ServiceAccount、Role、RoleBinding 等部署 Job。

因此组件级 Kubernetes 身份授权不能在空间里使用，不提供相应 DSL 或配置接口。开通空间时创建的 `eruun-runner` ServiceAccount、Role、RoleBinding 属于平台代理的执行身份，仍然保留。

以后如需让 Pod 操作集群，走平台代理和按任务签发的 capability，不发 Kubernetes token。此方向是后续设计，不是当前已实现能力。

## 9. 删除与保留清单

| 范围 | 变更 |
| --- | --- |
| DSL | 删除原始节点标签 trait、组件 RBAC trait 及规则规格，改用 nodePool |
| Traits | 删除 `processRBAC`、专用 ServiceAccount 字段和合并逻辑 |
| 部署执行 | 删除 ServiceAccount、Role、RoleBinding、ClusterRole、ClusterRoleBinding 控制器、Job 类型、解码和构建分支 |
| 清理 | 删除 RBAC 比较、生成、删除、等待与专用清理描述，保留组件自身资源和共享保护规则 |
| PR 1 配置 | 删除 RBAC 配置模型、仓储、服务、HTTP/gRPC 接口、装配和 35010–35012 错误码；已存在的旧表不做迁移清理 |
| YAML 转换 | nodeSelector/tolerations 只提示选择 nodePool；RBAC 对象只提示空间工作负载不使用 Kubernetes API 凭据，删除映射与 ServiceAccount 关联 |
| gRPC | 删除组件 RBAC 消息与配置服务，保留旧字段号，新增 node_pool |
| PR 3 | 删除 nodeSelector/rbacPolicies 历史系统设置；幂等清理三类废弃设置存量记录；Helm 和 stack 收回集群级 RBAC 管理授权 |
| 保留 | nodePool 模型与接口、空间开通的 eruun-runner 身份、全部空间安全检查；空间开通的 Role/RoleBinding 授权（含 bind、escalate） |

## 10. 验证与失败语义

覆盖未知名字、嵌套引用、command 步骤、Pod 渲染、缺失解析结果、仅 tolerations 变化、YAML 警告、HTTP/gRPC 一致性与严格解码。用真实生成的 Deployment 跑 `PrepareTask` 探针。

本地测试不证明真实 Kubernetes 的节点调度、污点容忍、空间 impersonation 或 Helm 权限；真实集群验收由用户执行。删除节点池后允许清理已部署组件，不允许使用占位配置重新部署。

## 11. PR 拆分

| PR | 内容 | 依赖 |
| --- | --- | --- |
| 1 | 节点池模型、仓储、管理员和成员 HTTP/gRPC、错误码；移除旧 Profile 导入并删表的启动迁移 | — |
| 2 | DSL 切换到 nodePool，写入和部署解析、tolerations 渲染比较；删除整条组件 RBAC 能力及 PR 1 的 RBAC 配置；YAML 只给警告；更新文档和示例 | PR 1 |
| 3（已实现） | 删除 nodeSelector/rbacPolicies 系统设置；在 AutoMigrate 后幂等删除 aliyunCloud、nodeSelector、rbacPolicies 存量记录（aliyunCloud 含凭据），不改其他设置或内部迁移标记；Helm 与 stack 收回 clusterroles/clusterrolebindings 授权，保留空间开通所需 roles/rolebindings（含 bind、escalate） | PR 2 |
| Console | 节点池管理和组件下拉选择，移除组件 RBAC 编辑器 | API 契约 |
