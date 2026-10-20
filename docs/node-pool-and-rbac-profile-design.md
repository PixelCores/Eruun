# 节点池与 RBAC 配置设计

> 状态：Draft / Proposal。本文设计“管理员定义带名字的节点池和 RBAC 配置，组件按名字引用”，用来替换只写不读的 `nodeSelector`、`rbacPolicies` 系统设置，以及组件里的 `traits.targetWorkEnv` 和 `traits.rbac`。设计已于 2026-10-21 确认，正在按第 11 节分 PR 实施。在对应 PR 合并前，不构成当前的 API、DSL 或数据库契约。

## 1. 现状（基线 origin/main af08427）

| 能力 | 现在的情况 | 问题 |
|---|---|---|
| `nodeSelector` 系统设置 | Console 设置页通过 `/api/v1/settings` 写入，后端只做 JSON 形状校验后存表 | 运行时没有任何代码读取，只写不读 |
| `rbacPolicies` 系统设置 | 同上，要求是 JSON 数组 | 同上 |
| `NodeSelectorProfile`、`RBACProfile` 模型 | 只在 `mysql/migrations.go` 里使用，用来把更早版本旧表里的数据导入上面两个设置 | 没有其他用途 |
| `traits.targetWorkEnv` | 组件级，`map[string]string`，渲染成 Pod 的 `nodeSelector`；只校验标签格式 | 空间用户可以填任意节点标签，平台没有约束 |
| `traits.rbac` | 组件级，原始 `RBACPolicySpec[]`，可渲染 ServiceAccount、Role/ClusterRole 和对应的 Binding | 空间作用域下 `workspace.ValidateTraits` 一律返回 Forbidden，而所有业务请求都带空间作用域，所以没有入口能用；Console 的 RBAC 编辑器提交必定失败 |
| Helm 运行时权限 | ClusterRole 授予 `roles`、`clusterroles` 的增删改查以及 `bind`、`escalate`，还有 `rolebindings`、`clusterrolebindings` | 集群级 RBAC 只有 `traits.rbac.clusterScope` 会创建 |

## 2. 目标与非目标

**目标**

- 由系统管理员维护两类带名字的配置：
  - **节点池**：Pod 调度到哪类节点；
  - **RBAC 配置**：组件的 ServiceAccount 在本命名空间内有哪些权限。
- 空间里的组件和 Job 步骤只能按名字引用这些配置，不能直接写节点标签或 RBAC 规则。
- 删掉只写不读的设置和用不到的字段，一个能力只有一种写法。

**已由用户确认的约束**（2026-10-21）

- 删除 `traits.targetWorkEnv` 和 `traits.rbac`，只保留按名字引用。
- 配置对**全部空间**可见、可引用，不做按空间授权。
- RBAC 只允许**命名空间内**的 Role/RoleBinding，不允许 ClusterRole。

**非目标**

- 不做节点容量管理、配额或调度器替换（见 `workflow-resource-capacity-scheduler.md`）。
- 不做 affinity、topology spread 等高级调度（见第 12 节第 1 点）。
- 不做按空间的配置授权，也不做多集群。
- 已在运行的工作负载不会因为配置修改而自动重新部署。

## 3. 整体结构

```text
系统管理员 ──► /api/v1/admin/node-pools、/api/v1/admin/rbac-profiles ──► MySQL（eruun_node_pools、eruun_rbac_profiles）
                                                                         │
空间成员 ──► /api/v1/node-pools、/api/v1/rbac-profiles（只读，名字+描述） ◄─┤
   │                                                                     │
   └─► 创建或更新应用 / Job ──写入时校验：引用的名字必须存在─────────────────┤
                                     │                                   │
                                     ▼                                   │
                        Job 构建（event/workflow）──部署时按名字解析───────┘
                                     │
                                     ▼
                     traits 处理器：nodeSelector + tolerations 写进 PodSpec；
                     RBAC 配置生成 ServiceAccount + Role + RoleBinding（组件命名空间内）
```

MySQL 是配置的唯一事实来源。部署时从 MySQL 读取，不做缓存。

## 4. 数据模型

两张新表，主键就是名字。路由和 DSL 都用名字，不另设 ID。现有的 `NodeSelectorProfile`、`RBACProfile` 模型删除，换成下面的结构。

```go
// pkg/apiserver/domain/model/node_pool.go
type NodePool struct {
	Name         string              `json:"name" gorm:"primaryKey;type:varchar(63);column:name"`
	Description  string              `json:"description,omitempty" gorm:"type:text;column:description"`
	NodeSelector map[string]string   `json:"nodeSelector" gorm:"serializer:json;column:node_selector"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty" gorm:"serializer:json;column:tolerations"`
	BaseModel
}

func (n *NodePool) TableName() string { return tableNamePrefix + "node_pools" }

// pkg/apiserver/domain/model/rbac_profile.go
type RBACProfile struct {
	Name        string              `json:"name" gorm:"primaryKey;type:varchar(63);column:name"`
	Description string              `json:"description,omitempty" gorm:"type:text;column:description"`
	Rules       []spec.RBACRuleSpec `json:"rules" gorm:"serializer:json;column:rules"`
	BaseModel
}

func (r *RBACProfile) TableName() string { return tableNamePrefix + "rbac_profiles" }
```

**校验规则**

所有规则都在写入接口里检查，不通过返回 400。

- `name`：DNS-1123 label（小写字母、数字、`-`，不超过 63 个字符），创建后不能修改。
- 节点池：
  - `nodeSelector` 至少一项，键和值都按 Kubernetes 标签规则校验（沿用 `targetWorkEnv` 现有的校验逻辑）；
  - `tolerations` 按 Kubernetes 规则校验，`operator` 只允许 `Equal`/`Exists`，`effect` 只允许 `NoSchedule`/`PreferNoSchedule`/`NoExecute`。
- RBAC 配置：
  - `rules` 至少一条，每条 `verbs` 和 `resources` 都不能为空；
  - **不允许 `nonResourceURLs`**，它只对 ClusterRole 有意义；
  - **不允许授予会让空间在本命名空间内自己提权的权限**：
    - `apiGroups` 包含 `rbac.authorization.k8s.io` 或 `*`；
    - `verbs` 包含 `bind`、`escalate`、`impersonate` 或 `*`；
    - `resources` 包含 `serviceaccounts/token`。

  这一条见第 12 节第 3 点。

## 5. 接口

### 5.1 系统管理员（`minimum = "system"`）

| 方法和路径 | 请求 | 响应 |
|---|---|---|
| `GET /api/v1/admin/node-pools` | — | `{"nodePools": [NodePool]}` |
| `POST /api/v1/admin/node-pools` | `NodePool`（不含时间字段） | `NodePool`，201 |
| `GET /api/v1/admin/node-pools/:name` | — | `NodePool` |
| `PUT /api/v1/admin/node-pools/:name` | `{description, nodeSelector, tolerations}` | `NodePool` |
| `DELETE /api/v1/admin/node-pools/:name` | — | 204 |
| `GET/POST /api/v1/admin/rbac-profiles`<br>`GET/PUT/DELETE /api/v1/admin/rbac-profiles/:name` | 同上，主体为 `RBACProfile` | 同上 |

请求示例：

```json
POST /api/v1/admin/node-pools
{
  "name": "gpu-a100",
  "description": "A100 推理节点",
  "nodeSelector": {"eruun.io/pool": "gpu-a100"},
  "tolerations": [{"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"}]
}

POST /api/v1/admin/rbac-profiles
{
  "name": "pod-reader",
  "description": "读取本空间的 Pod 和日志",
  "rules": [{"apiGroups": [""], "resources": ["pods", "pods/log"], "verbs": ["get", "list", "watch"]}]
}
```

**错误码**：新增 `utils/bcode/006_scheduling_profile.go`，沿用现有领域错误的写法。34xxx 号段已被 Job 错误码占用，因此使用 35xxx。

| 错误 | HTTP | 业务码 |
|---|---:|---:|
| `ErrNodePoolInvalid` | 400 | 35000 |
| `ErrNodePoolExists` | 409 | 35001 |
| `ErrNodePoolNotFound` | 404 | 35002 |
| `ErrRBACProfileInvalid` | 400 | 35010 |
| `ErrRBACProfileExists` | 409 | 35011 |
| `ErrRBACProfileNotFound` | 404 | 35012 |

### 5.2 空间成员（`minimum = "viewer"`，供 Console 下拉选择）

| 方法和路径 | 响应 |
|---|---|
| `GET /api/v1/node-pools` | `{"nodePools": [{"name": "gpu-a100", "description": "A100 推理节点"}]}` |
| `GET /api/v1/rbac-profiles` | `{"rbacProfiles": [{"name": "pod-reader", "description": "读取本空间的 Pod 和日志"}]}` |

成员接口只返回名字和描述，不暴露节点标签、污点和 RBAC 规则。

### 5.3 gRPC

和现有的 HTTP/gRPC 双入口保持一致，写法参照 `ProgrammingLanguagesService`，每类配置一个 service，按方法分别设置权限：

- `NodePoolsService`：`ListNodePools`、`GetNodePool`、`CreateNodePool`、`UpdateNodePool`、`DeleteNodePool`（system），`ListNodePoolOptions`（viewer）；
- `RBACProfilesService`：同样的 5 个管理方法（system），以及 `ListRBACProfileOptions`（viewer）。

proto 改动后用 `scripts/generate_grpc.sh` 重新生成，用 `check` 模式校验。

## 6. 组件 DSL

```json
{
  "name": "llm-server",
  "type": "webservice",
  "image": "vllm/vllm-openai:latest",
  "traits": {
    "nodePool": "gpu-a100",
    "rbacProfile": "pod-reader",
    "resources": {"gpu": "1"}
  }
}
```

```go
// pkg/apiserver/domain/spec/traits.go（只列出变化）
type Traits struct {
	// ...
	NodePool    string `json:"nodePool,omitempty"`    // 替换 TargetWorkEnv
	RBACProfile string `json:"rbacProfile,omitempty"` // 替换 RBAC []RBACPolicySpec
	// ...
}
```

**适用范围**

| 位置 | `nodePool` | `rbacProfile` |
|---|---|---|
| Application 组件（顶层 traits） | 支持 | 支持 |
| `init` / `sidecar` 嵌套 traits | 不支持（Pod 级字段，和 `targetWorkEnv` 现在的规则一样） | 不支持 |
| 模板覆盖 | 支持，整体替换 | 支持，整体替换 |
| 空间 Job 的 command 步骤 | 支持 | 不支持（和 `traits.rbac` 现在的规则一样） |
| 评测组件的规范化 | 计入“非评测 traits”的判断，和 `RBAC` 现在的处理一样 | 同左 |

**写入时校验**：在 `validation` 里按名字查表，不存在就返回 `ErrCodeInvalidTraitConfig`，字段指向 `component[i].traits.nodePool`。`workspace.ValidateTraits` 里对 `traits.RBAC` 的 Forbidden 分支随字段一起删除。

## 7. 部署时解析与渲染

**解析时机**：在 Job 构建阶段（`event/workflow` 构造组件 JobTask 的地方）按名字查表，得到这次部署要用的配置。查不到时，这一步 Job 直接失败，错误信息写明是哪个名字不存在，不做任何回退。

**节点池渲染**：
- `TraitResult` 新增 `Tolerations []corev1.Toleration`；
- 节点池的 `nodeSelector` 写入现有的 `TraitResult.NodeSelector`，`tolerations` 写入新字段，最后合并进 Pod 模板。
- `jobdeploy/podspec_compare.go` 的变更比较要把 tolerations 也算进去，否则修改节点池后，重新部署不会触发滚动更新。

**RBAC 渲染**：每个组件固定生成一个 ServiceAccount、一个 Role、一个 RoleBinding，都放在组件的命名空间里：

| 对象 | 名字 | 内容 |
|---|---|---|
| ServiceAccount | `<component>-sa` | `automountServiceAccountToken: true` |
| Role | `<component>-sa-role` | `rules` 取自 RBAC 配置 |
| RoleBinding | `<component>-sa-binding` | 把 Role 绑定到上面的 ServiceAccount |

Pod 的 `serviceAccountName` 设为 `<component>-sa`。这里复用现有的 `processRBAC` 和 `jobdeploy` 里的 ServiceAccount、Role、RoleBinding 控制器：把配置转换成一条命名空间内的策略交给它们处理。名字和现在 `traits.rbac` 的默认命名一致。

## 8. 生命周期

```text
配置：创建 ──► 修改（下一次部署时生效）──► 删除
组件：引用 X ──部署──► 资源按 X 渲染
      引用改为 Y ──部署──► 同名资源更新为 Y 的内容
      去掉引用 ──部署──► 版本更新清理删除 <component>-sa / -sa-role / -sa-binding，并去掉 Pod 上的 nodeSelector、tolerations
```

- **修改配置**：不触发任何部署。每个组件在下一次部署时读到新内容。已在运行的 Pod 不变。
- **删除配置**：允许删除，不做引用检查。之后引用它的组件，在写入时校验失败，或者在部署时 Job 失败。已在运行的工作负载不受影响。理由见第 12 节第 2 点。
- **清理**：RBAC 对象的名字由组件名确定，所以去掉引用时，版本更新清理按名字删除，删除规则沿用现有逻辑（`application_update_version_cleanup.go` 的 RBAC 部分）。组件删除时的清理也沿用现有的“只删自己拥有的资源”。

## 9. 删除的内容

| 位置 | 删除 |
|---|---|
| 组件 DSL | `traits.targetWorkEnv`；`traits.rbac`，以及 `RBACPolicySpec` 里除规则以外的字段（`serviceAccount`、`namespace`、`clusterScope`、各种 labels/annotations 等）。`RBACRuleSpec` 保留，给 RBAC 配置复用 |
| Job 类型与控制器 | `DeployClusterRoleJobCtl`、`DeployClusterRoleBindingJobCtl` 及对应的 JobType、JobInfo 解码、清理分支 |
| YAML 转换（`/applications/convert`） | 不再把 `nodeSelector`、`tolerations`、Role/RoleBinding/ClusterRole/ClusterRoleBinding 转成 traits，改为在 `warnings` 里提示“请在组件上选择节点池 / RBAC 配置”。删除 `kube_convert_rbac.go` 的映射逻辑 |
| 系统设置 | `nodeSelector`、`rbacPolicies` 两个类型及其 codec |
| MySQL 迁移 | `migrateSystemSettings` 里从旧 Profile 表导入设置并删表的部分，以及旧模型。**必须和新表在同一个 PR 里删除**：这段迁移每次启动都会删除名为 `eruun_rbac_profiles` 的表，而新表正好同名 |
| gRPC | `AppSpecTraits.rbac`、`target_work_env`、`AppSpecRBACPolicySpec`，新增 `node_pool`、`rbac_profile` |
| Helm | ClusterRole 里 `clusterroles`、`clusterrolebindings` 的授权；保留 `roles`、`rolebindings`（创建命名空间内的 Role 仍然需要 `bind`、`escalate`） |
| Console（另做 PR） | 设置页的“调度”和“访问控制（rbacPolicies）”区，换成节点池和 RBAC 配置的管理页；组件属性面板的 RBAC 编辑器换成下拉选择 |

Eruun 未发布，不做数据迁移和兼容：

- 接口请求是严格解码的（`DisallowUnknownFields`），删除字段后，带 `targetWorkEnv` 或 `rbac` 的请求会直接返回 400；
- 数据库里已存组件中的这两个旧字段，读取时会被忽略，不再生效。

## 10. 安全边界

- 节点池和 RBAC 配置只有系统管理员能写；空间成员只能看到名字和描述。
- 所有 RBAC 对象都限定在组件所在的命名空间。现有规则保证组件命名空间等于空间命名空间，所以空间之间不会互相授权。
- 第 4 节的规则限制，保证空间不能借 RBAC 配置在自己的命名空间内继续提权，比如自己再创建 Role 或 RoleBinding。
- 删掉集群级 RBAC 之后，运行时的 ClusterRole 不再需要创建 ClusterRole 和 ClusterRoleBinding 的权限，攻击面变小。

## 11. 实施拆分

| PR | 内容 | 依赖 |
|---|---|---|
| 1 | 模型和仓储、管理员与成员接口（HTTP 和 gRPC）、错误码、单元测试；**同时删除** `migrateSystemSettings` 中从旧 Profile 表导入并删表的逻辑，以及旧的 `NodeSelectorProfile`、`RBACProfile` 模型 | — |
| 2 | DSL 切换：新增 `nodePool`/`rbacProfile`，删除 `targetWorkEnv`/`rbac`；写入校验、部署时解析、tolerations 渲染和变更比较、RBAC 渲染复用、版本更新清理；删除 ClusterRole 控制器；YAML 转换改为给出警告；更新 Current 文档和示例 | PR 1 |
| 3 | 删除 `nodeSelector`/`rbacPolicies` 两个设置类型；Helm 收回集群级 RBAC 授权 | PR 2 |
| 4 | Eruun-Console：管理页和组件下拉选择 | PR 1、PR 2 |

**验证边界**：本地测试用 fake client，覆盖不到真实集群的调度、污点容忍、RBAC 生效和 Helm 授权。真实 Kubernetes 上的验收由用户执行。

## 12. 已确认的设计决定（2026-10-21）

1. **节点池字段**：只包括 `nodeSelector` 和 `tolerations`，不包括 `affinity`。GPU 节点通常带污点，所以 tolerations 是必需的；affinity 写法复杂，目前没有需求。
2. **删除配置时不检查引用**，在使用时快速失败。如果要检查，需要扫描所有组件、模板和 Job 规格里的 traits JSON（它们以 JSON 字段存储），实现和维护成本都高，而且仍然挡不住并发写入。
3. **RBAC 规则的限制**：禁止 `rbac.authorization.k8s.io` 组、`*` 通配，以及 `bind`、`escalate`、`impersonate` 动作和 `serviceaccounts/token`。
4. **命名**：DSL 用 `nodePool` 和 `rbacProfile`；路由用 `/admin/node-pools`、`/admin/rbac-profiles`；表名用 `eruun_node_pools`、`eruun_rbac_profiles`。
