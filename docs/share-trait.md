# Share Trait 策略说明

> 状态：Current。本文说明当前 namespace 内共享资源策略。

Share trait 用于在命名空间内复用组件资源。它只包含策略选择，不需要指定资源清单；组件生成的所有资源都会遵循同一策略。

带 `traits.share` 的组件是命名空间级公共组件。它的 Deployment/StatefulSet/Service/Ingress/Job/CronJob/PVC 相关资源命名 key 使用组件名本身，而不是 `appName + componentName`。未配置 `traits.share` 的普通组件仍按应用维度生成资源名。

## 策略选项

- `default`：命名空间内若已存在相同 share-name 的资源，则跳过该 Job；否则创建/更新资源。RBAC 采用下述专用调和语义。
- `ignore`：无论是否存在资源，直接在工作流中跳过该 Job。
- `force`：不进行共享判断，正常创建/更新资源（与未启用 share 的行为一致）。

除 RBAC 外，未配置 `traits.share` 时行为保持不变。

## RBAC 的隐式共享生命周期

ServiceAccount、Role、RoleBinding、ClusterRole 和 ClusterRoleBinding 即使没有配置 `traits.share`，也会自动写入 `strategy=default` 的共享标识，shareName 仍按组件 namespace 生成。该默认值只定义永久共享生命周期，不复用普通资源“发现同 share-name 即跳过”的行为：RBAC Job 会继续正常创建或更新目标资源。

RBAC 调和继续遵守现有托管边界：同名对象不是 Eruun 托管资源时不会覆盖。显式 `share=ignore` 仍直接将 RBAC Job 标记为 `skipped`；`default` 和 `force` 均执行正常调和。

五类 RBAC 都是永久保留资源。Job 失败局部回滚、`cleanup_resources`、组件移除、`cleanup_all`、应用同步资源清理和应用删除均不会删除这些对象。

## 异常状态下的回收例外

为避免 share 组件首次部署失败后资源“脱离掌控”，应用资源清理、取消清理等非版本更新路径新增以下规则：

- `share=default` 或 `share=ignore` 的 Pod 类型组件，默认仍受 share 保护。
- 当检测到该组件当前 Pod 处于异常状态时，允许执行资源删除（不再一律跳过）。
- `share=force` 不受影响，始终按普通组件清理。
- 若读取 Pod 状态失败，采用保守策略：保持 share 保护并跳过删除。

版本更新的 `remove` 操作不适用上述异常清理例外：`share=default` / `share=ignore` 组件会被直接拒绝移除，`share=force` 继续按普通组件处理。

当前识别的异常状态包括：

- `CrashLoopBackOff`
- `ImagePullBackOff`
- `ErrImagePull`
- `CreateContainerError`
- `CreateContainerConfigError`
- `OOMKilled`

## 共享标识

共享资源通过固定 label 识别：

- `eruun.io/share-name`：使用组件所属的 `namespace`，转换为 RFC1123 格式并裁剪至 63 字符。
- `eruun.io/share-strategy`：策略值（`default`/`ignore`/`force`）。

这些 label 会被写入该组件产生的所有资源（含 trait 追加的 PVC、RBAC、Ingress 等）。

运行时选择器与资源共享判断是两套不同语义：

- 共享判断使用 `eruun.io/share-name` 与 `eruun.io/share-strategy`，用于发现命名空间内已有共享资源。
- Pod label、默认 Service selector、日志/容器查询、等待、清理和状态同步仍使用稳定的 `AppID + bounded componentName`，避免同名 shared 资源的查询结果串到其他组件。

## 应用状态聚合

共享资源的生命周期不完全归属于引用它的单个应用，因此应用普通可用性聚合区分 managed 与 shared webservice：

- 未配置 `traits.share` 和 `strategy=force` 的 webservice 属于 managed availability，参与本应用的 `pending`、`running`、`stopped`、`not_deploy`、`unknown` 判断。
- `strategy=default` / `strategy=ignore` 的 webservice 不参与 managed availability，避免共享 proxy 的 `Running` 把已 stop 的应用显示为 `running`，也避免共享 proxy 的 `Pending` 把已就绪应用显示为 `pending`。
- `failed`、`deploying`、`updating`、`restarting`、`starting`、`cleaning` 是全局高优先级状态，仍会检查共享组件。
- 当应用只有共享 webservice 时，聚合会回退使用这些组件已经持久化的真实状态，不根据“共享资源存在”伪造 `Running`。

该规则只改变应用级聚合范围，不跨 APP 扇出共享资源健康状态，也不改变组件明细接口返回的原始状态。

## 应用生命周期操作

单应用生命周期命令不会控制 `share=default`、`share=ignore` 或未知 share 策略的组件：

- `/start`、`/stop`、`/restart` 不对这些组件发送 Kubernetes 变更，也不改写组件运行态字段；目标会出现在 `skippedResources` 中。
- `/version` 的组件 `action=restart` 会保留审计 Job，但 Job 状态为 `skipped`。
- 未知 share 策略按 `default` 的保守保护语义处理。
- `share=force` 继续按普通 managed 组件处理，可以被上述生命周期操作控制。

`database-reset` 不重启任何 `webservice`，因此无论是否配置 share、使用何种策略，服务组件都保持重置前的运行状态。调用方需要停机重置时应自行编排 `/stop`、`/database-reset`、`/start`。

## 实现细节

### shareName 生成规则

1) 优先使用组件所属 `namespace` 作为 shareName。  
2) 对 `namespace` 执行 RFC1123 规范化（小写、非法字符替换为 `-`、截断至 63 字符）。  
3) 若 `namespace` 为空或规范化后为空，则回退到 `component.name` + `component.type` 组合。

> 对 ClusterRole/ClusterRoleBinding，shareName 仍按组件 namespace 写入共享生命周期标签；`default/force` 不会基于该值执行共享 List 或跳过，`ignore` 仍直接跳过，已有同名对象是否更新由 Eruun 托管标签校验决定。

### 策略执行流程

- `default`
  1) Job 运行前读取资源标签中的 `eruun.io/share-name` 与 `eruun.io/share-strategy`。
  2) 通过 label selector（`eruun.io/share-name=<shareName>`）进行 List 判断是否已存在共享资源。
  3) 若存在，Job 标记为 `skipped` 并返回；否则进入创建/更新流程。
  4) RBAC 是例外：带隐式或显式 `default` 的五类 RBAC 不执行上述 List 跳过判断，始终进入正常调和。
- `ignore`
  - Job 直接标记为 `skipped`，不执行任何 K8s API 调用。
- `force`
  - 不做共享判断，行为与未启用 share 一致。

### 并发保护

普通资源 `default` 策略的「label list 判断」新增并发保护，避免并发工作流同时 list 为空导致重复创建：

- 使用 `pkg/apiserver/infrastructure/locker` 统一的锁抽象。
- 生产路径要求存在 Redis 分布式锁；缺少 Redis 客户端或锁初始化失败时，`default` 策略直接失败，不自动回退到内存锁或 no-op 锁。
- 锁 key 格式：`eruun-share:<resourceKind>:<shareName>`，涵盖 Deployment、StatefulSet、Service、ConfigMap、Secret、PVC、Ingress、Job 和 CronJob 等执行共享 List 判断的资源。
- 五类 RBAC 不执行共享 List 判断，也不获取该 Redis 锁；`default` / `force` 直接进入 Kubernetes 幂等调和，`ignore` 仍在调和前标记为 `skipped`。

### JobInfo 落库

对预先标记为 `skipped` 的 Job（如 share `ignore` 或 share `default` 判定已存在），
也会写入 JobInfo，确保 workflow 的执行记录完整可追溯。

## 配置示例

```yaml
traits:
  share:
    strategy: default
```

```yaml
traits:
  share:
    strategy: ignore
```

JSON 请求示例：

```json
{
  "name": "proxy",
  "componentType": "webservice",
  "namespace": "default",
  "image": "example/proxy:1.0.0",
  "replicas": 1,
  "traits": {
    "share": {
      "strategy": "default"
    }
  }
}
```

## 验证示例

1) 首次执行工作流：资源正常创建。
2) 再次执行：
   - `default`：普通资源对应 Job 状态为 `skipped`；RBAC Job 正常调和。
   - `ignore`：Job 直接为 `skipped`。
   - `force`：Job 正常执行。

也可以通过 label 验证：

```bash
kubectl get deploy -n <namespace> -l eruun.io/share-name=<namespace>
kubectl get svc -n <namespace> -l eruun.io/share-name=<namespace>
kubectl get ingress -n <namespace> -l eruun.io/share-name=<namespace>
```

将 `<namespace>` 替换为组件所属命名空间（已转为小写并规范化）。

## 测试用例

- `pkg/apiserver/event/workflow/job/job_run_test.go`
  - 验证 `skipped` 的 Job 仍会写入 JobInfo。
- `pkg/apiserver/event/workflow/job/shared_test.go`
  - 验证 `default` 策略使用 label selector 判断共享资源，并返回可释放锁。
- `pkg/apiserver/event/workflow/share_test.go`
  - 验证 shareName 优先使用 `namespace`，并在空 namespace 时回退为 `component.name + component.type`。
