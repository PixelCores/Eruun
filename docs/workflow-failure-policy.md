# Workflow Failure Policy

> 状态：Current。本文描述 workflow 部署失败后的资源清理，以及即时 Job 的有界 OOM 重试。清理策略影响部署 job `failed` / `timeout` 的运行时资源，不删除 App、Workflow 或 Component DB 实体。

## 字段

`failurePolicy` 是 workflow 级字段，随现有 workflow `steps` JSON 一起存储，不新增表或 DB column。

| 值 | 默认 | 行为 |
| --- | --- | --- |
| `cleanup_all` | 是 | 任一部署类 job `failed` 或 `timeout` 后，workflow 失败，并为该 App 下全部 DB 已知组件生成 `cleanup_resources` job，清理整条 workflow 对应 App 的普通运行资源；standalone PVC 和五类 RBAC 保留。 |
| `cleanup_failed` | 否 | 显式 opt-out 策略。多个组件部署时，失败 job 只清理该 job 自己负责且已创建的普通运行资源，其他部署成功组件、standalone PVC 和五类 RBAC 保留。 |

### Job 组件例外

`type=job` 的组件可以在 `properties` 中显式设置 `failurePolicy: cleanup_failed`，让该组件生成的主 Kubernetes Job 在失败或超时后只执行现有局部清理，不触发 workflow 的 `cleanup_all`：

```json
{
  "action": "add",
  "name": "mysql-update-job",
  "type": "job",
  "image": "skeema-tool:latest",
  "properties": {
    "runPolicy": "recreate",
    "failurePolicy": "cleanup_failed",
    "env": {
      "SQL_URL": "https://oss.example.com/update.sql"
    }
  }
}
```

- 创建应用或通过 `/version add` 新增普通 `type=job` 组件时，省略或传空值表示继承 workflow 的 `failurePolicy`；Job 层只允许显式 `cleanup_failed`，不接受 `cleanup_all` 或其他值。
- `/version update` 继续使用 Properties 全量替换语义：省略整个 `properties` 会保留已有 Properties（包括已有 Job override）；显式携带 `properties` 时，省略或清空其中的 `failurePolicy` 会清除已有 override 并改为继承 workflow。
- 该字段只支持顶层 `type=job` 组件；`scheduledjob`、`cloudjob`、其他组件类型或 init container properties 即使显式传空值也会被拒绝。
- Job 显式值优先于 workflow 策略，但只覆盖该组件的主 `instant_job` 任务；PVC、RBAC 等附属资源任务仍继承 workflow 策略。RBAC Job 即使进入失败清理，其 `Clean` 也不会删除对象。
- `runPolicy` 与 `failurePolicy` 相互独立：前者控制同名 Kubernetes Job 的重建/复用，后者只控制失败后是否扩大为 workflow 全量清理。
- 字段随现有 Component `properties` JSON 持久化，不新增数据库列或 Kubernetes annotation。
- 模板 Job 覆盖遵循字段存在性：请求省略 `failurePolicy` 时保留模板值，显式传空值时清除模板 override 并继承 workflow，显式传 `cleanup_failed` 时覆盖模板值。

### 即时 Job 的 OOM 重试

即时 `type=job` 组件可通过 `properties.jobRetryPolicy` 显式启用失败策略。省略该对象时保持原有 Kubernetes Job 运行行为。此字段与最终失败后的 `failurePolicy` 清理范围相互独立；只有重试耗尽、资源上限阻止增长、非 OOM 失败或超时后，才进入最终失败处理。

| 字段 | 取值与行为 |
| --- | --- |
| `onOOM` | `stop`：停止；`retry`：保持原资源重试；`resize`：增大资源后重试。其他失败始终停止。 |
| `maxRetries` | `retry` / `resize` 必填，1–10 次额外执行；总执行次数最多为该值加 1。 |
| `backoffSeconds` | `retry` / `resize` 必填，1–3600 秒，固定退避。退避、执行和恢复等待均计入同一个持久化的绝对 Job 超时。 |
| `memoryGrowthFactor` | `resize` 必填，整数 2–4，增长 OOM 容器的 memory requests 和 limits。 |
| `cpuGrowthFactor` | 可选整数 0–4；省略、0、1 均保持 CPU 不变，2–4 才显式增长 OOM 容器的 CPU requests 和 limits。 |
| `maxResources` | Kubernetes quantity 对象，仅支持 `memory`、`cpu`。`resize` 必须提供正值 memory 上限；CPU 增长时还必须提供正值 cpu 上限。上限分别约束每个容器的 requests 和 limits。 |

`stop` 不接受其余重试或增长字段；`retry` 不接受资源增长字段。`resize` 要求 Job 模板中每个普通容器与 init container 对待增长资源都预先设置正数 requests 和 limits，并满足 requests ≤ limits ≤ 对应上限。下一次资源按当前值乘以相应因子；任何增长超过上限就停止，不缩减因子或静默截断。只修改本次 OOM 容器，其他容器及未显式选择增长的 CPU 保持原值。

下面的 Job 首次请求 128Mi 内存、限制 256Mi；两次 OOM 后分别变为 256Mi/512Mi、512Mi/1Gi，CPU 保持 100m/200m：

```json
{
  "action": "add",
  "name": "memory-batch",
  "type": "job",
  "image": "example/batch:1.0.0",
  "properties": {
    "runPolicy": "recreate",
    "failurePolicy": "cleanup_failed",
    "jobRetryPolicy": {
      "onOOM": "resize",
      "maxRetries": 2,
      "backoffSeconds": 10,
      "memoryGrowthFactor": 2,
      "maxResources": { "memory": "1Gi" }
    }
  },
  "traits": {
    "resources": {
      "cpu": "100m",
      "cpuLimit": "200m",
      "memory": "128Mi",
      "memoryLimit": "256Mi"
    }
  }
}
```

- `jobRetryPolicy` 只支持顶层即时 Job。`startTime` 非零、`scheduledjob`、CloudJob、Deployment 等组件，以及 init container properties 中的该字段，均被写入校验拒绝。Job 中的 init container 若确实 OOM，可由顶层策略增长其资源。
- OOM 证据必须来自同一个 Job UID 控制的 Failed Pod，且当前容器终止原因是 `OOMKilled`。单独的退出码 137、CPU throttling、eviction、上一次重启的 OOM、同名但其他 Job UID 的 Pod 均不会触发增长。注入到 Pod、但不存在于提交的 Job 模板中的 OOM 容器无法 resize，执行停止。
- 显式启用后设置 Kubernetes `backoffLimit=0`、`restartPolicy=Never`，由 Eruun 统一计数。已有未完成 Job 若要切换到该策略，必须显式 `runPolicy=recreate`；已完成 Job 仍可由 `skip_if_completed` 跳过。
- 策略随 Component properties 持久化并写入 `eruun.io/job-retry-policy` annotation；每次执行写入 `eruun.io/job-attempt`。实际资源与次数保存在原有 `JobInfo.InternalInfo` / `Attempt`，不会回写 Component 配置或修改 Deployment、StatefulSet、CronJob。
- 首次创建和每次重试之前保存受 workflow generation/token 保护的 checkpoint。恢复保留目标 Job、次数、退避时间、超时及 UID；删除旧 Job 使用 UID/resourceVersion 前置条件，等待旧 UID Job 消失及其 Pod 终止后才创建下一次。已确认创建的 UID 消失或被同名其他对象替换时拒绝自动重放，进入基础设施恢复处理。
- 启用策略的 Job 在创建前设置覆盖剩余绝对超时加原有一小时清理余量的 TTL，确保整个恢复期限内保留 Job 及 Pod 证据。执行成功或已确定失败后，先采集日志并持久化终态，再立即清理；保存失败保留原 UID 供新 Worker 恢复，保存后进程退出或清理失败则由 TTL 最终回收。取消或超时仍及时停止活动资源。历史无 TTL 的检查点在恢复时补齐该期限。策略省略时原有成功清理与 TTL 默认值保持不变。
- 用户取消会先落盘父工作流状态，再发布取消信号。重试控制器识别同一 generation/token/worker 的 `cancelled` 状态并停止执行，按检查点 UID 清理当前尝试；执行租约改变或同名资源 UID 不匹配时仍拒绝删除。
- 重试会再次运行该 Job 的完整程序；程序须自行保证外部写入可安全重复。调度器不能撤销一次失败执行已经写出的数据库、文件或第三方服务副作用。
- 模板请求省略 `jobRetryPolicy` 时保留模板策略，传入对象时整体覆盖；`/version update` 沿用 Properties 整体替换语义，显式提供的 properties 不含该对象时清除已有策略。

## 请求格式

创建应用时支持新的 workflow 对象写法。`workflow.steps[].components` 引用的组件必须同时出现在 `components` 中：

```json
{
  "name": "cleanup-all-app",
  "workflow": {
    "failurePolicy": "cleanup_all",
    "steps": [
      {
        "name": "deploy-api",
        "mode": "DAG",
        "components": ["api"]
      }
    ]
  },
  "components": [
    {
      "name": "api",
      "type": "webservice",
      "image": "nginx:latest",
      "replicas": 1,
      "properties": {},
      "traits": {}
    }
  ]
}
```

历史数组写法继续兼容；因为没有显式 `failurePolicy`，现在等价于默认 `cleanup_all`：

```json
{
  "name": "default-policy-app",
  "workflow": [
    {
      "name": "deploy-all",
      "mode": "DAG",
      "components": ["api", "worker", "mysql"]
    }
  ]
}
```

更新 workflow 时在请求顶层传入：

```json
{
  "name": "deploy-cleanup-all",
  "failurePolicy": "cleanup_all",
  "steps": [
    {
      "name": "deploy-all",
      "mode": "DAG",
      "components": ["api", "worker", "mysql"]
    }
  ]
}
```

更新已有 workflow 时，如果请求省略 `failurePolicy`，服务端会保留该 workflow 已存储的策略；历史数据缺少该字段时按默认 `cleanup_all` 解释。只有显式传入 `cleanup_failed` 才会 opt out 到只清理失败 job 的旧行为。

`GET /api/v1/applications/:appID/workflows` 会在 workflow 对象上回显 `failurePolicy`。`/applications/try` 和 `/applications/:appID/workflow/try` 会校验非法值。

完整请求示例见 `examples/workflow-failure-policy/`。

## 运行时边界

- 默认 `cleanup_all` 只在部署类 job 的 `failed` 或 `timeout` 终态触发；成功、取消、审批拒绝、callback 失败、`cleanup_resources` 自身失败不会扩大成全量清理。
- 显式 `cleanup_failed` 不改变 `runJob -> jobCtl.Clean` 的局部清理入口。示例：5 个组件中 3 个部署失败时，只清理这 3 个失败 job 已创建的普通运行资源，另外 2 个部署成功组件保留；standalone PVC 和五类 RBAC 始终保留。
- Job 级 `properties.failurePolicy=cleanup_failed` 只阻止该主 Job 自身触发 `cleanup_all`；workflow 仍以失败终止，Job 控制器仍清理本次执行创建的 Kubernetes Job。
- 并行任务中，只要任一 `failed` / `timeout` 任务的有效策略仍为 `cleanup_all`，就执行全量清理；Job 级 opt-out 不会掩盖同批次其他组件的失败。
- 并行混合失败时仍保留首个失败任务作为 workflow 主原因；若另一个任务实际触发 `cleanup_all`，终态和 callback `reason` 会追加 `cleanup_all triggered by job <name> (status=<status>)`。
- 全量清理复用现有 `cleanup_resources` job 按组件串行尝试全部 cleanup jobs；单个 cleanup job 失败不会中断后续组件清理，多个 cleanup 失败会聚合到 workflow 终态原因中。普通共享资源继续沿用 share 保护规则；RBAC 不依赖标签或 share 策略，始终保留。
- workflow cleanup 不删除 standalone PVC：显式 `claimName` PVC、标签命中的 PVC 和命名空间共享日志 PVC 都会保留。StatefulSet `volumeClaimTemplates` PVC 由 Kubernetes retention policy 决定；需要删除或重建数据库 PVC 时使用 `database-reset`。
- workflow cleanup 不删除 ServiceAccount、Role、RoleBinding、ClusterRole 或 ClusterRoleBinding；该边界同时覆盖失败局部回滚、组件移除和 `cleanup_all`。
- 清理只更新运行状态和 Kubernetes 运行资源，不删除 App、Workflow、Component DB 实体。
