# Workflow Failure Policy

> 状态：Current。本文描述 workflow 部署失败后的资源清理策略。该策略只影响部署 job `failed` / `timeout` 的运行时清理，不删除 App、Workflow 或 Component DB 实体。

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
