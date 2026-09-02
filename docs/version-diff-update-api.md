# Version Diff Update API

> 状态：Current。当前路由为 `POST /api/v1/applications/:targetAppID/version/diff-update`。

该 API 用于把目标应用更新到源应用的已物化版本快照。源应用和目标应用都必须已经存在；如果源应用创建时引用过 `tmp.id`，这里比较的是创建后落库的完整组件结果。

## Endpoint

```http
POST /api/v1/applications/:targetAppID/version/diff-update
```

## Request

```json
{
  "sourceAppId": "app-version-101",
  "dryRun": true,
  "targetOnlyStrategy": "preserve",
  "strategy": "rolling",
  "executionScope": "changed_components",
  "workflowId": "wf-custom-update",
  "executeAt": 0,
  "autoExec": false,
  "description": "sync target to source version"
}
```

字段说明：

| 字段 | 必填 | 说明 |
|---|---|---|
| `sourceAppId` | 是 | 作为期望状态的源应用 ID |
| `dryRun` | 否 | `true` 时只返回差异，不写库、不触发工作流；默认 `false` |
| `targetOnlyStrategy` | 否 | 目标应用独有组件的处理策略：`preserve`（默认保留）、`remove`（执行时删除）、`block`（存在目标独有组件时阻断执行） |
| `strategy` / `executionScope` / `workflowId` / `executeAt` / `autoExec` / `description` | 否 | 执行时复用现有版本更新语义；`executionScope=changed_components` 会透传到内部 `/version` 请求 |

## Behavior

- 目标应用的新版本号使用源应用的 `version`。
- 同名同类型组件会比较 `image`、`replicas`、`properties`、`traits`，差异会生成 update。
- 源应用有、目标应用没有的组件会生成 add。
- 目标应用有、源应用没有的组件进入 `extraComponents`，由 `targetOnlyStrategy` 决定处理方式。
- 同名但 `type` 不一致的组件进入 `blockedComponents`；非 dry-run 执行会失败。
- `store` 的 update 差异会复用 `/version` 的 StatefulSet 不可变字段预检。若源快照会修改目标 StatefulSet 的 `serviceName`、`selector` 或 `volumeClaimTemplates` 不可变规格，该组件从 `updatedComponents` 移入 `blockedComponents`，`action=block`、`executable=false`，`reason` 给出需要显式迁移或重建的字段提示；预检只修改内部执行副本，dry-run 返回的 `before`/`after` 仍分别是目标/源快照，也不会把实际执行必然拒绝的变更误报为可执行。
- 若目标应用存在未完成的 StatefulSet cleanup v2/v3，且 diff 会生成组件 update/add/remove 动作，dry-run 返回 `executable=false`，并在对应动作的 `reason` 中提示先恢复未完成的迁移；只有版本号变化、没有组件动作时不受该 fence 影响。
- `dryRun=false` 且无阻断时，服务会把差异转换为现有 `UpdateVersionRequest` 并执行；`executionScope` 透传到该请求，默认仍为 `full_workflow`。

### Target-only Strategies

| 策略 | 行为 |
|---|---|
| `preserve` | 默认策略。目标独有组件只报告到 `extraComponents`，不会写库或删除资源。 |
| `remove` | 目标独有组件会生成 remove 操作，执行时复用现有 `UpdateVersion` 删除路径。 |
| `block` | dry-run 返回差异；非 dry-run 如果存在目标独有组件则失败，不做任何更新。 |

## Response

```json
{
  "targetAppId": "app-current",
  "sourceAppId": "app-version-101",
  "targetPreviousVersion": "1.0.0",
  "targetVersion": "1.0.1",
  "sourceVersion": "1.0.1",
  "dryRun": true,
  "targetOnlyStrategy": "preserve",
  "versionChanged": true,
  "hasChanges": true,
  "executable": true,
  "updatedComponents": [
    {
      "action": "update",
      "name": "backend",
      "type": "webservice",
      "fields": [
        {"field": "image", "before": "backend:v1.0.0", "after": "backend:v1.0.1"}
      ]
    }
  ],
  "addedComponents": [
    {
      "action": "add",
      "name": "worker",
      "type": "job"
    }
  ],
  "extraComponents": [
    {
      "action": "preserve",
      "name": "legacy",
      "reason": "target-only component is preserved"
    }
  ]
}
```

执行成功时，响应会额外包含 `updateResult`，结构与现有 `POST /api/v1/applications/:appID/version` 一致。

更多请求体示例见 `examples/version-diff-update/`。
