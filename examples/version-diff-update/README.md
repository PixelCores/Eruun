# Version Diff Update Examples

这些示例是 `POST /api/v1/applications/:targetAppID/version/diff-update` 的请求体。

JSON 文件保持为严格请求体，不写注释或 `_comment` 字段，这样可以直接用于 `curl -d @...`。策略含义见 [strategy.md](strategy.md)。

## Endpoint

```http
POST /api/v1/applications/:targetAppID/version/diff-update
```

## Path 参数

| 参数 | 必填 | 说明 |
|---|---|---|
| `targetAppID` | 是 | 需要被更新的目标应用 ID。目标应用会被对比并在执行模式下更新到源应用版本。 |

## Body 入参

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `sourceAppId` | string | 是 | 无 | 作为期望状态的源应用 ID。接口会读取源应用已经落库的完整组件快照。 |
| `dryRun` | boolean | 否 | `false` | `true` 时只返回差异，不写库、不触发工作流、不清理 Kubernetes 资源。 |
| `targetOnlyStrategy` | string | 否 | `preserve` | 目标应用独有组件的处理策略。可选值：`preserve`、`block`、`remove`。 |
| `strategy` | string | 否 | `rolling` | 执行更新时复用现有版本更新策略。可选值沿用版本更新接口，例如 `rolling`、`recreate`、`canary`、`blue-green`。 |
| `workflowId` | string | 否 | 空 | 指定自动执行的工作流。仅在 `autoExec=true` 且存在组件变更时使用。 |
| `executeAt` | number | 否 | `0` | 延迟执行时间，Unix 秒。仅在 `autoExec=true` 且存在组件变更时生效。 |
| `autoExec` | boolean | 否 | `true` | 是否在更新后自动执行工作流。 |
| `description` | string | 否 | 空 | 更新说明；执行模式下会透传给现有版本更新流程。 |

## targetOnlyStrategy

`targetOnlyStrategy` 只处理一种差异：目标应用有某个组件，但源应用没有这个组件。

| 策略 | 行为 |
|---|---|
| `preserve` | 默认策略。目标独有组件进入 `extraComponents`，`action=preserve`。不会删除这些组件，也不会因为这些组件清理 Kubernetes 资源；如果同时存在其他可执行差异，执行模式仍会更新那些差异。 |
| `block` | 目标独有组件进入 `extraComponents`，`action=block`。`dryRun=true` 时只报告差异；`dryRun=false` 且存在目标独有组件时，接口失败且不执行更新。 |
| `remove` | 目标独有组件进入 `extraComponents`，`action=remove`。`dryRun=false` 时会生成 remove 操作，复用现有版本更新删除路径。 |

## 返回参数

| 字段 | 类型 | 说明 |
|---|---|---|
| `targetAppId` | string | 目标应用 ID。 |
| `sourceAppId` | string | 源应用 ID。 |
| `targetPreviousVersion` | string | 目标应用更新前版本。 |
| `targetVersion` | string | 目标应用将要更新到的版本；执行成功后等于实际更新后的版本。 |
| `sourceVersion` | string | 源应用版本。 |
| `dryRun` | boolean | 是否为预览模式。 |
| `targetOnlyStrategy` | string | 本次请求实际使用的目标独有组件处理策略。 |
| `versionChanged` | boolean | 目标版本和源版本是否不同。 |
| `hasChanges` | boolean | 是否存在任意差异，包括版本差异、组件差异、目标独有组件或阻断项。 |
| `executable` | boolean | 当前差异是否可执行。存在类型不匹配或 `targetOnlyStrategy=block` 且有目标独有组件时为 `false`。 |
| `updatedComponents` | array | 同名同类型组件的字段差异。 |
| `addedComponents` | array | 源应用有、目标应用没有的组件。执行模式下会新增。 |
| `extraComponents` | array | 目标应用有、源应用没有的组件。处理方式由 `targetOnlyStrategy` 决定。 |
| `blockedComponents` | array | 阻断执行的组件差异，例如同名但组件类型不同。 |
| `updateResult` | object | `dryRun=false` 且实际执行更新时返回；结构与现有 `POST /api/v1/applications/:appID/version` 一致。 |

## 组件差异字段

`updatedComponents`、`addedComponents`、`extraComponents`、`blockedComponents` 中的元素使用相同结构：

| 字段 | 类型 | 说明 |
|---|---|---|
| `action` | string | 差异动作，例如 `update`、`add`、`remove`、`preserve`、`block`。 |
| `name` | string | 组件名称。 |
| `type` | string | 组件类型。 |
| `reason` | string | 差异原因，主要用于 preserve、block、类型不匹配等场景。 |
| `fields` | array | 字段级差异，仅 update 场景常见。 |
| `before` | object | 目标应用中的组件状态。 |
| `after` | object | 源应用中的组件状态。 |

`fields` 中的元素：

| 字段 | 类型 | 说明 |
|---|---|---|
| `field` | string | 发生变化的字段，目前包括 `image`、`replicas`、`properties`、`traits`。 |
| `before` | any | 更新前值。 |
| `after` | any | 更新后值。 |

`before` / `after` 中的组件状态：

| 字段 | 类型 | 说明 |
|---|---|---|
| `name` | string | 组件名称。 |
| `type` | string | 组件类型。 |
| `image` | string | 镜像地址。 |
| `replicas` | number | 副本数。 |
| `properties` | object | 组件属性。 |
| `traits` | object | 组件特性。 |

## 示例假设

- `targetAppID` 从 URL path 传入，例如 `app-current`
- `sourceAppId` 是期望同步到的完整应用快照，例如 `app-version-101`
- 如果源应用创建时引用过 `tmp.id`，该 API 比较的是源应用创建后已经落库的完整组件结果

## 示例文件

| 文件 | 场景 |
|---|---|
| `01-preserve-dry-run.json` | `targetOnlyStrategy=preserve`。默认安全预览：目标独有组件只报告，不删除。 |
| `02-block-dry-run.json` | `targetOnlyStrategy=block`。生产确认：目标独有组件会让执行变为不可执行，先 dry-run 看差异。 |
| `03-remove-execute.json` | `targetOnlyStrategy=remove`。严格同步：目标独有组件会在执行时删除，并复用现有版本更新流程。 |

## 示例请求

```bash
curl -X POST "$ERUUN_API_URL/api/v1/applications/app-current/version/diff-update" \
  -H "Content-Type: application/json" \
  -d @examples/version-diff-update/01-preserve-dry-run.json
```
