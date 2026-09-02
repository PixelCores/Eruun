# Rollout Trait


`rollout` 是组件级 trait，用于控制长期运行 workload 的发布/更新策略。

未配置 `traits.rollout` 时，Eruun 不声明 Kubernetes workload 的更新策略；已有 Deployment 上残留的非默认策略会在 reconcile 时被重置，使组件回到 Kubernetes 默认滚动更新行为。

## 适用范围

- `webservice`：渲染为 Kubernetes `Deployment.spec.strategy`。
- `store`：渲染为 Kubernetes `StatefulSet.spec.updateStrategy`。
- `job`、`scheduledjob`、`config`、`secret`、`cloudjob` 不支持 `rollout`；配置后会在校验阶段报错。
- `rollout` 只能放在组件顶层 `traits.rollout`，不能放在 `init[].traits` 或 `sidecar[].traits` 中。

## 字段

```json
{
  "traits": {
    "rollout": {
      "type": "RollingUpdate",
      "rollingUpdate": {
        "maxSurge": "25%",
        "maxUnavailable": "25%",
        "partition": 0
      }
    }
  }
}
```

### webservice

- `type` 支持 `RollingUpdate`、`Recreate`。
- `RollingUpdate` 必须配置 `rollingUpdate.maxSurge` 与 `rollingUpdate.maxUnavailable`。
- `Recreate` 不允许配置 `rollingUpdate`。
- `partition` 不适用于 Deployment。
- `rollingUpdate.maxSurge` 与 `rollingUpdate.maxUnavailable` 不能同时为数值 `0`。

### store

- `type` 支持 `RollingUpdate`、`OnDelete`。
- `RollingUpdate` 支持 `rollingUpdate.partition` 与 `rollingUpdate.maxUnavailable`。
- `OnDelete` 不允许配置 `rollingUpdate`。
- `maxSurge` 不适用于 StatefulSet。
- `rollingUpdate.maxUnavailable` 配置后必须大于 `0`。

## 校验规则

- `rollout` 只能放在组件顶层 `traits.rollout`。放在 `init[].traits` 或 `sidecar[].traits` 中会报错。
- 不支持 `rollout` 的组件类型配置该 trait 会报错。
- `webservice` 的 `RollingUpdate` 必须显式提供 `rollingUpdate.maxSurge` 与 `rollingUpdate.maxUnavailable`，不能只写 `{ "type": "RollingUpdate" }` 或 `rollingUpdate: {}`。
- `IntOrString` 数值字段支持非负 JSON 整数或百分比字符串，例如 `1`、`"25%"`；`"1"`、`"0"` 这类数字字符串会被拒绝。
- 零值判断会先解析数值；JSON 整数 `0`、`"00%"`、`"-0%"` 等解析为 `0` 的写法都会被 zero guard 拦截。

## 示例

### Deployment 滚动更新

```json
{
  "traits": {
    "rollout": {
      "type": "RollingUpdate",
      "rollingUpdate": {
        "maxSurge": "25%",
        "maxUnavailable": 0
      }
    }
  }
}
```

### Deployment 重建更新

```json
{
  "traits": {
    "rollout": {
      "type": "Recreate"
    }
  }
}
```

### StatefulSet 滚动更新

```json
{
  "traits": {
    "rollout": {
      "type": "RollingUpdate",
      "rollingUpdate": {
        "partition": 1,
        "maxUnavailable": 1
      }
    }
  }
}
```

### 非法示例

```json
{
  "traits": {
    "rollout": {
      "type": "RollingUpdate"
    }
  }
}
```

`webservice` 中该写法会被拒绝，因为 `RollingUpdate` 必须提供 `rollingUpdate.maxSurge` 与 `rollingUpdate.maxUnavailable`。

## convert 行为


## Reconcile 行为

- `GenerateWebService` 和 `GenerateStoreService` 只在配置了 `traits.rollout` 时写入 Kubernetes strategy。
- Deployment 更新前会比较当前资源与期望资源。若移除了 `traits.rollout`，期望 strategy 为空，Eruun 会把当前非默认 Deployment strategy 视为需要重置的差异。
- 已经是 Kubernetes 默认滚动策略的 Deployment 不会因为 API Server 默认化出的 `maxSurge/maxUnavailable=25%` 被反复 patch。
