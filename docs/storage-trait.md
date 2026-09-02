# Storage Trait

> 状态：Current。本文说明 `traits.storage[]` 的当前公共契约，尤其是 `subPath` 与 `subPathExpr` 的区别和使用边界。

## 字段契约

`traits.storage[]` 用于把 PVC、emptyDir、ConfigMap 或 Secret 挂载到主容器、init container 或 sidecar container。

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `name` | string | 否 | Eruun 侧的 Volume 名称。为空时部分存储类型会使用来源资源名。 |
| `type` | string | 是 | 支持 `persistent`、`ephemeral`、`config`、`secret`。 |
| `mountPath` | string | 是 | 容器内挂载路径。 |
| `subPath` | string | 否 | 固定子目录，渲染到 Kubernetes `VolumeMount.SubPath`。 |
| `subPathExpr` | string | 否 | 带环境变量展开的子目录，渲染到 Kubernetes `VolumeMount.SubPathExpr`。 |
| `readOnly` | bool | 否 | 是否只读挂载。 |
| `sourceName` | string | 否 | `config`/`secret` 类型引用的 ConfigMap 或 Secret 名称。 |
| `claimName` | string | 否 | `persistent` 且 `tmpCreate=false` 时使用的 standalone PVC 名称。为空时使用 `name`。 |
| `tmpCreate` | bool | 否 | `persistent` 类型是否由 StatefulSet `volumeClaimTemplates` 创建 PVC。 |
| `size` | string | 否 | 创建缺失 PVC 时使用的容量，默认按现有 storage trait 规则处理。若同名 PVC 已存在，不会按该字段更新已有 PVC。 |
| `storageClass` | string | 否 | 创建缺失 PVC 时使用的 StorageClass。若同名 PVC 已存在，不会按该字段更新已有 PVC。 |

`subPath` 和 `subPathExpr` 不能同时配置。Kubernetes 对同一个 `VolumeMount` 也要求二者互斥；Eruun 会在 validation 阶段拒绝同时设置的请求。

## PVC 归属语义

- `tmpCreate=true`：Eruun 将 PVC 模板写入 StatefulSet `volumeClaimTemplates`，由 Kubernetes 为每个 Pod 创建实际 PVC。
- `tmpCreate=false` 且未填写 `claimName`：Eruun 创建或复用名为 `name` 的 standalone PVC。
- `tmpCreate=false` 且显式填写 `claimName`：Eruun 创建或复用名为 `claimName` 的 standalone PVC。
- standalone PVC 部署时如果同名 PVC 已存在，Eruun 不更新容量、StorageClass、AccessModes 或其他 spec，直接视为成功。
- workflow cleanup 和同步资源 cleanup 都不主动删除 standalone PVC；显式 `claimName` PVC、标签命中的 PVC、命名空间共享日志 PVC 都会保留。
- `tmpCreate=true` 的 StatefulSet `volumeClaimTemplates` PVC 不属于 standalone PVC；StatefulSet 删除后的 PVC 生命周期由 Kubernetes retention policy 决定。
- `database-reset` 是独立的数据库重置契约，仍会按目标组件删除或重建数据库 PVC。

## `subPathExpr` 来源

`subPathExpr` 是为 PaaS workflow 试玩环境日志挂载补齐的 storage trait 能力。PaaS 需要把业务容器日志 PVC 挂载到按运行时环境变量隔离的目录：

```text
$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)
```

其中 `POD_IP` 是 Pod 创建后的运行时字段，PaaS 在生成 Eruun 请求时无法提前算出完整固定路径；把该表达式写入 `subPath` 也不会触发 Kubernetes 环境变量展开。因此 Eruun 需要正式接收 `traits.storage[].subPathExpr`，并把它渲染到 Kubernetes `VolumeMount.SubPathExpr`。

## 示例

```json
{
  "storage": [
    {
      "name": "logs",
      "type": "persistent",
      "claimName": "developer-pvc",
      "mountPath": "/app/log",
      "subPathExpr": "$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)"
    }
  ]
}
```

生成的容器挂载语义为：

- `mountPath` 写入容器内路径 `/app/log`。
- `claimName` 指定 standalone PVC 名 `developer-pvc`。如果它不存在，Eruun 会创建；如果它已存在，Eruun 不会按默认容量更新它。
- `subPathExpr` 写入 Kubernetes `VolumeMount.SubPathExpr`，由 kubelet 在 Pod 创建时按容器环境变量展开。

固定目录仍使用 `subPath`：

```json
{
  "storage": [
    {
      "name": "data",
      "type": "persistent",
      "mountPath": "/var/lib/mysql",
      "subPath": "mysql"
    }
  ]
}
```

## 发布依赖

发送 `subPathExpr` 的调用方必须确认目标环境的 Eruun 版本包含该字段契约。旧版本使用严格 JSON 绑定，可能会直接拒绝带有未知 `subPathExpr` 字段的 create/create-and-exec 请求；即使绕过绑定，旧版本也不会把该字段渲染到 Kubernetes YAML。
