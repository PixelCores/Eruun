# 组件容器信息查询 API

> 状态：Current。当前路由为 `GET /api/v1/applications/:appID/components/:componentName/containers`。

## 接口说明
- 方法：`GET`
- 路径：`/api/v1/applications/:appID/components/:componentName/containers`
- 作用：按组件返回其当前 Pod 下的普通容器（`spec.containers`）信息。

## 返回语义
- 组件为 Pod 型（如 Deployment/StatefulSet/Job/ScheduledJob）：
  - 返回该组件标签匹配的 Pod 列表（过滤删除中的 Pod）。
  - 每个 Pod 下返回普通容器列表，包含基础信息和运行状态。
- 组件为非 Pod 型（如 `config`、`secret`）：
  - 返回 `200`，`pods` 为空数组。
- Pod 型组件当前无 Pod：
  - 返回 `200`，`pods` 为空数组。

## 示例文件

- 调用示例：`examples/component-containers/README.md`
- Pod 型组件返回示例：`examples/component-containers/list-component-containers-response.json`
- 非 Pod 型/无 Pod 返回示例：`examples/component-containers/list-component-containers-empty-response.json`

## 字段说明
- 顶层：
  - `appId`：应用 ID
  - `componentName`：组件名称
  - `componentType`：组件类型
  - `pods`：Pod 维度结果列表
- `pods[*]`：
  - `podName`：Pod 名称
  - `namespace`：Pod 命名空间
  - `phase`：Pod Phase（如 `Running`、`Pending`）
  - `containers`：容器信息列表（仅普通容器）
- `containers[*]`：
  - `name`：容器名
  - `image`：镜像名
  - `ready`：是否 Ready
  - `restartCount`：重启次数
  - `state`：运行态（`running` / `waiting` / `terminated` / `unknown`）
  - `reason`：等待或终止原因（可选）
