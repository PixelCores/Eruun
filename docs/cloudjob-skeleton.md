# CloudJob 说明

> 状态：Implemented Reference。本文说明当前 `cloudjob` 组件类型与内置 provider 结构。

## 背景

`cloudjob` 是一个新的组件类型，用于在工作流中执行云厂商 API 调用（例如先创建云侧基础资源，再执行后续配置/部署步骤）。

当前版本提供：

- 统一契约层（`contracts`）约束 provider/action/runtime 行为
- provider 通过统一 `CloudRuntime` 接口适配不同云厂商运行时依赖
- 内置 `aliyun` provider（目录化拆分 action 实现）
- provider 未注册或 action 不在白名单时统一 fail-fast
- `aliyun` provider 已接入真实阿里云 NAS SDK 和 Kubernetes StorageClass 创建逻辑

## 代码组织

- `pkg/apiserver/event/workflow/cloudjob/contracts`：接口、请求/响应、进度结构、通用参数拷贝
- `pkg/apiserver/event/workflow/cloudjob`：provider 注册中心与内置 provider 装配
- `pkg/apiserver/event/workflow/cloudjob/aliyun`：aliyun provider、action registry、action 分文件实现

## 组件类型与字段

`component.type` 新增支持：

- `cloudjob`

`cloudjob` 组件字段约束：

- `image` 非必填
- `properties.cloud.provider` 必填
- `properties.cloud.action` 必填
- `properties.cloud.params` 可选（map）

示例结构：

```json
{
  "name": "bootstrap-cloud",
  "type": "cloudjob",
  "properties": {
    "cloud": {
      "provider": "aliyun",
      "action": "create-ecs",
      "params": {
        "region": "cn-hangzhou",
        "instanceType": "ecs.g6.large"
      }
    }
  }
}
```

## 模板自动分阶段顺序

模板自动分阶段顺序调整为：

1. `phase-1-job`：仅 `cloudjob`
2. `phase-2-config-secret`
3. `phase-3-store`
4. `phase-4-job`
5. `phase-5-webservice`

用途：保证云侧基础资源可优先创建，再执行设置与工作负载部署。

## 执行行为

- `cloudjob` 通过 `provider + action` 分发到 provider 实现
- provider 先初始化对应云厂商 runtime（统一接口），再通过 `ActionRegistry` 解析 action
- action 支持状态机推进（`state + requeueAfter`），可表达多阶段流程
- provider 未注册或 action 不在 provider 白名单会返回错误并标记任务失败
- 执行记录会写入 `job.Info` 并持久化到任务记录

## 内置 action（aliyun provider）

当前内置了以下阿里云 action（代码内置映射）：

- `aliyun.nas.ensure_filesystem`
- `aliyun.nas.ensure_mount_target`
- `aliyun.k8s.ensure_storage_class`

其中：

- `aliyun.nas.ensure_mount_target` 在创建挂载点后，会调用 `aliyun.nas.describe_mount_target` 查询状态，只有当 `mountTargetStatus=active` 且存在 `mountTargetConfirmInfo` 时才完成。
- `aliyun.k8s.ensure_storage_class` 会复用上述状态查询结果，确认挂载点就绪后再创建 StorageClass。
- `pollIntervalSeconds`（可选）可控制等待中的轮询间隔，默认 15 秒。
- 任一步骤失败会直接返回错误并中断当前 `cloudjob`。

## 阿里云前置条件

- 阿里云凭证和默认拓扑统一从 `system_setting.type=aliyunCloud` 加载，不再从默认凭证链或 `cloud.params` 回退。
- `aliyunCloud` 当前支持字段：
  - `accessKeyId`
  - `accessKeySecret`
  - `endpoint`
  - `regionId`
  - `zoneId`
  - `vpcId`
  - `vswId`
- `cloud.params` 中如果继续传 `regionId`、`zoneId`、`vpcId`、`vswId`，会直接报错，不做兼容兜底。
- `aliyun.nas.ensure_filesystem` 会用 `tenantId` 给 NAS 文件系统打 tag（`tenantId=<value>`），后续步骤依赖这个 tag 做租户级定位。

## 参数约束（aliyun provider）

`aliyun.nas.ensure_filesystem`：

- 必填：`tenantId`、`storageType`、`protocolType`
- 可选：`capacityGiB`、`fileSystemType`、`description`
- `zoneId` 默认从 `aliyunCloud.zoneId` 读取；若未配置，则按阿里云接口默认行为处理

`aliyun.nas.ensure_mount_target`：

- 必填：`tenantId`
- 可选：`accessGroupName`、`securityGroupId`、`pollIntervalSeconds`
- `vpcId`、`vswId` 从 `aliyunCloud` 读取；缺失时直接报错
- 未显式提供 `accessGroupName` 时，当前实现会使用阿里云 VPC 默认权限组 `DEFAULT_VPC_GROUP_NAME`

`aliyun.k8s.ensure_storage_class`：

- 必填：`tenantId`、`storageClassName`
- 可选：`reclaimPolicy`、`volumeBindingMode`、`serverPath`、`pollIntervalSeconds`
- 创建出的 StorageClass 使用：
  - `provisioner=nasplugin.csi.alibabacloud.com`
  - `parameters.server=<mountTargetDomain>:<serverPath>`
  - `parameters.volumeAs=subpath`
- 若同名 StorageClass 已存在但与上述契约不一致，流程会直接报错，不做静默覆盖

## 边界（当前版本）

- 不做跨步骤变量透传（建议按 `tenantId` 从云侧状态来源读取）
- 不引入云资源反向清理逻辑
- 当前只实现 NAS 文件系统、挂载点以及对应的 Kubernetes StorageClass 引导链路
