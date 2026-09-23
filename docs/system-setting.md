# 系统设置

> 状态：Current。所有 `/api/v1/settings` 接口必须 Bearer 登录且为系统管理员。账号、OAuth、会话及空间策略由部署 Secret 配置，见 [账号与空间](account-auth-workspaces.md)。

## 数据与接口

`eruun_system_setting` 使用 `type` 唯一键和 JSON `value`。当前支持六种配置：

| type | value 契约 |
| --- | --- |
| `nodeSelector` | 调度 nodeSelector、affinity、tolerations 对象或 profile 数组 |
| `rbacPolicies` | RBAC policy 数组，面向系统管理；普通空间拒绝 RBAC trait |
| `aliyunCloud` | 云适配器配置；普通空间拒绝 CloudJob |
| `urlSecurityPolicy` | 服务端出站 URL 私网白名单，见 [URL 安全策略](url-security-policy.md) |
| `podRestartMonitor` | `{enabled,windowSeconds,threshold}`，默认 true、1800、3 |
| `workflow_scheduler` | 全局 Job 准入、评测时长和创建速率策略，见下文；不可删除 |

| 方法 / 路径 | 请求 / 返回 |
| --- | --- |
| `GET /api/v1/settings` | `data.settings` 配置列表 |
| `GET /api/v1/settings/:type` | 单条配置，不存在时返回 404 |
| `POST /api/v1/settings` | `{type,value}` 创建 |
| `PUT /api/v1/settings/:type` | `{value}` 更新 |
| `DELETE /api/v1/settings/:type` | 删除指定配置 |

例如：

```sh
curl --fail "$SERVER/api/v1/settings/urlSecurityPolicy" \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

`aliyunCloud` 包含 accessKeyId、accessKeySecret、endpoint、regionId、zoneId、vpcId、vswId。读取只返回掩码密钥，写入必须提供真实值，不能提交掩码。保存前调用阿里云 NAS 只读接口验证凭据和默认区域，失败不落库。该云配置和账号登录短信配置相互独立。

运行时只自动补齐 `urlSecurityPolicy`、`podRestartMonitor` 和 `workflow_scheduler` 默认记录。`scripts/init-system-setting.sql` 可幂等补齐其余设置，保留现有值。前端字段和保存交互见 [设置页接入](settings-page-api.md)。

旧认证设置类型、JWT 角色映射及动态 authz 路由接口已移除；新接入必须使用统一账号和空间 API。

## Job 全局调度

`workflow_scheduler` 的 `strategy` 支持 `priority` 和 `fifo`；全局并发范围 1..10000，空间并发范围 1..全局并发，老化间隔范围 1..86400 秒。省略字段使用默认值，未知字段和 null 被拒绝。更新后下轮准入生效；降低上限不终止正在执行的 Job。

`maxEvaluationTimeoutSeconds` 默认 1209600（14 天），管理员可在线设置为 60..1209600。每个评测的 `traits.eval.timeoutSeconds` 默认仍为 3600；提交和首次构建执行时检查当前上限。降低上限后，尚未构建的执行可能被拒绝，已提交运行检查点的执行在恢复时保留原期限。Harbor 任务作者设置的 agent/verifier 超时仍独立生效；普通 command 的 1..86400 秒范围不变。

`resourceCreationQPS` 默认 5，范围 0.1..1000；`resourceCreationBurst` 默认 10，范围 1..1000。所有 API/Worker 副本的 command/评测 Runner Job 与 trial Sandbox 创建尝试共用数据库预算，重试也消耗额度，关联已存在资源不消耗创建额度。此预算与客户端发往 Kubernetes 的 QPS/Burst、已运行并发和空间 ResourceQuota 分别计量。预算债务持久化，重启或写入策略不会重新获得一轮突发额度；调小配置可能先等待已有债务消退。默认值是保守的部署起点，不能据此宣称 ACR 拉取速率或目标容量已验证。

```sh
curl --fail --request PUT "$SERVER/api/v1/settings/workflow_scheduler" \
  -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  --data '{"value":{"strategy":"priority","maxConcurrentJobs":100,"maxConcurrentJobsPerWorkspace":10,"agingSeconds":60,"maxEvaluationTimeoutSeconds":1209600,"resourceCreationQPS":5,"resourceCreationBurst":10}}'
```

`maxStartingSandboxes` 默认 100，范围 1..10000，约束所有空间、副本的启动中 Sandbox。取得额度后，创建结果不确定、等待 Pod/Ready 时继续占位；Ready 或确定停止/清理后释放启动额度。保留中的环境仍占运行资源预算。下调不驱逐已有实例，在占用降至新限额前暂停新启动；响应中的 `starting_capacity` 与 `creation_rate_limited` 分别说明容量与速率等待。完整更新示例可在上述 value 中增加 `"maxStartingSandboxes":100`；省略使用默认值。

并发槽位、声明资源预算和实际 Kubernetes 配额分别生效，不能把 `maxConcurrentJobs` 当作 Pod 数。步骤 `schedulingClass` 的继承、排序、失败重试和升级边界见 [Job 全局调度](workflow-global-scheduler-design.md)。

升级时先完成所有 Server 读方升级，再保存新增字段并启用新 Runner。旧 Server 会拒绝不认识的策略字段；混用旧读方与新字段不能作为可用的滚动部署状态。更新是完整 value 替换，保留需要的现有字段，不把省略字段当 PATCH。
