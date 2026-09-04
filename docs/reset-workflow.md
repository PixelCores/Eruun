# 显式 Reset Workflow

> 状态：Deprecated。本文仅作为“指定组件 reset workflow”兼容参考；新接入不要把它当作全量清理或全量重建主路径。

显式 Reset Workflow 仍可用于“清理指定组件资源，再重新部署指定组件”的顺序。全量清理、全量部署或清理后重建的主路径是 `/api/v1/applications/:appID/version` 的保留组件动作（`remove cleanup_all` / `add all`），由 `/version` 在提交版本、组件变更和 workflow task 时统一处理。

## 基本结构

```json
{
  "name": "reset-web",
  "alias": "Reset web component",
  "workflowType": "workflow",
  "workflow": [
    {
      "name": "cleanup-web",
      "jobType": "cleanup_resources",
      "mode": "StepByStep",
      "components": ["web"]
    },
    {
      "name": "deploy-web",
      "jobType": "deploy",
      "mode": "StepByStep",
      "components": ["web"]
    }
  ]
}
```

调用入口：

```bash
APP_ID="your-app-id"

curl -sS -X PUT "http://127.0.0.1:8000/api/v1/applications/${APP_ID}/workflow" \
  -H "Content-Type: application/json" \
  -d @examples/reset-workflow/01-reset-single-component-workflow.json
```

返回值里的 `workflowId` 用于执行：

```bash
curl -sS -X POST "http://127.0.0.1:8000/api/v1/applications/${APP_ID}/workflow/exec" \
  -H "Content-Type: application/json" \
  -d '{"workflowId":"<workflowId>"}'
```

## 字段说明

| 字段 | 说明 |
|------|------|
| `workflowType` | 外层工作流分类。显式 reset workflow 使用 `workflow`。 |
| `workflow[].jobType` | Step 内部执行类型。清理指定组件使用 `cleanup_resources`，部署步骤使用 `deploy`，也可以省略。 |
| `workflow[].mode` | `StepByStep` 表示顺序执行，`DAG` 表示同一步里的组件并发执行。 |
| `workflow[].components` | 当前 Step 引用的应用组件名。组件必须已存在。 |

## 执行语义

1. `cleanup_resources` Step 会为指定组件生成低优先级清理 Job。
2. 清理 Job 会删除组件生成或归属的普通 Kubernetes 运行资源，包括 Deployment、StatefulSet、Job、CronJob、默认 Service、`traits.service` 显式定义的 Service、ConfigMap、Secret 和 Ingress。standalone PVC 与 ServiceAccount、Role、RoleBinding、ClusterRole、ClusterRoleBinding 永久保留；StatefulSet `volumeClaimTemplates` PVC 的生命周期由 Kubernetes retention policy 决定。
3. 清理 Job 会等待目标资源消失；对于 workload 类组件，还会等待组件 Pod 消失。
4. 清理成功后组件状态会回到 `Not Deploy`。
5. 只有清理 Step 成功后，后续部署 Step 才会开始。
6. 如果清理失败、取消或超时，workflow 会停在失败状态，后续部署不会启动。

## 多组件示例

```bash
APP_ID="your-app-id"

curl -sS -X PUT "http://127.0.0.1:8000/api/v1/applications/${APP_ID}/workflow" \
  -H "Content-Type: application/json" \
  -d @examples/reset-workflow/02-reset-multi-component-workflow.json
```

`02-reset-multi-component-workflow.json` 中第一步使用 `DAG` 并发清理 `api`、`worker`、`redis`。后续步骤按声明顺序部署配置、状态组件和无状态工作负载。

## 全量清理与全量部署

当调用方需要清理当前应用全部 DB 已知组件时，使用 `/version` 创建可观察的 workflow task；`/version` 接口本身不会直接删除 Kubernetes 资源：

```json
{
  "version": "1.1.0",
  "components": [
    {
      "action": "remove",
      "name": "cleanup_all"
    }
  ]
}
```

需要清理后重新部署全部组件时，在同一次 `/version` 请求里组合 `remove cleanup_all` 与 `add all`：

```json
{
  "version": "1.1.0",
  "workflowId": "wf-deploy-all",
  "components": [
    {
      "action": "remove",
      "name": "cleanup_all"
    },
    {
      "action": "add",
      "name": "all"
    }
  ]
}
```

完整示例见 `examples/version-update/12-cleanup-all-resources.json`、`13-deploy-all-components.json`、`14-recreate-all-components.json`。

## 适用场景

- 重建某个组件的普通 Kubernetes 运行资源；standalone PVC 和五类 RBAC 保持不变。
- 删除旧 workload、Service 或 Ingress 后重新部署；standalone PVC 和五类 RBAC 不属于 reset 删除范围。
- 将原来的“先调用清理接口，再调用部署接口”收敛成一个可观察、可取消、可审计的 workflow task。

## 示例文件

- `examples/reset-workflow/01-reset-single-component-workflow.json`
- `examples/reset-workflow/02-reset-multi-component-workflow.json`
- `examples/reset-workflow/03-exec-reset-workflow-request.json`
- `examples/reset-workflow/README.md`
