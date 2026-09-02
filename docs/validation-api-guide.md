# 验证 API (Try/DryRun) 使用指南

> 状态：Current。当前路由为 `POST /api/v1/applications/try` 与 `POST /api/v1/applications/:appID/workflow/try`。

本文档介绍 Eruun 的验证 API，用于在不实际创建资源的情况下验证应用配置和工作流配置的合法性。

## 概述

验证 API 提供了两个端点，允许用户在提交创建/更新请求前预先验证配置的正确性：

1. **Try Application API** - 验证应用创建请求
2. **Try Workflow API** - 验证工作流更新请求

## API 端点

### 1. Try Application API

**端点**: `POST /api/v1/applications/try`

**用途**: 验证应用创建请求是否符合规范（命名规则、Traits 规则、组件配置、工作流引用）

**请求体**: 与创建应用的请求体相同 (`CreateApplicationsRequest`)

Try Application 会校验 workflow 对象里的 `failurePolicy`。非法值返回 `INVALID_WORKFLOW_FAILURE_POLICY`，字段为 `workflow.failurePolicy`。

Try Application 会执行与 `POST /api/v1/applications` 一致的资源名校验：提前计算 Deployment/StatefulSet/Service/Ingress/Job/CronJob/ConfigMap/Secret 等独占资源名，并检查同一请求内以及同命名空间普通应用之间的冲突。资源命名遵循当前运行时契约：非 shared 组件使用 `appName + componentName`，shared 组件使用 `componentName`，模板版本不参与运行时资源名。standalone PVC 只校验 Kubernetes 名称合法性，允许同命名空间内多个组件或应用共享；`tmpCreate: true` 的 StatefulSet `volumeClaimTemplates` 不作为 standalone PVC 参与冲突校验。

当请求携带 `ID` 用于校验更新/upsert 时，Try Application 会先读取已保存的 App，并以其 `namespace`、`version`、`templateEnabled` 等元数据作为默认值；请求体中显式传入的字段会覆盖这些默认值。这样 dry-run 的资源名校验与真实创建/更新路径保持一致。

Try/DryRun 不会创建数据库记录或 Kubernetes 资源，也不是原子创建保证。它基于请求内容和当前可读取的持久化数据做预校验；真实创建仍可能因为并发提交、Kubernetes 集群状态变化、外部依赖不可用或运行期执行错误而失败。

例如应用名为 `game`，同时包含组件 `api` 与 `game-api` 时，两者都可能解析为同一个 Deployment/Service 名 `game-api`，`/applications/try` 应返回 `valid=false`。

**示例调用**:
```bash
curl -X POST http://localhost:8000/api/v1/applications/try \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-app",
    "namespace": "default",
    "version": "1.0.0",
    "component": [...],
    "workflow": [...]
  }'
```

### 2. Try Workflow API

**端点**: `POST /api/v1/applications/:appID/workflow/try`

**用途**: 验证工作流配置是否引用了存在的组件，并校验与真实更新接口一致的 workflow callback 规则

**请求体**: 与 `PUT /api/v1/applications/:appID/workflow` 的更新工作流请求体兼容。历史字段 `workflow` 仍可用，读接口返回的 `steps` 也可直接作为步骤列表提交；如果包含 `callback`，Try Workflow 会按真实更新路径校验 method、URL、timeout 与 URL 安全策略，但不会写入 Workflow。Try Workflow 也会校验顶层 `failurePolicy`，非法值返回 `INVALID_WORKFLOW_FAILURE_POLICY`，字段为 `failurePolicy`。

Workflow 的组件引用（`components`、`properties.policies`、`properties[].policies`、`subSteps[]` 以及 `log_archive_upload` 的 step name fallback）按大小写不敏感方式匹配已存在组件；真实创建或更新工作流时，持久化引用会使用组件自身 `Name` 的实际大小写。

Try Application 与 Try Workflow 都会校验公开 workflow jobType 的附加契约。对于 `log_archive_upload`：

- `properties.path` 必填；缺失时返回 `MISSING_REQUIRED_FIELD`。
- 目标组件必须是会产生 Pod 的组件类型（`webservice`、`store`、`job`、`scheduledjob`）；`config`、`secret`、`cloudjob` 等非 Pod 组件会返回 `INVALID_WORKFLOW_STEP_TYPE`。
- 顶层 step 与 `subSteps[]` 使用同一套校验规则。

**示例调用**:
```bash
curl -X POST http://localhost:8000/api/v1/applications/your-app-id/workflow/try \
  -H "Content-Type: application/json" \
  -d '{
    "name": "new-workflow",
    "steps": [...]
  }'
```

## 响应格式

### 验证通过
```json
{
  "valid": true,
  "errors": []
}
```

### 验证失败
```json
{
  "valid": false,
  "errors": [
    {
      "field": "component[0].name",
      "code": "INVALID_NAME_FORMAT",
      "message": "name must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)"
    },
    {
      "field": "component[1].traits.probes[0]",
      "code": "INVALID_PROBE_CONFIG",
      "message": "probe must specify exactly one of exec, httpGet, or tcpSocket"
    },
    {
      "field": "workflow[0].components[2]",
      "code": "COMPONENT_NOT_FOUND",
      "message": "component 'missing-comp' not found in application"
    }
  ]
}
```

## 组件类型补充说明

新增两类 Job 组件类型：

- `job`：即时执行 Job，对应 Kubernetes Job。
- `scheduledjob`：定时执行 Job，对应 Kubernetes CronJob。

Job 组件暂时只支持最小集能力：

- 通过容器镜像 + `properties.command` 执行脚本（Shell/SQL 等）。
- 不自动生成 ConfigMap 挂载脚本（避免大小限制）。

校验要点：

- `job` 不允许 `properties.schedule`，允许 `properties.startTime`。
- `scheduledjob` 必须提供 `properties.schedule`。
- `properties.startTime` 使用 Unix 秒时间戳（仅对 `job` 生效）。
- Cron 表达式支持 **5 段或 6 段**；6 段时秒字段必须为 0，系统会去掉秒字段用于 CronJob。
- `properties.runPolicy` 可选，支持 `recreate` / `skip_if_completed`，默认 `skip_if_completed`。
- `properties.failurePolicy` 仅支持顶层 `type=job` 组件，唯一显式值为 `cleanup_failed`；Job 空值继承 workflow，`cleanup_all`、未知值、其他组件类型（包括显式空值）和 init container 中使用时返回 `INVALID_JOB_FAILURE_POLICY`。
- 模板请求会在展开后校验实际组件类型，但字段错误仍使用原始请求的 `component[i]` 下标；模板自动生成且没有对应 override 的组件继续使用展开后的下标。
- `scheduledjob` 可选 `properties.successfulJobsHistoryLimit` / `properties.failedJobsHistoryLimit` 控制 CronJob 保留历史数。

## Job 组件 JSON 示例

### 示例 1：job

```json
{
  "name": "job-demo-instant",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "Instant job demo",
  "component": [
    {
      "name": "instant-task",
      "type": "job",
      "image": "busybox:1.36",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "command": ["/bin/sh", "-c", "echo instant job"]
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "run-instant-task",
      "mode": "StepByStep",
      "components": ["instant-task"]
    }
  ]
}
```

### 示例 2：job（startTime 一次性延迟）

> 注意：`startTime` 需要改成当前时间之后的 Unix 秒时间戳。

```json
{
  "name": "job-demo-delay",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "Job (startTime) demo",
  "component": [
    {
      "name": "delay-task",
      "type": "job",
      "image": "busybox:1.36",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "startTime": 1893456000,
        "runPolicy": "skip_if_completed",
        "command": ["/bin/sh", "-c", "echo delayed job"]
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "create-delay-task",
      "mode": "StepByStep",
      "components": ["delay-task"]
    }
  ]
}
```

### 示例 3：scheduledjob（cron）

```json
{
  "name": "job-demo-cron",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "Scheduled job (cron) demo",
  "component": [
    {
      "name": "cron-task",
      "type": "scheduledjob",
      "image": "busybox:1.36",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "schedule": "0 0 * * *",
        "successfulJobsHistoryLimit": 3,
        "failedJobsHistoryLimit": 3,
        "command": ["/bin/sh", "-c", "date"]
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "create-cron-task",
      "mode": "StepByStep",
      "components": ["cron-task"]
    }
  ]
}
```

### 快速验证

```bash
curl -X POST http://localhost:8000/api/v1/applications/try \
  -H "Content-Type: application/json" \
  -d @examples/job-components/instant-job.json
```

## 请求示例

### 示例 1: 简单有效应用

```json
{
  "name": "simple-backend",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "Simple backend application",
  "component": [
    {
      "name": "backend",
      "type": "webservice",
      "image": "nginx:1.24",
      "namespace": "default",
      "replicas": 2,
      "properties": {
        "ports": [
          {
            "port": 8080,
            "expose": true
          }
        ],
        "env": {
          "APP_ENV": "production"
        }
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "deploy-backend",
      "mode": "StepByStep",
      "components": ["backend"]
    }
  ]
}
```

### 示例 2: 完整应用配置（包含所有 Traits）

```json
{
  "name": "demo-app",
  "namespace": "default",
  "version": "1.0.0",
  "project": "demo-project",
  "description": "Complete demo application with all traits",
  "component": [
    {
      "name": "app-config",
      "type": "config",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "conf": {
          "database.host": "mysql.default.svc",
          "database.port": "3306"
        }
      }
    },
    {
      "name": "backend",
      "type": "webservice",
      "image": "myregistry/backend:v1.0.0",
      "namespace": "default",
      "replicas": 3,
      "properties": {
        "ports": [{"port": 8080, "expose": true}],
        "env": {"APP_ENV": "production"}
      },
      "traits": {
        "probes": [
          {
            "type": "liveness",
            "httpGet": {
              "path": "/healthz",
              "port": 8080
            },
            "initialDelaySeconds": 30,
            "periodSeconds": 10
          },
          {
            "type": "readiness",
            "httpGet": {
              "path": "/ready",
              "port": 8080
            },
            "initialDelaySeconds": 5,
            "periodSeconds": 5
          }
        ],
        "resources": {
          "cpu": "500m",
          "memory": "512Mi"
        },
        "storage": [
          {
            "type": "persistent",
            "name": "data",
            "mountPath": "/data",
            "tmpCreate": true,
            "size": "10Gi"
          }
        ],
        "envFrom": [
          {
            "type": "configMap",
            "sourceName": "app-config"
          }
        ],
        "rbac": [
          {
            "serviceAccount": "backend-sa",
            "rules": [
              {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": ["get", "list", "watch"]
              }
            ]
          }
        ],
        "ingress": [
          {
            "name": "backend-ingress",
            "ingressClassName": "nginx",
            "routes": [
              {
                "path": "/api",
                "backend": {
                  "serviceName": "backend",
                  "servicePort": 8080
                }
              }
            ]
          }
        ]
      }
    }
  ],
  "workflow": [
    {
      "name": "config-step",
      "mode": "StepByStep",
      "components": ["app-config"]
    },
    {
      "name": "deploy-backend",
      "mode": "DAG",
      "components": ["backend"]
    }
  ]
}
```

### 示例 3: 包含 Init 容器和 Sidecar 的应用

```json
{
  "name": "app-with-init-sidecar",
  "namespace": "default",
  "version": "1.0.0",
  "component": [
    {
      "name": "backend",
      "type": "webservice",
      "image": "myregistry/backend:v1.0.0",
      "namespace": "default",
      "replicas": 2,
      "traits": {
        "init": [
          {
            "name": "init-config",
            "image": "busybox:latest",
            "properties": {
              "command": ["sh", "-c", "cp /config/* /app/config/"]
            },
            "traits": {
              "storage": [
                {
                  "type": "config",
                  "name": "app-config",
                  "sourceName": "my-configmap",
                  "mountPath": "/config",
                  "readOnly": true
                }
              ]
            }
          }
        ],
        "sidecar": [
          {
            "name": "logging-sidecar",
            "image": "fluent/fluentd:v1.14",
            "env": {
              "FLUENTD_CONF": "fluent.conf"
            },
            "traits": {
              "resources": {
                "cpu": "100m",
                "memory": "128Mi"
              }
            }
          }
        ]
      }
    }
  ],
  "workflow": [
    {
      "name": "deploy",
      "mode": "StepByStep",
      "components": ["backend"]
    }
  ]
}
```

### 示例 4: 工作流验证请求

```json
{
  "workflowId": "",
  "name": "new-workflow",
  "alias": "New Deployment Workflow",
  "workflow": [
    {
      "name": "config-step",
      "mode": "StepByStep",
      "components": ["app-config", "app-secret"]
    },
    {
      "name": "database-step",
      "mode": "DAG",
      "components": ["mysql", "redis"]
    },
    {
      "name": "services-step",
      "mode": "DAG",
      "components": ["backend", "frontend"]
    }
  ]
}
```

## 无效配置示例

### 示例 1: 无效的应用名称

```json
{
  "name": "My_Invalid_App",
  "namespace": "default",
  "component": [...]
}
```

**预期错误**:
```json
{
  "valid": false,
  "errors": [
    {
      "field": "name",
      "code": "INVALID_NAME_FORMAT",
      "message": "name must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)"
    }
  ]
}
```

### 示例 2: 缺少镜像

```json
{
  "name": "my-app",
  "component": [
    {
      "name": "backend",
      "type": "webservice",
      "image": ""
    }
  ]
}
```

**预期错误**:
```json
{
  "valid": false,
  "errors": [
    {
      "field": "component[0].image",
      "code": "MISSING_IMAGE",
      "message": "image is required for webservice and store component types"
    }
  ]
}
```

### 示例 3: 无效的探针配置

```json
{
  "name": "my-app",
  "component": [
    {
      "name": "backend",
      "type": "webservice",
      "image": "nginx:latest",
      "traits": {
        "probes": [
          {
            "type": "liveness"
          }
        ]
      }
    }
  ]
}
```

**预期错误**:
```json
{
  "valid": false,
  "errors": [
    {
      "field": "component[0].traits.probes[0]",
      "code": "INVALID_PROBE_CONFIG",
      "message": "probe must specify exactly one of exec, httpGet, or tcpSocket"
    }
  ]
}
```

### 示例 4: 嵌套 Sidecar（禁止）

```json
{
  "name": "my-app",
  "component": [
    {
      "name": "backend",
      "type": "webservice",
      "image": "nginx:latest",
      "traits": {
        "sidecar": [
          {
            "name": "sidecar-1",
            "image": "fluent/fluentd:v1.14",
            "traits": {
              "sidecar": [
                {
                  "name": "nested-sidecar",
                  "image": "busybox:latest"
                }
              ]
            }
          }
        ]
      }
    }
  ]
}
```

**预期错误**:
```json
{
  "valid": false,
  "errors": [
    {
      "field": "component[0].traits.sidecar[0].traits.sidecar[0]",
      "code": "NESTED_TRAIT_FORBIDDEN",
      "message": "sidecar trait cannot be nested inside another init or sidecar trait"
    }
  ]
}
```

### 示例 5: 工作流引用不存在的组件

```json
{
  "name": "my-app",
  "component": [
    {
      "name": "backend",
      "type": "webservice",
      "image": "nginx:latest"
    }
  ],
  "workflow": [
    {
      "name": "deploy-all",
      "mode": "StepByStep",
      "components": ["backend", "frontend", "database"]
    }
  ]
}
```

**预期错误**:
```json
{
  "valid": false,
  "errors": [
    {
      "field": "workflow[0].components[1]",
      "code": "COMPONENT_NOT_FOUND",
      "message": "component 'frontend' not found in application"
    },
    {
      "field": "workflow[0].components[2]",
      "code": "COMPONENT_NOT_FOUND",
      "message": "component 'database' not found in application"
    }
  ]
}
```

## 错误码参考

### 命名错误

| 错误码 | 说明 |
|--------|------|
| `INVALID_NAME` | 名称无效 |
| `NAME_TOO_SHORT` | 名称太短 (< 2 字符) |
| `NAME_TOO_LONG` | 名称太长 (> 63 字符) |
| `INVALID_NAME_FORMAT` | 名称格式不符合 DNS-1123 子域名规范 |
| `INVALID_COMPONENT_NAME` | 组件名称无效 |
| `INVALID_STEP_NAME` | 工作流步骤名称无效 |

### 组件错误

| 错误码 | 说明 |
|--------|------|
| `INVALID_COMPONENT_TYPE` | 无效的组件类型 |
| `MISSING_IMAGE` | 缺少镜像 (webservice/store 类型必填) |
| `DUPLICATE_COMPONENT` | 重复的组件名称 |

### Traits 错误

| 错误码 | 说明 |
|--------|------|
| `INVALID_TRAIT_CONFIG` | Trait 配置无效 |
| `MISSING_REQUIRED_FIELD` | 缺少必填字段 |
| `INVALID_STORAGE_TYPE` | 无效的存储类型 |
| `INVALID_STORAGE_SIZE` | 无效的存储大小格式 |
| `INVALID_PROBE_TYPE` | 无效的探针类型 |
| `INVALID_PROBE_CONFIG` | 探针配置无效 |
| `NESTED_TRAIT_FORBIDDEN` | 禁止嵌套 Trait |
| `MISSING_RBAC_RULES` | RBAC 缺少规则 |
| `MISSING_RBAC_VERBS` | RBAC 规则缺少 verbs |
| `MISSING_INGRESS_ROUTES` | Ingress 缺少路由 |
| `INVALID_ENVFROM_TYPE` | envFrom 类型无效 |
| `INVALID_ENV_VALUE_SOURCE` | 环境变量值来源无效 |

### 工作流错误

| 错误码 | 说明 |
|--------|------|
| `COMPONENT_NOT_FOUND` | 组件不存在 |
| `INVALID_WORKFLOW_MODE` | 无效的工作流模式 |
| `EMPTY_WORKFLOW_STEP` | 空的工作流步骤 |
| `DUPLICATE_WORKFLOW_STEP` | 重复的工作流步骤名称 |
| `WORKFLOW_STEP_NO_COMPONENT` | 工作流步骤没有组件 |

## 验证规则详解

### 命名规则

所有名称（应用名、组件名、工作流步骤名）必须符合 **DNS-1123 子域名规范**：

- **长度**: 2-63 字符
- **格式**: `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
- 只能包含小写字母、数字和连字符
- 必须以字母或数字开头和结尾
- 不能以连字符开头或结尾

**有效示例**: `my-app`, `backend-v1`, `mysql01`

**无效示例**: `My_App`, `-app`, `app-`, `a` (太短)

### 组件类型

| 类型 | 说明 | 是否需要镜像 |
|------|------|--------------|
| `webservice` | 无状态服务 (Deployment) | 是 |
| `store` | 有状态存储服务 (StatefulSet) | 是 |
| `config` | ConfigMap 配置 | 否 |
| `secret` | Secret 密钥 | 否 |

### 工作流模式

| 模式 | 说明 |
|------|------|
| `StepByStep` | 串行模式，组件按顺序依次执行 |
| `DAG` | 并行模式，同一 Step 内的组件并行执行 |

### Traits 嵌套规则

- `init` 和 `sidecar` Trait 支持嵌套以下 Traits:
  - `storage`, `envs`, `envFrom`, `probes`, `resources`, `securityPolicy`, `rbac`, `ingress`
- **禁止**在 `init` 或 `sidecar` 中再嵌套 `init` 或 `sidecar`
- `service` Trait 为组件级能力，建议仅在组件顶层 `traits.service` 使用
- `targetWorkEnv` Trait 为组件级能力，建议仅在组件顶层 `traits.targetWorkEnv` 使用；当前会按对象内容映射到 Pod `nodeSelector`

### `targetWorkEnv` 示例

请求示例见：

- `examples/validation-try/16-valid-target-work-env.json`

示例片段：

```json
{
  "traits": {
    "targetWorkEnv": {
      "app": "lab"
    }
  }
}
```

服务端会将其映射为：

```yaml
spec:
  template:
    spec:
      nodeSelector:
        app: lab
```

### 探针规则

- `type` 必填，可选值: `liveness`, `readiness`, `startup`
- 探测方法**三选一**: `exec`, `httpGet`, `tcpSocket`
- 不能同时指定多个探测方法
- `httpGet` 和 `tcpSocket` 的 `port` 必须为正整数

### 存储规则

- `type` 必填，可选值: `persistent`, `ephemeral`, `config`, `secret`
- `mountPath` 必填
- `persistent` 类型配合 `tmpCreate: true` 时，`size` 需要符合 Kubernetes 资源量格式 (如 `1Gi`, `500Mi`)
- `config` 和 `secret` 类型需要指定 `sourceName` 或 `name`

### RBAC 规则

- `rules` 数组必填且不能为空
- 每个 rule 的 `verbs` 数组必填且不能为空

### Ingress 规则

- `routes` 数组必填且不能为空
- 每个 route 的 `backend.serviceName` 在单一默认 Service 场景下可选，未填写时默认使用当前组件生成的 Service 名称（会做规范化与长度裁剪）
- 当组件配置了 `traits.service` 时，Ingress 默认后端会优先从 `traits.service` 推导；若未配置，则回退到基于 `properties.ports` 的默认 Service 推导
- 当同一组件配置多个非 `external` 的 `traits.service[]` 时，每个 Ingress route 必须显式填写 `backend.serviceName`，避免默认后端选择错误
- `traits.ingress[].label` 不能覆盖 Eruun 托管 label，例如 `eruun.io/app-id`、`eruun.io/component-name`、`eruun.io/component-id`、`app.kubernetes.io/managed-by`

示例（省略 `serviceName` 时自动补全）：

```json
{
  "ingress": [
    {
      "name": "backend-ingress",
      "routes": [
        {
          "path": "/api",
          "backend": {
            "servicePort": 8080
          }
        }
      ]
    }
  ]
}
```

### Service Trait 规则

- `traits.service[].ports` 必填且不能为空
- 每个 `ports[].port` 必须为正整数
- `type` 建议使用口语化值：
  - `internal`：集群内访问（对应 Kubernetes `ClusterIP`）
  - `node`：节点端口访问（对应 Kubernetes `NodePort`）
  - `public`：负载均衡公网访问（对应 Kubernetes `LoadBalancer`）
  - `external`：外部域名映射（对应 Kubernetes `ExternalName`）
- 非 `external` 类型必须提供 `selector`
- `headless=true` 仅允许 `type=internal`
- 为兼容历史配置，仍接受 `ClusterIP/NodePort/LoadBalancer/ExternalName`
- `traits.service[].labels` 不能覆盖 Eruun 托管 label，例如 `eruun.io/app-id`、`eruun.io/component-name`、`eruun.io/component-id`、`app.kubernetes.io/managed-by`

### Label 规则

- `properties.labels` 只能放业务自定义 label，不能覆盖 Eruun 托管 label
- 保留键也包括共享判定标签 `eruun.io/share-name`、`eruun.io/share-strategy`；它们只能由 `traits.share` 语义下沉生成，不能通过 `properties.labels`、`traits.service[].labels` 或 `traits.ingress[].label` 手工注入
- 真实生成 Pod/Service/Ingress/ConfigMap/Secret 等资源时，托管 label 会最终覆盖同名用户 label，保证 Service selector 和 Ingress 资源归属关系稳定

## 使用建议

1. **开发阶段**: 在提交创建应用请求前，先使用 Try API 验证配置
2. **CI/CD 集成**: 在部署流水线中添加验证步骤，提前发现配置错误
3. **调试**: 当创建应用失败时，使用 Try API 获取详细的验证错误信息
4. **批量验证**: 可以批量验证多个配置文件，确保配置规范一致性

## 相关文件

- 示例 JSON 文件: `examples/validation-try/`
- DTO 定义: `pkg/apiserver/interfaces/api/dto/v1/validation.go`
- 验证服务实现: `pkg/apiserver/domain/service/validation*.go`
- 单元测试: `pkg/apiserver/domain/service/validation_test.go`
