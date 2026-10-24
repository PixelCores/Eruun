# 验证 API (Try/DryRun) 使用指南

> 状态：Current。当前路由为 `POST /api/v1/applications/try` 与 `POST /api/v1/applications/:appID/workflow/try`。

本文档介绍 Eruun 的验证 API，用于在不实际创建资源的情况下验证应用配置和工作流配置的合法性。

## 概述

验证 API 提供了两个端点，允许用户在提交创建/更新请求前预先验证配置的正确性：

1. **Try Application API** - 验证应用创建请求
2. **Try Workflow API** - 验证工作流更新请求

Try 与写入接口使用同一套组件、Trait 和工作流步骤规则：`POST /api/v1/applications`、`PUT /api/v1/applications/:appID/workflow` 和版本更新的 Trait 校验与 Try 调用相同的实现，Try 能拒绝的请求，写入接口同样拒绝且不保存。区别只在返回形式：Try 返回全部错误及其字段路径；写入接口返回第一条错误，消息中带同样的字段路径。写入接口的业务码为：组件或 init 容器 `properties` 下的错误返回 `10004`，其余组件与 Trait 错误返回 `10000`，工作流步骤错误返回 `20000`。

## API 端点

### 1. Try Application API

**端点**: `POST /api/v1/applications/try`

**用途**: 验证应用创建请求是否符合规范（命名规则、Traits 规则、组件配置、工作流引用）

**请求体**: 与创建应用的请求体相同 (`CreateApplicationsRequest`)

Try Application 会校验根级 `failurePolicy`。非法值返回 `INVALID_WORKFLOW_FAILURE_POLICY`，错误路径为 `/failurePolicy`。

顶层 `type: "job"` 组件可使用与独立 Job 相同的 `traits.eval`。Try 会校验 `env: "ack"`、任务包 ID、模型、执行适配器和有界试验参数，并返回默认 attempts/concurrency/timeout 与 Runner、sandbox 各自的资源规格。该组件无需用户镜像，拒绝冲突的镜像、命令、明文环境变量、延时、Cron、runPolicy、重试覆盖及不支持的运行 Traits；其他组件类型、init 和 sidecar 不允许携带 `eval`。任务包的空间归属和凭据可用性仍由执行入口验证。

`GET /api/v1/schemas/v1/canonical.json` 的 `$defs.Job` 描述独立 Job，`$defs.EvaluationTraitSpec` 描述两种入口共享的评测 Trait。独立 Job 的提交校验在 `POST /api/v1/jobs` 中执行；Try Application 仍接收完整 Application，不能直接提交独立 Job 请求。详见 [Canonical JSON Profile](canonical-json-profile.md)。

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
    "components": [...],
    "workflow": [...]
  }'
```

### 2. Try Workflow API

**端点**: `POST /api/v1/applications/:appID/workflow/try`

**用途**: 验证工作流配置是否引用了存在的组件，并校验与真实更新接口一致的 workflow callback 规则

**请求体**: 与 `PUT /api/v1/applications/:appID/workflow` 的更新工作流请求体同构，步骤只使用根级 `workflow`。如果包含 `callback`，Try Workflow 会按真实更新路径校验 method、URL、timeout 与 URL 安全策略，但不会写入 Workflow。Try Workflow 也会校验顶层 `failurePolicy`，非法值返回 `INVALID_WORKFLOW_FAILURE_POLICY`，错误路径为 `/failurePolicy`；显式空值表示重置为默认策略，返回的 `normalizedSpec` 会将其写成 `cleanup_all`，而省略字段仍表示更新时保留现值。

Workflow 的组件引用（`components`、`properties.policies`、`properties[].policies`、`subSteps[]` 以及 `log_archive_upload` 的 step name fallback）按大小写不敏感方式匹配已存在组件；真实创建或更新工作流时，持久化引用会使用组件自身 `Name` 的实际大小写。

顶层 `workflow[]` 的 `mode` 只接受省略、空字符串、`StepByStep` 或 `DAG`；空值仍默认为 `StepByStep`。非空步骤名按大小写不敏感方式判重。Try Application / Try Workflow 与真实应用创建、工作流更新使用相同的 mode 和重名规则：Try 分别返回 `INVALID_WORKFLOW_MODE`（`/workflow/{index}/mode`）和 `DUPLICATE_WORKFLOW_STEP`（`/workflow/{index}/name`），写入接口返回工作流配置错误且不保存请求。

迁移旧请求时，请将未知 mode 改为上述合法值，并为重名步骤指定不同名称；写入接口不再将未知 mode 静默当作 `StepByStep`。`stepType` 的既有非法值拒绝规则保持不变。

Try Application 与 Try Workflow 都会校验公开 workflow jobType 的附加契约。对于 `log_archive_upload`：

- `properties.path` 必填；缺失时返回 `MISSING_REQUIRED_FIELD`。
- 目标组件必须是会产生 Pod 的组件类型（`webservice`、`store`、`job`、`scheduledjob`）；`config`、`secret` 等非 Pod 组件会返回 `INVALID_WORKFLOW_STEP_TYPE`。
- 顶层 step 与 `subSteps[]` 使用同一套校验规则。

**示例调用**:
```bash
curl -X POST http://localhost:8000/api/v1/applications/your-app-id/workflow/try \
  -H "Content-Type: application/json" \
  -d '{
    "name": "new-workflow",
    "workflow": [...]
  }'
```

## 响应格式

### 验证通过
```json
{
  "valid": true,
  "errors": [],
  "normalizedSpec": {
    "name": "my-app",
    "components": []
  },
  "plan": {
    "actions": []
  }
}
```

### 验证失败
```json
{
  "valid": false,
  "errors": [
    {
      "path": "/components/0/name",
      "code": "INVALID_NAME_FORMAT",
      "message": "name must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)"
    },
    {
      "path": "/components/1/traits/probes/0",
      "code": "INVALID_PROBE_CONFIG",
      "message": "probe must specify exactly one of exec, httpGet, or tcpSocket"
    },
    {
      "path": "/workflow/0/components/2",
      "code": "COMPONENT_NOT_FOUND",
      "message": "component 'missing-comp' not found in application"
    }
  ],
  "normalizedSpec": {
    "name": "my-app",
    "components": [],
    "workflow": []
  },
  "plan": {"actions": []}
}
```

`errors` 始终返回数组。`path` 是指向 `normalizedSpec` 的 RFC 6901 JSON Pointer；`code` 用于程序判断，`message` 只用于展示。`normalizedSpec` 可直接提交到对应写接口，`plan.actions` 是根据规范化请求生成的有序逻辑动作。完整契约见 [Canonical JSON Profile](canonical-json-profile.md)。

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
- 模板请求会在展开后校验实际组件类型，但错误路径仍使用原始请求的 `/components/{i}` 下标；模板自动生成且没有对应 override 的组件继续使用展开后的下标。
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
  "components": [
    {
      "name": "instant-task",
      "type": "job",
      "image": "busybox:1.36",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "command": [
          "/bin/sh",
          "-c",
          "echo instant job"
        ]
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "run-instant-task",
      "mode": "StepByStep",
      "components": [
        "instant-task"
      ]
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
  "components": [
    {
      "name": "delay-task",
      "type": "job",
      "image": "busybox:1.36",
      "namespace": "default",
      "replicas": 1,
      "properties": {
        "startTime": 1893456000,
        "runPolicy": "skip_if_completed",
        "command": [
          "/bin/sh",
          "-c",
          "echo delayed job"
        ]
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "create-delay-task",
      "mode": "StepByStep",
      "components": [
        "delay-task"
      ]
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
  "components": [
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
        "command": [
          "/bin/sh",
          "-c",
          "date"
        ]
      },
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "create-cron-task",
      "mode": "StepByStep",
      "components": [
        "cron-task"
      ]
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
  "components": [
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
      "components": [
        "backend"
      ]
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
  "components": [
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

        "gateway": {
          "routes": [
            {
              "path": "/api",
              "backend": {
                "port": 8080
              }
            }
          ]
        }
      }
    }
  ],
  "workflow": [
    {
      "name": "config-step",
      "mode": "StepByStep",
      "components": [
        "app-config"
      ]
    },
    {
      "name": "deploy-backend",
      "mode": "DAG",
      "components": [
        "backend"
      ]
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
  "components": [
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
              "command": [
                "sh",
                "-c",
                "cp /config/* /app/config/"
              ]
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
      "components": [
        "backend"
      ]
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
      "components": [
        "app-config",
        "app-secret"
      ]
    },
    {
      "name": "database-step",
      "mode": "DAG",
      "components": [
        "mysql",
        "redis"
      ]
    },
    {
      "name": "services-step",
      "mode": "DAG",
      "components": [
        "backend",
        "frontend"
      ]
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
  "components": [...]
}
```

**预期错误**:
```json
{
  "valid": false,
  "errors": [
    {
      "path": "/name",
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
  "components": [
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
      "path": "/components/0/image",
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
  "components": [
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
      "path": "/components/0/traits/probes/0",
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
  "components": [
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
      "path": "/components/0/traits/sidecar/0/traits/sidecar/0",
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
  "components": [
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
      "components": [
        "backend",
        "frontend",
        "database"
      ]
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
      "path": "/workflow/0/components/1",
      "code": "COMPONENT_NOT_FOUND",
      "message": "component 'frontend' not found in application"
    },
    {
      "path": "/workflow/0/components/2",
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
| `NAME_TOO_SHORT` | 名称太短 (< 2 字符) |
| `NAME_TOO_LONG` | 名称太长 (> 63 字符) |
| `INVALID_NAME_FORMAT` | 名称格式不符合 DNS-1123 子域名规范 |

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
| `MISSING_GATEWAY_ROUTES` | `traits.gateway` 缺少路由 |
| `INVALID_GATEWAY_ROUTE` | 路由的 path、match、rewrite、timeout 无效，路由重复或超过 16 条 |
| `INVALID_GATEWAY_BACKEND` | 路由后端不是本组件可选的 Service 或 TCP 端口 |
| `INVALID_GATEWAY_HOST` | host 不属于当前空间、格式无效、重复或超过 8 个 |
| `GATEWAY_NOT_ALLOWED` | 平台未配置入口后端、组件类型不支持，或在 init/sidecar 中声明 |
| `GATEWAY_UNSUPPORTED` | 当前入口后端不支持该字段（`ingress` 不支持 rewrite、timeout） |
| `INVALID_ENVFROM_TYPE` | envFrom 类型无效 |
| `INVALID_ENV_VALUE_SOURCE` | 环境变量值来源无效 |

### 工作流错误

| 错误码 | 说明 |
|--------|------|
| `COMPONENT_NOT_FOUND` | 组件不存在 |
| `INVALID_WORKFLOW_MODE` | 无效的工作流模式 |
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
  - `storage`, `envs`, `envFrom`, `probes`, `resources`, `securityPolicy`
- **禁止**在 `init` 或 `sidecar` 中再嵌套 `init` 或 `sidecar`
- `service` Trait 为组件级能力，建议仅在组件顶层 `traits.service` 使用
- `nodePool` 只支持组件顶层，init/sidecar 嵌套中返回 `INVALID_TRAIT_CONFIG`。
- `gateway` 只支持组件顶层，init/sidecar 嵌套中返回 `GATEWAY_NOT_ALLOWED`；模板组件覆盖字段中的嵌套 `gateway` 同样被拒绝，而不是在合并时丢弃。
- 按名字查询 MySQL，引用不存在时返回 `INVALID_TRAIT_CONFIG`，字段为 `component[i].traits.nodePool`。
- command Job 步骤支持 `nodePool`。

### 命名配置示例

管理员须先通过 `/api/v1/admin/node-pools` 创建配置。成员使用只读选项接口选择名字。有效示例见 `examples/validation-try/16-valid-node-pool.json`（需先创建 `gpu-a100`）；不存在的配置示例见 `09-invalid-unknown-node-pool.json`。

```json
{"traits": {"nodePool": "gpu-a100"}}
```

部署构建时重新读取配置：节点池内容渲染为 Pod 的 `nodeSelector` 与 `tolerations`。配置变更在下一次部署时使用，不自动重新部署。YAML 转换不会从原始调度字段或 RBAC 对象生成引用，分别提示选择 nodePool、空间工作负载不使用 Kubernetes API 凭据。

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


- 模板覆盖整体替换节点池引用，克隆不重写配置名。

### Gateway 规则

`traits.gateway` 声明组件经平台入口对外提供的 HTTP 路由，首期只有入站方向。入口后端在部署时选定（Helm `gateway` values 或 `--gateway-*` flag，见 [Helm 部署契约](helm-deployment.md#入口网关)），当前只实现 `ingress` 后端。设计背景见 [Gateway Trait 设计](gateway-trait-design.md)。

- 只允许在 `webservice`、`store` 组件顶层使用，其他组件类型和平台未配置入口后端时返回 `GATEWAY_NOT_ALLOWED`；command Job 步骤和 `type: "job"` 评测组件同样拒绝 `gateway`。
- `hosts` 可选，最多 8 个且不能重复。每个 host 由一个 DNS label 和平台域名组成：`<前缀>-<空间 namespace>.<domain>`，前缀非空，label 为小写且不超过 63 个字符；不满足时返回 `INVALID_GATEWAY_HOST`。省略时使用默认 host `<小写组件名>-<空间 namespace>.<domain>`；组件名过长使默认 label 超过 63 个字符时，需要显式声明 `hosts`。
- `routes` 必填，1–16 条，为空返回 `MISSING_GATEWAY_ROUTES`。每条路由：
  - `path` 以 `/` 开头，不含 `?`、`#`、`..`、`//`，省略时为 `/`；
  - `match` 为 `prefix`（默认）或 `exact`，区分大小写；`path` 与 `match` 的组合在组件内唯一；
  - `rewrite` 只能配合 `prefix`，以 `/` 开头；
  - `timeout` 使用 Gateway API Duration 格式（如 `600s`、`1h`），取值为 `0s` 或 1s–1h；
  - 以上格式错误返回 `INVALID_GATEWAY_ROUTE`。`ingress` 后端不支持 `rewrite` 与 `timeout`，格式正确时返回 `GATEWAY_UNSUPPORTED`。
- `backend` 可选，只能指向本组件的 Service，无法确定时返回 `INVALID_GATEWAY_BACKEND`：
  - `service` 为空：组件只有一个非 `external` Service Trait 时使用它（未命名时为生成的 Service 名）；没有 Service Trait 时使用由 `properties.ports` 生成的默认 Service；有多个非 `external` Service Trait 时必须填写；
  - `service` 非空：必须是本组件已命名、非 `external` 的 Service Trait 名（与部署时一样去除首尾空白后比较），不接受生成的默认 Service 名；
  - `port` 为空时取所选 Service 的第一个 TCP 端口，非空时必须是该 Service 的 TCP 端口。
- TLS 在平台入口终止，组件不声明证书。`ingress` 后端为每个组件生成一个 Ingress：名称为 `GatewayRouteName(组件名, 应用资源名)`，`ingressClassName` 取平台配置，每个 host 一条规则且共享同一组 path，不带 TLS 和默认后端；注解只有 Eruun 用于任务聚合的 `eruun.io/component-name`。
- 这些检查只在带空间身份的请求中执行，需要空间 namespace 判断 host 归属。Try 返回字段级错误（`component[i].traits.gateway...`）；创建与版本更新使用同一套规则，以客户端可见的消息返回第一条错误（含嵌套位置的 `GATEWAY_NOT_ALLOWED`）（版本更新的字段前缀为 `component[<组件名>].traits.gateway`）。

示例（只有一个默认 Service，`backend` 可省略）：

```json
{
  "gateway": {
    "routes": [
      {"path": "/v1"},
      {"path": "/healthz", "match": "exact"}
    ]
  }
}
```

更多请求示例见 `examples/validation-try/13-valid-gateway-default-service.json`、`15-valid-gateway-service-trait.json`。

YAML 转换（`POST /api/v1/applications/convert`）把与组件同名的 Ingress 转为该组件的 `traits.gateway`：每条规则须有 host 和 HTTP paths，所有 host 的 path 集合相同（顺序可以不同，输出沿用第一条规则的顺序）；后端须为同一组件的 Service 并使用端口号；pathType 只接受 `Prefix` 与 `Exact`。带注解、TLS、默认后端、一个组件对应多个 Ingress，或同名组件不是 `webservice`/`store` 时转换失败，返回 400 并说明原因；没有同名组件的 Ingress 被跳过并返回警告。转换保留原 host，保存时仍按上述规则校验空间归属。

### Service Trait 规则

- `traits.service[].ports` 必填且不能为空
- 每个 `ports[].port` 必须为正整数
- `type` 只接受以下值，空值等同 `internal`：
  - `internal`：集群内访问（对应 Kubernetes `ClusterIP`）
  - `node`：节点端口访问（对应 Kubernetes `NodePort`）
  - `public`：负载均衡公网访问（对应 Kubernetes `LoadBalancer`）
  - `external`：外部域名映射（对应 Kubernetes `ExternalName`）
- 非 `external` 类型必须提供 `selector`
- `headless=true` 仅允许 `type=internal`
- Kubernetes 原生值 `ClusterIP/NodePort/LoadBalancer/ExternalName` 会被拒绝
- `traits.service[].labels` 不能覆盖 Eruun 托管 label，例如 `eruun.io/app-id`、`eruun.io/component-name`、`eruun.io/component-id`、`app.kubernetes.io/managed-by`

### Label 规则

- `properties.labels` 只能放业务自定义 label，不能覆盖 Eruun 托管 label
- 保留键也包括共享判定标签 `eruun.io/share-name`、`eruun.io/share-strategy`；它们只能由 `traits.share` 语义下沉生成，不能通过 `properties.labels` 或 `traits.service[].labels` 手工注入
- 真实生成 Pod/Service/入口路由/ConfigMap/Secret 等资源时，托管 label 会最终覆盖同名用户 label，保证 Service selector 和入口路由的归属关系稳定

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
