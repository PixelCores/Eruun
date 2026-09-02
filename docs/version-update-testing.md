# 版本更新功能测试指南

> 状态：Historical / Audit。本文是手工测试流程记录，接口事实以 `version-update-api.md` 为准。

## 概述

本文档以实际操作流程的方式，演示如何使用版本更新 API。

---

## 场景一：简单镜像更新

### 步骤 1：创建应用

首先，创建一个版本为 `1.0.0` 的应用：

**请求**：`POST /api/v1/applications`

```json
{
  "name": "my-backend-app",
  "namespace": "default",
  "version": "1.0.0",
  "description": "My backend application",
  "component": [
    {
      "name": "backend",
      "type": "webservice",
      "image": "myapp/backend:v1.0.0",
      "replicas": 2,
      "properties": {
        "ports": [
          {"port": 8080}
        ],
        "env": {
          "ENV": "production",
          "LOG_LEVEL": "info"
        }
      }
    }
  ]
}
```

**响应**：

```json
{
  "id": "abc123xyz456",
  "name": "my-backend-app",
  "version": "1.0.0",
  "workflow_id": "wf-789def",
  "createTime": "2024-01-15T10:30:00Z",
  "updateTime": "2024-01-15T10:30:00Z"
}
```

### 步骤 2：更新镜像版本

现在，将 backend 组件的镜像从 `v1.0.0` 更新到 `v1.1.0`：

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "1.1.0",
  "strategy": "rolling",
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.1.0"
    }
  ],
  "description": "Update backend image to v1.1.0"
}
```

**响应**：

```json
{
  "appId": "abc123xyz456",
  "version": "1.1.0",
  "previousVersion": "1.0.0",
  "strategy": "rolling",
  "taskId": "task-update-001",
  "updatedComponents": ["backend"],
  "addedComponents": [],
  "removedComponents": []
}
```

### 步骤 3：查看更新状态

使用返回的 `taskId` 查询工作流执行状态：

**请求**：`GET /api/v1/workflow/tasks/task-update-001/status`

**响应**：

```json
{
  "taskId": "task-update-001",
  "status": "completed",
  "workflowId": "wf-789def",
  "workflowName": "my-backend-app-workflow",
  "appId": "abc123xyz456",
  "components": [
    {
      "name": "backend",
      "type": "deploy",
      "status": "completed",
      "startTime": 1705312200,
      "endTime": 1705312260
    }
  ]
}
```

### 镜像 Ready 观测回归点

镜像更新请求默认会使用 300 秒观测窗口等待新镜像 Pod Ready；也可以显式传入 `imageReadyTimeoutSeconds`：

```json
{
  "version": "1.1.0",
  "imageReadyTimeoutSeconds": 300,
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v1.1.0"
    }
  ],
  "callback": {
    "failure": "https://callback.example.com/eruun/failure",
    "timeoutSeconds": 5
  }
}
```

验证要点：
- 旧镜像 Pod 仍然 Ready 时，不应让本次镜像更新 workflow 提前完成。
- 新镜像 Pod Ready 后，`deploy` job 才能完成。
- 新镜像 Pod 持续 CrashLoopBackOff / ImagePullBackOff / readiness 未通过时，等待窗口结束后 workflow 进入失败或超时，并触发现有 failure / timeout callback。
- `imageReadyTimeoutSeconds` 是 Pod Ready 观测窗口；`callback.timeoutSeconds` 只控制 callback HTTP 请求超时。
- 仅版本号、仅副本数或环境变量更新、`config` / `secret` 更新，不应触发镜像 Ready 目标记录。

### 重启 Ready 观测回归点

`restart` 不修改组件规格，但 workflow job 在写入 `kubectl.kubernetes.io/restartedAt` 后会等待本次 restart 产生的新 Pod Ready：

```json
{
  "version": "1.1.2",
  "components": [
    {
      "action": "restart",
      "name": "backend"
    }
  ],
  "callback": {
    "failure": "https://callback.example.com/eruun/failure",
    "timeoutSeconds": 5
  }
}
```

验证要点：
- patch 成功但只有旧 Pod Ready 时，`version_restart` job 不应完成。
- 带有本次 `kubectl.kubernetes.io/restartedAt` 注解的新 Pod Ready 后，`version_restart` job 才能完成。
- 带有本次注解的新 Pod 持续 CrashLoopBackOff / ImagePullBackOff / readiness 未通过时，job 超时后 workflow 进入失败或超时，并触发现有 failure / timeout callback。
- `imageReadyTimeoutSeconds` 不控制 restart 等待窗口；restart 使用 `version_restart` job 的部署超时。

---

## 场景二：扩容副本数

### 当前状态

应用 `my-backend-app` 当前版本为 `1.1.0`，backend 组件有 2 个副本。

### 执行扩容

将副本数从 2 扩展到 5：

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "1.1.1",
  "components": [
    {
      "name": "backend",
      "replicas": 5
    }
  ],
  "description": "Scale backend to 5 replicas for high traffic"
}
```

**响应**：

```json
{
  "appId": "abc123xyz456",
  "version": "1.1.1",
  "previousVersion": "1.1.0",
  "strategy": "rolling",
  "taskId": "task-scale-002",
  "updatedComponents": ["backend"],
  "addedComponents": [],
  "removedComponents": []
}
```

---

## 场景三：添加缓存组件

### 当前状态

应用 `my-backend-app` 当前版本为 `1.1.1`，只有一个 backend 组件。

### 添加 Redis 缓存

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "2.0.0",
  "components": [
    {
      "action": "add",
      "name": "redis-cache",
      "type": "store",
      "image": "redis:7-alpine",
      "replicas": 1,
      "properties": {
        "ports": [
          {"port": 6379}
        ]
      },
      "traits": {
        "resources": {
          "cpu": "100m",
          "memory": "256Mi"
        }
      }
    }
  ],
  "description": "Add Redis cache component"
}
```

**响应**：

```json
{
  "appId": "abc123xyz456",
  "version": "2.0.0",
  "previousVersion": "1.1.1",
  "strategy": "rolling",
  "taskId": "task-add-003",
  "updatedComponents": [],
  "addedComponents": ["redis-cache"],
  "removedComponents": []
}
```

### 验证组件列表

**请求**：`GET /api/v1/applications/abc123xyz456/components`

**响应**：

```json
{
  "components": [
    {
      "id": 1,
      "appId": "abc123xyz456",
      "name": "backend",
      "namespace": "default",
      "image": "myapp/backend:v1.1.0",
      "replicas": 5,
      "type": "webservice",
      "status": "Running",
      "externalLinks": [{"type":"ingress","value":"backend.example.com/"}]
    },
    {
      "id": 2,
      "appId": "abc123xyz456",
      "name": "redis-cache",
      "namespace": "default",
      "image": "redis:7-alpine",
      "replicas": 1,
      "type": "store",
      "status": "Not Deploy",
      "externalLinks": [{"type":"svc","value":"redis-cache.default.svc:6379"}]
    }
  ]
}
```

---

## 场景四：删除废弃组件

### 当前状态

假设应用有一个废弃的 `legacy-worker` 组件需要移除。

### 删除组件

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "2.1.0",
  "components": [
    {
      "action": "remove",
      "name": "legacy-worker"
    }
  ],
  "description": "Remove deprecated legacy-worker component"
}
```

**响应**：

```json
{
  "appId": "abc123xyz456",
  "version": "2.1.0",
  "previousVersion": "2.0.0",
  "strategy": "rolling",
  "taskId": "task-remove-004",
  "updatedComponents": [],
  "addedComponents": [],
  "removedComponents": ["legacy-worker"]
}
```

---

## 场景五：混合操作（更新 + 新增 + 删除）

### 当前状态

应用版本 `2.1.0`，包含 backend 和 redis-cache 组件。

### 执行架构重构

一次性完成：
- 更新 backend 镜像
- 新增 message-queue 组件
- 删除 old-scheduler 组件

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "3.0.0",
  "strategy": "rolling",
  "components": [
    {
      "action": "update",
      "name": "backend",
      "image": "myapp/backend:v3.0.0",
      "replicas": 3,
      "env": {
        "API_VERSION": "v3",
        "FEATURE_NEW_UI": "enabled"
      }
    },
    {
      "action": "add",
      "name": "message-queue",
      "type": "store",
      "image": "rabbitmq:3-management",
      "replicas": 1,
      "properties": {
        "ports": [
          {"port": 5672},
          {"port": 15672}
        ]
      }
    },
    {
      "action": "remove",
      "name": "old-scheduler"
    }
  ],
  "autoExec": true,
  "description": "Major architecture refactoring - v3.0.0"
}
```

**响应**：

```json
{
  "appId": "abc123xyz456",
  "version": "3.0.0",
  "previousVersion": "2.1.0",
  "strategy": "rolling",
  "taskId": "task-refactor-005",
  "updatedComponents": ["backend"],
  "addedComponents": ["message-queue"],
  "removedComponents": ["old-scheduler"]
}
```

---

## 场景六：自动执行但无可用工作流

### 用例

应用存在组件变更，但未配置任何可执行工作流（或工作流不可用）。`autoExec=true` 表示请求要求提交工作流，因此缺少可执行工作流时请求会失败。

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "3.0.1",
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v3.0.1"
    }
  ],
  "autoExec": true,
  "description": "No workflow configured"
}
```

**响应**：`404 Not Found`

```json
{
  "BusinessCode": 20005,
  "Message": "workflow not found"
}
```

---

## 场景七：仅更新版本号（不部署）

### 用例

需要记录一个版本号变更，但不触发实际部署（例如文档更新）。

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "3.0.1",
  "autoExec": false,
  "description": "Documentation update - no deployment needed"
}
```

**响应**：

```json
{
  "appId": "abc123xyz456",
  "version": "3.0.1",
  "previousVersion": "3.0.0",
  "strategy": "rolling",
  "taskId": "task-version-007",
  "updatedComponents": [],
  "addedComponents": [],
  "removedComponents": []
}
```

> **注意**：未触发工作流时，`taskId` 对应版本更新任务。

---

## 场景八：金丝雀发布

### 用例

使用金丝雀策略，先部署少量副本测试新版本。

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "3.1.0-canary",
  "strategy": "canary",
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v3.1.0",
      "replicas": 1
    }
  ],
  "description": "Canary release - testing v3.1.0 with 1 replica"
}
```

**响应**：

```json
{
  "appId": "abc123xyz456",
  "version": "3.1.0-canary",
  "previousVersion": "3.0.1",
  "strategy": "canary",
  "taskId": "task-canary-006",
  "updatedComponents": ["backend"],
  "addedComponents": [],
  "removedComponents": []
}
```

---

## 场景九：取消延迟版本更新

### 步骤 1：提交延迟更新

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "3.2.0",
  "executeAt": 1893456000,
  "components": [
    {
      "name": "backend",
      "image": "myapp/backend:v3.2.0"
    }
  ]
}
```

**响应示例**：

```json
{
  "appId": "abc123xyz456",
  "version": "3.2.0",
  "taskId": "task-delay-009"
}
```

### 步骤 2：取消该延迟任务

**请求**：`POST /api/v1/applications/abc123xyz456/version/cancel`

```json
{
  "taskId": "task-delay-009",
  "user": "admin",
  "reason": "change window updated"
}
```

**响应示例**：

```json
{
  "taskId": "task-delay-009",
  "status": "cancelled"
}
```

### 步骤 3：验证不可取消场景

当任务已经开始执行或已结束时，取消请求返回 `409`（业务码 `10033`）。

---

## 错误场景

### 错误 1：应用不存在

**请求**：`POST /api/v1/applications/non-existent-app/version`

```json
{
  "version": "1.0.0"
}
```

**响应**（HTTP 404）：

```json
{
  "HTTPCode": 404,
  "BusinessCode": 10005,
  "Message": "application not found"
}
```

### 错误 2：缺少版本号

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "components": [
    {"name": "backend", "image": "new-image"}
  ]
}
```

**响应**（HTTP 400）：

```json
{
  "HTTPCode": 400,
  "BusinessCode": 10000,
  "Message": "Key: 'UpdateVersionRequest.Version' Error:Field validation for 'Version' failed on the 'required' tag"
}
```

### 错误 3：组件不存在（更新时跳过）

**请求**：`POST /api/v1/applications/abc123xyz456/version`

```json
{
  "version": "1.2.0",
  "components": [
    {"name": "non-existent-component", "image": "some-image"}
  ]
}
```

**响应**（HTTP 200 - 成功但无更新）：

```json
{
  "appId": "abc123xyz456",
  "version": "1.2.0",
  "previousVersion": "1.1.0",
  "strategy": "rolling",
  "taskId": "task-version-008",
  "updatedComponents": [],
  "addedComponents": [],
  "removedComponents": []
}
```

> **注意**：不存在的组件会被跳过，不会报错。日志中会记录警告。

---

## cURL 命令示例

### 创建应用

```bash
curl -X POST "http://localhost:8000/api/v1/applications" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-backend-app",
    "namespace": "default",
    "version": "1.0.0",
    "component": [
      {
        "name": "backend",
        "type": "webservice",
        "image": "myapp/backend:v1.0.0",
        "replicas": 2,
        "properties": {
          "ports": [{"port": 8080}]
        }
      }
    ]
  }'
```

### 更新版本

```bash
# 替换 APP_ID 为实际的应用 ID
APP_ID="abc123xyz456"

curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version" \
  -H "Content-Type: application/json" \
  -d '{
    "version": "1.1.0",
    "strategy": "rolling",
    "components": [
      {"name": "backend", "image": "myapp/backend:v1.1.0"}
    ]
  }'
```

### 查看任务状态

```bash
TASK_ID="task-update-001"

curl "http://localhost:8000/api/v1/workflow/tasks/${TASK_ID}/status"
```

### 查看应用组件

```bash
curl "http://localhost:8000/api/v1/applications/${APP_ID}/components"
```

### 取消延迟版本更新任务

```bash
TASK_ID="task-delay-009"

curl -X POST "http://localhost:8000/api/v1/applications/${APP_ID}/version/cancel" \
  -H "Content-Type: application/json" \
  -d "{
    \"taskId\": \"${TASK_ID}\",
    \"user\": \"admin\",
    \"reason\": \"manual cancel\"
  }"
```

---

## 单元测试文件

| 文件 | 说明 |
|------|------|
| `pkg/apiserver/domain/service/application_version_test.go` | Service 层测试 |
| `pkg/apiserver/interfaces/api/workflow_test.go` | API 层测试 |

### 运行测试

```bash
# 运行版本更新相关测试
go test ./pkg/apiserver/domain/service/... -v -run TestUpdateVersion -count=1
go test ./pkg/apiserver/interfaces/api/... -v -run TestUpdateVersion -count=1

# 运行完整测试套件
go test ./pkg/apiserver/domain/service/... ./pkg/apiserver/interfaces/api/... -v -count=1
```

---

## 测试检查清单

| 场景 | 测试用例 | 状态 |
|------|---------|------|
| 镜像更新 | `TestUpdateVersionWithImageUpdate` | ✅ |
| 副本数更新 | `TestUpdateVersionWithReplicasUpdate` | ✅ |
| 版本记录 | `TestUpdateVersionWithPreviousVersion` | ✅ |
| 新增组件 | `TestUpdateVersionAddComponent` | ✅ |
| 删除组件 | `TestUpdateVersionRemoveComponent` | ✅ |
| 混合操作 | `TestUpdateVersionMixedOperations` | ✅ |
| 应用不存在 | `TestUpdateVersionMissingApp` | ✅ |
| 更新不存在组件失败 | `TestUpdateVersionRejectsMissingUpdateComponent` | ✅ |
| 新增已存在组件幂等忽略 | `TestUpdateVersionIgnoresExistingAddComponent` | ✅ |
| 删除不存在组件幂等忽略 | `TestUpdateVersionIgnoresMissingRemoveComponent` | ✅ |
| 非法组件 action 失败 | `TestUpdateVersionRejectsInvalidComponentAction` | ✅ |
| 同请求 remove/update 同组件失败 | `TestUpdateVersionAutoExecFalseRejectsSameRequestRemoveUpdate` | ✅ |
| 更新描述 | `TestUpdateVersionWithDescription` | ✅ |
| 默认策略 | `TestUpdateVersionDefaultStrategy` | ✅ |
| 无变更 | `TestUpdateVersionNoChanges` | ✅ |
| 取消延迟更新 | `TestCancelDelayedVersionUpdateEndpoint` | ✅ |
| API 完整流程 | `TestUpdateVersionEndpoint` | ✅ |
| API 最简请求 | `TestUpdateVersionEndpointMinimalRequest` | ✅ |
