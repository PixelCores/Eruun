# Canonical JSON Profile

> 状态：Current。本文定义面向用户、SDK 与 Agent 的唯一 JSON 请求形态，以及校验、读取、提交和后续动作发现契约。

## 1. 设计原则

Eruun 的公开请求采用一个可发现、可校验、可回读、可重放的规范形态：

- 同一语义只有一个字段名和一种 JSON 类型。
- 严格拒绝未知字段、历史别名和同一字段的多种表示。
- Try API 返回的规范化结果可直接提交，不要求调用方再次映射。
- 读接口返回可直接编辑并重新提交的写模型。
- 提交方可用幂等键避免重复创建 workflow task。
- 任务响应用机器可执行的 `allowedActions` 描述下一步，不要求 Agent 猜测路由。

当前版本不引入步骤数据流输出、`dependsOn` 或条件分支。

## 2. 唯一请求形态

### 2.1 Application

Application 的组件列表只使用根级 `components`，工作流步骤只使用根级数组 `workflow`。工作流级配置与该数组并列放在 Application 根级：

```json
{
  "name": "demo",
  "version": "1.0.0",
  "components": [
    {
      "name": "web",
      "type": "webservice",
      "image": "nginx:1.27",
      "replicas": 1,
      "properties": {},
      "traits": {}
    }
  ],
  "workflow": [
    {
      "name": "deploy-web",
      "jobType": "deploy",
      "components": [
        "web"
      ],
      "mode": "StepByStep"
    }
  ],
  "failurePolicy": "cleanup_all"
}
```

以下入口共享该形态：

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/v1/applications` | 创建或刷新 Application |
| `POST /api/v1/applications/create-and-exec` | 创建并提交默认或指定 Workflow |
| `POST /api/v1/applications/try` | 校验并规范化 Application |

根级 `component`、根级 `steps` 和 `workflow: {"steps": [...]}` 都是未知或类型错误。服务端不提供兼容别名或自动迁移。Application 根级 `callback` 与 `failurePolicy` 同时应用于它创建的默认 Workflow；需要为某个 Workflow 单独设置 callback 时，使用 Workflow 更新接口。

### 2.2 Workflow

独立 Workflow 请求的步骤列表同样只使用根级 `workflow`：

```json
{
  "workflowId": "wf-demo",
  "name": "deploy-demo",
  "workflowType": "workflow",
  "failurePolicy": "cleanup_all",
  "workflow": [
    {
      "name": "deploy-web",
      "jobType": "deploy",
      "components": [
        "web"
      ],
      "mode": "StepByStep"
    }
  ]
}
```

以下入口共享该形态：

| 方法与路径 | 用途 |
| --- | --- |
| `PUT /api/v1/applications/:appID/workflow` | 创建或更新 Workflow |
| `POST /api/v1/applications/:appID/workflow/try` | 校验并规范化 Workflow |

根级 `steps` 不再作为 `workflow` 的别名。

### 2.3 Step 与 properties

规范 Step 使用 `jobType`，使用组件名数组表达目标。需要额外参数时，`properties` 固定为对象数组：

```json
{
  "name": "archive-web",
  "jobType": "log_archive_upload",
  "components": ["web"],
  "properties": [
    {
      "policies": ["web"],
      "path": "/var/log/web",
      "container": "web"
    }
  ]
}
```

Schema 和所有服务端规范化输出只生成这一形态。请求解码器对本版本尚未移除的旧 Step 表示保持现有行为；新调用方不得依赖未出现在 Schema 中的表示。

## 3. JSON Schema

```http
GET /api/v1/schemas/v1/canonical.json
Accept: application/schema+json
```

返回 Draft 2020-12 Schema bundle，Schema ID 为：

```text
https://eruun.io/schemas/v1/canonical-profile.json
```

`$defs` 中的稳定入口为：

- `Application`
- `Component`
- `Trait`
- `Workflow`

对象默认使用 `additionalProperties: false`；业务上本来就是键值集合的字段，例如 labels、annotations、env 和 secret data，按字段值类型开放动态键。Schema 包含现有枚举、必填字段与数组下限，适用于编辑器提示、预提交校验和 Agent constrained decoding。

规范 Application 的 `name` 为 2–31 字符的小写 DNS-1123 名称；`components` 必须输出为数组，即使没有组件也使用 `[]`。

Schema 是规范 profile，不是历史输入兼容表。未出现在 Schema 中的字段或表示不应由新客户端生成。

## 4. Try：规范化、计划与结构化错误

Try API 成功处理请求时统一返回：

```json
{
  "valid": false,
  "errors": [
    {
      "path": "/workflow/0/components/0",
      "code": "COMPONENT_NOT_FOUND",
      "message": "component missing-web not found"
    }
  ],
  "normalizedSpec": {
    "name": "demo",
    "components": [],
    "workflow": [
      {
        "name": "deploy-web",
        "jobType": "deploy",
        "components": ["missing-web"]
      }
    ]
  },
  "plan": {
    "actions": [
      {
        "order": 1,
        "action": "execute",
        "target": "deploy-web",
        "targetType": "workflowStep",
        "jobType": "deploy",
        "components": ["missing-web"]
      }
    ]
  }
}
```

- `errors` 始终是数组；无错误时返回 `[]`。
- `path` 是 RFC 6901 JSON Pointer，指向 `normalizedSpec` 中的规范字段。
- `code` 用于程序分支，`message` 用于展示；客户端不应解析 message。
- `normalizedSpec` 与对应写接口同构。Application Try 的结果可提交到 Application 创建入口；Workflow Try 的结果可提交到 Workflow 更新入口。Workflow 请求显式传入空 `failurePolicy` 时，规范输出使用等价的 `cleanup_all`，从而保留“重置为默认策略”而不是“省略并保留现值”的语义。
- 模板覆盖项在展开前包含非法嵌套 Job 策略时，Application Try 保留覆盖项的原始 `components` 形态于 `normalizedSpec`，使错误路径仍能定位到需要修改的字段。此时直接组件的本地配置错误仍按原索引返回；模板相关校验待修正后再次 Try，届时才返回展开后的组件列表。
- `plan.actions` 是有序的逻辑计划，不承诺 Kubernetes Job 名称或实际开始时间。

JSON 绑定失败（未知字段、错误类型、多余 JSON 值）仍返回入口对应的 4xx 业务错误，因为此时无法构造可信的 `normalizedSpec`。

## 5. Read-edit-submit

读取完整可重提 Application 规范：

```http
GET /api/v1/applications/:appID/spec
```

响应 `data` 就是 `CreateApplicationsRequest` 的规范 JSON，可执行以下循环：

```text
GET spec -> 编辑 data -> POST /applications/try -> 取 normalizedSpec -> POST /applications
```

带 `ID` 的 Application 写入只在根级显式提供 `callback` 时覆盖 App 及其全部 Workflow callback。为了保证原样重提不会抹掉某个 Workflow 的独立 callback，当任一已存 Workflow callback 与 App callback 不一致或依赖 App fallback 时，GET spec 会省略根级 `callback`；省略该字段会保留现有 App/Workflow callback。需要统一覆盖全部 callback 时由调用方显式加入根级 `callback`，只修改单个 Workflow 时使用 Workflow `spec`。

只有 `native` 管理模式的 Application 支持该接口；observe/adopted 资源不是 Eruun 完整拥有的声明，因此不会伪造可重提 spec。该响应可能包含组件 credential 或 secret 属性，viewer 角色不能读取，调用方也不得记录或公开原始响应。

`GET /api/v1/applications/:appID/workflows` 返回的每个 Workflow 还包含 `spec` 字段。该字段可直接作为 `PUT /api/v1/applications/:appID/workflow` 的请求体：

```text
GET workflows -> 编辑 workflow.spec -> POST workflow/try -> 取 normalizedSpec -> PUT workflow
```

## 6. 提交幂等

以下执行入口接受可选请求头：

```http
Idempotency-Key: deploy-demo-20260915-001
```

- `POST /api/v1/applications/:appID/workflow/exec`
- `POST /api/v1/applications/create-and-exec`

键长度为 1 到 128，只允许字母、数字、点、下划线、冒号和连字符，不能有首尾空白或重复 header 值。

对相同 workspace、Application、幂等键、Workflow 和等价 `executeAt` 的重试，服务端返回第一次创建的 `taskId` 和当前状态，不创建重复 task。同一 Application 作用域的键如果改用于不同 Workflow 或不同 `executeAt`，直接执行入口返回 HTTP 409 的 `ErrWorkflowIdempotencyConflict`；`create-and-exec` 保持其组合接口语义，在 `execStatus=failed` 与 `execError` 中报告执行阶段冲突。客户端应为一次逻辑提交生成稳定键，为新的逻辑提交生成新键。

`create-and-exec` 的幂等边界是 Workflow task 入队。Application 创建/刷新仍按该接口现有的声明式 upsert 语义执行，然后复用原 task；幂等键不会把不同 Application 声明冻结为同一份请求。

服务端只存储作用域化哈希，不持久化调用方的原始幂等键。

## 7. allowedActions

Workflow 提交、任务列表、任务状态和任务阶段响应都返回 `allowedActions`。数组项是可直接执行的 HTTP 描述：

```json
{
  "name": "cancel",
  "method": "POST",
  "path": "/api/v1/applications/app-1/workflow/cancel",
  "body": {
    "taskId": "task-1"
  }
}
```

等待人工审批的任务返回 `continue` 与 `cancel`，并指向 task approval API；普通活跃任务返回取消动作；终态任务返回空数组。调用方应以返回的动作集合为准，不根据状态字符串自行拼接路由。身份认证 header 不出现在动作对象中，调用方沿用当前会话凭据。

## 8. 迁移和兼容边界

这是 `/api/v1` 的显式破坏性请求收敛。升级前必须迁移所有受控调用方：

- `component` 改为 `components`。
- Workflow 更新和 Try Workflow 的根级 `steps` 改为 `workflow`。
- Application 的 `workflow: {"steps": [...]}` 改为根级数组 `workflow: [...]`，嵌套的 `callback` 与 `failurePolicy` 移到 Application 根级。
- Try 错误项的 `field` 改为 RFC 6901 `path`；客户端必须按 `code` 分支，不解析 `message`。
- `/applications/try` 只接收完整 Application 形态；校验已有 Application 的 Workflow 使用路径携带 ID 的 `/applications/:appID/workflow/try`。

服务端不会静默转换这些已移除形态。其他尚未收敛的历史 Step 表示不属于本次兼容承诺，只以 Schema 生成结果作为新客户端依据。

## 9. 验收边界

当前 profile 完成以下五项能力：

1. 唯一 canonical JSON profile，以及同步的 Current 文档和 examples。
2. Application、Component、Trait、Workflow 的严格 JSON Schema。
3. `normalizedSpec + plan + structured errors` Try 响应。
4. Application 与 Workflow 的 read-edit-submit 同构链路。
5. Workflow 提交幂等与结构化 `allowedActions`。

真正的数据流输出、`dependsOn` 与条件分支留给后续独立设计，不在当前实现中声明。
