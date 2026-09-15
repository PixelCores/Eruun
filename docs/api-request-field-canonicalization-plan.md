# API 请求字段规范化方案

> 状态：Draft / Proposal。本文定义 Application 与 Workflow 写请求的字段收敛方案；在实现、测试和 Current 文档同步完成前，不代表当前 `/api/v1` 已拒绝历史字段。

## 1. 背景

当前 Application 与 Workflow 请求为了兼容历史调用方，在同一语义上接受了多组字段或形态：

- Application 组件列表同时接受 `component` 与 `components`。
- Workflow 更新和 Try Workflow 请求的步骤列表同时接受 `workflow` 与 `steps`。
- Application 创建请求的 `workflow` 既可以是步骤数组，也可以是包含 `steps`、`callback` 和 `failurePolicy` 的对象。

这些兼容形态增加了调用方、文档和 Agent 生成请求时的选择空间，也使读接口返回值与写接口的规范形态不够明确。本次变更采用一次严格切换，不继续保留同义字段或同一字段的多种 JSON 类型。

## 2. 目标

本次变更只完成以下公共契约收敛：

1. Application 组件列表只使用 `components`。
2. Workflow 步骤列表只使用 `steps`。
3. Application 内嵌 Workflow 只使用对象形态，步骤位于 `workflow.steps`。
4. 读接口返回的 `components` 与 `steps` 可以直接用于对应写请求。
5. 历史字段和历史形态被严格拒绝，不静默忽略、不自动改写，也不继续增加兼容分支。

## 3. 非目标

以下内容不在本次变更范围内：

- 不调整 Application、Component、Workflow、Task 或 Job 的领域边界。
- 不调整组件类型、Trait、Workflow job type、执行模式、失败清理、callback 或调度语义。
- 不处理 `jobType` / `workflowType`、Workflow `properties` 对象/数组等其他兼容形态。
- 不引入新的 DSL、API 版本、数据库表、字段、迁移或 Kubernetes 资源。
- 不改变独立空间 Job 的 `type + spec + traits` 请求契约。
- 不增加弃用宽限、兼容开关、请求自动迁移或 best-effort fallback。

## 4. 规范请求契约

### 4.1 Application 组件列表

以下入口只接受根级 `components`：

| 方法与路径 | 请求类型 | 规范字段 |
| --- | --- | --- |
| `POST /api/v1/applications` | 创建或刷新 Application | `components` |
| `POST /api/v1/applications/create-and-exec` | 创建并执行 Application | `components` |
| `POST /api/v1/applications/try` | 校验 Application | `components` |

规范示例：

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
  ]
}
```

根级 `component` 作为未知字段拒绝。请求同时携带 `component` 和 `components` 时同样按未知字段失败，不再保留专用的 alias conflict 分支。

本变更只处理上述 Application 请求中的组件列表。其他 API 中具有独立业务含义的 `component`、`componentName` 或路径参数不受影响。

### 4.2 Workflow 步骤列表

以下入口只接受根级 `steps`：

| 方法与路径 | 请求类型 | 规范字段 |
| --- | --- | --- |
| `PUT /api/v1/applications/:appID/workflow` | 创建或更新 Workflow | `steps` |
| `POST /api/v1/applications/:appID/workflow/try` | 校验 Workflow | `steps` |

规范示例：

```json
{
  "workflowId": "wf-demo",
  "name": "deploy-demo",
  "workflowType": "workflow",
  "steps": [
    {
      "name": "deploy-web",
      "jobType": "deploy",
      "components": ["web"],
      "mode": "StepByStep"
    }
  ]
}
```

根级 `workflow` 作为未知字段拒绝。请求同时携带 `workflow` 和 `steps` 时同样按未知字段失败，不再保留专用的 alias conflict 分支。

### 4.3 Application 内嵌 Workflow

Application 请求仍使用根级 `workflow` 表示 Workflow 配置容器；这里的 `workflow` 不再承担“步骤列表”的别名语义。它只接受对象，对象中的步骤字段固定为 `steps`：

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
  "workflow": {
    "failurePolicy": "cleanup_all",
    "callback": {
      "success": "https://example.com/workflow/success"
    },
    "steps": [
      {
        "name": "deploy-web",
        "jobType": "deploy",
        "components": ["web"],
        "mode": "StepByStep"
      }
    ]
  }
}
```

下列历史数组形态不再接受：

```json
{
  "components": [],
  "workflow": [
    {"name": "deploy-web", "components": ["web"]}
  ]
}
```

该边界保留 Workflow 级 `callback` 和 `failurePolicy` 的明确归属，并使所有公开步骤列表都只使用 `steps`。Application 根级 callback 的现有语义和 Workflow callback 覆盖规则保持不变。

## 5. 缺失、空值与错误语义

字段改名不改变现有业务校验：

- Application 是否允许省略或传入空 `components`，继续由现有 Application 校验和模板展开规则决定。
- Application 省略 `workflow`，或 `workflow.steps` 为空时，继续沿用现有默认 Workflow 生成规则。
- Workflow 更新和 Try Workflow 的 `steps` 仍必须至少包含一个步骤。
- `null`、错误 JSON 类型、未知字段和尾随 JSON 值继续由严格 JSON 绑定拒绝。
- 字段名精确区分大小写，只接受小写开头的 `components` 和 `steps`。
- 绑定错误继续使用对应入口现有的 Application 或 Workflow 业务错误封装，不把无效请求降级为成功。

## 6. 影响链路

实现需要按以下链路检查，不改变 Domain、数据库或 Kubernetes 执行语义：

```text
route
  -> request DTO / strict JSON decoder
  -> Application 或 Workflow service 的既有输入
  -> DTO 与 handler 回归测试
  -> Current 文档和可执行 examples
```

主要代码位置：

- `pkg/apiserver/interfaces/api/dto/v1/types.go`
- `pkg/apiserver/interfaces/api/dto/v1/types_workflow.go`
- `pkg/apiserver/interfaces/api/dto/v1/application_request_json.go`
- `pkg/apiserver/interfaces/api/dto/v1/types_test.go`
- `pkg/apiserver/interfaces/api/application_lifecycle.go`
- `pkg/apiserver/interfaces/api/application_workflow.go`
- `pkg/apiserver/interfaces/api/workflow_create_exec_test.go`
- `pkg/apiserver/interfaces/api/workflow_validation_query_version_test.go`

实现时应把请求 DTO 的 Go 字段同步收敛为 `Components` 和 `Steps`，避免公共 JSON 已规范化但内部仍继续使用历史单数或别名命名。只有在现有 service 边界需要保持稳定时才做一次明确映射，不新增平行 DTO。

## 7. 文档与示例迁移

实现 PR 必须同步更新：

- `docs/validation-api-guide.md`
- `docs/create-and-exec-application-api.md`
- `docs/app-workflow-callback.md`
- 其他 Current 文档中的 Application 创建或 Workflow 更新请求
- `examples/` 下所有可执行 Application 创建和 Workflow 更新请求
- README 中复制的当前请求示例

Historical / Audit 文档和旧 devlog 可以保留当时事实，但如果其中的载荷可能被误认为当前示例，应增加已经被本方案替代的说明。不得继续新增使用 `component`、根级 Workflow `workflow` 步骤数组或 Application `workflow` 数组的新示例。

## 8. 测试与验收

实现阶段至少覆盖以下行为：

1. 三个 Application 入口都接受 `components`。
2. 三个 Application 入口都拒绝 `component`。
3. Workflow 更新和 Try Workflow 都接受 `steps`。
4. Workflow 更新和 Try Workflow 都拒绝根级 `workflow`。
5. Application 创建入口接受 `workflow: {steps: [...]}`。
6. Application 创建入口拒绝 `workflow: [...]`。
7. Workflow 级 callback、failurePolicy、组件引用和执行顺序保持不变。
8. 未知字段、错误 JSON 类型、空步骤以及新旧字段同时出现时均返回确定性错误。
9. Current 文档和可执行 examples 不再生成已移除的请求形态。

建议验证命令：

```sh
go test ./pkg/apiserver/interfaces/api/dto/v1
go test ./pkg/apiserver/interfaces/api
go test ./pkg/apiserver/domain/service/application ./pkg/apiserver/domain/service/validation
go test ./... -race -cover
git diff --check
```

全量 race/cover 用于验证公共 DTO 变更没有遗漏跨包调用方；如果执行环境无法完成，PR 必须准确记录未验证范围。

## 9. 发布与迁移

这是 `/api/v1` 的显式破坏性请求契约变更。服务端不提供兼容期，因此部署前必须确认所有受控调用方已生成规范字段：

- Application 请求使用 `components`。
- Workflow 更新和 Try Workflow 请求使用 `steps`。
- Application 内嵌 Workflow 使用 `workflow: {steps: [...]}`。

未迁移调用方在服务端升级后会收到明确的请求绑定错误。发布说明应列出受影响入口和替换示例；不通过静默转换、配置开关或兼容 alias 延长旧契约。

## 10. 完成标准

本方案完成实现的判定条件是：

- 每一类公开列表语义只有一个规范字段：`components` 或 `steps`。
- 每一个受影响入口都严格拒绝历史字段或历史数组形态。
- 读写字段对称，读取到的 `components` 和 `steps` 可以直接用于对应更新请求。
- Domain、持久化、调度、Workflow 状态机和 Kubernetes 行为没有变化。
- 测试、Current 文档和可执行 examples 与新契约一致。
