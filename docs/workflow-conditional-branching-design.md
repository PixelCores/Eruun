# 工作流条件分支设计方案 (v1)

> 状态：Draft / Proposal。本文是条件分支设计方案，不代表当前 workflow 运行时已支持该语义。

> 示例说明：本文代码、JSON、YAML 和流程片段均为概念伪代码，不代表当前已实现契约。

## 背景

当前 Eruun 工作流引擎支持：

- 线性步骤推进
- Step 内串行或并行执行
- `approval` 审批暂停/继续
- 工作流终态 callback

但它不支持“根据上一步结果选择不同下一步”。现状里：

- `WorkflowStep` 没有分支字段
- `WorkflowQueue` 只有单个 `CurrentStep`
- `WorkflowCtl.Run` 按 `[]StepExecution` 线性推进
- 任一步骤失败后，工作流直接进入终态并返回

这导致一个常见需求无法表达：

- 发布成功后进入验证链路
- 发布失败后进入回滚或告警链路
- 最终两条路径可以重新汇合到同一个后置通知步骤

## 目标

本设计只解决“互斥条件分支”问题：

- 一个步骤结束后，根据该步骤结果选择一个后继步骤
- 运行时始终只有一条活动执行线
- 允许不同路径在后续步骤重新汇合
- 与现有 `CurrentStep` 单指针模型兼容

## 非目标

v1 明确不做以下能力：

- 同一个任务实例同时派生多个活动分支
- `wait-all` / `wait-any` 类型的 join 汇合节点
- 回跳、循环、自引用分支
- 审批步骤的条件分支
- `StepByStep` 顺序 `subSteps` 上的分支

如果未来要支持“一个步骤后产生多个流”，那已经不是当前这套顺序工作流的小扩展，而是要进入真正的 DAG/图执行引擎范畴。

## 总体思路

### 1. 只支持单选分支

给步骤增加 `branch` 字段，按步骤执行结果跳转到某个后续步骤名。

建议的数据模型：

```go
type WorkflowStepBranch struct {
    Success   string `json:"success,omitempty"`
    Failure   string `json:"failure,omitempty"`
    Timeout   string `json:"timeout,omitempty"`
    Cancelled string `json:"cancelled,omitempty"`
}
```

挂载位置：

```go
type WorkflowStep struct {
    Name         string                  `json:"name"`
    StepType     config.WorkflowStepType `json:"stepType,omitempty"`
    WorkflowType config.JobType          `json:"workflowType,omitempty"`
    Mode         config.WorkflowMode     `json:"mode,omitempty"`
    Approval     *WorkflowStepApproval   `json:"approval,omitempty"`
    Properties   []Policies              `json:"properties,omitempty"`
    SubSteps     []*WorkflowSubStep      `json:"subSteps,omitempty"`
    Branch       *WorkflowStepBranch     `json:"branch,omitempty"`
}
```

同样需要扩展：

- `pkg/apiserver/interfaces/api/dto/v1/types.go`
- workflow assembler/detail DTO
- workflow 校验逻辑

### 2. 分支目标使用现有 `name`

v1 不新增 `stepId`，直接复用步骤现有 `name` 作为跳转目标。

原因：

- 当前校验已对非空步骤名做重复检查
- 对外 JSON 结构改动更小
- 能保持用户配置简单

约束：

- 被引用为分支目标的步骤必须有非空 `name`
- 只允许跳到“后面的步骤”
- 分支目标必须唯一解析到一个执行节点

## 适用范围

由于当前 `GenerateJobTasks` 会把工作流编译为 `[]StepExecution`，而且 `StepByStep` 的 `subSteps` 会被展开成多个执行节点，所以 v1 只允许在“能稳定映射到单个执行节点”的步骤上定义分支。

允许：

- 普通组件步骤，无 `subSteps`
- `mode=DAG` 且 `subSteps` 被聚合为单个执行节点的步骤

不允许：

- `stepType=approval` 的步骤定义 `branch`
- `mode=StepByStep` 且包含多个 `subSteps` 的步骤定义 `branch`
- 分支目标指向一个会展开成多个执行节点的逻辑步骤

这是为了避免“一个逻辑步骤对应多个 `CurrentStep` 索引”带来的恢复歧义。

## 运行时语义

### 步骤结果归一化

分支判断基于“步骤结果”，不是单个 job 原始状态。

建议引入内部枚举：

```go
type StepResult string

const (
    StepResultSuccess   StepResult = "success"
    StepResultFailure   StepResult = "failure"
    StepResultTimeout   StepResult = "timeout"
    StepResultCancelled StepResult = "cancelled"
)
```

归一化规则：

- 该步骤所有 job 均为成功态时：`success`
- 任一 job 为 `timeout`：`timeout`
- 任一 job 为 `cancelled`：`cancelled`
- 其他非成功态：`failure`

成功态继续沿用现有语义：

- `completed`
- `passed`
- `skipped`
- 部分特殊 job 的 `distributed` / `queued` / `waiting`

### 下一步解析规则

对某一步执行完成后，按以下顺序解析下一步：

1. 计算该步骤的 `StepResult`
2. 查看当前步骤是否配置 `branch`
3. 若当前结果对应的分支目标非空，则跳到目标步骤
4. 若没有匹配分支：
   - `success` 走默认顺序，即 `CurrentStep + 1`
   - 非成功结果维持现有行为，工作流直接终止

这保证了：

- 不配置 `branch` 的工作流完全保持兼容
- 只想给失败场景加补偿链路时，不需要重写成功路径
- 需要“跳过后续某些步骤”时，可以显式配置 `success`

### 失败补偿后的最终状态

这是 v1 最关键的语义：

- 如果一个步骤先失败，然后通过分支进入回滚/补偿路径
- 即使补偿路径后面的步骤都执行成功
- 整个工作流最终状态仍应保留原始失败语义，而不是变成 `completed`

否则外部观察会误以为主流程成功了。

为此建议在 `WorkflowQueue` 增加两个可空字段：

```go
type WorkflowQueue struct {
    // 已有字段略
    DeferredTerminalStatus config.Status `json:"deferredTerminalStatus,omitempty" gorm:"type:varchar(32);column:deferred_terminal_status"`
    DeferredTerminalStep   string        `json:"deferredTerminalStep,omitempty" gorm:"type:varchar(255);column:deferred_terminal_step"`
}
```

语义：

- 当 `failure/timeout/cancelled` 分支被命中时：
  - `DeferredTerminalStatus` 记录原始失败态
  - `DeferredTerminalStep` 记录触发分支的步骤名
- 如果后续补偿链路全部成功，工作流结束时：
  - 最终状态取 `DeferredTerminalStatus`
- 如果补偿链路再次失败：
  - 以后一次真实失败为准

这样可以把“主流程失败，但补偿执行完成”与“整个流程成功”严格区分开。

## 例子

示例工作流：

```json
[
  {
    "name": "deploy",
    "components": ["backend"],
    "branch": {
      "success": "smoke-test",
      "failure": "rollback",
      "timeout": "rollback"
    }
  },
  {
    "name": "smoke-test",
    "components": ["smoke-job"],
    "branch": {
      "success": "notify-success",
      "failure": "rollback"
    }
  },
  {
    "name": "rollback",
    "components": ["rollback-job"],
    "branch": {
      "success": "notify-failure"
    }
  },
  {
    "name": "notify-success",
    "components": ["notify-success-job"]
  },
  {
    "name": "notify-failure",
    "components": ["notify-failure-job"]
  }
]
```

路径示意：

- `deploy success -> smoke-test success -> notify-success -> completed`
- `deploy failure -> rollback success -> notify-failure -> failed`
- `smoke-test failure -> rollback success -> notify-failure -> failed`

注意：

- 这里虽然两条路径都会流向“通知”步骤，但运行时始终只有一条活动路径
- 这不是多活分叉，也不需要 join 节点

## 对现有代码的影响

### 模型层

涉及文件：

- `pkg/apiserver/domain/model/workflow.go`
- `pkg/apiserver/domain/model/workflow_queue.go`

变更：

- 为 `WorkflowStep` 增加 `Branch`
- 为 `WorkflowQueue` 增加延迟终态字段

### DTO / API 层

涉及文件：

- `pkg/apiserver/interfaces/api/dto/v1/types.go`
- workflow 相关 assembler

变更：

- `CreateWorkflowStepRequest` 增加 `branch`
- `WorkflowStepDetail` 增加 `branch`
- 任务详情接口可选暴露 `deferredTerminalStatus` / `deferredTerminalStep`

### 校验层

涉及文件：

- `pkg/apiserver/domain/service/validation.go`

新增校验规则：

1. 配置了 `branch` 的步骤必须有非空 `name`
2. 每个分支目标必须存在
3. 分支目标必须位于当前步骤之后
4. 不允许自跳转和回跳
5. 不允许 `approval` 步骤定义 `branch`
6. 不允许顺序 `subSteps` 步骤定义 `branch`
7. 不允许目标步骤解析到多个执行节点

### 运行时控制器

涉及文件：

- `pkg/apiserver/event/workflow/controller.go`
- `pkg/apiserver/event/workflow/job_builder.go`

建议改动：

1. 在生成 `StepExecution` 时保留 `Branch` 元数据
2. 编译执行计划时建立 `stepName -> executionIndex` 映射
3. 每一步执行完成后：
   - 先算 `StepResult`
   - 再决定是顺序前进、分支跳转还是终止
4. `updateWorkflowStatus` 在收尾时优先使用 `DeferredTerminalStatus`

## 与现有特性的兼容关系

### 与审批步骤

v1 不支持“审批结果分支”：

- `continue` 仍然是 `CurrentStep + 1`
- `cancel` 仍然直接进入 `cancelled`

原因：

- 现有审批逻辑在 `ApproveWorkflowTask` 中直接修改 `CurrentStep`
- 如果要让审批也走分支，必须同步改造 `domain/service/workflow.go` 的审批状态机

这部分建议放到后续阶段再做。

### 与 callback

工作流终态 callback 继续复用现有机制，但终态来源需要调整：

- 没有延迟终态时：保持现状
- 有 `DeferredTerminalStatus` 且后续链路成功时：
  - callback 走对应失败态回调地址

例如：

- 发布失败进入 rollback，rollback 成功
- 最终仍调用 `failure` callback，而不是 `success`

### 与任务恢复

分支不引入多活动路径，所以恢复模型仍然基于：

- `CurrentStep`
- `ApprovalPending`
- `PendingApprovalStep`
- 新增 `DeferredTerminalStatus`

服务重启后，任务只需要从当前 `CurrentStep` 继续，不需要恢复多个分支上下文。

## 迁移与兼容性

兼容性目标：

- 老工作流 JSON 不变
- 不配置 `branch` 的行为完全不变
- 新增数据库列应允许为空

数据迁移：

- SQL 存储需要为 `workflow_queue` 增加两个 nullable 列
- 已存在任务默认 `deferred_terminal_status = ''`

## 建议的实施顺序

### 阶段一

- 扩展模型与 DTO
- 扩展 workflow 校验
- 完成文档与示例

### 阶段二

- 在 `job_builder` 中保留分支编译信息
- 在 `controller` 中实现分支跳转
- 引入 `DeferredTerminalStatus`

### 阶段三

- 补充任务详情可观测字段
- 补充分支路径相关日志
- 覆盖异常恢复与 callback 测试

## 最小测试矩阵

至少应覆盖：

1. 无 `branch` 工作流保持原行为
2. `success` 分支跳转成功
3. `failure` 分支触发 rollback
4. `timeout` 分支命中
5. `cancelled` 分支命中
6. 失败后补偿成功，但最终状态仍为原始失败态
7. 分支目标不存在时校验失败
8. 分支回跳时校验失败
9. 顺序 `subSteps` 配置分支时校验失败
10. 服务重启后从分支目标步骤恢复执行

## 风险与后续演进

v1 的主要限制是“单活动路径”。这意味着它适合：

- 成功/失败走不同后续步骤
- 补偿、回滚、告警、通知
- 单路径重新汇合

但它不适合：

- 同时创建多个后继流
- 一个节点等待两个前驱都完成
- 真正的图执行依赖

如果后续明确需要“多个流并行产生并汇合”，建议另起一版设计，核心变化应包括：

- `CurrentStep` 升级为 `ActiveSteps`
- 引入显式图节点和边
- 引入 join 节点语义
- 引入分支上下文和依赖计数

那将不再是本方案的演进补丁，而是工作流引擎模型升级。
