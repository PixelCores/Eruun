# 自定义更新工作流设计方案 (v2)

> 状态：Draft / Proposal。本文是更新工作流策略演进方案，当前能力以 `version-update-api.md` 为准。

> 示例说明：本文代码和结构示例均为概念伪代码，不是当前可调用接口或可直接合入的实现。

## 现状分析

### 1. 创建应用时自动生成 Update Workflow

调用 `CreateApplications` 时，系统会自动创建两个工作流：

```go
// pkg/apiserver/domain/service/application.go
func (c *applicationsServiceImpl) CreateApplications(...) {
    // 1. 创建默认工作流 (workflow 类型)
    wf, err := c.upsertDefaultWorkflow(ctx, store, application, req, resolvedComponents)
    
    // 2. 创建更新工作流 (update 类型) ← 新行为
    if _, err := c.upsertUpdateWorkflow(ctx, store, application, req, resolvedComponents); err != nil {
        return err
    }
}
```

**Update Workflow 命名规则**：
- Name: `{appName}-update-workflow`
- Alias: `{appID}-update-workflow`
- WorkflowType: `update`

### 2. UpdateVersion 工作流选择逻辑

```go
// 优先选 update 类型，fallback 到 default
target := pickWorkflowByType(workflows, config.WorkflowTaskTypeUpdate)
if target == nil {
    target = pickDefaultWorkflow(workflows, "", "")
}
```

**用户可以通过 `UpdateApplicationWorkflow` API 自定义 update 类型工作流**，`UpdateVersion` 会优先选择它。

### 3. syncWorkflowSteps 一致性风险

```go
// pkg/apiserver/domain/service/application.go: 2178-2179
func (c *applicationsServiceImpl) syncWorkflowSteps(...) {
    // ⚠️ 只更新第一个工作流 workflows[0]
    workflow := workflows[0]
    // ...
}
```

| 问题 | 影响 |
|------|------|
| **只同步第一个工作流** | 如果 update workflow 不是第一个，组件增删不会同步到它 |
| **用户自定义步骤被覆盖** | 若用户定义了 Pre/Post Job 步骤，同步时可能被意外覆盖 |

---

## 需求场景

复杂更新场景需要：
- 更新前执行初始化 Job（如数据库迁移脚本）
- 更新后执行验证或通知任务
- 每次更新可选不同更新流程（多套更新策略）

---

## 解决方案

### 方案一：优化同步策略（解决一致性问题）

**问题**：`syncWorkflowSteps` 仅同步第一个工作流。

**改进**：
1. 显式同步 **默认 workflow** 和 **默认 update workflow**
2. 识别"系统生成"的工作流（通过别名规则或标识字段）
3. 用户自定义的 update workflow 不自动同步（避免破坏手工步骤）

```go
func (c *applicationsServiceImpl) syncWorkflowSteps(...) {
    workflows, _ := c.WorkflowRepo.FindByAppID(ctx, appID)
    
    // 同步默认工作流
    defaultWf := pickDefaultWorkflow(workflows, "", "")
    if defaultWf != nil && isSystemGenerated(defaultWf) {
        syncSteps(defaultWf, added, removed)
    }
    
    // 同步默认 update 工作流
    updateWf := pickWorkflowByType(workflows, config.WorkflowTaskTypeUpdate)
    if updateWf != nil && isSystemGenerated(updateWf) {
        syncSteps(updateWf, added, removed)
    }
}

func isSystemGenerated(wf *model.Workflow) bool {
    // 通过别名规则判断，如 "{appID}-update-workflow"
    return strings.HasSuffix(wf.Alias, "-update-workflow") ||
           strings.HasSuffix(wf.Alias, "-workflow")
}
```

### 方案二：添加 workflowId 参数（支持多套更新策略）

**场景**：用户创建多个 update 工作流，每次更新选择不同策略。

**实现**：

```go
// pkg/apiserver/interfaces/api/dto/v1/types.go
type UpdateVersionRequest struct {
    // ... 现有字段
    WorkflowID string `json:"workflowId,omitempty"` // 可选指定工作流
}
```

**校验规则**：

| 校验项 | 说明 |
|--------|------|
| **归属校验** | `workflow.AppID == request.AppID` |
| **禁用状态** | `workflow.Disabled == false` |
| **类型兼容** | 可选：限制为 `update` 类型，或允许任意类型 |
| **失败兜底** | 校验失败时 fallback 到默认 update workflow |

```go
// pkg/apiserver/domain/service/application.go UpdateVersion 方法
if autoExec && hasChanges {
    var target *model.Workflow
    
    if req.WorkflowID != "" {
        wf, err := repository.WorkflowByID(ctx, c.Store, req.WorkflowID)
        if err == nil && wf.AppID == app.ID && !wf.Disabled {
            target = wf
        } else {
            klog.Warningf("specified workflowId %s invalid, fallback to default", req.WorkflowID)
        }
    }
    
    if target == nil {
        // 原有逻辑
        target = pickWorkflowByType(workflows, config.WorkflowTaskTypeUpdate)
        if target == nil {
            target = pickDefaultWorkflow(workflows, "", "")
        }
    }
    
    c.execWorkflow(ctx, target)
}
```

---

## 建议的实施顺序

1. **阶段一**：修复 syncWorkflowSteps 一致性问题
   - 同时同步 default 和 update 两个系统工作流
   - 添加系统生成标识，保护用户自定义步骤

2. **阶段二**：添加 workflowId 参数（可选）
   - 扩展 UpdateVersionRequest
   - 添加校验逻辑和 fallback 机制

---

## 影响评估

| 方面 | 阶段一 | 阶段二 |
|------|--------|--------|
| **代码变更量** | ~40 行 | ~60 行 |
| **API 兼容** | ✅ 内部优化 | ✅ 向后兼容（新增可选字段） |
| **破坏性变更** | 无 | 无 |
| **测试范围** | syncWorkflowSteps 单测 | UpdateVersion 集成测试 |
