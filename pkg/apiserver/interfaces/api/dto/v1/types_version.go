package v1

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// UpdateVersionRequest 版本更新请求
type UpdateVersionRequest struct {
	// Version 新版本号（必填）
	Version string `json:"version" validate:"required"`

	// Strategy 历史占位字段，仅接受空值或 rolling；实际更新策略由组件 traits.rollout 控制
	Strategy string `json:"strategy,omitempty"`

	// ExecutionScope workflow 执行范围：full_workflow（默认）或 changed_components
	ExecutionScope string `json:"executionScope,omitempty"`

	// Components 要更新的组件列表（可选，不填则仅更新版本号）
	Components []ComponentUpdateSpec `json:"components,omitempty"`

	// WorkflowID 指定自动执行的工作流（可选）
	WorkflowID string `json:"workflowId,omitempty"`

	// ExecuteAt 延迟执行时间（Unix 秒）；仅在 autoExec=true 且有组件变更时生效
	ExecuteAt int64 `json:"executeAt,omitempty"`

	// ImageReadyTimeoutSeconds workload 更新后的 Pod Ready 观测窗口（秒）；默认 300，仅 autoExec=true 且有真实 workload 变更时生效
	ImageReadyTimeoutSeconds int64 `json:"imageReadyTimeoutSeconds,omitempty"`

	// AutoExec 是否自动执行工作流，默认 true
	AutoExec *bool `json:"autoExec,omitempty"`

	// Callback 本次版本更新任务的终态回调覆盖；autoExec=true 且创建 workflow task 时挂到 workflow task，
	// 无实际组件/资源变更时挂到本次 update operation task；autoExec=false 时忽略
	Callback *WorkflowCallback `json:"callback,omitempty"`

	// Description 更新说明
	Description string `json:"description,omitempty"`
}

// ComponentUpdateSpec is the canonical component change specification.
type ComponentUpdateSpec = spec.ComponentUpdateSpec

// UpdateVersionResponse 版本更新响应
type UpdateVersionResponse struct {
	// AppID 应用ID
	AppID string `json:"appId"`

	// Version 新版本号
	Version string `json:"version"`

	// PreviousVersion 更新前版本号
	PreviousVersion string `json:"previousVersion"`

	// Strategy 历史字段固定回显 rolling，不表示执行了特定发布策略
	Strategy string `json:"strategy"`

	// ExecutionScope 使用的 workflow 执行范围
	ExecutionScope string `json:"executionScope"`

	// TaskID 工作流任务ID；未触发工作流时返回版本更新任务ID
	TaskID string `json:"taskId,omitempty"`

	// WorkflowID 工作流ID；未关联工作流时为空
	WorkflowID string `json:"workflowId,omitempty"`

	// UpdatedComponents 已更新的组件名称列表
	UpdatedComponents []string `json:"updatedComponents,omitempty"`

	// AddedComponents 新增的组件名称列表
	AddedComponents []string `json:"addedComponents,omitempty"`

	// RemovedComponents 已删除的组件名称列表
	RemovedComponents []string `json:"removedComponents,omitempty"`

	// RestartedComponents 已接受重启的组件名称列表
	RestartedComponents []string `json:"restartedComponents,omitempty"`
}
