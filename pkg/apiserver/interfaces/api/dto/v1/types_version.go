package v1

import "github.com/PixelCores/Eruun/pkg/apiserver/config"

// UpdateVersionRequest 版本更新请求
type UpdateVersionRequest struct {
	// Version 新版本号（必填）
	Version string `json:"version" validate:"required"`

	// Strategy 更新策略：rolling（默认）、recreate、canary、blue-green
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

// ComponentUpdateSpec 组件更新规格
type ComponentUpdateSpec struct {
	// Action 操作类型：update（默认）、add、remove、restart
	Action string `json:"action,omitempty"`

	// Name 组件名称
	Name string `json:"name" validate:"required"`

	// 以下字段仅在 action 为 update 或 add 时有效

	// Image 新镜像地址（可选）
	Image string `json:"image,omitempty"`

	// Replicas 新副本数（可选，必须大于 0；/version 不支持 scale-to-zero）
	Replicas *int32 `json:"replicas,omitempty"`

	// Env 环境变量覆盖（可选，合并更新）
	Env map[string]string `json:"env,omitempty"`

	// 以下字段仅在 action 为 add 时需要

	// ComponentType 组件类型（新增时必填）
	ComponentType config.JobType `json:"type,omitempty"`

	// Properties 组件属性（新增时可选）
	Properties *Properties `json:"properties,omitempty"`

	// Traits 组件特性（新增时可选）
	Traits *Traits `json:"traits,omitempty"`
}

// UpdateVersionResponse 版本更新响应
type UpdateVersionResponse struct {
	// AppID 应用ID
	AppID string `json:"appId"`

	// Version 新版本号
	Version string `json:"version"`

	// PreviousVersion 更新前版本号
	PreviousVersion string `json:"previousVersion"`

	// Strategy 使用的更新策略
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

const (
	DiffUpdateTargetOnlyStrategyPreserve = "preserve"
	DiffUpdateTargetOnlyStrategyRemove   = "remove"
	DiffUpdateTargetOnlyStrategyBlock    = "block"

	DiffUpdateComponentActionPreserve = "preserve"
	DiffUpdateComponentActionBlock    = "block"
)

// DiffUpdateVersionRequest compares a source app with the target app from the path
// and optionally applies the generated version update.
type DiffUpdateVersionRequest struct {
	// SourceAppID source application ID used as the desired version/component snapshot
	SourceAppID string `json:"sourceAppId" validate:"required"`

	// DryRun returns the diff without applying it when true
	DryRun bool `json:"dryRun,omitempty"`

	// TargetOnlyStrategy handles components that exist only in the target app:
	// preserve (default), remove, or block.
	TargetOnlyStrategy string `json:"targetOnlyStrategy,omitempty"`

	// Strategy 更新策略：rolling（默认）、recreate、canary、blue-green
	Strategy string `json:"strategy,omitempty"`

	// ExecutionScope workflow 执行范围：full_workflow（默认）或 changed_components
	ExecutionScope string `json:"executionScope,omitempty"`

	// WorkflowID 指定自动执行的工作流（可选）
	WorkflowID string `json:"workflowId,omitempty"`

	// ExecuteAt 延迟执行时间（Unix 秒）；仅在 autoExec=true 且有组件变更时生效
	ExecuteAt int64 `json:"executeAt,omitempty"`

	// AutoExec 是否自动执行工作流，默认 true
	AutoExec *bool `json:"autoExec,omitempty"`

	// Description 更新说明
	Description string `json:"description,omitempty"`
}

// DiffUpdateVersionResponse describes component differences and the optional
// update result produced from those differences.
type DiffUpdateVersionResponse struct {
	TargetAppID           string                 `json:"targetAppId"`
	SourceAppID           string                 `json:"sourceAppId"`
	TargetPreviousVersion string                 `json:"targetPreviousVersion"`
	TargetVersion         string                 `json:"targetVersion"`
	SourceVersion         string                 `json:"sourceVersion"`
	DryRun                bool                   `json:"dryRun"`
	TargetOnlyStrategy    string                 `json:"targetOnlyStrategy"`
	VersionChanged        bool                   `json:"versionChanged"`
	HasChanges            bool                   `json:"hasChanges"`
	Executable            bool                   `json:"executable"`
	UpdatedComponents     []VersionComponentDiff `json:"updatedComponents,omitempty"`
	AddedComponents       []VersionComponentDiff `json:"addedComponents,omitempty"`
	ExtraComponents       []VersionComponentDiff `json:"extraComponents,omitempty"`
	BlockedComponents     []VersionComponentDiff `json:"blockedComponents,omitempty"`
	UpdateResult          *UpdateVersionResponse `json:"updateResult,omitempty"`
}

type VersionComponentDiff struct {
	Action string                  `json:"action"`
	Name   string                  `json:"name"`
	Type   config.JobType          `json:"type,omitempty"`
	Reason string                  `json:"reason,omitempty"`
	Fields []VersionComponentField `json:"fields,omitempty"`
	Before *VersionComponentState  `json:"before,omitempty"`
	After  *VersionComponentState  `json:"after,omitempty"`
}

type VersionComponentField struct {
	Field  string `json:"field"`
	Before any    `json:"before,omitempty"`
	After  any    `json:"after,omitempty"`
}

type VersionComponentState struct {
	Name       string         `json:"name"`
	Type       config.JobType `json:"type,omitempty"`
	Image      string         `json:"image,omitempty"`
	Replicas   int32          `json:"replicas"`
	Properties Properties     `json:"properties"`
	Traits     Traits         `json:"traits"`
}
