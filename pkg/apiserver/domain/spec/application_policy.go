package spec

import "strings"

type VersionUpdateExecutionScope string
type ManagementMode string

const (
	ManagementModeNative  ManagementMode = "native"
	ManagementModeObserve ManagementMode = "observe"
	ManagementModeAdopted ManagementMode = "adopted"
)

func NormalizeManagementMode(value string) (ManagementMode, bool) {
	switch ManagementMode(strings.ToLower(strings.TrimSpace(value))) {
	case ManagementModeNative:
		return ManagementModeNative, true
	case ManagementModeObserve:
		return ManagementModeObserve, true
	case ManagementModeAdopted:
		return ManagementModeAdopted, true
	default:
		return "", false
	}
}

// UpdateStrategy 版本更新策略类型
type UpdateStrategy string

const (
	// UpdateStrategyRolling 滚动更新（默认）- 逐步替换Pod，保证服务可用性
	UpdateStrategyRolling UpdateStrategy = "rolling"
	// UpdateStrategyRecreate 重建更新 - 先删除所有旧Pod，再创建新Pod
	UpdateStrategyRecreate UpdateStrategy = "recreate"
	// UpdateStrategyCanary 金丝雀更新 - 先更新部分Pod，验证后再全量更新
	UpdateStrategyCanary UpdateStrategy = "canary"
	// UpdateStrategyBlueGreen 蓝绿部署 - 创建新版本，切换流量后销毁旧版本
	UpdateStrategyBlueGreen UpdateStrategy = "blue-green"
)

// ParseUpdateStrategy 解析更新策略，默认返回滚动更新
func ParseUpdateStrategy(strategy string) UpdateStrategy {
	switch UpdateStrategy(strategy) {
	case UpdateStrategyRecreate:
		return UpdateStrategyRecreate
	case UpdateStrategyCanary:
		return UpdateStrategyCanary
	case UpdateStrategyBlueGreen:
		return UpdateStrategyBlueGreen
	case UpdateStrategyRolling:
		return UpdateStrategyRolling
	default:
		return UpdateStrategyRolling
	}
}

const (
	VersionUpdateExecutionScopeFullWorkflow      VersionUpdateExecutionScope = "full_workflow"
	VersionUpdateExecutionScopeChangedComponents VersionUpdateExecutionScope = "changed_components"
)

func NormalizeVersionUpdateExecutionScope(scope string) (VersionUpdateExecutionScope, bool) {
	switch VersionUpdateExecutionScope(strings.ToLower(strings.TrimSpace(scope))) {
	case "":
		return VersionUpdateExecutionScopeFullWorkflow, true
	case VersionUpdateExecutionScopeFullWorkflow:
		return VersionUpdateExecutionScopeFullWorkflow, true
	case VersionUpdateExecutionScopeChangedComponents:
		return VersionUpdateExecutionScopeChangedComponents, true
	default:
		return "", false
	}
}

// ComponentAction 组件操作类型
type ComponentAction string

const (
	// ComponentActionUpdate 更新组件（默认）
	ComponentActionUpdate ComponentAction = "update"

	// ComponentActionAdd 新增组件
	ComponentActionAdd ComponentAction = "add"

	// ComponentActionRemove 删除组件
	ComponentActionRemove ComponentAction = "remove"

	// ComponentActionRestart 重启组件工作负载
	ComponentActionRestart ComponentAction = "restart"
)

// ParseComponentAction 解析组件操作类型，默认返回更新
func ParseComponentAction(action string) ComponentAction {
	switch ComponentAction(action) {
	case ComponentActionAdd:
		return ComponentActionAdd
	case ComponentActionRemove:
		return ComponentActionRemove
	case ComponentActionRestart:
		return ComponentActionRestart
	case ComponentActionUpdate:
		return ComponentActionUpdate
	default:
		return ComponentActionUpdate
	}
}
