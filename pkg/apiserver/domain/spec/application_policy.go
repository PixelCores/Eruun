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

// UpdateStrategy is the legacy version-update request field; workload rollout
// behavior is controlled by component traits, not this value.
type UpdateStrategy string

const UpdateStrategyRolling UpdateStrategy = "rolling"

// NormalizeUpdateStrategy accepts only the historical default; unimplemented
// strategies must not be echoed as though they change execution behavior.
func NormalizeUpdateStrategy(strategy string) (UpdateStrategy, bool) {
	switch UpdateStrategy(strategy) {
	case "", UpdateStrategyRolling:
		return UpdateStrategyRolling, true
	default:
		return "", false
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
