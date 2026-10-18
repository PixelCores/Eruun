package spec

import "strings"

type VersionUpdateExecutionScope string

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
