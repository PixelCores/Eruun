package workflow

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func filterVersionUpdateExecutionScopeWorkflowSteps(
	steps *model.WorkflowSteps,
	info model.VersionUpdateResourceActionInfo,
) (*model.WorkflowSteps, error) {
	scope, ok := config.NormalizeVersionUpdateExecutionScope(string(info.ExecutionScope))
	if !ok {
		return nil, fmt.Errorf("unsupported version update executionScope %q", info.ExecutionScope)
	}
	if scope != config.VersionUpdateExecutionScopeChangedComponents {
		return steps, nil
	}
	filtered := &model.WorkflowSteps{}
	if steps != nil {
		filtered.FailurePolicy = steps.FailurePolicy
	}
	allowed := versionUpdateExecutionComponentSet(info.ExecutionComponents)
	if steps == nil {
		return filtered, nil
	}
	filtered.Steps = make([]*model.WorkflowStep, 0, len(steps.Steps))
	for _, step := range steps.Steps {
		filteredStep := filterWorkflowStepByExecutionComponents(step, allowed)
		if filteredStep == nil {
			filteredStep = &model.WorkflowStep{}
		}
		filtered.Steps = append(filtered.Steps, filteredStep)
	}
	return filtered, nil
}

func filterWorkflowStepByExecutionComponents(step *model.WorkflowStep, allowed map[string]struct{}) *model.WorkflowStep {
	if step == nil {
		return nil
	}
	if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
		return step
	}
	stepCopy := *step
	if len(step.SubSteps) > 0 {
		subSteps := make([]*model.WorkflowSubStep, 0, len(step.SubSteps))
		for _, sub := range step.SubSteps {
			filteredSub := filterWorkflowSubStepByExecutionComponents(sub, allowed)
			if filteredSub != nil {
				subSteps = append(subSteps, filteredSub)
			}
		}
		if len(subSteps) == 0 {
			return nil
		}
		stepCopy.SubSteps = subSteps
		return &stepCopy
	}
	if !versionUpdateExecutionScopeJobTypeDeploysComponents(step.WorkflowType) {
		return nil
	}
	if len(step.Properties) > 0 {
		properties := filterWorkflowPoliciesByExecutionComponents(step.Properties, allowed)
		if len(properties) == 0 {
			return nil
		}
		stepCopy.Properties = properties
		return &stepCopy
	}
	if !versionUpdateExecutionComponentAllowed(step.Name, allowed) {
		return nil
	}
	return &stepCopy
}

func filterWorkflowSubStepByExecutionComponents(sub *model.WorkflowSubStep, allowed map[string]struct{}) *model.WorkflowSubStep {
	if sub == nil {
		return nil
	}
	if !versionUpdateExecutionScopeJobTypeDeploysComponents(sub.WorkflowType) {
		return nil
	}
	subCopy := *sub
	if len(sub.Properties) > 0 {
		properties := filterWorkflowPoliciesByExecutionComponents(sub.Properties, allowed)
		if len(properties) == 0 {
			return nil
		}
		subCopy.Properties = properties
		return &subCopy
	}
	if !versionUpdateExecutionComponentAllowed(sub.Name, allowed) {
		return nil
	}
	return &subCopy
}

func filterWorkflowPoliciesByExecutionComponents(policies []model.Policies, allowed map[string]struct{}) []model.Policies {
	filtered := make([]model.Policies, 0, len(policies))
	for _, policy := range policies {
		names := make([]string, 0, len(policy.Policies))
		for _, name := range policy.Policies {
			if versionUpdateExecutionComponentAllowed(name, allowed) {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			continue
		}
		policyCopy := policy
		policyCopy.Policies = names
		filtered = append(filtered, policyCopy)
	}
	return filtered
}

func versionUpdateExecutionComponentSet(components []string) map[string]struct{} {
	allowed := make(map[string]struct{}, len(components))
	for _, name := range components {
		key := strings.ToLower(strings.TrimSpace(name))
		if key != "" {
			allowed[key] = struct{}{}
		}
	}
	return allowed
}

func versionUpdateExecutionComponentAllowed(name string, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return false
	}
	_, ok := allowed[strings.ToLower(strings.TrimSpace(name))]
	return ok
}

func versionUpdateExecutionScopeJobTypeDeploysComponents(jobType config.JobType) bool {
	switch config.JobType(strings.TrimSpace(string(jobType))) {
	case "", config.JobDeploy:
		return true
	default:
		return false
	}
}
