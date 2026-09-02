package application

import (
	"fmt"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func normalizeVersionUpdateExecutionScope(raw string) (config.VersionUpdateExecutionScope, error) {
	scope, ok := config.NormalizeVersionUpdateExecutionScope(raw)
	if !ok {
		return "", fmt.Errorf("%w: unsupported executionScope %q", bcode.ErrApplicationConfig, strings.TrimSpace(raw))
	}
	return scope, nil
}

func validateVersionUpdateExecutionScopeActions(scope config.VersionUpdateExecutionScope, actions versionUpdateResourceActions) error {
	if scope != config.VersionUpdateExecutionScopeChangedComponents {
		return nil
	}
	if actions.deployAll {
		return fmt.Errorf("%w: executionScope %s cannot be combined with add all", bcode.ErrApplicationConfig, scope)
	}
	if actions.fullCleanup {
		return fmt.Errorf("%w: executionScope %s cannot be combined with remove cleanup_all", bcode.ErrApplicationConfig, scope)
	}
	return nil
}

func versionUpdateExecutionComponents(updatedComponents, addedComponents []string) []string {
	seen := make(map[string]struct{}, len(updatedComponents)+len(addedComponents))
	result := make([]string, 0, len(updatedComponents)+len(addedComponents))
	appendComponent := func(name string) {
		trimmed := strings.TrimSpace(name)
		key := strings.ToLower(trimmed)
		if key == "" {
			return
		}
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		result = append(result, trimmed)
	}
	for _, name := range updatedComponents {
		appendComponent(name)
	}
	for _, name := range addedComponents {
		appendComponent(name)
	}
	return result
}

func validateVersionUpdateExecutionScopeWorkflowCoverage(workflow *model.Workflow, components []string) error {
	if len(components) == 0 {
		return nil
	}
	if workflow == nil {
		return bcode.ErrWorkflowNotExist
	}
	required := make(map[string]string, len(components))
	for _, name := range components {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		required[strings.ToLower(trimmed)] = trimmed
	}
	if len(required) == 0 {
		return nil
	}

	covered := make(map[string]struct{}, len(required))
	var steps model.WorkflowSteps
	if workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return fmt.Errorf("%w: invalid workflow steps: %v", bcode.ErrWorkflowConfig, err)
		}
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
			continue
		}
		if len(step.SubSteps) == 0 && versionUpdateWorkflowJobTypeDeploysComponents(step.WorkflowType) {
			for _, name := range step.ComponentNames() {
				key := strings.ToLower(strings.TrimSpace(name))
				if key != "" {
					covered[key] = struct{}{}
				}
			}
		}
		for _, sub := range step.SubSteps {
			if sub == nil || !versionUpdateWorkflowJobTypeDeploysComponents(sub.WorkflowType) {
				continue
			}
			for _, name := range sub.ComponentNames() {
				key := strings.ToLower(strings.TrimSpace(name))
				if key != "" {
					covered[key] = struct{}{}
				}
			}
		}
	}
	missing := make([]string, 0)
	for key, name := range required {
		if _, ok := covered[key]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("%w: executionScope changed_components workflow %s does not cover changed components: %s", bcode.ErrWorkflowConfig, strings.TrimSpace(workflow.ID), strings.Join(missing, ","))
	}
	return nil
}
