package application

import (
	"fmt"
	"sort"
	"strings"

	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func normalizeVersionUpdateExecutionScope(raw string) (domainspec.VersionUpdateExecutionScope, error) {
	scope, ok := domainspec.NormalizeVersionUpdateExecutionScope(raw)
	if !ok {
		return "", fmt.Errorf("%w: unsupported executionScope %q", bcode.ErrApplicationConfig, strings.TrimSpace(raw))
	}
	return scope, nil
}

func validateVersionUpdateExecutionScopeActions(scope domainspec.VersionUpdateExecutionScope, actions versionUpdateResourceActions) error {
	if scope != domainspec.VersionUpdateExecutionScopeChangedComponents {
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
		if name == "" {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		result = append(result, name)
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
	var steps model.WorkflowSteps
	if workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return fmt.Errorf("%w: invalid workflow steps: %v", bcode.ErrWorkflowConfig, err)
		}
	}
	covered := versionUpdateWorkflowDeploymentComponents(steps)
	missing := make([]string, 0)
	for _, name := range components {
		if _, ok := covered[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("%w: executionScope changed_components workflow %s does not cover changed components: %s", bcode.ErrWorkflowConfig, strings.TrimSpace(workflow.ID), strings.Join(missing, ","))
	}
	return nil
}
