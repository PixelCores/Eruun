package v1

import (
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

// NormalizeWorkflowSteps is the shared HTTP/gRPC presentation normalization
// before workflow validation and domain execution.
func NormalizeWorkflowSteps(steps []CreateWorkflowStepRequest) {
	for i := range steps {
		step := &steps[i]
		normalizeWorkflowStepNode(&step.Name, step.Components, step.Properties.Policies)
		step.Properties.Path = strings.TrimSpace(step.Properties.Path)
		step.Properties.Container = strings.TrimSpace(step.Properties.Container)
		step.StepType = config.WorkflowStepType(strings.ToLower(strings.TrimSpace(string(step.StepType))))
		if step.Approval != nil {
			step.Approval.NotifyURL = strings.TrimSpace(step.Approval.NotifyURL)
			step.Approval.Message = strings.TrimSpace(step.Approval.Message)
			step.Approval.Method = strings.ToUpper(strings.TrimSpace(step.Approval.Method))
		}
		for j := range step.SubSteps {
			subStep := &step.SubSteps[j]
			normalizeWorkflowStepNode(&subStep.Name, subStep.Components, subStep.Properties.Policies)
			subStep.Properties.Path = strings.TrimSpace(subStep.Properties.Path)
			subStep.Properties.Container = strings.TrimSpace(subStep.Properties.Container)
		}
	}
}

func normalizeWorkflowStepNode(name *string, components []string, policies []string) {
	*name = strings.ToLower(*name)
	for i := range components {
		components[i] = strings.TrimSpace(components[i])
	}
	for i := range policies {
		policies[i] = strings.TrimSpace(policies[i])
	}
}
