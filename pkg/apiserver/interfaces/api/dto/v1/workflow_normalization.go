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
		normalizeWorkflowStepNode(&step.Name, step.Components, step.Properties)
		step.StepType = config.WorkflowStepType(strings.ToLower(strings.TrimSpace(string(step.StepType))))
		if step.Approval != nil {
			step.Approval.NotifyURL = strings.TrimSpace(step.Approval.NotifyURL)
			step.Approval.Message = strings.TrimSpace(step.Approval.Message)
			step.Approval.Method = strings.ToUpper(strings.TrimSpace(step.Approval.Method))
		}
		for j := range step.SubSteps {
			subStep := &step.SubSteps[j]
			normalizeWorkflowStepNode(&subStep.Name, subStep.Components, subStep.Properties)
		}
	}
}

func normalizeWorkflowStepNode(name *string, components []string, properties []WorkflowProperties) {
	*name = strings.ToLower(*name)
	for i := range components {
		components[i] = strings.TrimSpace(components[i])
	}
	for i := range properties {
		item := &properties[i]
		for j := range item.Policies {
			item.Policies[j] = strings.TrimSpace(item.Policies[j])
		}
		item.Path = strings.TrimSpace(item.Path)
		item.Container = strings.TrimSpace(item.Container)
	}
}
