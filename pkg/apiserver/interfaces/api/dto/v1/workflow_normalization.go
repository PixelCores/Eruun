package v1

import (
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

// NormalizeApplicationNames gives component names, template target refs,
// Service trait names and workflow step names of a create request their stored
// form; create and Try apply it before validation.
func NormalizeApplicationNames(req *CreateApplicationsRequest) {
	for i := range req.Components {
		component := &req.Components[i]
		component.Name = spec.NormalizeName(component.Name)
		spec.NormalizeServiceTraitNames(&component.Traits)
		if component.Template != nil {
			component.Template.Target = spec.NormalizeName(component.Template.Target)
		}
	}
	NormalizeWorkflowSteps(req.Workflow)
}

// NormalizeWorkflowSteps is the write-boundary normalization the domain
// services apply before validating or storing workflow steps.
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
	*name = spec.NormalizeName(*name)
	for i := range components {
		components[i] = spec.NormalizeName(components[i])
	}
	for i := range properties {
		item := &properties[i]
		for j := range item.Policies {
			item.Policies[j] = spec.NormalizeName(item.Policies[j])
		}
		item.Path = strings.TrimSpace(item.Path)
		item.Container = strings.TrimSpace(item.Container)
	}
}
