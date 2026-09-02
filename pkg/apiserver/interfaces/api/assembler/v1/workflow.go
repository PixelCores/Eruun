package v1

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func convertWorkflowSteps(raw *model.JSONStruct) (config.WorkflowFailurePolicy, []apisv1.WorkflowStepDetail, error) {
	if raw == nil {
		policy, _ := config.NormalizeWorkflowFailurePolicy("")
		return policy, nil, nil
	}
	var steps model.WorkflowSteps
	if err := decodeJSONStruct(raw, &steps); err != nil {
		return "", nil, err
	}
	failurePolicy, _ := config.NormalizeWorkflowFailurePolicy(steps.FailurePolicy)
	result := make([]apisv1.WorkflowStepDetail, 0, len(steps.Steps))
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		detail := apisv1.WorkflowStepDetail{
			Name:         step.Name,
			StepType:     step.StepType,
			WorkflowType: step.WorkflowType,
			Mode:         step.Mode,
			Approval:     convertWorkflowStepApproval(step.Approval),
			Components:   flattenPolicies(step.Properties),
			Properties:   convertWorkflowProperties(step.Properties),
		}
		if len(step.SubSteps) > 0 {
			subDetails := make([]apisv1.WorkflowSubStepDetail, 0, len(step.SubSteps))
			for _, sub := range step.SubSteps {
				if sub == nil {
					continue
				}
				subDetails = append(subDetails, apisv1.WorkflowSubStepDetail{
					Name:         sub.Name,
					WorkflowType: sub.WorkflowType,
					Components:   flattenPolicies(sub.Properties),
					Properties:   convertWorkflowProperties(sub.Properties),
				})
			}
			detail.SubSteps = subDetails
		}
		result = append(result, detail)
	}
	return failurePolicy, result, nil
}

func convertWorkflowStepApproval(approval *model.WorkflowStepApproval) *apisv1.WorkflowStepApproval {
	if approval == nil {
		return nil
	}
	return &apisv1.WorkflowStepApproval{
		NotifyURL:      approval.NotifyURL,
		Message:        approval.Message,
		Method:         approval.Method,
		Headers:        approval.Headers,
		TimeoutSeconds: approval.TimeoutSeconds,
	}
}

func convertWorkflowProperties(policies []model.Policies) []apisv1.WorkflowProperties {
	if len(policies) == 0 {
		return nil
	}
	result := make([]apisv1.WorkflowProperties, 0, len(policies))
	for _, policy := range policies {
		if len(policy.Policies) == 0 && policy.Path == "" && policy.Container == "" {
			continue
		}
		result = append(result, apisv1.WorkflowProperties{
			Policies:  append([]string(nil), policy.Policies...),
			Path:      policy.Path,
			Container: policy.Container,
		})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func flattenPolicies(policies []model.Policies) []string {
	if len(policies) == 0 {
		return nil
	}
	var components []string
	for _, policy := range policies {
		if len(policy.Policies) == 0 {
			continue
		}
		components = append(components, policy.Policies...)
	}
	if len(components) == 0 {
		return nil
	}
	return components
}
