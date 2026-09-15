package v1

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func convertWorkflowSteps(raw *model.JSONStruct) (workflowconfig.WorkflowFailurePolicy, []apisv1.WorkflowStepDetail, error) {
	if raw == nil {
		policy, _ := workflowconfig.NormalizeWorkflowFailurePolicy("")
		return policy, nil, nil
	}
	var steps model.WorkflowSteps
	if err := decodeJSONStruct(raw, &steps); err != nil {
		return "", nil, err
	}
	failurePolicy, _ := workflowconfig.NormalizeWorkflowFailurePolicy(steps.FailurePolicy)
	result := make([]apisv1.WorkflowStepDetail, 0, len(steps.Steps))
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		detail := apisv1.WorkflowStepDetail{
			SchedulingClass: step.SchedulingClass,
			Name:            step.Name,
			StepType:        step.StepType,
			WorkflowType:    step.WorkflowType,
			Mode:            step.Mode,
			Approval:        convertWorkflowStepApproval(step.Approval),
			Components:      flattenPolicies(step.Properties),
			Properties:      convertWorkflowProperties(step.Properties),
		}
		if len(step.SubSteps) > 0 {
			subDetails := make([]apisv1.WorkflowSubStepDetail, 0, len(step.SubSteps))
			for _, sub := range step.SubSteps {
				if sub == nil {
					continue
				}
				subDetails = append(subDetails, apisv1.WorkflowSubStepDetail{
					SchedulingClass: sub.SchedulingClass,
					Name:            sub.Name,
					WorkflowType:    sub.WorkflowType,
					Components:      flattenPolicies(sub.Properties),
					Properties:      convertWorkflowProperties(sub.Properties),
				})
			}
			detail.SubSteps = subDetails
		}
		result = append(result, detail)
	}
	return failurePolicy, result, nil
}

func ConvertWorkflowModelToUpdateRequest(workflow *model.Workflow) (*apisv1.UpdateApplicationWorkflowRequest, error) {
	if workflow == nil {
		return nil, nil
	}
	failurePolicy, details, err := convertWorkflowSteps(workflow.Steps)
	if err != nil {
		return nil, err
	}
	callback, err := workflowCallbackFromModel(workflow)
	if err != nil {
		return nil, err
	}
	return &apisv1.UpdateApplicationWorkflowRequest{
		WorkflowID:       workflow.ID,
		Name:             workflow.Name,
		Alias:            workflow.Alias,
		Callback:         callback,
		WorkflowType:     workflow.WorkflowType,
		FailurePolicy:    failurePolicy,
		FailurePolicySet: true,
		Workflow:         workflowDetailsToCreateRequests(details),
	}, nil
}

func workflowDetailsToCreateRequests(details []apisv1.WorkflowStepDetail) []apisv1.CreateWorkflowStepRequest {
	steps := make([]apisv1.CreateWorkflowStepRequest, 0, len(details))
	for _, detail := range details {
		step := apisv1.CreateWorkflowStepRequest{
			SchedulingClass: detail.SchedulingClass,
			Name:            detail.Name,
			StepType:        detail.StepType,
			WorkflowType:    detail.WorkflowType,
			Approval:        detail.Approval,
			Components:      append([]string(nil), detail.Components...),
			Mode:            string(detail.Mode),
		}
		step.SetWorkflowPropertiesList(detail.Properties)
		for _, subDetail := range detail.SubSteps {
			subStep := apisv1.CreateWorkflowSubStepRequest{
				SchedulingClass: subDetail.SchedulingClass,
				Name:            subDetail.Name,
				WorkflowType:    subDetail.WorkflowType,
				Components:      append([]string(nil), subDetail.Components...),
			}
			subStep.SetWorkflowPropertiesList(subDetail.Properties)
			step.SubSteps = append(step.SubSteps, subStep)
		}
		steps = append(steps, step)
	}
	return steps
}

func workflowCallbackFromModel(workflow *model.Workflow) (*apisv1.WorkflowCallback, error) {
	if workflow == nil || workflow.Callback == nil {
		return nil, nil
	}
	var callback apisv1.WorkflowCallback
	if err := decodeJSONStruct(workflow.Callback, &callback); err != nil {
		return nil, err
	}
	return &callback, nil
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
