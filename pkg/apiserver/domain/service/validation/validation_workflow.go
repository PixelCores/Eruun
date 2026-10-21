package validation

import (
	"context"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

// TryWorkflow validates a workflow update request against existing components
func (v *validationServiceImpl) TryWorkflow(ctx context.Context, appID string, req apisv1.TryWorkflowRequest) *apisv1.TryWorkflowResponse {
	var errors []apisv1.ValidationError

	// 1. Validate workflow name if provided
	if req.Name != "" {
		errors = append(errors, applicationservice.ValidateLabelName(req.Name, "name")...)
	}
	workflowType := config.NormalizeWorkflowTaskType(req.WorkflowType)
	if workflowType != "" && !config.IsSupportedWorkflowTaskType(workflowType) {
		errors = append(errors, apisv1.ValidationError{
			Field:   "workflowType",
			Code:    apisv1.ErrCodeInvalidWorkflowStepType,
			Message: fmt.Sprintf("unsupported workflow workflowType: %s", req.WorkflowType),
		})
	}
	errors = append(errors, v.validateWorkflowCallback(ctx, req.Callback)...)
	if _, ok := workflowconfig.NormalizeWorkflowFailurePolicy(req.FailurePolicy); !ok {
		errors = append(errors, apisv1.ValidationError{
			Field:   "failurePolicy",
			Code:    apisv1.ErrCodeInvalidWorkflowFailurePolicy,
			Message: fmt.Sprintf("unsupported workflow failurePolicy: %s", req.FailurePolicy),
		})
	}

	// 2. Get existing components for the application
	componentIndex := make(map[string]config.JobType)
	if appID != "" && v.ComponentRepo != nil {
		components, err := v.ComponentRepo.FindByAppID(ctx, appID)
		if err == nil {
			for _, comp := range components {
				if comp != nil {
					name := strings.ToLower(strings.TrimSpace(comp.Name))
					if name != "" {
						componentIndex[name] = comp.ComponentType
					}
				}
			}
		}
	}

	// 3. Validate workflow steps
	if len(req.Workflow) == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   "workflow",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "workflow must contain at least one step",
		})
	} else {
		errors = append(errors, applicationservice.ValidateWorkflowSteps(req.Workflow, componentIndex, "workflow")...)
	}

	normalizedFailurePolicy := req.FailurePolicy
	if policy, ok := workflowconfig.NormalizeWorkflowFailurePolicy(req.FailurePolicy); ok &&
		(req.FailurePolicySet || strings.TrimSpace(string(req.FailurePolicy)) != "") {
		normalizedFailurePolicy = policy
	}
	normalized := &apisv1.UpdateApplicationWorkflowRequest{
		WorkflowID:    req.WorkflowID,
		Name:          strings.ToLower(req.Name),
		Alias:         req.Alias,
		Callback:      req.Callback,
		WorkflowType:  workflowType,
		FailurePolicy: normalizedFailurePolicy,
		Workflow:      req.Workflow,
	}
	return &apisv1.TryWorkflowResponse{
		Valid:          len(errors) == 0,
		Errors:         apisv1.FinalizeValidationErrors(errors, apisv1.ValidationPathWorkflow),
		NormalizedSpec: normalized,
		Plan:           apisv1.NewWorkflowExecutionPlan(req.Workflow),
	}
}

func (v *validationServiceImpl) validateWorkflowCallback(ctx context.Context, callback *apisv1.WorkflowCallback) []apisv1.ValidationError {
	if err := applicationservice.ValidateWorkflowCallback(ctx, v.Cfg, v.URLSecurityPolicyProvider, callback); err != nil {
		return []apisv1.ValidationError{{
			Field:   "callback",
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: err.Error(),
		}}
	}
	return nil
}

func workflowComponentIndexFromCreateComponents(components []apisv1.CreateComponentRequest) map[string]config.JobType {
	componentIndex := make(map[string]config.JobType, len(components))
	for _, component := range components {
		name := strings.ToLower(strings.TrimSpace(component.Name))
		if name == "" {
			continue
		}
		componentIndex[name] = component.ComponentType
	}
	return componentIndex
}
