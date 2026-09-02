package validation

import (
	"context"
	"fmt"
	"net/url"
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
		errors = append(errors, v.validateName(req.Name, "name")...)
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
	componentIndex := make(workflowComponentIndex)
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
		errors = append(errors, v.validateWorkflowSteps(req.Workflow, componentIndex, "workflow")...)
	}

	return &apisv1.TryWorkflowResponse{
		Valid:  len(errors) == 0,
		Errors: errors,
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

type workflowComponentIndex map[string]config.JobType

func workflowComponentIndexFromCreateComponents(components []apisv1.CreateComponentRequest) workflowComponentIndex {
	componentIndex := make(workflowComponentIndex, len(components))
	for _, component := range components {
		name := strings.ToLower(strings.TrimSpace(component.Name))
		if name == "" {
			continue
		}
		componentIndex[name] = component.ComponentType
	}
	return componentIndex
}

// validateWorkflowSteps validates workflow steps and their component references
func (v *validationServiceImpl) validateWorkflowSteps(steps []apisv1.CreateWorkflowStepRequest, componentIndex workflowComponentIndex, fieldPrefix string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	stepNames := make(map[string]bool)

	for i, step := range steps {
		stepField := fmt.Sprintf("%s[%d]", fieldPrefix, i)
		stepType := config.WorkflowStepType(strings.ToLower(strings.TrimSpace(string(step.StepType))))
		if stepType == "" {
			stepType = config.WorkflowStepTypeComponent
		}

		// Validate step name
		if step.Name != "" {
			nameErrors := v.validateName(step.Name, fmt.Sprintf("%s.name", stepField))
			errors = append(errors, nameErrors...)

			// Check for duplicate step names
			lowerName := strings.ToLower(step.Name)
			if stepNames[lowerName] {
				errors = append(errors, apisv1.ValidationError{
					Field:   fmt.Sprintf("%s.name", stepField),
					Code:    apisv1.ErrCodeDuplicateWorkflowStep,
					Message: fmt.Sprintf("duplicate workflow step name: %s", step.Name),
				})
			} else {
				stepNames[lowerName] = true
			}
		}

		// Validate mode
		if !validWorkflowModes[step.Mode] {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.mode", stepField),
				Code:    apisv1.ErrCodeInvalidWorkflowMode,
				Message: fmt.Sprintf("invalid workflow mode: %s, must be one of: StepByStep, DAG", step.Mode),
			})
		}
		if _, ok := validWorkflowStepTypes[stepType]; !ok {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.stepType", stepField),
				Code:    apisv1.ErrCodeInvalidWorkflowStepType,
				Message: fmt.Sprintf("invalid workflow step type: %s", step.StepType),
			})
			continue
		}

		if stepType == config.WorkflowStepTypeApproval {
			if len(step.Components) > 0 || len(step.Properties.Policies) > 0 || len(step.SubSteps) > 0 {
				errors = append(errors, apisv1.ValidationError{
					Field:   stepField,
					Code:    apisv1.ErrCodeInvalidApprovalConfig,
					Message: "approval step cannot define components, properties or substeps",
				})
			}
			errors = append(errors, validateWorkflowApproval(step.Approval, fmt.Sprintf("%s.approval", stepField))...)
			continue
		}

		if !config.IsSupportedWorkflowJobType(step.WorkflowType) {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.jobType", stepField),
				Code:    apisv1.ErrCodeInvalidWorkflowStepType,
				Message: fmt.Sprintf("unsupported workflow jobType: %s", step.WorkflowType),
			})
			continue
		}

		propertyItems, propertyErrors := workflowPropertiesValidationItems(
			step.Name,
			step.WorkflowType,
			step.Components,
			step.Properties,
			step.WorkflowPropertiesList(),
			step.WorkflowPropertiesFromArray(),
			stepField,
		)
		errors = append(errors, propertyErrors...)

		// Check if step has any components
		if workflowPropertiesComponentRefCount(propertyItems) == 0 && len(step.SubSteps) == 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   stepField,
				Code:    apisv1.ErrCodeWorkflowStepNoComponent,
				Message: "workflow step must have at least one component or substep",
			})
		}

		for _, item := range propertyItems {
			errors = append(errors, validateLogArchiveUploadWorkflowStep(step.WorkflowType, item.properties, item.componentRefs, componentIndex, item.propertiesField)...)
			errors = append(errors, validateWorkflowComponentRefs(item.componentRefs, componentIndex)...)
		}

		// Validate substeps
		for j, subStep := range step.SubSteps {
			subStepField := fmt.Sprintf("%s.subSteps[%d]", stepField, j)
			if !config.IsSupportedWorkflowJobType(subStep.WorkflowType) {
				errors = append(errors, apisv1.ValidationError{
					Field:   fmt.Sprintf("%s.jobType", subStepField),
					Code:    apisv1.ErrCodeInvalidWorkflowStepType,
					Message: fmt.Sprintf("unsupported workflow jobType: %s", subStep.WorkflowType),
				})
				continue
			}

			// Validate substep name
			if subStep.Name != "" {
				errors = append(errors, v.validateName(subStep.Name, fmt.Sprintf("%s.name", subStepField))...)
			}

			subPropertyItems, subPropertyErrors := workflowPropertiesValidationItems(
				subStep.Name,
				subStep.WorkflowType,
				subStep.Components,
				subStep.Properties,
				subStep.WorkflowPropertiesList(),
				subStep.WorkflowPropertiesFromArray(),
				subStepField,
			)
			errors = append(errors, subPropertyErrors...)

			for _, item := range subPropertyItems {
				errors = append(errors, validateLogArchiveUploadWorkflowStep(subStep.WorkflowType, item.properties, item.componentRefs, componentIndex, item.propertiesField)...)
				errors = append(errors, validateWorkflowComponentRefs(item.componentRefs, componentIndex)...)
			}
		}
	}

	return errors
}

type workflowComponentValidationRef struct {
	name  string
	field string
}

type workflowPropertiesValidationItem struct {
	properties      apisv1.WorkflowProperties
	propertiesField string
	componentRefs   []workflowComponentValidationRef
}

func workflowPropertiesValidationItems(name string, jobType config.JobType, explicit []string, properties apisv1.WorkflowProperties, propertiesList []apisv1.WorkflowProperties, fromArray bool, fieldPrefix string) ([]workflowPropertiesValidationItem, []apisv1.ValidationError) {
	if !fromArray {
		return []workflowPropertiesValidationItem{{
			properties:      properties,
			propertiesField: fmt.Sprintf("%s.properties", fieldPrefix),
			componentRefs:   workflowTargetComponentRefs(name, jobType, explicit, properties.Policies, fmt.Sprintf("%s.components", fieldPrefix), fmt.Sprintf("%s.components", fieldPrefix)),
		}}, nil
	}
	if len(propertiesList) == 0 {
		return []workflowPropertiesValidationItem{{
			properties:      apisv1.WorkflowProperties{},
			propertiesField: fmt.Sprintf("%s.properties", fieldPrefix),
			componentRefs:   workflowTargetComponentRefs(name, jobType, explicit, nil, fmt.Sprintf("%s.components", fieldPrefix), fmt.Sprintf("%s.components", fieldPrefix)),
		}}, nil
	}

	items := make([]workflowPropertiesValidationItem, 0, len(propertiesList))
	var errors []apisv1.ValidationError
	seenComponents := make(map[string]struct{}, len(propertiesList))
	for i, item := range propertiesList {
		propertiesField := fmt.Sprintf("%s.properties[%d]", fieldPrefix, i)
		policiesField := fmt.Sprintf("%s.policies", propertiesField)
		componentRefs := workflowPolicyComponentRefs(item.Policies, policiesField)
		if len(propertiesList) == 1 {
			componentRefs = workflowTargetComponentRefs(name, jobType, explicit, item.Policies, fmt.Sprintf("%s.components", fieldPrefix), policiesField)
		} else if len(componentRefs) == 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   policiesField,
				Code:    apisv1.ErrCodeMissingRequiredField,
				Message: "properties.policies is required when multiple workflow properties are provided",
			})
			continue
		} else {
			var duplicateErrors []apisv1.ValidationError
			componentRefs, duplicateErrors = rejectDuplicateWorkflowPropertyRefs(componentRefs, seenComponents)
			errors = append(errors, duplicateErrors...)
		}
		items = append(items, workflowPropertiesValidationItem{
			properties:      item,
			propertiesField: propertiesField,
			componentRefs:   componentRefs,
		})
	}
	if len(propertiesList) > 1 {
		errors = append(errors, validateExplicitComponentsMatchPropertyRefs(explicit, fmt.Sprintf("%s.components", fieldPrefix), seenComponents)...)
	}
	return items, errors
}

func workflowPropertiesComponentRefCount(items []workflowPropertiesValidationItem) int {
	var count int
	for _, item := range items {
		count += len(item.componentRefs)
	}
	return count
}

func workflowTargetComponentRefs(name string, jobType config.JobType, explicit []string, policies []string, explicitField string, policiesField string) []workflowComponentValidationRef {
	refs := workflowComponentRefs(explicit, explicitField, nil)
	refs = workflowComponentRefs(policies, policiesField, refs)
	if len(refs) > 0 || config.JobType(strings.TrimSpace(string(jobType))) != config.JobLogArchiveUpload {
		return refs
	}
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return nil
	}
	return []workflowComponentValidationRef{{
		name:  name,
		field: fmt.Sprintf("%s[0]", explicitField),
	}}
}

func workflowPolicyComponentRefs(policies []string, policiesField string) []workflowComponentValidationRef {
	return workflowComponentRefs(policies, policiesField, nil)
}

func workflowComponentRefs(values []string, fieldPrefix string, existing []workflowComponentValidationRef) []workflowComponentValidationRef {
	seen := make(map[string]struct{}, len(values)+len(existing))
	for _, ref := range existing {
		seen[ref.name] = struct{}{}
	}
	refs := append([]workflowComponentValidationRef{}, existing...)
	for i, value := range values {
		name := strings.ToLower(strings.TrimSpace(value))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		refs = append(refs, workflowComponentValidationRef{
			name:  name,
			field: fmt.Sprintf("%s[%d]", fieldPrefix, i),
		})
	}
	return refs
}

func rejectDuplicateWorkflowPropertyRefs(componentRefs []workflowComponentValidationRef, seen map[string]struct{}) ([]workflowComponentValidationRef, []apisv1.ValidationError) {
	var uniqueRefs []workflowComponentValidationRef
	var errors []apisv1.ValidationError
	for _, ref := range componentRefs {
		if _, ok := seen[ref.name]; ok {
			errors = append(errors, apisv1.ValidationError{
				Field:   ref.field,
				Code:    apisv1.ErrCodeDuplicateComponent,
				Message: fmt.Sprintf("component '%s' is referenced by multiple workflow properties entries", ref.name),
			})
			continue
		}
		seen[ref.name] = struct{}{}
		uniqueRefs = append(uniqueRefs, ref)
	}
	return uniqueRefs, errors
}

func validateExplicitComponentsMatchPropertyRefs(explicit []string, explicitField string, propertyRefs map[string]struct{}) []apisv1.ValidationError {
	componentRefs := workflowComponentRefs(explicit, explicitField, nil)
	if len(componentRefs) == 0 {
		return nil
	}
	message := "components must match properties policies when multiple workflow properties are provided"
	if len(componentRefs) != len(propertyRefs) {
		return []apisv1.ValidationError{{
			Field:   explicitField,
			Code:    apisv1.ErrCodeInvalidWorkflowStepType,
			Message: message,
		}}
	}
	for _, ref := range componentRefs {
		if _, ok := propertyRefs[ref.name]; !ok {
			return []apisv1.ValidationError{{
				Field:   ref.field,
				Code:    apisv1.ErrCodeInvalidWorkflowStepType,
				Message: message,
			}}
		}
	}
	return nil
}

func validateWorkflowComponentRefs(componentRefs []workflowComponentValidationRef, componentIndex workflowComponentIndex) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	for _, ref := range componentRefs {
		if _, ok := componentIndex[ref.name]; !ok {
			errors = append(errors, apisv1.ValidationError{
				Field:   ref.field,
				Code:    apisv1.ErrCodeComponentNotFound,
				Message: fmt.Sprintf("component '%s' not found in application", ref.name),
			})
		}
	}
	return errors
}

func validateLogArchiveUploadWorkflowStep(jobType config.JobType, properties apisv1.WorkflowProperties, componentRefs []workflowComponentValidationRef, componentIndex workflowComponentIndex, propertiesField string) []apisv1.ValidationError {
	if config.JobType(strings.TrimSpace(string(jobType))) != config.JobLogArchiveUpload {
		return nil
	}
	var errors []apisv1.ValidationError
	if strings.TrimSpace(properties.Path) == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.path", propertiesField),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: fmt.Sprintf("properties.path is required for workflow jobType: %s", jobType),
		})
	}
	for _, ref := range componentRefs {
		componentType, ok := componentIndex[ref.name]
		if !ok {
			continue
		}
		if !config.ComponentTypeUsesPods(componentType) {
			errors = append(errors, apisv1.ValidationError{
				Field:   ref.field,
				Code:    apisv1.ErrCodeInvalidWorkflowStepType,
				Message: fmt.Sprintf("component '%s' with type '%s' does not use pods for workflow jobType: %s", ref.name, componentType, jobType),
			})
		}
	}
	return errors
}

func validateWorkflowApproval(approval *apisv1.WorkflowStepApproval, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if approval == nil {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: "approval config is required for approval step",
		}}
	}

	notifyURL := strings.TrimSpace(approval.NotifyURL)
	if notifyURL == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.notifyUrl", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "approval notifyUrl is required",
		})
	} else {
		parsed, err := url.ParseRequestURI(notifyURL)
		if err != nil || parsed == nil || parsed.Host == "" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.notifyUrl", field),
				Code:    apisv1.ErrCodeInvalidApprovalConfig,
				Message: fmt.Sprintf("invalid approval notifyUrl: %s", approval.NotifyURL),
			})
		} else if scheme := strings.ToLower(parsed.Scheme); scheme != "http" && scheme != "https" {
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.notifyUrl", field),
				Code:    apisv1.ErrCodeInvalidApprovalConfig,
				Message: "approval notifyUrl must use http or https",
			})
		}
	}

	method := strings.ToUpper(strings.TrimSpace(approval.Method))
	if method != "" && !validApprovalMethods[method] {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.method", field),
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: "approval method must be one of GET/POST/PUT/DELETE",
		})
	}
	if approval.TimeoutSeconds < 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.timeoutSeconds", field),
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: "approval timeoutSeconds must be >= 0",
		})
	}
	return errors
}
