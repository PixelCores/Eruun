package application

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

const (
	templatePhaseConfigSecret = iota
	templatePhaseStore
	templatePhaseJob
	templatePhaseWebService
	templatePhaseCount
)

var templatePhaseNames = [templatePhaseCount]string{
	"phase-2-config-secret",
	"phase-3-store",
	"phase-4-job",
	"phase-5-webservice",
}

var legacyTemplatePhaseNameAliases = map[string]int{
	"phase-1-config-secret": templatePhaseConfigSecret,
	"phase-2-store":         templatePhaseStore,
	"phase-3-job":           templatePhaseJob,
	"phase-4-webservice":    templatePhaseWebService,
}

func defaultWorkflowBodyForCreate(req apisv1.CreateApplicationsRequest, resolvedComponents []apisv1.CreateComponentRequest) interface{} {
	var workflowSteps *model.WorkflowSteps
	if len(req.Workflow) > 0 {
		workflowSteps = convertWorkflowStepsFromRequest(req.Workflow, workflowComponentNamesFromRequests(resolvedComponents))
	} else {
		workflowSteps = convertWorkflowStepByTemplatePhases(resolvedComponents)
	}
	applyWorkflowFailurePolicy(workflowSteps, req.FailurePolicy)
	return workflowSteps
}

func applyWorkflowFailurePolicy(steps *model.WorkflowSteps, policy workflowconfig.WorkflowFailurePolicy) {
	if steps == nil {
		return
	}
	normalized, _ := workflowconfig.NormalizeWorkflowFailurePolicy(policy)
	steps.FailurePolicy = normalized
}

func updateWorkflowFailurePolicySpecified(req apisv1.UpdateApplicationWorkflowRequest) bool {
	return req.FailurePolicySet || strings.TrimSpace(string(req.FailurePolicy)) != ""
}

func storedWorkflowFailurePolicy(raw *model.JSONStruct) (workflowconfig.WorkflowFailurePolicy, error) {
	var steps model.WorkflowSteps
	if raw == nil {
		policy, _ := workflowconfig.NormalizeWorkflowFailurePolicy("")
		return policy, nil
	}
	if err := decodeJSONStruct(raw, &steps); err != nil {
		return "", err
	}
	policy, ok := workflowconfig.NormalizeWorkflowFailurePolicy(steps.FailurePolicy)
	if !ok {
		return "", fmt.Errorf("%w: unsupported stored workflow failurePolicy %q", bcode.ErrWorkflowConfig, steps.FailurePolicy)
	}
	return policy, nil
}

func validateWorkflowFailurePolicy(policy workflowconfig.WorkflowFailurePolicy) error {
	if _, ok := workflowconfig.NormalizeWorkflowFailurePolicy(policy); ok {
		return nil
	}
	return bcode.ErrWorkflowConfig
}

func convertWorkflowStepByTemplatePhases(components []apisv1.CreateComponentRequest) *model.WorkflowSteps {
	workflowSteps := new(model.WorkflowSteps)
	if len(components) == 0 {
		return workflowSteps
	}

	phaseComponents := make([][]string, templatePhaseCount)
	seen := make(map[string]struct{}, len(components))
	for _, component := range components {
		name := strings.TrimSpace(component.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		phase := templatePhaseByComponentType(component.ComponentType)
		phaseComponents[phase] = append(phaseComponents[phase], name)
	}

	for index, names := range phaseComponents {
		if len(names) == 0 {
			continue
		}
		workflowSteps.Steps = append(workflowSteps.Steps, &model.WorkflowStep{
			Name:         templatePhaseNames[index],
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeDAG,
			Properties: []model.Policies{{
				Policies: names,
			}},
		})
	}

	return workflowSteps
}

func convertWorkflowStepByTemplatePhasesFromComponents(components []*model.ApplicationComponent) *model.WorkflowSteps {
	reqs := make([]apisv1.CreateComponentRequest, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		reqs = append(reqs, apisv1.CreateComponentRequest{
			Name:          component.Name,
			ComponentType: component.ComponentType,
		})
	}
	return convertWorkflowStepByTemplatePhases(reqs)
}

func isTemplatePhasedWorkflow(steps *model.WorkflowSteps) bool {
	if steps == nil || len(steps.Steps) == 0 {
		return false
	}
	phaseStepCount := 0
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if _, ok := templatePhaseIndexByName(step.Name); !ok {
			return false
		}
		if step.Mode != "" && step.Mode != config.WorkflowModeDAG {
			return false
		}
		if step.WorkflowType != "" && step.WorkflowType != config.JobDeploy {
			return false
		}
		phaseStepCount++
	}
	return phaseStepCount > 0
}

func templatePhaseByComponentType(componentType config.JobType) int {
	switch componentType {
	case config.ConfJob, config.SecretJob:
		return templatePhaseConfigSecret
	case config.StoreJob:
		return templatePhaseStore
	case config.InstantJob:
		return templatePhaseJob
	case config.ServerJob, config.ScheduledJob, config.Service:
		return templatePhaseWebService
	default:
		return templatePhaseWebService
	}
}

func templatePhaseIndexByName(name string) (int, bool) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	for idx, phaseName := range templatePhaseNames {
		if normalized == phaseName {
			return idx, true
		}
	}
	if idx, ok := legacyTemplatePhaseNameAliases[normalized]; ok {
		return idx, true
	}
	return 0, false
}

func convertWorkflowStepsFromRequest(steps []apisv1.CreateWorkflowStepRequest, componentNames map[string]string) *model.WorkflowSteps {
	workflowSteps := new(model.WorkflowSteps)
	for _, reqStep := range steps {
		stepType := config.ParseWorkflowStepType(string(reqStep.StepType))
		step := &model.WorkflowStep{
			SchedulingClass: reqStep.SchedulingClass,
			Name:            reqStep.Name,
			StepType:        stepType,
			WorkflowType:    reqStep.WorkflowType,
			Mode:            config.ParseWorkflowMode(reqStep.Mode),
			Approval:        convertWorkflowStepApprovalFromRequest(reqStep.Approval),
		}
		if stepType == config.WorkflowStepTypeApproval {
			workflowSteps.Steps = append(workflowSteps.Steps, step)
			continue
		}
		step.Properties = workflowModelPoliciesFromRequest(
			reqStep.Name,
			reqStep.WorkflowType,
			reqStep.Components,
			reqStep.Properties,
			componentNames,
		)
		for _, subReq := range reqStep.SubSteps {
			subStep := &model.WorkflowSubStep{
				SchedulingClass: subReq.SchedulingClass,
				Name:            subReq.Name,
				WorkflowType:    subReq.WorkflowType,
			}
			subStep.Properties = workflowModelPoliciesFromRequest(
				subReq.Name,
				subReq.WorkflowType,
				subReq.Components,
				subReq.Properties,
				componentNames,
			)
			step.SubSteps = append(step.SubSteps, subStep)
		}
		workflowSteps.Steps = append(workflowSteps.Steps, step)
	}
	return workflowSteps
}

func workflowModelPoliciesFromRequest(name string, jobType config.JobType, explicit []string, propertiesList []apisv1.WorkflowProperties, componentNames map[string]string) []model.Policies {
	if len(propertiesList) == 0 {
		policyComponentNames := workflowTargetComponents(name, jobType, explicit, nil, componentNames)
		if len(policyComponentNames) == 0 {
			return nil
		}
		return []model.Policies{{Policies: policyComponentNames}}
	}

	result := make([]model.Policies, 0, len(propertiesList))
	for _, item := range propertiesList {
		policyComponentNames := canonicalWorkflowComponentRefs(item.Policies, componentNames)
		if len(propertiesList) == 1 {
			policyComponentNames = workflowTargetComponents(name, jobType, explicit, item.Policies, componentNames)
		}
		if len(policyComponentNames) == 0 {
			continue
		}
		result = append(result, model.Policies{
			Policies:   policyComponentNames,
			Path:       strings.TrimSpace(item.Path),
			Container:  strings.TrimSpace(item.Container),
			InitSQLURL: strings.TrimSpace(item.InitSQLURL),
		})
	}
	return result
}

func convertWorkflowStepApprovalFromRequest(req *apisv1.WorkflowStepApproval) *model.WorkflowStepApproval {
	if req == nil {
		return nil
	}
	headers := req.Headers
	if len(headers) == 0 {
		headers = nil
	}
	return &model.WorkflowStepApproval{
		NotifyURL:      strings.TrimSpace(req.NotifyURL),
		Message:        strings.TrimSpace(req.Message),
		Method:         strings.ToUpper(strings.TrimSpace(req.Method)),
		Headers:        headers,
		TimeoutSeconds: req.TimeoutSeconds,
	}
}

func mergeWorkflowComponents(explicit []string, policies []string, componentNames map[string]string) []string {
	combined := append([]string{}, explicit...)
	combined = append(combined, policies...)
	return canonicalWorkflowComponentRefs(combined, componentNames)
}

func workflowTargetComponents(name string, jobType config.JobType, explicit []string, policies []string, componentNames map[string]string) []string {
	components := mergeWorkflowComponents(explicit, policies, componentNames)
	if len(components) > 0 || config.JobType(strings.TrimSpace(string(jobType))) != config.JobLogArchiveUpload {
		return components
	}
	return canonicalWorkflowComponentRefs([]string{name}, componentNames)
}

func canonicalWorkflowComponentRefs(values []string, componentNames map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	var result []string
	for _, v := range values {
		value := strings.TrimSpace(v)
		key := workflowComponentRefKey(value)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		if componentNames != nil {
			if canonical, ok := componentNames[key]; ok {
				result = append(result, canonical)
				continue
			}
		}
		result = append(result, value)
	}
	return result
}

func workflowComponentRefKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func workflowComponentNamesFromRequests(components []apisv1.CreateComponentRequest) map[string]string {
	componentNames := make(map[string]string, len(components))
	for _, component := range components {
		name := strings.TrimSpace(component.Name)
		key := workflowComponentRefKey(name)
		if key == "" {
			continue
		}
		componentNames[key] = name
	}
	return componentNames
}

func workflowComponentNamesFromModels(components []*model.ApplicationComponent) map[string]string {
	componentNames := make(map[string]string, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		name := strings.TrimSpace(component.Name)
		key := workflowComponentRefKey(name)
		if key == "" {
			continue
		}
		componentNames[key] = name
	}
	return componentNames
}

func ensureUniqueWorkflowName(base string, workflows []*model.Workflow) string {
	return ensureUniqueWorkflowNameExcluding(base, workflows, "")
}

func deriveWorkflowMetadata(app *model.Applications, workflows []*model.Workflow) (namespace, projectID, description string) {
	if app != nil {
		namespace = app.Namespace
		projectID = app.Project
		description = app.Description
	}
	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		if wf.Namespace != "" {
			namespace = wf.Namespace
		}
		if wf.ProjectID != "" {
			projectID = wf.ProjectID
		}
		if wf.Description != "" {
			description = wf.Description
		}
		break
	}
	return
}

func workflowComponentTypesFromRequests(components []apisv1.CreateComponentRequest) map[string]config.JobType {
	componentTypes := make(map[string]config.JobType, len(components))
	for _, component := range components {
		name := strings.ToLower(strings.TrimSpace(component.Name))
		if name == "" {
			continue
		}
		componentTypes[name] = component.ComponentType
	}
	return componentTypes
}

func workflowComponentTypesFromModels(components []*model.ApplicationComponent) map[string]config.JobType {
	componentTypes := make(map[string]config.JobType, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(component.Name))
		if name == "" {
			continue
		}
		componentTypes[name] = component.ComponentType
	}
	return componentTypes
}

var (
	validWorkflowStepTypes = map[config.WorkflowStepType]bool{
		config.WorkflowStepTypeComponent: true,
		config.WorkflowStepTypeApproval:  true,
	}
	validApprovalMethods = map[string]bool{
		"GET":    true,
		"POST":   true,
		"PUT":    true,
		"DELETE": true,
	}
)

// validateWorkflowComponentRefs rejects a workflow write with the first error
// of the rule set Try reports in full.
func validateWorkflowComponentRefs(steps []apisv1.CreateWorkflowStepRequest, existing map[string]config.JobType) error {
	return firstValidationError(bcode.ErrWorkflowConfig, ValidateWorkflowSteps(steps, existing, "workflow"))
}

// ValidateWorkflowSteps is the single rule set for workflow steps. components
// maps lower-cased component names to their types.
func ValidateWorkflowSteps(steps []apisv1.CreateWorkflowStepRequest, components map[string]config.JobType, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	stepNames := make(map[string]bool)
	for i, step := range steps {
		stepField := fmt.Sprintf("%s[%d]", field, i)
		if err := workflowconfig.ValidateJobSchedulingClass(step.SchedulingClass); err != nil {
			errors = append(errors, apisv1.ValidationError{Field: stepField + ".schedulingClass", Code: apisv1.ErrCodeInvalidWorkflowStepType, Message: err.Error()})
		}
		stepType := config.WorkflowStepType(strings.ToLower(strings.TrimSpace(string(step.StepType))))
		if stepType == "" {
			stepType = config.WorkflowStepTypeComponent
		}
		if step.Name != "" {
			errors = append(errors, ValidateLabelName(step.Name, stepField+".name")...)
			if key := strings.ToLower(step.Name); stepNames[key] {
				errors = append(errors, apisv1.ValidationError{
					Field:   stepField + ".name",
					Code:    apisv1.ErrCodeDuplicateWorkflowStep,
					Message: fmt.Sprintf("duplicate workflow step name: %s", step.Name),
				})
			} else {
				stepNames[key] = true
			}
		}
		switch config.WorkflowMode(step.Mode) {
		case "", config.WorkflowModeStepByStep, config.WorkflowModeDAG:
		default:
			errors = append(errors, apisv1.ValidationError{
				Field:   stepField + ".mode",
				Code:    apisv1.ErrCodeInvalidWorkflowMode,
				Message: fmt.Sprintf("invalid workflow mode: %s, must be one of: StepByStep, DAG", step.Mode),
			})
		}
		if !validWorkflowStepTypes[stepType] {
			errors = append(errors, apisv1.ValidationError{
				Field:   stepField + ".stepType",
				Code:    apisv1.ErrCodeInvalidWorkflowStepType,
				Message: fmt.Sprintf("invalid workflow step type: %s", step.StepType),
			})
			continue
		}
		if stepType == config.WorkflowStepTypeApproval {
			if len(step.Components) > 0 || len(step.Properties) > 0 || len(step.SubSteps) > 0 {
				errors = append(errors, apisv1.ValidationError{
					Field:   stepField,
					Code:    apisv1.ErrCodeInvalidApprovalConfig,
					Message: "approval step cannot define components, properties or substeps",
				})
			}
			errors = append(errors, validateWorkflowApproval(step.Approval, stepField+".approval")...)
			continue
		}
		if !config.IsSupportedWorkflowJobType(step.WorkflowType) {
			errors = append(errors, apisv1.ValidationError{
				Field:   stepField + ".jobType",
				Code:    apisv1.ErrCodeInvalidWorkflowStepType,
				Message: fmt.Sprintf("unsupported workflow jobType: %s", step.WorkflowType),
			})
			continue
		}

		items, itemErrors := workflowPropertiesValidationItems(step.Name, step.WorkflowType, step.Components, step.Properties, stepField)
		errors = append(errors, itemErrors...)
		if workflowPropertiesComponentRefCount(items) == 0 && len(step.SubSteps) == 0 {
			errors = append(errors, apisv1.ValidationError{
				Field:   stepField,
				Code:    apisv1.ErrCodeWorkflowStepNoComponent,
				Message: "workflow step must have at least one component or substep",
			})
		}
		errors = append(errors, validateWorkflowPropertiesItems(step.WorkflowType, items, len(step.SubSteps) == 0, components)...)

		for j, subStep := range step.SubSteps {
			subStepField := fmt.Sprintf("%s.subSteps[%d]", stepField, j)
			if err := workflowconfig.ValidateJobSchedulingClass(subStep.SchedulingClass); err != nil {
				errors = append(errors, apisv1.ValidationError{Field: subStepField + ".schedulingClass", Code: apisv1.ErrCodeInvalidWorkflowStepType, Message: err.Error()})
			}
			if !config.IsSupportedWorkflowJobType(subStep.WorkflowType) {
				errors = append(errors, apisv1.ValidationError{
					Field:   subStepField + ".jobType",
					Code:    apisv1.ErrCodeInvalidWorkflowStepType,
					Message: fmt.Sprintf("unsupported workflow jobType: %s", subStep.WorkflowType),
				})
				continue
			}
			if subStep.Name != "" {
				errors = append(errors, ValidateLabelName(subStep.Name, subStepField+".name")...)
			}
			subItems, subItemErrors := workflowPropertiesValidationItems(subStep.Name, subStep.WorkflowType, subStep.Components, subStep.Properties, subStepField)
			errors = append(errors, subItemErrors...)
			errors = append(errors, validateWorkflowPropertiesItems(subStep.WorkflowType, subItems, true, components)...)
		}
	}
	return errors
}

func validateWorkflowPropertiesItems(jobType config.JobType, items []workflowPropertiesValidationItem, executable bool, components map[string]config.JobType) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	for _, item := range items {
		if err := validateWorkflowInitSQLURL(jobType, item.properties.InitSQLURL, len(item.componentRefs) > 0, executable); err != nil {
			errors = append(errors, apisv1.ValidationError{Field: item.propertiesField + ".initSqlUrl", Code: apisv1.ErrCodeInvalidWorkflowStepType, Message: err.Error()})
		}
		errors = append(errors, validateLogArchiveUploadWorkflowStep(jobType, item, components)...)
		for _, ref := range item.componentRefs {
			if _, ok := components[ref.name]; !ok {
				errors = append(errors, apisv1.ValidationError{
					Field:   ref.field,
					Code:    apisv1.ErrCodeComponentNotFound,
					Message: fmt.Sprintf("component '%s' not found in application", ref.name),
				})
			}
		}
	}
	return errors
}

// validateWorkflowInitSQLURL keeps database reset parameters within the same
// HTTP(S) URL contract as the database reset submission endpoint and requires
// an executable target component so the URL survives persistence and execution.
func validateWorkflowInitSQLURL(jobType config.JobType, raw string, hasTarget, executable bool) error {
	if raw == "" {
		return nil
	}
	if jobType != config.JobDatabaseReset {
		return fmt.Errorf("initSqlUrl is only supported for database_reset jobType")
	}
	if _, err := normalizeDatabaseResetInitSQLURL(raw, true); err != nil {
		return fmt.Errorf("initSqlUrl must be a non-empty absolute HTTP(S) URL")
	}
	if !executable {
		return fmt.Errorf("initSqlUrl cannot be set on a parent step with subSteps")
	}
	if !hasTarget {
		return fmt.Errorf("initSqlUrl requires a target component")
	}
	return nil
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

func workflowPropertiesValidationItems(name string, jobType config.JobType, explicit []string, propertiesList []apisv1.WorkflowProperties, field string) ([]workflowPropertiesValidationItem, []apisv1.ValidationError) {
	explicitField := field + ".components"
	if len(propertiesList) == 0 {
		return []workflowPropertiesValidationItem{{
			propertiesField: field + ".properties",
			componentRefs:   workflowTargetComponentRefs(name, jobType, explicit, nil, explicitField, explicitField),
		}}, nil
	}

	items := make([]workflowPropertiesValidationItem, 0, len(propertiesList))
	var errors []apisv1.ValidationError
	seenComponents := make(map[string]struct{}, len(propertiesList))
	for i, item := range propertiesList {
		propertiesField := fmt.Sprintf("%s.properties[%d]", field, i)
		policiesField := propertiesField + ".policies"
		componentRefs := workflowComponentRefs(item.Policies, policiesField, nil)
		if len(propertiesList) == 1 {
			componentRefs = workflowTargetComponentRefs(name, jobType, explicit, item.Policies, explicitField, policiesField)
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
		errors = append(errors, validateExplicitComponentsMatchPropertyRefs(explicit, explicitField, seenComponents)...)
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
	return []workflowComponentValidationRef{{name: name, field: explicitField + "[0]"}}
}

func workflowComponentRefs(values []string, field string, existing []workflowComponentValidationRef) []workflowComponentValidationRef {
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
		refs = append(refs, workflowComponentValidationRef{name: name, field: fmt.Sprintf("%s[%d]", field, i)})
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
	mismatch := apisv1.ValidationError{
		Field:   explicitField,
		Code:    apisv1.ErrCodeInvalidWorkflowStepType,
		Message: "components must match properties policies when multiple workflow properties are provided",
	}
	if len(componentRefs) != len(propertyRefs) {
		return []apisv1.ValidationError{mismatch}
	}
	for _, ref := range componentRefs {
		if _, ok := propertyRefs[ref.name]; !ok {
			mismatch.Field = ref.field
			return []apisv1.ValidationError{mismatch}
		}
	}
	return nil
}

func validateLogArchiveUploadWorkflowStep(jobType config.JobType, item workflowPropertiesValidationItem, components map[string]config.JobType) []apisv1.ValidationError {
	if config.JobType(strings.TrimSpace(string(jobType))) != config.JobLogArchiveUpload {
		return nil
	}
	var errors []apisv1.ValidationError
	if strings.TrimSpace(item.properties.Path) == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   item.propertiesField + ".path",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: fmt.Sprintf("properties.path is required for workflow jobType: %s", jobType),
		})
	}
	for _, ref := range item.componentRefs {
		componentType, ok := components[ref.name]
		if ok && !config.ComponentTypeUsesPods(componentType) {
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
	if approval == nil {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: "approval config is required for approval step",
		}}
	}
	var errors []apisv1.ValidationError
	notifyURL := strings.TrimSpace(approval.NotifyURL)
	if notifyURL == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".notifyUrl",
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "approval notifyUrl is required",
		})
	} else if parsed, err := url.ParseRequestURI(notifyURL); err != nil || parsed.Host == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".notifyUrl",
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: fmt.Sprintf("invalid approval notifyUrl: %s", approval.NotifyURL),
		})
	} else if scheme := strings.ToLower(parsed.Scheme); scheme != "http" && scheme != "https" {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".notifyUrl",
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: "approval notifyUrl must use http or https",
		})
	}
	if method := strings.ToUpper(strings.TrimSpace(approval.Method)); method != "" && !validApprovalMethods[method] {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".method",
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: "approval method must be one of GET/POST/PUT/DELETE",
		})
	}
	if approval.TimeoutSeconds < 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   field + ".timeoutSeconds",
			Code:    apisv1.ErrCodeInvalidApprovalConfig,
			Message: "approval timeoutSeconds must be >= 0",
		})
	}
	return errors
}
