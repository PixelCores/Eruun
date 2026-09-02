package application

import (
	"fmt"
	"net/url"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

const (
	templatePhaseCloudJob = iota
	templatePhaseConfigSecret
	templatePhaseStore
	templatePhaseJob
	templatePhaseWebService
	templatePhaseCount
)

var templatePhaseNames = [templatePhaseCount]string{
	"phase-1-job",
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
	if len(req.WorkflowSteps) > 0 {
		workflowSteps = convertWorkflowStepsFromRequest(req.WorkflowSteps, workflowComponentNamesFromRequests(resolvedComponents))
	} else {
		workflowSteps = convertWorkflowStepByTemplatePhases(resolvedComponents)
	}
	applyWorkflowFailurePolicy(workflowSteps, req.WorkflowFailurePolicy)
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
	case config.CloudJob:
		return templatePhaseCloudJob
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
			Name:         reqStep.Name,
			StepType:     stepType,
			WorkflowType: reqStep.WorkflowType,
			Mode:         config.ParseWorkflowMode(reqStep.Mode),
			Approval:     convertWorkflowStepApprovalFromRequest(reqStep.Approval),
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
			reqStep.WorkflowPropertiesList(),
			reqStep.WorkflowPropertiesFromArray(),
			componentNames,
		)
		for _, subReq := range reqStep.SubSteps {
			subStep := &model.WorkflowSubStep{
				Name:         subReq.Name,
				WorkflowType: subReq.WorkflowType,
			}
			subStep.Properties = workflowModelPoliciesFromRequest(
				subReq.Name,
				subReq.WorkflowType,
				subReq.Components,
				subReq.Properties,
				subReq.WorkflowPropertiesList(),
				subReq.WorkflowPropertiesFromArray(),
				componentNames,
			)
			step.SubSteps = append(step.SubSteps, subStep)
		}
		workflowSteps.Steps = append(workflowSteps.Steps, step)
	}
	return workflowSteps
}

func workflowModelPoliciesFromRequest(name string, jobType config.JobType, explicit []string, properties apisv1.WorkflowProperties, propertiesList []apisv1.WorkflowProperties, fromArray bool, componentNames map[string]string) []model.Policies {
	if !fromArray {
		return workflowModelPoliciesFromSingleProperty(name, jobType, explicit, properties, componentNames)
	}
	if len(propertiesList) == 0 {
		return workflowModelPoliciesFromSingleProperty(name, jobType, explicit, apisv1.WorkflowProperties{}, componentNames)
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
			Policies:  policyComponentNames,
			Path:      strings.TrimSpace(item.Path),
			Container: strings.TrimSpace(item.Container),
		})
	}
	return result
}

func workflowModelPoliciesFromSingleProperty(name string, jobType config.JobType, explicit []string, properties apisv1.WorkflowProperties, componentNames map[string]string) []model.Policies {
	policyComponentNames := workflowTargetComponents(name, jobType, explicit, properties.Policies, componentNames)
	if len(policyComponentNames) == 0 {
		return nil
	}
	return []model.Policies{{
		Policies:  policyComponentNames,
		Path:      strings.TrimSpace(properties.Path),
		Container: strings.TrimSpace(properties.Container),
	}}
}

func workflowPolicyComponentNames(policies []model.Policies) []string {
	if len(policies) == 0 {
		return nil
	}
	var components []string
	for _, policy := range policies {
		components = append(components, policy.Policies...)
	}
	return canonicalWorkflowComponentRefs(components, nil)
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

func workflowComponentRefKeys(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	var result []string
	for _, value := range values {
		key := workflowComponentRefKey(value)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, key)
	}
	return result
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

func validateWorkflowComponentRefs(steps []apisv1.CreateWorkflowStepRequest, existing map[string]config.JobType) error {
	for _, step := range steps {
		stepType := config.WorkflowStepType(strings.ToLower(strings.TrimSpace(string(step.StepType))))
		if stepType == "" {
			stepType = config.WorkflowStepTypeComponent
		}
		stepPolicies, err := validateWorkflowRequestProperties(
			step.Name,
			step.WorkflowType,
			step.Components,
			step.Properties,
			step.WorkflowPropertiesList(),
			step.WorkflowPropertiesFromArray(),
		)
		if err != nil {
			return err
		}
		stepComponents := workflowPolicyComponentNames(stepPolicies)
		switch stepType {
		case config.WorkflowStepTypeComponent:
			if err := validateWorkflowJobType(step.WorkflowType, step.Name); err != nil {
				return err
			}
			if err := validateLogArchiveUploadWorkflowProperties(step.WorkflowType, step.Name, stepPolicies); err != nil {
				return err
			}
			for _, sub := range step.SubSteps {
				subPolicies, err := validateWorkflowRequestProperties(
					sub.Name,
					sub.WorkflowType,
					sub.Components,
					sub.Properties,
					sub.WorkflowPropertiesList(),
					sub.WorkflowPropertiesFromArray(),
				)
				if err != nil {
					return err
				}
				if err := validateWorkflowJobType(sub.WorkflowType, sub.Name); err != nil {
					return err
				}
				if err := validateLogArchiveUploadWorkflowProperties(sub.WorkflowType, sub.Name, subPolicies); err != nil {
					return err
				}
			}
		case config.WorkflowStepTypeApproval:
			if len(stepComponents) > 0 || len(step.SubSteps) > 0 {
				return fmt.Errorf("%w: approval step %q cannot contain components/properties/substeps", bcode.ErrWorkflowConfig, step.Name)
			}
			if step.Approval == nil || strings.TrimSpace(step.Approval.NotifyURL) == "" {
				return fmt.Errorf("%w: approval step %q requires approval.notifyUrl", bcode.ErrWorkflowConfig, step.Name)
			}
			parsed, err := url.ParseRequestURI(strings.TrimSpace(step.Approval.NotifyURL))
			if err != nil || parsed == nil || parsed.Host == "" {
				return fmt.Errorf("%w: approval step %q notifyUrl is invalid", bcode.ErrWorkflowConfig, step.Name)
			}
			scheme := strings.ToLower(parsed.Scheme)
			if scheme != "http" && scheme != "https" {
				return fmt.Errorf("%w: approval step %q notifyUrl must use http or https", bcode.ErrWorkflowConfig, step.Name)
			}
			method := strings.ToUpper(strings.TrimSpace(step.Approval.Method))
			if method != "" && method != "GET" && method != "POST" && method != "PUT" && method != "DELETE" {
				return fmt.Errorf("%w: approval step %q method is invalid", bcode.ErrWorkflowConfig, step.Name)
			}
			if step.Approval.TimeoutSeconds < 0 {
				return fmt.Errorf("%w: approval step %q timeoutSeconds must be >= 0", bcode.ErrWorkflowConfig, step.Name)
			}
			continue
		default:
			return fmt.Errorf("%w: step %q has invalid stepType %q", bcode.ErrWorkflowConfig, step.Name, step.StepType)
		}
		if err := ensureComponentsExist(stepComponents, existing); err != nil {
			klog.Errorf("workflow step=%s references missing components: %v", step.Name, err)
			return err
		}
		if err := validateLogArchiveUploadWorkflowComponents(step.WorkflowType, step.Name, stepComponents, existing); err != nil {
			return err
		}
		for _, sub := range step.SubSteps {
			subPolicies, err := validateWorkflowRequestProperties(
				sub.Name,
				sub.WorkflowType,
				sub.Components,
				sub.Properties,
				sub.WorkflowPropertiesList(),
				sub.WorkflowPropertiesFromArray(),
			)
			if err != nil {
				return err
			}
			subComponents := workflowPolicyComponentNames(subPolicies)
			if err := ensureComponentsExist(subComponents, existing); err != nil {
				klog.Errorf("workflow substep=%s references missing components: %v", sub.Name, err)
				return err
			}
			if err := validateLogArchiveUploadWorkflowComponents(sub.WorkflowType, sub.Name, subComponents, existing); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateWorkflowRequestProperties(name string, jobType config.JobType, explicit []string, properties apisv1.WorkflowProperties, propertiesList []apisv1.WorkflowProperties, fromArray bool) ([]model.Policies, error) {
	if fromArray && len(propertiesList) > 1 {
		seen := make(map[string]struct{})
		var propertyComponents []string
		for _, item := range propertiesList {
			componentNames := workflowComponentRefKeys(item.Policies)
			if len(componentNames) == 0 {
				return nil, fmt.Errorf("%w: workflow step %q properties entries require policies when multiple properties are provided", bcode.ErrWorkflowConfig, name)
			}
			for _, componentName := range componentNames {
				if _, ok := seen[componentName]; ok {
					return nil, fmt.Errorf("%w: workflow step %q properties entries duplicate policy %q", bcode.ErrWorkflowConfig, name, componentName)
				}
				seen[componentName] = struct{}{}
				propertyComponents = append(propertyComponents, componentName)
			}
		}
		if err := validateWorkflowComponentsMatchProperties(name, explicit, propertyComponents); err != nil {
			return nil, err
		}
	}
	return workflowModelPoliciesFromRequest(name, jobType, explicit, properties, propertiesList, fromArray, nil), nil
}

func validateWorkflowComponentsMatchProperties(name string, explicit []string, properties []string) error {
	components := workflowComponentRefKeys(explicit)
	if len(components) == 0 {
		return nil
	}
	propertyComponents := workflowComponentRefKeys(properties)
	if len(components) != len(propertyComponents) {
		return fmt.Errorf("%w: workflow step %q components must match properties policies when multiple properties are provided", bcode.ErrWorkflowConfig, name)
	}
	seen := make(map[string]struct{}, len(propertyComponents))
	for _, componentName := range propertyComponents {
		seen[componentName] = struct{}{}
	}
	for _, componentName := range components {
		if _, ok := seen[componentName]; !ok {
			return fmt.Errorf("%w: workflow step %q components must match properties policies when multiple properties are provided", bcode.ErrWorkflowConfig, name)
		}
	}
	return nil
}

func validateLogArchiveUploadWorkflowProperties(jobType config.JobType, stepName string, policies []model.Policies) error {
	if jobType != config.JobLogArchiveUpload {
		return nil
	}
	if len(policies) == 0 {
		return fmt.Errorf("%w: workflow step %q requires properties.path for jobType %q", bcode.ErrWorkflowConfig, stepName, jobType)
	}
	for _, policy := range policies {
		if strings.TrimSpace(policy.Path) == "" {
			return fmt.Errorf("%w: workflow step %q requires properties.path for jobType %q", bcode.ErrWorkflowConfig, stepName, jobType)
		}
	}
	return nil
}

func validateLogArchiveUploadWorkflowComponents(jobType config.JobType, stepName string, componentNames []string, existing map[string]config.JobType) error {
	if config.JobType(strings.TrimSpace(string(jobType))) != config.JobLogArchiveUpload {
		return nil
	}
	for _, name := range componentNames {
		componentType, ok := existing[strings.ToLower(strings.TrimSpace(name))]
		if !ok {
			continue
		}
		if !config.ComponentTypeUsesPods(componentType) {
			return fmt.Errorf("%w: workflow step %q component %q with type %q does not use pods for jobType %q", bcode.ErrWorkflowConfig, stepName, name, componentType, jobType)
		}
	}
	return nil
}

func validateWorkflowJobType(jobType config.JobType, stepName string) error {
	if config.IsSupportedWorkflowJobType(jobType) {
		return nil
	}
	return fmt.Errorf("%w: workflow step %q has unsupported jobType %q", bcode.ErrWorkflowConfig, stepName, jobType)
}

func ensureComponentsExist(names []string, existing map[string]config.JobType) error {
	for _, name := range names {
		lower := strings.ToLower(strings.TrimSpace(name))
		if _, ok := existing[lower]; !ok {
			return fmt.Errorf("%w: component %q not found", bcode.ErrWorkflowConfig, name)
		}
	}
	return nil
}
