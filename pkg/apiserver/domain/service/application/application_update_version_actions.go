package application

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	versionUpdateCleanupAllSentinelName = "cleanup_all"
	versionUpdateDeployAllSentinelName  = "all"
)

type versionUpdateResourceActions struct {
	components        []apisv1.ComponentUpdateSpec
	fullCleanup       bool
	deployAll         bool
	restartComponents []string
}

func parseVersionUpdateResourceActions(specs []apisv1.ComponentUpdateSpec) (versionUpdateResourceActions, error) {
	actions := versionUpdateResourceActions{
		components: make([]apisv1.ComponentUpdateSpec, 0, len(specs)),
	}
	restartSeen := make(map[string]string)
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return versionUpdateResourceActions{}, err
		}
		name := strings.TrimSpace(spec.Name)
		key := strings.ToLower(name)
		switch {
		case action == config.ComponentActionRemove && key == versionUpdateCleanupAllSentinelName:
			if actions.fullCleanup {
				return versionUpdateResourceActions{}, fmt.Errorf("%w: component %s cannot be used more than once in one version update request", bcode.ErrDuplicateComponentName, name)
			}
			if err := validateVersionUpdateSentinelSpec(spec); err != nil {
				return versionUpdateResourceActions{}, err
			}
			actions.fullCleanup = true
		case action == config.ComponentActionAdd && key == versionUpdateDeployAllSentinelName:
			if actions.deployAll {
				return versionUpdateResourceActions{}, fmt.Errorf("%w: component %s cannot be used more than once in one version update request", bcode.ErrDuplicateComponentName, name)
			}
			if err := validateVersionUpdateSentinelSpec(spec); err != nil {
				return versionUpdateResourceActions{}, err
			}
			actions.deployAll = true
		case action == config.ComponentActionRestart:
			if err := validateVersionUpdateSentinelSpec(spec); err != nil {
				return versionUpdateResourceActions{}, err
			}
			if _, exists := restartSeen[key]; exists {
				return versionUpdateResourceActions{}, fmt.Errorf("%w: component %s cannot be restarted more than once in one version update request", bcode.ErrDuplicateComponentName, name)
			}
			restartSeen[key] = name
			actions.restartComponents = append(actions.restartComponents, name)
		default:
			if isReservedVersionUpdateSentinelName(key) {
				return versionUpdateResourceActions{}, fmt.Errorf("%w: component name %q is reserved for version resource actions", bcode.ErrApplicationConfig, name)
			}
			actions.components = append(actions.components, spec)
		}
	}
	if len(actions.restartComponents) > 0 && (actions.fullCleanup || actions.deployAll) {
		return versionUpdateResourceActions{}, fmt.Errorf("%w: restart cannot be combined with cleanup_all or all resource actions", bcode.ErrApplicationConfig)
	}
	return actions, nil
}

func validateVersionUpdateSentinelSpec(spec apisv1.ComponentUpdateSpec) error {
	if spec.Image != "" ||
		spec.Replicas != nil ||
		len(spec.Env) > 0 ||
		spec.ComponentType != "" ||
		spec.Properties != nil ||
		spec.Traits != nil {
		return fmt.Errorf("%w: version resource action %s must not include component fields", bcode.ErrApplicationConfig, strings.TrimSpace(spec.Name))
	}
	return nil
}

func validateVersionUpdateRestartActions(restartComponents []string, componentMap map[string]*model.ApplicationComponent) error {
	for _, name := range restartComponents {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			return fmt.Errorf("%w: restart component name is required", bcode.ErrApplicationConfig)
		}
		comp, exists := componentMap[key]
		if !exists || comp == nil {
			return fmt.Errorf("%w: component %s not found for restart", bcode.ErrComponentNotFound, strings.TrimSpace(name))
		}
		switch comp.ComponentType {
		case config.ServerJob, config.StoreJob:
		default:
			return fmt.Errorf("%w: component %s with type %q cannot be restarted by version update", bcode.ErrApplicationConfig, comp.Name, comp.ComponentType)
		}
	}
	return nil
}

func marshalVersionUpdateResourceActionInfo(info *model.VersionUpdateResourceActionInfo) (string, error) {
	if versionUpdateResourceActionInfoEmpty(info) {
		return "", nil
	}
	info.Source = config.JobInfoSourceVersionUpdateAction
	info.Version = 1
	payload, err := model.NewJSONStructByStruct(info)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode version update resource action info: %w", err)
	}
	return string(data), nil
}

func versionUpdateResourceActionInfoEmpty(info *model.VersionUpdateResourceActionInfo) bool {
	return info == nil
}

func isReservedVersionUpdateSentinelName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case versionUpdateCleanupAllSentinelName, versionUpdateDeployAllSentinelName:
		return true
	default:
		return false
	}
}

func validateVersionUpdateDeployAllWorkflow(workflow *model.Workflow, components []*model.ApplicationComponent) error {
	if workflow == nil {
		return bcode.ErrWorkflowNotExist
	}
	if workflow.Disabled {
		return fmt.Errorf("%w: workflow %s is disabled", bcode.ErrExecWorkflow, strings.TrimSpace(workflow.ID))
	}
	required := make(map[string]string, len(components))
	for _, comp := range components {
		if comp == nil {
			continue
		}
		name := strings.TrimSpace(comp.Name)
		if name == "" {
			continue
		}
		required[strings.ToLower(name)] = name
	}
	if len(required) == 0 {
		return bcode.ErrApplicationNoComponents
	}
	var steps model.WorkflowSteps
	if workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return fmt.Errorf("%w: decode workflow steps: %v", bcode.ErrExecWorkflow, err)
		}
	}
	if len(steps.Steps) == 0 {
		return bcode.ErrWorkflowEmpty
	}
	if err := validateVersionUpdateWorkflowStepsJobTypes(&steps); err != nil {
		return err
	}
	covered := make(map[string]struct{}, len(required))
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
			continue
		}
		if len(step.SubSteps) == 0 && versionUpdateWorkflowJobTypeDeploysComponents(step.WorkflowType) {
			for _, name := range step.ComponentNames() {
				key := strings.ToLower(strings.TrimSpace(name))
				if key != "" {
					covered[key] = struct{}{}
				}
			}
		}
		for _, sub := range step.SubSteps {
			if sub == nil {
				continue
			}
			if !versionUpdateWorkflowJobTypeDeploysComponents(sub.WorkflowType) {
				continue
			}
			for _, name := range sub.ComponentNames() {
				key := strings.ToLower(strings.TrimSpace(name))
				if key != "" {
					covered[key] = struct{}{}
				}
			}
		}
	}
	var missing []string
	for key, name := range required {
		if _, ok := covered[key]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("%w: deploy all workflow %s does not cover components: %s", bcode.ErrWorkflowConfig, strings.TrimSpace(workflow.ID), strings.Join(missing, ","))
	}
	return nil
}

func validateVersionUpdateReadyWorkflowCoverage(workflow *model.Workflow, componentNames []string) error {
	if len(componentNames) == 0 {
		return nil
	}
	if workflow == nil {
		return bcode.ErrWorkflowNotExist
	}
	covered := make(map[string]struct{}, len(componentNames))
	var steps model.WorkflowSteps
	if workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return fmt.Errorf("%w: decode workflow steps: %v", bcode.ErrExecWorkflow, err)
		}
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
			continue
		}
		if len(step.SubSteps) == 0 && versionUpdateWorkflowJobTypeDeploysComponents(step.WorkflowType) {
			for _, name := range step.ComponentNames() {
				key := strings.ToLower(strings.TrimSpace(name))
				if key != "" {
					covered[key] = struct{}{}
				}
			}
		}
		for _, sub := range step.SubSteps {
			if sub == nil || !versionUpdateWorkflowJobTypeDeploysComponents(sub.WorkflowType) {
				continue
			}
			for _, name := range sub.ComponentNames() {
				key := strings.ToLower(strings.TrimSpace(name))
				if key != "" {
					covered[key] = struct{}{}
				}
			}
		}
	}

	missing := make([]string, 0)
	for _, name := range componentNames {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		if _, ok := covered[key]; !ok {
			missing = append(missing, strings.TrimSpace(name))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("%w: version update workflow %s does not cover Ready-observed components: %s", bcode.ErrWorkflowConfig, strings.TrimSpace(workflow.ID), strings.Join(missing, ","))
	}
	return nil
}

func versionUpdateWorkflowJobTypeDeploysComponents(jobType config.JobType) bool {
	switch config.JobType(strings.TrimSpace(string(jobType))) {
	case "", config.JobDeploy:
		return true
	default:
		return false
	}
}

func validateVersionUpdateWorkflowJobTypes(workflow *model.Workflow) error {
	if workflow == nil {
		return bcode.ErrWorkflowNotExist
	}
	var steps model.WorkflowSteps
	if workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return fmt.Errorf("%w: decode workflow steps: %v", bcode.ErrExecWorkflow, err)
		}
	}
	return validateVersionUpdateWorkflowStepsJobTypes(&steps)
}

func validateVersionUpdateWorkflowStepsJobTypes(steps *model.WorkflowSteps) error {
	if steps == nil {
		return nil
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
			continue
		}
		if !config.IsSupportedWorkflowJobType(step.WorkflowType) {
			return fmt.Errorf("%w: workflow jobType %q is not supported", bcode.ErrWorkflowConfig, step.WorkflowType)
		}
		for _, sub := range step.SubSteps {
			if sub == nil {
				continue
			}
			if !config.IsSupportedWorkflowJobType(sub.WorkflowType) {
				return fmt.Errorf("%w: workflow jobType %q is not supported", bcode.ErrWorkflowConfig, sub.WorkflowType)
			}
		}
	}
	return nil
}
