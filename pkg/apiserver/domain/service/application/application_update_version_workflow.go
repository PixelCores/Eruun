package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (c *applicationsServiceImpl) markComponentsUpdating(ctx context.Context, appID string, components []string) error {
	if appID == "" || len(components) == 0 {
		return nil
	}
	targets := make(map[string]struct{}, len(components))
	for _, name := range components {
		key := strings.ToLower(strings.TrimSpace(name))
		if key != "" {
			targets[key] = struct{}{}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	items, err := c.ComponentRepo.FindByAppID(ctx, appID)
	if err != nil {
		return fmt.Errorf("list components for app %s: %w", appID, err)
	}
	for _, comp := range items {
		if comp == nil {
			continue
		}
		if _, ok := targets[strings.ToLower(comp.Name)]; !ok {
			continue
		}
		if comp.Status == string(config.ComponentStatusCleaning) {
			continue
		}
		status := config.ComponentStatusUpdating
		lastAbnormal := ""
		comp.Status = string(status)
		comp.LastAbnormal = lastAbnormal
		if err := repository.UpdateComponentRuntimeFields(ctx, c.Store, comp, map[string]interface{}{
			"status":        string(status),
			"last_abnormal": lastAbnormal,
		}); err != nil {
			return fmt.Errorf("update component %s status to Updating: %w", comp.Name, err)
		}
	}
	return nil
}

func (c *applicationsServiceImpl) MarkInitialDeployingWorkflowComponents(ctx context.Context, appID, workflowID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil
	}
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		return fmt.Errorf("%w: workflow id is required", bcode.ErrWorkflowNotExist)
	}
	if c.ComponentRepo == nil || c.WorkflowRepo == nil || c.Store == nil {
		return fmt.Errorf("component runtime dependencies are not initialized")
	}
	workflow, err := c.WorkflowRepo.FindByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow %s for initial deploy status: %w", workflowID, err)
	}
	if workflow == nil || strings.TrimSpace(workflow.AppID) != appID {
		return fmt.Errorf("%w: workflow %s does not belong to app %s", bcode.ErrWorkflowNotExist, workflowID, appID)
	}
	targets, err := initialDeployingWorkflowComponentTargets(workflow)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}
	items, err := c.ComponentRepo.FindByAppID(ctx, appID)
	if err != nil {
		return fmt.Errorf("list components for app %s: %w", appID, err)
	}
	changed := false
	for _, comp := range items {
		if comp == nil {
			continue
		}
		if _, ok := targets[strings.ToLower(strings.TrimSpace(comp.Name))]; !ok {
			continue
		}
		if !shouldMarkInitialDeployingComponent(comp) {
			continue
		}
		status := config.ComponentStatusDeploying
		lastAbnormal := ""
		comp.Status = string(status)
		comp.LastAbnormal = lastAbnormal
		if err := repository.UpdateComponentRuntimeFields(ctx, c.Store, comp, map[string]interface{}{
			"status":        string(status),
			"last_abnormal": lastAbnormal,
		}); err != nil {
			return fmt.Errorf("update component %s status to Deploying: %w", comp.Name, err)
		}
		changed = true
	}
	if changed {
		c.invalidateApplicationComponentsCache(appID)
	}
	return nil
}

func initialDeployingWorkflowComponentTargets(workflow *model.Workflow) (map[string]struct{}, error) {
	if workflow == nil {
		return nil, bcode.ErrWorkflowNotExist
	}
	var steps model.WorkflowSteps
	if workflow.Steps != nil {
		if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
			return nil, fmt.Errorf("%w: invalid workflow steps: %v", bcode.ErrWorkflowConfig, err)
		}
	}
	targets := make(map[string]struct{})
	addNames := func(names []string) {
		for _, name := range names {
			key := strings.ToLower(strings.TrimSpace(name))
			if key != "" {
				targets[key] = struct{}{}
			}
		}
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) != config.WorkflowStepTypeComponent {
			continue
		}
		if len(step.SubSteps) == 0 && versionUpdateWorkflowJobTypeDeploysComponents(step.WorkflowType) {
			addNames(step.ComponentNames())
		}
		for _, sub := range step.SubSteps {
			if sub == nil || !versionUpdateWorkflowJobTypeDeploysComponents(sub.WorkflowType) {
				continue
			}
			addNames(sub.ComponentNames())
		}
	}
	return targets, nil
}

func shouldMarkInitialDeployingComponent(component *model.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	if !componentSupportsInitialDeployingStatus(component.ComponentType) {
		return false
	}
	status := strings.TrimSpace(component.Status)
	return status == "" || strings.EqualFold(status, string(config.ComponentStatusNotDeploy))
}

func componentSupportsInitialDeployingStatus(componentType config.JobType) bool {
	switch config.JobType(strings.TrimSpace(string(componentType))) {
	case config.ServerJob, config.StoreJob, config.InstantJob, config.ScheduledJob, config.ConfJob, config.SecretJob:
		return true
	default:
		return false
	}
}

func (c *applicationsServiceImpl) markComponentsRestarting(ctx context.Context, appID string, components []string) error {
	if appID == "" || len(components) == 0 {
		return nil
	}
	targets := make(map[string]struct{}, len(components))
	for _, name := range components {
		key := strings.ToLower(strings.TrimSpace(name))
		if key != "" {
			targets[key] = struct{}{}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	items, err := c.ComponentRepo.FindByAppID(ctx, appID)
	if err != nil {
		return fmt.Errorf("list components for app %s: %w", appID, err)
	}
	for _, comp := range items {
		if comp == nil {
			continue
		}
		if _, ok := targets[strings.ToLower(comp.Name)]; !ok {
			continue
		}
		if comp.Status == string(config.ComponentStatusCleaning) {
			continue
		}
		status := config.ComponentStatusRestarting
		lastAbnormal := ""
		comp.Status = string(status)
		comp.LastAbnormal = lastAbnormal
		if err := repository.UpdateComponentRuntimeFields(ctx, c.Store, comp, map[string]interface{}{
			"status":        string(status),
			"last_abnormal": lastAbnormal,
		}); err != nil {
			return fmt.Errorf("update component %s status to Restarting: %w", comp.Name, err)
		}
	}
	return nil
}

func (c *applicationsServiceImpl) syncWorkflowSteps(ctx context.Context, appID string, added, removed []string) error {
	workflows, err := c.WorkflowRepo.FindByAppID(ctx, appID)
	if err != nil || len(workflows) == 0 {
		return err
	}

	workflow := pickDefaultWorkflow(workflows, "", "")
	if workflow == nil {
		// Fallback for legacy data without a default workflow.
		workflow = workflows[0]
	}
	if workflow.Steps == nil {
		return nil
	}

	return applyVersionUpdateWorkflowStepSync(workflow, added, removed,
		func() ([]*model.ApplicationComponent, error) {
			return c.ComponentRepo.FindByAppID(ctx, appID)
		},
		func(workflow *model.Workflow) error {
			return c.WorkflowRepo.Update(ctx, workflow)
		},
	)
}

func syncWorkflowStepsInStore(ctx context.Context, store datastore.DataStore, appID, workflowID string, added, removed []string) error {
	workflowID = strings.TrimSpace(workflowID)
	var workflow *model.Workflow
	if workflowID != "" {
		selected, err := repository.WorkflowByID(ctx, store, workflowID)
		if err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return bcode.ErrWorkflowNotExist
			}
			return err
		}
		if selected.AppID == "" || selected.AppID != appID {
			return bcode.ErrWorkflowConfig
		}
		workflow = selected
	} else {
		workflows, err := repository.FindWorkflowsByAppID(ctx, store, appID)
		if err != nil || len(workflows) == 0 {
			return err
		}
		workflow = pickDefaultWorkflow(workflows, "", "")
		if workflow == nil {
			workflow = workflows[0]
		}
	}
	if workflow.Steps == nil {
		return nil
	}

	return applyVersionUpdateWorkflowStepSync(workflow, added, removed,
		func() ([]*model.ApplicationComponent, error) {
			return repository.FindComponentsByAppID(ctx, store, appID)
		},
		func(workflow *model.Workflow) error {
			return store.Put(ctx, workflow)
		},
	)
}

func applyVersionUpdateWorkflowStepSync(
	workflow *model.Workflow,
	added, removed []string,
	loadCurrentComponents func() ([]*model.ApplicationComponent, error),
	updateWorkflow func(*model.Workflow) error,
) error {
	var steps model.WorkflowSteps
	if err := decodeJSONStruct(workflow.Steps, &steps); err != nil {
		return err
	}
	if isTemplatePhasedWorkflow(&steps) {
		currentComponents, err := loadCurrentComponents()
		if err != nil {
			return err
		}
		rebuilt := convertWorkflowStepByTemplatePhasesFromComponents(currentComponents)
		applyWorkflowFailurePolicy(rebuilt, steps.FailurePolicy)
		newSteps, err := model.NewJSONStructByStruct(rebuilt)
		if err != nil {
			return err
		}
		workflow.Steps = newSteps
		return updateWorkflow(workflow)
	}

	removedSet := make(map[string]struct{}, len(removed))
	for _, name := range removed {
		removedSet[strings.ToLower(name)] = struct{}{}
	}

	filteredSteps := make([]*model.WorkflowStep, 0, len(steps.Steps))
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		stepName := strings.ToLower(step.Name)
		if _, shouldRemove := removedSet[stepName]; !shouldRemove {
			filteredSteps = append(filteredSteps, step)
		}
	}

	for _, name := range added {
		newStep := &model.WorkflowStep{
			Name:         name,
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
			Properties: []model.Policies{{
				Policies: []string{name},
			}},
		}
		filteredSteps = append(filteredSteps, newStep)
	}

	steps.Steps = filteredSteps

	newSteps, err := model.NewJSONStructByStruct(&steps)
	if err != nil {
		return err
	}
	workflow.Steps = newSteps

	return updateWorkflow(workflow)
}

func hasWorkflowStructureChanges(specs []apisv1.ComponentUpdateSpec, componentMap map[string]*model.ApplicationComponent) (bool, error) {
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return false, err
		}
		compName := strings.ToLower(strings.TrimSpace(spec.Name))
		if compName == "" {
			continue
		}
		switch action {
		case config.ComponentActionAdd:
			if _, exists := componentMap[compName]; exists {
				return false, fmt.Errorf("%w: component %s already exists for add", bcode.ErrComponentAlreadyExists, strings.TrimSpace(spec.Name))
			}
			return true, nil
		case config.ComponentActionRemove:
			if _, exists := componentMap[compName]; !exists {
				return false, fmt.Errorf("%w: component %s not found for remove", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			return true, nil
		}
	}
	return false, nil
}
