package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func loadWorkflowJobTaskInputs(ctx context.Context, task *model.WorkflowQueue, ds datastore.DataStore) (*model.WorkflowSteps, map[string]*model.ApplicationComponent, error) {
	if err := workflowjob.ValidateApplicationManagementModeForJob(ctx, ds, task.AppID); err != nil {
		return nil, nil, err
	}
	workflow := model.Workflow{ID: task.WorkflowID}
	if err := ds.Get(ctx, &workflow); err != nil {
		return nil, nil, fmt.Errorf("get workflow %s: %w", task.WorkflowID, err)
	}

	stepsBytes, err := json.Marshal(workflow.Steps)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal workflow steps: %w", err)
	}

	var workflowSteps model.WorkflowSteps
	if err := json.Unmarshal(stepsBytes, &workflowSteps); err != nil {
		return nil, nil, fmt.Errorf("unmarshal workflow steps: %w", err)
	}
	if err := validateWorkflowJobTypes(&workflowSteps); err != nil {
		return nil, nil, err
	}

	componentEntities, err := ds.List(ctx, &model.ApplicationComponent{AppID: task.AppID}, &datastore.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list application components for app %s: %w", task.AppID, err)
	}
	resourceAppName := ""
	if len(componentEntities) > 0 {
		resourceAppName, err = loadResourceAppName(ctx, task, ds)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve application name for resource naming: %w", err)
		}
	}
	componentMap := make(map[string]*model.ApplicationComponent)
	for _, entity := range componentEntities {
		if component, ok := entity.(*model.ApplicationComponent); ok {
			componentCopy := *component
			componentCopy.ResourceAppName = resourceAppName
			componentMap[component.Name] = &componentCopy
		}
	}
	if err := validateLogArchiveUploadWorkflowInputs(&workflowSteps, componentMap); err != nil {
		return nil, nil, err
	}
	return &workflowSteps, componentMap, nil
}

func validateWorkflowJobTypes(steps *model.WorkflowSteps) error {
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
		if err := validateWorkflowJobType(step.WorkflowType, step.Name); err != nil {
			return err
		}
		for _, sub := range step.SubSteps {
			if sub == nil {
				continue
			}
			if err := validateWorkflowJobType(sub.WorkflowType, sub.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateWorkflowJobType(jobType config.JobType, stepName string) error {
	if config.IsSupportedWorkflowJobType(jobType) {
		return nil
	}
	return fmt.Errorf("workflow step %q has unsupported jobType %q", stepName, jobType)
}

func validateLogArchiveUploadWorkflowInputs(steps *model.WorkflowSteps, componentMap map[string]*model.ApplicationComponent) error {
	if steps == nil {
		return nil
	}
	componentsByName := make(map[string]*model.ApplicationComponent, len(componentMap))
	for name, component := range componentMap {
		normalizedName := strings.ToLower(strings.TrimSpace(name))
		if normalizedName == "" || component == nil {
			continue
		}
		componentsByName[normalizedName] = component
	}
	for _, step := range steps.Steps {
		if step == nil || config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
			continue
		}
		if err := validateLogArchiveUploadStepInputs(step.Name, step.WorkflowType, step.Properties, step.ComponentNames(), componentsByName); err != nil {
			return err
		}
		for _, sub := range step.SubSteps {
			if sub == nil {
				continue
			}
			if err := validateLogArchiveUploadStepInputs(sub.Name, sub.WorkflowType, sub.Properties, sub.ComponentNames(), componentsByName); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateLogArchiveUploadStepInputs(stepName string, jobType config.JobType, properties []model.Policies, componentNames []string, componentsByName map[string]*model.ApplicationComponent) error {
	if config.JobType(strings.TrimSpace(string(jobType))) != config.JobLogArchiveUpload {
		return nil
	}
	for _, name := range componentNames {
		componentName := strings.TrimSpace(name)
		component := componentsByName[strings.ToLower(componentName)]
		if component == nil {
			return fmt.Errorf("workflow step %q references missing component %q for jobType %q", stepName, componentName, jobType)
		}
		if !config.ComponentTypeUsesPods(component.ComponentType) {
			return fmt.Errorf("workflow step %q component %q with type %q does not use pods for jobType %q", stepName, component.Name, component.ComponentType, jobType)
		}
		path, _ := logArchiveUploadOptionsForComponent(componentName, properties)
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("workflow step %q requires properties.path for component %q and jobType %q", stepName, componentName, jobType)
		}
	}
	return nil
}

func loadResourceAppName(ctx context.Context, task *model.WorkflowQueue, ds datastore.DataStore) (string, error) {
	app, err := loadWorkflowTaskApplication(ctx, task, ds)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(app.Name) == "" {
		return "", fmt.Errorf("application %s name is empty", task.AppID)
	}
	return naming.ApplicationResourceKey(app.Name, "", false), nil
}

func loadWorkflowTaskApplication(ctx context.Context, task *model.WorkflowQueue, ds datastore.DataStore) (*model.Applications, error) {
	if task == nil {
		return nil, fmt.Errorf("workflow task is nil")
	}
	if strings.TrimSpace(task.AppID) == "" {
		return nil, fmt.Errorf("workflow task appID is empty")
	}
	app := &model.Applications{ID: task.AppID}
	if err := ds.Get(ctx, app); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, fmt.Errorf("application %s not found: %w", task.AppID, err)
		}
		return nil, fmt.Errorf("get application %s: %w", task.AppID, err)
	}
	return app, nil
}

func ParseProperties(ctx context.Context, properties *model.JSONStruct) model.Properties {
	logger := klog.FromContext(ctx)
	cProperties, err := json.Marshal(properties)
	if err != nil {
		logger.Error(err, "Component.Properties deserialization failure")
		return model.Properties{}
	}

	var propertied model.Properties
	err = json.Unmarshal(cProperties, &propertied)
	if err != nil {
		logger.Error(err, "WorkflowSteps deserialization failure")
		return model.Properties{}
	}
	return propertied
}
