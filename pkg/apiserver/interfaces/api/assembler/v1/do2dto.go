package v1

import (
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

// ConvertAppModelToBase assemble the Application model to DTO
func ConvertAppModelToBase(app *model.Applications, workflowID string) *apisv1.ApplicationBase {
	appBase := &apisv1.ApplicationBase{
		ID:              app.ID,
		Name:            app.Name,
		Namespace:       app.Namespace,
		Version:         app.Version,
		Project:         app.Project,
		Alias:           app.Alias,
		CreateTime:      app.CreateTime,
		UpdateTime:      app.UpdateTime,
		Description:     app.Description,
		Icon:            app.Icon,
		WorkflowID:      workflowID,
		TemplateEnabled: app.TemplateEnabled,
		ManagementMode:  app.EffectiveManagementMode(),
	}
	return appBase
}

// ConvertWorkflowModelToDTO converts the workflow model into an API-friendly structure.
func ConvertWorkflowModelToDTO(workflow *model.Workflow) (*apisv1.ApplicationWorkflow, error) {
	if workflow == nil {
		return nil, nil
	}
	failurePolicy, steps, err := convertWorkflowSteps(workflow.Steps)
	if err != nil {
		return nil, fmt.Errorf("convert workflow %s steps: %w", workflow.ID, err)
	}
	var callback *apisv1.WorkflowCallback
	if workflow.Callback != nil {
		var cfg apisv1.WorkflowCallback
		if err := decodeJSONStruct(workflow.Callback, &cfg); err != nil {
			return nil, fmt.Errorf("convert workflow %s callback: %w", workflow.ID, err)
		}
		callback = &cfg
	}
	return &apisv1.ApplicationWorkflow{
		ID:            workflow.ID,
		Name:          workflow.Name,
		Alias:         workflow.Alias,
		Namespace:     workflow.Namespace,
		ProjectID:     workflow.ProjectID,
		Description:   workflow.Description,
		Status:        string(workflow.Status),
		Disabled:      workflow.Disabled,
		FailurePolicy: failurePolicy,
		Steps:         steps,
		Callback:      callback,
		CreateTime:    workflow.CreateTime,
		UpdateTime:    workflow.UpdateTime,
		WorkflowType:  workflow.WorkflowType,
	}, nil
}

func decodeJSONStruct(raw *model.JSONStruct, target interface{}) error {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	if string(data) == "null" {
		return nil
	}
	return json.Unmarshal(data, target)
}
