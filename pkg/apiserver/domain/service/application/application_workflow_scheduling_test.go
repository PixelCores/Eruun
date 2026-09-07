package application

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/stretchr/testify/require"
)

func TestWorkflowSchedulingClassRequestPersistenceAndResponse(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Project: "proj-1"}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1", ComponentType: config.ServerJob}
	svc := newMockServiceWithStore(store)
	var req apis.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{"name":"schedule","workflow":[{"name":"deploy","mode":"DAG","schedulingClass":"high","subSteps":[{"name":"web","jobType":"deploy","schedulingClass":"background","components":["web"]}]}]}`), &req))
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)
	stored := store.workflows[resp.WorkflowID]
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Equal(t, "high", steps.Steps[0].SchedulingClass)
	require.Equal(t, "background", steps.Steps[0].SubSteps[0].SchedulingClass)
	dto, err := assembler.ConvertWorkflowModelToDTO(stored)
	require.NoError(t, err)
	require.Equal(t, "high", dto.Steps[0].SchedulingClass)
	require.Equal(t, "background", dto.Steps[0].SubSteps[0].SchedulingClass)
	// The returned step shape can be sent back to the workflow update endpoint
	// without losing the explicit child override.
	roundTrip, err := json.Marshal(dto.Steps)
	require.NoError(t, err)
	var returnedSteps []apis.CreateWorkflowStepRequest
	require.NoError(t, json.Unmarshal(roundTrip, &returnedSteps))
	require.Equal(t, "high", returnedSteps[0].SchedulingClass)
	require.Equal(t, "background", returnedSteps[0].SubSteps[0].SchedulingClass)
	for _, invalidSub := range []bool{false, true} {
		t.Run(map[bool]string{false: "step", true: "substep"}[invalidSub], func(t *testing.T) {
			bad := req
			bad.Workflow = append([]apis.CreateWorkflowStepRequest(nil), req.Workflow...)
			bad.Workflow[0].SubSteps = append([]apis.CreateWorkflowSubStepRequest(nil), req.Workflow[0].SubSteps...)
			if invalidSub {
				bad.Workflow[0].SubSteps[0].SchedulingClass = "urgent"
			} else {
				bad.Workflow[0].SchedulingClass = "urgent"
			}
			_, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", bad)
			require.ErrorContains(t, err, "scheduling class")
		})
	}
}

func TestWorkflowSchedulingClassSurvivesTemplatePhaseSync(t *testing.T) {
	for _, oldWebPhaseName := range []string{"phase-5-webservice", "phase-4-webservice"} {
		t.Run(oldWebPhaseName, func(t *testing.T) {
			workflow := &model.Workflow{ID: "wf-1", Steps: mustJSONStruct(&model.WorkflowSteps{
				Steps: []*model.WorkflowStep{
					{Name: "phase-2-config-secret", SchedulingClass: "background", Mode: config.WorkflowModeDAG, WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"cfg"}}}},
					{Name: oldWebPhaseName, SchedulingClass: "high", Mode: config.WorkflowModeDAG, WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"api", "removed"}}}},
				},
			})}
			var saved *model.Workflow
			err := applyVersionUpdateWorkflowStepSync(workflow, []string{"store"}, []string{"removed"}, func() ([]*model.ApplicationComponent, error) {
				return []*model.ApplicationComponent{
					{Name: "cfg", ComponentType: config.ConfJob},
					{Name: "store", ComponentType: config.StoreJob},
					{Name: "api", ComponentType: config.ServerJob},
				}, nil
			}, func(updated *model.Workflow) error {
				saved = updated
				return nil
			})
			require.NoError(t, err)
			require.NotNil(t, saved)
			steps := decodeWorkflowSteps(t, saved.Steps)
			require.Len(t, steps.Steps, 3)
			require.Equal(t, "background", steps.Steps[0].SchedulingClass)
			require.Empty(t, steps.Steps[1].SchedulingClass, "new phases use the default class")
			require.Equal(t, "high", steps.Steps[2].SchedulingClass)
			require.Equal(t, []string{"api"}, steps.Steps[2].ComponentNames())
		})
	}
}
