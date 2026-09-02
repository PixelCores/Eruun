package application

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"

	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionTemplateRejectsRequestedVersionCollision(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "foo",
		Version:         "1.0.0",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.apps["tmpl-2"] = &model.Applications{
		ID:              "tmpl-2",
		Name:            "foo",
		Version:         "2.0.0",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "tmpl-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.0",
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "api",
			Image: "nginx:2.0",
		}},
		AutoExec: boolPtr(false),
	}

	_, err := svc.UpdateVersion(context.Background(), "tmpl-1", req)
	require.ErrorIs(t, err, bcode.ErrApplicationExist)
	require.Equal(t, "1.0.0", store.apps["tmpl-1"].Version)
	require.Equal(t, "nginx:1.0", store.components["api"].Image)
}

func TestUpdateVersionTemplateAllowsRequestedVersionAcrossNamespaces(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "foo",
		Version:         "1.0.0",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.apps["tmpl-team-b"] = &model.Applications{
		ID:              "tmpl-team-b",
		Name:            "foo",
		Version:         "2.0.0",
		Namespace:       "team-b",
		TemplateEnabled: true,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "tmpl-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.0",
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "api",
			Image: "nginx:2.0",
		}},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "tmpl-1", req)
	require.NoError(t, err)
	require.Equal(t, "2.0.0", resp.Version)
	require.Equal(t, "2.0.0", store.apps["tmpl-1"].Version)
	require.Equal(t, "team-b", store.apps["tmpl-team-b"].Namespace)
	require.Equal(t, "nginx:2.0", store.components["api"].Image)
}

func TestUpdateVersionTemplateDoesNotUseVersionForResourceKey(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-standard"] = &model.Applications{
		ID:        "app-standard",
		Name:      "foo-2-0-0",
		Namespace: config.DefaultNamespace,
	}
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "foo",
		Version:         "1.0.0",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "tmpl-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.0",
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "api",
			Image: "nginx:2.0",
		}},
		AutoExec: boolPtr(false),
	}

	_, err := svc.UpdateVersion(context.Background(), "tmpl-1", req)
	require.NoError(t, err)
	require.Equal(t, "2.0.0", store.apps["tmpl-1"].Version)
	require.Equal(t, "nginx:2.0", store.components["api"].Image)
}

func TestUpdateVersionKeepsCommittedChangesWhenWorkflowStepSyncFails(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	svc.WorkflowRepo = &syncFailWorkflowRepo{
		mockWorkflowRepo: &mockWorkflowRepo{store: store},
		updateErr:        errors.New("workflow update failed"),
	}

	replicas := int32(1)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action:        "add",
				Name:          "redis-cache",
				ComponentType: config.StoreJob,
				Image:         "redis:7-alpine",
				Replicas:      &replicas,
			},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "2.0.0", resp.Version)
	require.Contains(t, resp.AddedComponents, "redis-cache")
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["redis-cache"])
}

func TestUpdateVersionRejectsStructureChangesWhenWorkflowTaskActive(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}
	store.tasks["task-active"] = &model.WorkflowQueue{
		TaskID:          "task-active",
		AppID:           "app-1",
		Status:          config.StatusWaitingApprove,
		ApprovalPending: true,
	}

	svc := newMockServiceWithStore(store)

	replicas := int32(1)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action:        "add",
				Name:          "redis-cache",
				ComponentType: config.StoreJob,
				Image:         "redis:7-alpine",
				Replicas:      &replicas,
			},
		},
		AutoExec: boolPtr(false),
	}

	_, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Nil(t, store.components["redis-cache"])

	steps := decodeWorkflowSteps(t, store.workflows["wf-1"].Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, "backend", steps.Steps[0].Name)
}

func TestUpdateVersionSyncTemplatePhasedWorkflowOnAdd(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["cfg"] = &model.ApplicationComponent{
		Name:          "cfg",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ConfJob,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(convertWorkflowStepByTemplatePhases([]apisv1.CreateComponentRequest{
			{Name: "cfg", ComponentType: config.ConfJob},
			{Name: "api", ComponentType: config.ServerJob},
		})),
	}

	svc := newMockServiceWithStore(store)
	replicas := int32(1)
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action:        "add",
				Name:          "redis",
				ComponentType: config.StoreJob,
				Image:         "redis:7-alpine",
				Replicas:      &replicas,
			},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Contains(t, resp.AddedComponents, "redis")

	steps := decodeWorkflowSteps(t, store.workflows["wf-default"].Steps)
	require.Len(t, steps.Steps, 3)
	require.Equal(t, "phase-2-config-secret", steps.Steps[0].Name)
	require.Equal(t, "phase-3-store", steps.Steps[1].Name)
	require.Equal(t, "phase-5-webservice", steps.Steps[2].Name)
	require.ElementsMatch(t, []string{"cfg"}, steps.Steps[0].ComponentNames())
	require.ElementsMatch(t, []string{"redis"}, steps.Steps[1].ComponentNames())
	require.ElementsMatch(t, []string{"api"}, steps.Steps[2].ComponentNames())
	for _, step := range steps.Steps {
		require.NotEqual(t, "redis", step.Name)
	}
}

func TestUpdateVersionRejectsUnknownWorkflowID(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:     "backend",
		AppID:    "app-1",
		Image:    "backend:v1",
		Replicas: 1,
		Status:   string(config.ComponentStatusRunning),
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		WorkflowID: "missing-workflow",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	_, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.Error(t, err)
	require.True(t, errors.Is(err, bcode.ErrWorkflowNotExist))
	require.Equal(t, "backend:v1", store.components["backend"].Image)
}
