package application

import (
	"context"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"

	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestUpdateVersionWithImageUpdate(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
		Project:   "proj-1",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:      "backend",
		AppID:     "app-1",
		Namespace: "default",
		Image:     "myapp/backend:v1.0.0",
		Replicas:  2,
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:        "wf-1",
		Name:      "demoapp-workflow",
		AppID:     "app-1",
		ProjectID: "proj-1",
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		Strategy: "rolling",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "myapp/backend:v1.1.0",
			},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "app-1", resp.AppID)
	require.Equal(t, "1.1.0", resp.Version)
	require.Equal(t, "1.0.0", resp.PreviousVersion)
	require.Equal(t, "rolling", resp.Strategy)
	require.Contains(t, resp.UpdatedComponents, "backend")
	require.Empty(t, resp.AddedComponents)
	require.Empty(t, resp.RemovedComponents)

	updatedComp := store.components["backend"]
	require.Equal(t, "myapp/backend:v1.1.0", updatedComp.Image)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
}

func TestUpdateVersionWithReplicasUpdate(t *testing.T) {
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
		Replicas: 2,
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
	}

	svc := newMockServiceWithStore(store)

	newReplicas := int32(5)
	req := apisv1.UpdateVersionRequest{
		Version: "1.0.1",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:     "backend",
				Replicas: &newReplicas,
			},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "1.0.1", resp.Version)
	require.Contains(t, resp.UpdatedComponents, "backend")
	require.Equal(t, int32(5), store.components["backend"].Replicas)
}

func TestUpdateVersionWithPreviousVersion(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "2.3.5",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
		Image: "old-image",
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version: "2.3.6",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "new-image"},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "2.3.6", resp.Version)
	require.Equal(t, "2.3.5", resp.PreviousVersion)
}

func TestUpdateVersionAddComponent(t *testing.T) {
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
	require.Empty(t, resp.UpdatedComponents)
	require.Empty(t, resp.RemovedComponents)

	addedComp := store.components["redis-cache"]
	require.NotNil(t, addedComp)
	require.Equal(t, "redis:7-alpine", addedComp.Image)
	require.Equal(t, config.StoreJob, addedComp.ComponentType)
	require.Equal(t, "app-1", addedComp.AppID)
}

func TestUpdateVersionMixedOperations(t *testing.T) {
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
		Image: "backend:v1",
	}
	store.components["old-worker"] = &model.ApplicationComponent{
		Name:  "old-worker",
		AppID: "app-1",
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
				{Name: "old-worker", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)

	replicas := int32(1)
	req := apisv1.UpdateVersionRequest{
		Version:  "3.0.0",
		Strategy: "rolling",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "update",
				Name:   "backend",
				Image:  "backend:v3",
			},
			{
				Action:        "add",
				Name:          "cache",
				ComponentType: config.StoreJob,
				Image:         "redis:7",
				Replicas:      &replicas,
			},
			{
				Action: "remove",
				Name:   "old-worker",
			},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "3.0.0", resp.Version)
	require.Equal(t, "rolling", resp.Strategy)
	require.Contains(t, resp.UpdatedComponents, "backend")
	require.Contains(t, resp.AddedComponents, "cache")
	require.Contains(t, resp.RemovedComponents, "old-worker")

	require.Equal(t, "backend:v3", store.components["backend"].Image)
	require.NotNil(t, store.components["cache"])
	require.Equal(t, "redis:7", store.components["cache"].Image)
}

func TestUpdateVersionWithDescription(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:          "app-1",
		Name:        "DemoApp",
		Version:     "1.0.0",
		Description: "Original description",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version:     "1.1.0",
		Description: "Bug fixes and improvements",
		AutoExec:    boolPtr(false),
	}

	_, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "Bug fixes and improvements", store.apps["app-1"].Description)
}

func TestUpdateVersionDefaultStrategy(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
		Image: "old",
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "new"},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "rolling", resp.Strategy)
}

func TestUpdateVersionNoChanges(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version:     "1.1.0",
		Description: "Version bump only",
		AutoExec:    boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "1.1.0", resp.Version)
	require.Empty(t, resp.UpdatedComponents)
	require.Empty(t, resp.AddedComponents)
	require.Empty(t, resp.RemovedComponents)
	require.NotEmpty(t, resp.TaskID)
}
