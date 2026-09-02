package application

import (
	"context"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionRejectsNonPositiveReplicas(t *testing.T) {
	testCases := []struct {
		name     string
		action   string
		replicas int32
	}{
		{name: "update zero", action: string(config.ComponentActionUpdate), replicas: 0},
		{name: "update negative", action: string(config.ComponentActionUpdate), replicas: -1},
		{name: "add zero", action: string(config.ComponentActionAdd), replicas: 0},
		{name: "add negative", action: string(config.ComponentActionAdd), replicas: -1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "DemoApp", Version: "1.0.0", Namespace: "default"}
			store.components["backend"] = &model.ApplicationComponent{Name: "backend", AppID: "app-1", ComponentType: config.ServerJob, Replicas: 2}
			store.workflows["wf-1"] = &model.Workflow{ID: "wf-1", AppID: "app-1"}
			svc := newMockServiceWithStore(store)
			componentName := "backend"
			componentType := config.ServerJob
			if tc.action == string(config.ComponentActionAdd) {
				componentName = "worker"
			}

			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version: "1.1.0",
				Components: []apisv1.ComponentUpdateSpec{{
					Action:        tc.action,
					Name:          componentName,
					ComponentType: componentType,
					Image:         "example/app:v2",
					Replicas:      &tc.replicas,
				}},
				AutoExec: boolPtr(false),
			})

			require.Nil(t, resp)
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.ErrorContains(t, err, "replicas must be greater than 0")
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			require.Equal(t, int32(2), store.components["backend"].Replicas)
			require.NotContains(t, store.components, "worker")
			require.Empty(t, store.tasks)
		})
	}
}

func TestUpdateVersionRejectsGeneratedResourceNameCollisionWithinApp(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "shop",
		Version:   "1.0.0",
		Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Ports: []spec.Ports{{Port: 8080}},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	replicas := int32(1)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action:        "add",
			Name:          "shop-api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      &replicas,
			Properties: &apisv1.Properties{
				Ports: []spec.Ports{{Port: 8081}},
			},
		}},
		AutoExec: boolPtr(false),
	}

	_, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate deployment")
	require.Nil(t, store.components["shop-api"])
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
}

func TestUpdateVersionRejectsGeneratedResourceNameCollisionAcrossApps(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["other-app"] = &model.Applications{
		ID:        "other-app",
		Name:      "foo",
		Namespace: config.DefaultNamespace,
	}
	store.components["bar-baz"] = &model.ApplicationComponent{
		Name:          "bar-baz",
		AppID:         "other-app",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Ports: []spec.Ports{{Port: 8080}},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "foo-bar",
		Version:   "1.0.0",
		Namespace: config.DefaultNamespace,
	}
	store.components["worker"] = &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
	}

	svc := newMockServiceWithStore(store)
	replicas := int32(1)
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action:        "add",
			Name:          "baz",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      &replicas,
			Properties: &apisv1.Properties{
				Ports: []spec.Ports{{Port: 8081}},
			},
		}},
		AutoExec: boolPtr(false),
	}

	_, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate deployment")
	require.Nil(t, store.components["baz"])
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
}

func TestUpdateVersionAddComponentRejectsInvalidExplicitServiceName(t *testing.T) {
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
	traits := apisv1.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "Backend_Service",
				Type: string(spec.ServiceAccessInternal),
				Ports: []spec.ServicePortTraitSpec{
					{Port: 6379, TargetPort: 6379, Protocol: "TCP"},
				},
			},
		},
	}
	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action:        "add",
				Name:          "redis-cache",
				ComponentType: config.StoreJob,
				Image:         "redis:7-alpine",
				Replicas:      &replicas,
				Traits:        &traits,
			},
		},
		AutoExec: boolPtr(false),
	}

	_, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, store.components["redis-cache"])
}

func TestUpdateVersionMissingApp(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version: "1.0.0",
	}

	_, err := svc.UpdateVersion(context.Background(), "missing-app", req)
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationNotExist)
}

func TestUpdateVersionRejectsMissingUpdateComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
		Image: "old-image",
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "new-image"},
			{Name: "non-existent", Image: "whatever"},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrComponentNotFound)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "old-image", store.components["backend"].Image)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func TestUpdateVersionRejectsExistingAddComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Image:         "api:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}

	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "add", Name: "api", Image: "api:v2", ComponentType: config.ServerJob},
		},
	})

	require.ErrorIs(t, err, bcode.ErrComponentAlreadyExists)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "api:v1", store.components["api"].Image)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func TestUpdateVersionRejectsInvalidComponentAction(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Image:         "api:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}

	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remvoe", Name: "api"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrInvalidComponentAction)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "api:v1", store.components["api"].Image)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func TestUpdateVersionRejectsDuplicateUpdateSpec(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "DemoApp", Version: "1.0.0", Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		ID: 1, Name: "api", AppID: "app-1", Namespace: config.DefaultNamespace,
		Image: "nginx:1.27", Replicas: 1, ComponentType: config.ServerJob,
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "update", Name: "api", Image: "nginx:1.28"},
			{Name: " API ", Image: "nginx:1.29"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrDuplicateComponentName)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "nginx:1.27", store.components["api"].Image)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionRejectsNegativeExecuteAt(t *testing.T) {
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

	queueRepo := &mockWorkflowQueueRepo{}
	svc := newMockServiceWithStore(store)
	svc.WorkflowQueueRepo = queueRepo

	req := apisv1.UpdateVersionRequest{
		Version:   "1.1.0",
		ExecuteAt: -1,
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	_, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Nil(t, queueRepo.lastQueue)
}
