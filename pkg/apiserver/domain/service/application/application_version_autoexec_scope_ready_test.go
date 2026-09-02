package application

import (
	"context"

	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionDefaultExecutionScope(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
	})

	require.NoError(t, err)
	require.Equal(t, string(config.VersionUpdateExecutionScopeFullWorkflow), resp.ExecutionScope)
}

func TestUpdateVersionChangedComponentsExecutionScopePersistsResourceActionInfo(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["frontend"] = &model.ApplicationComponent{
		Name:          "frontend",
		AppID:         "app-1",
		Image:         "frontend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-1",
		Image:         "mysql:5.7",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{
					Name:         "phase-store",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"mysql"}}},
				},
				{
					Name:         "phase-web",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"backend", "frontend"}}},
				},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:        "1.1.0",
		ExecutionScope: string(config.VersionUpdateExecutionScopeChangedComponents),
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "backend:v2"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, string(config.VersionUpdateExecutionScopeChangedComponents), resp.ExecutionScope)
	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, config.VersionUpdateExecutionScopeChangedComponents, info.ExecutionScope)
	require.Equal(t, []string{"backend"}, info.ExecutionComponents)
	require.Equal(t, []string{"backend"}, info.ImageReadyComponents)
}

func TestUpdateVersionRejectsInvalidExecutionScope(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:        "1.1.0",
		ExecutionScope: "partial",
		AutoExec:       boolPtr(false),
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "executionScope")
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionRejectsChangedComponentsExecutionScopeWithFullResourceActions(t *testing.T) {
	tests := []struct {
		name string
		spec apisv1.ComponentUpdateSpec
	}{
		{
			name: "deploy all",
			spec: apisv1.ComponentUpdateSpec{Action: string(config.ComponentActionAdd), Name: "all"},
		},
		{
			name: "cleanup all",
			spec: apisv1.ComponentUpdateSpec{Action: string(config.ComponentActionRemove), Name: "cleanup_all"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{
				ID:      "app-1",
				Name:    "DemoApp",
				Version: "1.0.0",
			}

			svc := newMockServiceWithStore(store)
			_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:        "1.1.0",
				ExecutionScope: string(config.VersionUpdateExecutionScopeChangedComponents),
				Components:     []apisv1.ComponentUpdateSpec{tt.spec},
			})

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Contains(t, err.Error(), "executionScope")
			require.Empty(t, store.tasks)
		})
	}
}

func TestUpdateVersionChangedComponentsExecutionScopeRequiresWorkflowCoverageForActualChanges(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["config"] = &model.ApplicationComponent{
		Name:          "config",
		AppID:         "app-1",
		Image:         "config:v1",
		ComponentType: config.ConfJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["frontend"] = &model.ApplicationComponent{
		Name:          "frontend",
		AppID:         "app-1",
		Image:         "frontend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "frontend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:        "1.1.0",
		WorkflowID:     "wf-1",
		ExecutionScope: string(config.VersionUpdateExecutionScopeChangedComponents),
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "config", Image: "config:v2"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Contains(t, err.Error(), "does not cover changed components")
	require.Contains(t, err.Error(), "config")
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "config:v1", store.components["config"].Image)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionChangedComponentsExecutionScopeRejectsPartialWorkflowCoverage(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["config"] = &model.ApplicationComponent{
		Name:          "config",
		AppID:         "app-1",
		Image:         "config:v1",
		ComponentType: config.ConfJob,
		Status:        string(config.ComponentStatusRunning),
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
	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:        "1.1.0",
		WorkflowID:     "wf-1",
		ExecutionScope: string(config.VersionUpdateExecutionScopeChangedComponents),
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "backend:v2"},
			{Name: "config", Image: "config:v2"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Contains(t, err.Error(), "does not cover changed components")
	require.Contains(t, err.Error(), "config")
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Equal(t, "config:v1", store.components["config"].Image)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionAutoExecPersistsImageReadyTargetsWithDefaultTimeout(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["config"] = &model.ApplicationComponent{
		Name:          "config",
		AppID:         "app-1",
		Image:         "config:v1",
		ComponentType: config.ConfJob,
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
				{Name: "config", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "backend:v2"},
			{Name: "config", Image: "config:v2"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, []string{"backend"}, info.ImageReadyComponents)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), info.ImageReadyTimeoutSeconds)
	require.Empty(t, info.RestartComponents)
}

func TestUpdateVersionAutoExecPersistsImageReadyTargetsWithRequestTimeout(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-1",
		Image:         "mysql:5.7",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "mysql", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:                  "1.1.0",
		ImageReadyTimeoutSeconds: 120,
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "mysql", Image: "mysql:8.0"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, []string{"mysql"}, info.ImageReadyComponents)
	require.Equal(t, int64(120), info.ImageReadyTimeoutSeconds)
}

func TestUpdateVersionAutoExecPersistsReadyTargetForTraitsOnlyWorkloadUpdate(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
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

	traits := apisv1.Traits{
		Resources: &spec.ResourceTraitsSpec{Memory: "512Mi"},
	}
	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Traits: &traits},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, []string{"backend"}, resp.UpdatedComponents)
	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, []string{"backend"}, info.ImageReadyComponents)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), info.ImageReadyTimeoutSeconds)
}

func TestUpdateVersionAutoExecPersistsReadyTargetsForWorkloadConfigUpdates(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Image:         "api:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["worker"] = &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-1",
		Image:         "worker:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-1",
		Image:         "mysql:5.7",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    mustJSONStruct(&apisv1.Properties{Env: map[string]string{"MODE": "old"}}),
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
				{Name: "worker", WorkflowType: config.JobDeploy},
				{Name: "mysql", WorkflowType: config.JobDeploy},
			},
		}),
	}

	replicas := int32(2)
	props := apisv1.Properties{Env: map[string]string{"MODE": "worker"}}
	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "api", Replicas: &replicas},
			{Name: "worker", Properties: &props},
			{Name: "mysql", Env: map[string]string{"MODE": "new"}},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, []string{"api", "worker", "mysql"}, resp.UpdatedComponents)
	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, []string{"api", "worker", "mysql"}, info.ImageReadyComponents)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), info.ImageReadyTimeoutSeconds)
}

func TestUpdateVersionAutoExecPersistsReadyTargetForEnvOnlyWorkloadUpdateWithNilProperties(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
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
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Env: map[string]string{"MODE": "new"}},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, []string{"backend"}, resp.UpdatedComponents)
	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, []string{"backend"}, info.ImageReadyComponents)

	var props apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["backend"].Properties, &props))
	require.Equal(t, map[string]string{"MODE": "new"}, props.Env)
}

func TestUpdateVersionAutoExecNoopWorkloadUpdateDoesNotPersistReadyTarget(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    mustJSONStruct(&apisv1.Properties{Env: map[string]string{"MODE": "stable"}}),
		Status:        string(config.ComponentStatusRunning),
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

	replicas := int32(1)
	svc := newMockServiceWithStore(store)
	queueRepo, ok := svc.WorkflowQueueRepo.(*mockWorkflowQueueRepo)
	require.True(t, ok)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "backend:v1", Replicas: &replicas, Env: map[string]string{"MODE": "stable"}},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Empty(t, resp.UpdatedComponents)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, resp.TaskID, queueRepo.lastQueue.TaskID)
	require.Empty(t, queueRepo.lastQueue.ResourceActionInfo)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
}

func TestUpdateVersionAutoExecConfigAndSecretUpdatesDoNotPersistReadyTargets(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["config"] = &model.ApplicationComponent{
		Name:          "config",
		AppID:         "app-1",
		Image:         "config:v1",
		ComponentType: config.ConfJob,
	}
	store.components["secret"] = &model.ApplicationComponent{
		Name:          "secret",
		AppID:         "app-1",
		Image:         "secret:v1",
		ComponentType: config.SecretJob,
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "config", WorkflowType: config.JobDeploy},
				{Name: "secret", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "config", Image: "config:v2"},
			{Name: "secret", Image: "secret:v2"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, []string{"config", "secret"}, resp.UpdatedComponents)
	require.NotNil(t, store.tasks[resp.TaskID])
	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Empty(t, info.ImageReadyComponents)
	require.Empty(t, info.RestartComponents)
}

func TestUpdateVersionAutoExecTaskCallbackSharesWorkflowTaskWithImageReadyTarget(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
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
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "backend", Image: "backend:v2"},
		},
		Callback: &apisv1.WorkflowCallback{
			Success: "https://example.com/version/success",
			Failure: "https://example.com/version/failure",
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, "wf-1", resp.WorkflowID)
	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	require.Equal(t, "wf-1", task.WorkflowID)
	requireWorkflowCallbackSuccess(t, task.Callback, "https://example.com/version/success")
	info := requireVersionUpdateResourceActionInfo(t, task)
	require.Equal(t, []string{"backend"}, info.ImageReadyComponents)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), info.ImageReadyTimeoutSeconds)
}

func TestUpdateVersionRejectsInvalidImageReadyTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout int64
	}{
		{name: "negative", timeout: -1},
		{name: "exceeds deploy timeout", timeout: int64(config.DeployTimeout) + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{
				ID:        "app-1",
				Name:      "DemoApp",
				Version:   "1.0.0",
				Namespace: "default",
			}
			store.components["backend"] = &model.ApplicationComponent{
				Name:          "backend",
				AppID:         "app-1",
				Image:         "backend:v1",
				Replicas:      1,
				ComponentType: config.ServerJob,
			}
			store.workflows["wf-1"] = &model.Workflow{
				ID:    "wf-1",
				AppID: "app-1",
				Steps: mustJSONStruct(&model.WorkflowSteps{
					Steps: []*model.WorkflowStep{{Name: "backend", WorkflowType: config.JobDeploy}},
				}),
			}

			svc := newMockServiceWithStore(store)
			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:                  "1.1.0",
				ImageReadyTimeoutSeconds: tt.timeout,
				Components: []apisv1.ComponentUpdateSpec{
					{Name: "backend", Image: "backend:v2"},
				},
			})

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Nil(t, resp)
			require.Empty(t, store.tasks)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			require.Equal(t, "backend:v1", store.components["backend"].Image)
		})
	}
}
