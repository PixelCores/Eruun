package application

import (
	"context"

	"errors"

	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionMarksUpdatingOnAutoExec(t *testing.T) {
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

	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, string(config.ComponentStatusUpdating), store.components["backend"].Status)
}

func TestUpdateVersionAutoExecRejectsContendedAppLockWithoutWrites(t *testing.T) {
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
	releaseLock := holdApplicationTestAppScheduleLock(t, svc.ScheduleLocker, "app-1")
	defer releaseLock()

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "backend",
			Image: "backend:v1.1",
		}},
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionAutoExecRejectsReadyUpdateTargetMissingFromWorkflow(t *testing.T) {
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
	store.components["worker"] = &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-1",
		Image:         "worker:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "worker", WorkflowType: config.JobDeploy},
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

	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Contains(t, err.Error(), "does not cover Ready-observed components: backend")
	require.Nil(t, resp)
	require.Empty(t, store.tasks)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Nil(t, store.components["backend"].Traits)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
}

func TestUpdateVersionPreservesQueuedTaskWhenMarkUpdatingFails(t *testing.T) {
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
	store.runtimeUpdateErr = errors.New("status store unavailable")
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.TaskID)
	require.NotNil(t, store.tasks[resp.TaskID])
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1.1", store.components["backend"].Image)
}

func TestUpdateVersionAutoExecRejectsExistingAddComponent(t *testing.T) {
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
		Namespace:     "default",
		Image:         "api:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}

	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "add", Name: "api", Image: "api:v2", ComponentType: config.ServerJob},
		},
	})

	require.ErrorIs(t, err, bcode.ErrComponentAlreadyExists)
	require.Nil(t, resp)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "api:v1", store.components["api"].Image)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["api"].Status)
}

func TestUpdateVersionAutoExecEmptyWorkflowRollsBack(t *testing.T) {
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
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{},
		}),
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrWorkflowEmpty)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func TestUpdateVersionAutoExecQueueCreateFailureRollsBack(t *testing.T) {
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
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}
	store.addWorkflowQueueErr = errors.New("queue create failed")

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "auto exec workflow")
	require.Contains(t, err.Error(), "queue create failed")
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionAutoExecAddsFirstComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action:        "add",
				Name:          "api",
				Image:         "nginx:1.28",
				ComponentType: config.ServerJob,
			},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.ElementsMatch(t, []string{"api"}, resp.AddedComponents)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["api"])
	require.Len(t, store.tasks, 1)
}

func TestUpdateVersionAutoExecIdleRaceRollsBack(t *testing.T) {
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
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}
	store.beforeTransaction = func(tx *inMemoryAppStore) {
		tx.tasks["task-active"] = &model.WorkflowQueue{
			TaskID:          "task-active",
			AppID:           "app-1",
			Status:          config.StatusWaitingApprove,
			ApprovalPending: true,
		}
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionTemplateVersionOnlyDoesNotAutoExecWhenResourceKeyIsStable(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "mysql",
		Version:         "1.0.0",
		Namespace:       "default",
		TemplateEnabled: true,
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:     "backend",
		AppID:    "tmpl-1",
		Image:    "backend:v1",
		Replicas: 1,
		Status:   string(config.ComponentStatusRunning),
	}
	store.workflows["wf-template"] = &model.Workflow{
		ID:           "wf-template",
		AppID:        "tmpl-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		BaseModel:    model.BaseModel{UpdateTime: time.Now()},
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	queueRepo := &mockWorkflowQueueRepo{}
	svc := newMockServiceWithStore(store)
	svc.WorkflowQueueRepo = queueRepo

	resp, err := svc.UpdateVersion(context.Background(), "tmpl-1", apisv1.UpdateVersionRequest{
		Version: "2.0.0",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, config.WorkflowTaskTypeUpdate, queueRepo.lastQueue.Type)
	require.Empty(t, queueRepo.lastQueue.WorkflowID)
	require.Equal(t, "2.0.0", store.apps["tmpl-1"].Version)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
}

func TestUpdateVersionNonTemplateVersionOnlyDoesNotAutoExecWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
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
	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		BaseModel:    model.BaseModel{UpdateTime: time.Now()},
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	queueRepo := &mockWorkflowQueueRepo{}
	svc := newMockServiceWithStore(store)
	svc.WorkflowQueueRepo = queueRepo

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, config.WorkflowTaskTypeUpdate, queueRepo.lastQueue.Type)
	require.Empty(t, queueRepo.lastQueue.WorkflowID)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
}

func TestUpdateVersionAutoExecWithFutureExecuteAtSchedulesWorkflow(t *testing.T) {
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
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)

	executeAt := time.Now().Add(10 * time.Minute).Unix()
	req := apisv1.UpdateVersionRequest{
		Version:   "1.1.0",
		ExecuteAt: executeAt,
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	require.Equal(t, executeAt, task.ExecuteAt)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
}

func TestUpdateVersionAutoExecWithPastExecuteAtRunsImmediately(t *testing.T) {
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
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version:   "1.1.0",
		ExecuteAt: time.Now().Add(-10 * time.Minute).Unix(),
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	require.Equal(t, int64(0), task.ExecuteAt)
	require.Equal(t, string(config.ComponentStatusUpdating), store.components["backend"].Status)
}

func TestUpdateVersionAutoExecDefaultSkipsDisabledWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	store.workflows["wf-1"].UpdateTime = time.Now().Add(-time.Hour)
	store.workflows["wf-disabled"] = &model.Workflow{
		ID:           "wf-disabled",
		Name:         "disabled-newer",
		AppID:        "app-1",
		ProjectID:    "proj-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Disabled:     true,
		BaseModel:    model.BaseModel{UpdateTime: time.Now()},
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
				{Name: "worker", WorkflowType: config.JobDeploy},
			},
		}),
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, "wf-1", resp.WorkflowID)
	require.Equal(t, "wf-1", store.tasks[resp.TaskID].WorkflowID)
	require.Len(t, store.jobs, 2)
}
