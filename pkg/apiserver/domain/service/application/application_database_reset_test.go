package application

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestResetApplicationDatabasesCreatesWorkflowAndQueue(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "default",
		Project:   "project-1",
	}
	store.components["mysql"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "mysql",
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Replicas:      1,
	}
	store.components["redis"] = &model.ApplicationComponent{
		ID:            2,
		AppID:         "app-1",
		Name:          "redis",
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Replicas:      1,
	}
	store.components["api"] = &model.ApplicationComponent{
		ID:            3,
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
		Components: []string{"mysql", "redis"},
		InitSQLURL: "  https://files.example/game-1.0.8.sql  ",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "app-1", resp.AppID)
	require.NotEmpty(t, resp.WorkflowID)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, []string{"mysql", "redis"}, resp.DatabaseComponents)
	require.Empty(t, resp.RestartComponents)
	responseJSON, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(responseJSON), `"restartComponents":[]`)

	workflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, workflow)
	require.Equal(t, config.WorkflowTaskTypeDatabaseReset, workflow.WorkflowType)
	require.Equal(t, config.StatusCreated, workflow.Status)
	steps := decodeWorkflowSteps(t, workflow.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, config.JobDatabaseReset, steps.Steps[0].WorkflowType)
	require.Equal(t, config.WorkflowModeStepByStep, steps.Steps[0].Mode)
	require.Equal(t, []model.Policies{{
		Policies:   []string{"mysql", "redis"},
		InitSQLURL: "https://files.example/game-1.0.8.sql",
	}}, steps.Steps[0].Properties)

	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	require.Equal(t, workflow.ID, task.WorkflowID)
	require.Equal(t, config.WorkflowTaskTypeDatabaseReset, task.Type)
	require.Equal(t, config.StatusWaiting, task.Status)
}

func TestResetApplicationDatabasesRejectsInvalidInitSQLURL(t *testing.T) {
	tests := []struct {
		name    string
		request string
	}{
		{
			name:    "empty string",
			request: `{"components":["mysql"],"initSqlUrl":""}`,
		},
		{
			name:    "whitespace",
			request: `{"components":["mysql"],"initSqlUrl":"   "}`,
		},
		{
			name:    "unsupported scheme",
			request: `{"components":["mysql"],"initSqlUrl":"ftp://files.example/game.sql"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
			store.components["mysql"] = &model.ApplicationComponent{
				ID:            1,
				AppID:         "app-1",
				Name:          "mysql",
				Namespace:     "default",
				ComponentType: config.StoreJob,
				Replicas:      1,
			}
			svc := newMockServiceWithStore(store)

			var req apisv1.DatabaseResetRequest
			require.NoError(t, json.Unmarshal([]byte(tt.request), &req))
			resp, err := svc.ResetApplicationDatabases(context.Background(), "app-1", req)

			require.Nil(t, resp)
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Contains(t, err.Error(), "initSqlUrl")
			require.Empty(t, store.workflows)
			require.Empty(t, store.tasks)
		})
	}
}

func TestResetApplicationDatabasesAllowsOmittedInitSQLURL(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "default",
		Project:   "project-1",
	}
	store.components["mysql"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "mysql",
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Replicas:      1,
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
		Components: []string{"mysql"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	workflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, workflow)
	steps := decodeWorkflowSteps(t, workflow.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, []model.Policies{{
		Policies: []string{"mysql"},
	}}, steps.Steps[0].Properties)
	require.NotNil(t, store.tasks[resp.TaskID])
}

func TestResetApplicationDatabasesRejectsActiveWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.components["mysql"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "mysql",
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Replicas:      1,
	}
	store.tasks["running-task"] = &model.WorkflowQueue{
		TaskID: "running-task",
		AppID:  "app-1",
		Status: config.StatusRunning,
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
		Components: []string{"mysql"},
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.Empty(t, store.workflows)
	require.Len(t, store.tasks, 1)
}

func TestResetApplicationDatabasesRejectsPendingStatefulSetCleanup(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.components["mysql"] = &model.ApplicationComponent{
		ID: 1, AppID: "app-1", Name: "mysql", Namespace: "default", ComponentType: config.StoreJob, Replicas: 1,
	}
	addStatefulSetDeletionV2History(t, store, config.StatusFailed, config.StatusCompleted)
	svc := newMockServiceWithStore(store)

	resp, err := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
		Components: []string{"mysql"},
	})

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Empty(t, store.workflows)
	require.Len(t, store.tasks, 1)
}

func TestResetApplicationDatabasesUsesApplicationScheduleLock(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.components["mysql"] = &model.ApplicationComponent{
		ID: 1, AppID: "app-1", Name: "mysql", Namespace: "default", ComponentType: config.StoreJob, Replicas: 1,
	}
	svc := newMockServiceWithStore(store)
	svc.ScheduleLocker = locker.NewMemoryLocker("test-app-schedule")
	blockingRepo := &blockingApplicationRepository{
		ApplicationRepository: svc.AppRepo,
		entered:               make(chan struct{}),
		release:               make(chan struct{}),
	}
	svc.AppRepo = blockingRepo

	firstDone := make(chan error, 1)
	go func() {
		_, err := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
			Components: []string{"mysql"},
		})
		firstDone <- err
	}()

	<-blockingRepo.entered
	secondResp, secondErr := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
		Components: []string{"mysql"},
	})
	require.Nil(t, secondResp)
	require.ErrorIs(t, secondErr, bcode.ErrApplicationOperationLocked)
	require.Equal(t, int32(1), blockingRepo.reads.Load(), "the rejected reset must not read mutable application state")

	close(blockingRepo.release)
	require.NoError(t, <-firstDone)
}

func TestResetApplicationDatabasesRejectsContendedAppLockWithoutWrites(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.components["mysql"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "mysql",
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Replicas:      1,
	}
	svc := newMockServiceWithStore(store)
	releaseLock := holdApplicationTestAppScheduleLock(t, svc.ScheduleLocker, "app-1")
	defer releaseLock()

	resp, err := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
		Components: []string{"mysql"},
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Empty(t, store.workflows)
	require.Empty(t, store.tasks)
}

func TestResetApplicationDatabasesRejectsUnknownOrNonStoreComponent(t *testing.T) {
	tests := []struct {
		name       string
		components []string
	}{
		{name: "unknown component", components: []string{"missing"}},
		{name: "non store component", components: []string{"api"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
			store.components["api"] = &model.ApplicationComponent{
				ID:            1,
				AppID:         "app-1",
				Name:          "api",
				Namespace:     "default",
				ComponentType: config.ServerJob,
				Replicas:      1,
			}
			svc := newMockServiceWithStore(store)

			resp, err := svc.ResetApplicationDatabases(context.Background(), "app-1", apisv1.DatabaseResetRequest{
				Components: tt.components,
			})
			require.Nil(t, resp)
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Empty(t, store.workflows)
			require.Empty(t, store.tasks)
		})
	}
}
