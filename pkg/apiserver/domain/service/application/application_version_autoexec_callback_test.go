package application

import (
	"context"

	"errors"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionAutoExecPersistsTaskCallbackWithoutMutatingDefaults(t *testing.T) {
	store := newInMemoryAppStore()
	appCallback := mustJSONStruct(&model.WorkflowCallback{Success: "https://example.com/app-success"})
	workflowCallback := mustJSONStruct(&model.WorkflowCallback{Success: "https://example.com/workflow-success"})
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
		Callback:  appCallback,
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:     "backend",
		AppID:    "app-1",
		Image:    "backend:v1",
		Replicas: 1,
		Status:   string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:       "wf-1",
		AppID:    "app-1",
		Callback: workflowCallback,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "backend",
			Image: "backend:v1.1",
		}},
		Callback: &apisv1.WorkflowCallback{
			Success:        " https://example.com/version-success ",
			Methods:        map[string]string{"success": "post"},
			Headers:        map[string]string{" X-Source ": " eruun "},
			TimeoutSeconds: 30,
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	requireWorkflowCallbackSuccess(t, task.Callback, "https://example.com/version-success")
	requireWorkflowCallbackSuccess(t, store.apps["app-1"].Callback, "https://example.com/app-success")
	requireWorkflowCallbackSuccess(t, store.workflows["wf-1"].Callback, "https://example.com/workflow-success")

	var callback model.WorkflowCallback
	require.NoError(t, decodeJSONStruct(task.Callback, &callback))
	require.Equal(t, "POST", callback.Methods["success"])
	require.Equal(t, "eruun", callback.Headers["X-Source"])
	require.Equal(t, int64(30), callback.TimeoutSeconds)
}

func TestUpdateVersionNoopAutoExecTriggersTaskCallbackWithoutWorkflow(t *testing.T) {
	callbackReceived := make(chan string, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- string(body):
		default:
		}
	}))
	defer callbackServer.Close()

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
	setTestURLSecurityPolicy(t, store, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "backend",
			Image: "backend:v1",
		}},
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Empty(t, resp.UpdatedComponents)
	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	require.Equal(t, config.WorkflowTaskTypeUpdate, task.Type)
	require.Empty(t, task.WorkflowID)
	requireWorkflowCallbackSuccess(t, task.Callback, callbackServer.URL)

	var callbackBody string
	select {
	case callbackBody = <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("version no-op callback not received")
	}
	require.Contains(t, callbackBody, `"event":"success"`)
	require.Contains(t, callbackBody, `"status":"completed"`)
	require.Contains(t, callbackBody, `"taskId":"`+resp.TaskID+`"`)
	require.Contains(t, callbackBody, `"workflowId":""`)
	require.Contains(t, callbackBody, `"workflowType":"update"`)
}

func TestUpdateVersionNoopAutoExecRollsBackWhenCallbackRecordsFail(t *testing.T) {
	tests := []struct {
		name          string
		injectFailure func(*inMemoryAppStore)
		wantError     string
	}{
		{
			name: "workflow task create fails",
			injectFailure: func(store *inMemoryAppStore) {
				store.addWorkflowQueueErr = errors.New("queue create failed")
			},
			wantError: "queue create failed",
		},
		{
			name: "job info create fails",
			injectFailure: func(store *inMemoryAppStore) {
				store.addJobInfoErr = errors.New("job info create failed")
			},
			wantError: "job info create failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackReceived := make(chan string, 1)
			callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				w.WriteHeader(http.StatusOK)
				select {
				case callbackReceived <- string(body):
				default:
				}
			}))
			defer callbackServer.Close()

			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{
				ID:          "app-1",
				Name:        "DemoApp",
				Version:     "1.0.0",
				Description: "old description",
				Namespace:   "default",
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
					Steps: []*model.WorkflowStep{{Name: "backend", WorkflowType: config.JobDeploy}},
				}),
			}
			setTestURLSecurityPolicy(t, store, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
			tt.injectFailure(store)

			svc := newMockServiceWithStore(store)
			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:     "1.1.0",
				Description: "new description",
				WorkflowID:  "wf-1",
				Components: []apisv1.ComponentUpdateSpec{{
					Name:  "backend",
					Image: "backend:v1",
				}},
				Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
			})

			require.Error(t, err)
			require.Contains(t, err.Error(), "record update-version callback task")
			require.Contains(t, err.Error(), tt.wantError)
			require.Nil(t, resp)
			require.Empty(t, store.tasks)
			require.Empty(t, store.jobs)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			require.Equal(t, "old description", store.apps["app-1"].Description)
			require.Equal(t, "backend:v1", store.components["backend"].Image)
			requireNoCallbackReceived(t, callbackReceived)
		})
	}
}

func TestUpdateVersionNoopAutoExecPropagatesRequestedWorkflowIDToCallback(t *testing.T) {
	callbackReceived := make(chan string, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- string(body):
		default:
		}
	}))
	defer callbackServer.Close()

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
			Steps: []*model.WorkflowStep{{Name: "backend", WorkflowType: config.JobDeploy}},
		}),
	}
	setTestURLSecurityPolicy(t, store, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		WorkflowID: "wf-1",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "backend",
			Image: "backend:v1",
		}},
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, "wf-1", resp.WorkflowID)
	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	require.Equal(t, config.WorkflowTaskTypeUpdate, task.Type)
	require.Equal(t, "wf-1", task.WorkflowID)

	var callbackBody string
	select {
	case callbackBody = <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("version no-op callback not received")
	}
	require.Contains(t, callbackBody, `"event":"success"`)
	require.Contains(t, callbackBody, `"status":"completed"`)
	require.Contains(t, callbackBody, `"taskId":"`+resp.TaskID+`"`)
	require.Contains(t, callbackBody, `"workflowId":"wf-1"`)
	require.Contains(t, callbackBody, `"workflowType":"update"`)
}

func TestUpdateVersionAutoExecFalseIgnoresInvalidTaskCallback(t *testing.T) {
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
		Version:    "1.1.0",
		WorkflowID: "wf-1",
		AutoExec:   boolPtr(false),
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "backend",
			Image: "backend:v1.1",
		}},
		Callback: &apisv1.WorkflowCallback{Success: "ftp://example.com/callback"},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Empty(t, resp.WorkflowID)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1.1", store.components["backend"].Image)
	queueRepo, ok := svc.WorkflowQueueRepo.(*mockWorkflowQueueRepo)
	require.True(t, ok)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, resp.TaskID, queueRepo.lastQueue.TaskID)
	require.Empty(t, queueRepo.lastQueue.WorkflowID)
	require.Nil(t, queueRepo.lastQueue.Callback)
}

func TestUpdateVersionAutoExecInvalidTaskCallbackRollsBack(t *testing.T) {
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
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "backend",
			Image: "backend:v1.1",
		}},
		Callback: &apisv1.WorkflowCallback{Success: "ftp://example.com/callback"},
	})

	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionNoopAutoExecInvalidTaskCallbackRollsBack(t *testing.T) {
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
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "backend",
			Image: "backend:v1",
		}},
		Callback: &apisv1.WorkflowCallback{Success: "ftp://example.com/callback"},
	})

	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Empty(t, store.tasks)
}

func TestUpdateVersionAutoExecNoWorkflowReturnsError(t *testing.T) {
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
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Name:  "backend",
				Image: "backend:v1.1",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrWorkflowNotExist)
	require.Nil(t, resp)
	require.Empty(t, store.tasks)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
}

func TestUpdateVersionAutoExecInvalidWorkflowReturnsError(t *testing.T) {
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
	}

	queueRepo := &mockWorkflowQueueRepo{}
	svc := newMockServiceWithStore(store)
	svc.WorkflowQueueRepo = queueRepo

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
	require.ErrorIs(t, err, bcode.ErrExecWorkflow)
	require.Contains(t, err.Error(), "auto exec workflow")
	require.Contains(t, err.Error(), "invalid workflow")
	require.Nil(t, resp)
	require.Nil(t, queueRepo.lastQueue)
	require.Empty(t, queueRepo.queues)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, "backend:v1", store.components["backend"].Image)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["backend"].Status)
}

func TestUpdateVersionAutoExecUsesSpecifiedWorkflow(t *testing.T) {
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
	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		BaseModel:    model.BaseModel{UpdateTime: time.Now().Add(-time.Hour)},
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
			},
		}),
	}
	store.workflows["wf-custom"] = &model.Workflow{
		ID:           "wf-custom",
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

	req := apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		WorkflowID: "wf-custom",
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
	require.Equal(t, "wf-custom", resp.WorkflowID)
	require.Equal(t, "wf-custom", task.WorkflowID)
	require.Equal(t, config.WorkflowTaskTypeWorkflow, task.Type)
}

func TestUpdateVersionAutoExecSyncsSpecifiedWorkflowOnRemove(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	for _, name := range []string{"api", "worker"} {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			Namespace:     "default",
			Image:         "nginx:1.27",
			Replicas:      1,
			ComponentType: config.ServerJob,
		}
	}
	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		BaseModel:    model.BaseModel{UpdateTime: time.Now()},
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
				{Name: "worker", WorkflowType: config.JobDeploy},
			},
		}),
	}
	store.workflows["wf-custom"] = &model.Workflow{
		ID:           "wf-custom",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		BaseModel:    model.BaseModel{UpdateTime: time.Now().Add(-time.Hour)},
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
				{Name: "worker", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		WorkflowID: "wf-custom",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "api"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, "wf-custom", store.tasks[resp.TaskID].WorkflowID)

	customSteps := decodeWorkflowSteps(t, store.workflows["wf-custom"].Steps)
	require.Len(t, customSteps.Steps, 1)
	require.Equal(t, "worker", customSteps.Steps[0].Name)

	defaultSteps := decodeWorkflowSteps(t, store.workflows["wf-default"].Steps)
	require.Len(t, defaultSteps.Steps, 2)
	require.Equal(t, "api", defaultSteps.Steps[0].Name)
	require.Equal(t, "worker", defaultSteps.Steps[1].Name)

	require.Len(t, store.jobs, 1)
	require.JSONEq(t, `{"source":"version_update_remove"}`, store.jobs[0].InternalInfo)
	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	cleanupPayload := requireVersionUpdateCleanupComponent(t, cleanupInfo, "api")
	require.Equal(t, 0, cleanupPayload.InsertBeforeStepIndex)
}
