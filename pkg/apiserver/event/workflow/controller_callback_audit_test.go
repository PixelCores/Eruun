package workflow

import (
	"context"

	"net/http"
	"net/http/httptest"

	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestCallbackContextUsesDefaultWhenTimeoutIsZero(t *testing.T) {
	ctx, cancel := callbackContext(nil, 0, 72*time.Hour)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(config.DefaultWorkflowCallbackTimeout), deadline, 2*time.Second)
}

func TestCallbackContextCapsTimeoutByMax(t *testing.T) {
	ctx, cancel := callbackContext(nil, int64((96*time.Hour)/time.Second), 72*time.Hour)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(72*time.Hour), deadline, 2*time.Second)
}

func TestTriggerWorkflowCallbackUsesTaskCallbackBeforeWorkflowCallback(t *testing.T) {
	var taskCallbackCount int32
	taskCallbackReceived := make(chan struct{}, 1)
	taskCallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&taskCallbackCount, 1)
		w.WriteHeader(http.StatusOK)
		select {
		case taskCallbackReceived <- struct{}{}:
		default:
		}
	}))
	defer taskCallbackServer.Close()

	var workflowCallbackCount int32
	workflowCallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&workflowCallbackCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer workflowCallbackServer.Close()

	taskCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: taskCallbackServer.URL})
	require.NoError(t, err)
	workflowCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: workflowCallbackServer.URL})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-callback-priority",
		Status:       config.StatusCompleted,
		AppID:        "app-1",
		WorkflowID:   "wf-callback-priority",
		WorkflowName: "deploy",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		Callback:     taskCallback,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:       "wf-callback-priority",
			AppID:    "app-1",
			Name:     "deploy",
			Callback: workflowCallback,
		},
	}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)

	ctl.triggerWorkflowCallback(context.Background(), config.StatusCompleted, "")

	select {
	case <-taskCallbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("task callback not received")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&taskCallbackCount))
	require.Equal(t, int32(0), atomic.LoadInt32(&workflowCallbackCount))
}

func TestTriggerWorkflowCallbackUsesTaskCallbackWhenWorkflowMissing(t *testing.T) {
	var taskCallbackCount int32
	taskCallbackReceived := make(chan struct{}, 1)
	taskCallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&taskCallbackCount, 1)
		w.WriteHeader(http.StatusOK)
		select {
		case taskCallbackReceived <- struct{}{}:
		default:
		}
	}))
	defer taskCallbackServer.Close()

	taskCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: taskCallbackServer.URL})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-callback-without-workflow",
		Status:       config.StatusCompleted,
		AppID:        "app-1",
		WorkflowID:   "wf-missing",
		WorkflowName: "deploy",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		Callback:     taskCallback,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), &controllerTestStore{})

	ctl.triggerWorkflowCallback(context.Background(), config.StatusCompleted, "")

	select {
	case <-taskCallbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("task callback should be triggered without workflow fallback")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&taskCallbackCount))
}
