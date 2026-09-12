package workflow

import (
	"context"
	"encoding/json"

	"net/http"
	"net/http/httptest"

	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestCancelledCallbackUsesDurablePendingReason(t *testing.T) {
	received := make(chan job.CallbackPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var payload job.CallbackPayload
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		received <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Cancelled: server.URL})
	require.NoError(t, err)
	task := &model.WorkflowQueue{
		TaskID: "task-cancelled-reason", WorkflowID: "workflow-1", Status: config.StatusCancelled,
		Callback: callback, SchedulingReason: wf.TerminalCallbackPendingReason("cancelled by user"),
		BaseModel: model.BaseModel{CreateTime: time.Now()},
	}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), &controllerTestStore{})

	ctl.triggerWorkflowCallback(context.Background(), config.StatusCancelled, "")

	select {
	case payload := <-received:
		require.Equal(t, "cancelled by user", payload.Reason)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled callback was not received")
	}
}

func TestCallbackContextUsesDefaultWhenTimeoutIsZero(t *testing.T) {
	ctx, cancel := callbackContext(context.Background(), 0, 72*time.Hour)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(workflowconfig.DefaultWorkflowCallbackTimeout), deadline, 2*time.Second)
}

func TestCallbackContextCapsTimeoutByMax(t *testing.T) {
	ctx, cancel := callbackContext(context.Background(), int64((96*time.Hour)/time.Second), 72*time.Hour)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(72*time.Hour), deadline, 2*time.Second)
}

func TestCallbackContextBoundsAdmissionWithLiveParent(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := callbackContext(parent, 2, time.Minute)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(2*time.Second), deadline, time.Second)
	cancelParent()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestCancelledTerminalCallbackOutlivesTaskCancellationWhileRenewingLease(t *testing.T) {
	requestStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		time.Sleep(120 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	lease := time.Now().Add(30 * time.Millisecond)
	task := &model.WorkflowQueue{
		TaskID: "task-cancelled-callback-lease", AppID: "app-1", WorkspaceID: "space-1",
		Status: config.StatusCancelled, RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-2",
		LeaseExpiresAt: &lease,
	}
	store := &controllerTestStore{task: task}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)
	ctl.runtimeConfig.Workflow.LeaseDuration = 30 * time.Millisecond
	callbackJob := &model.JobTask{
		Name: "workflow-callback-task-cancelled-callback-lease", TaskID: task.TaskID,
		AppID: task.AppID, WorkspaceID: task.WorkspaceID, JobType: string(config.JobDeployCallback),
		ExecutionKey: "cancelled-callback-execution", RunGeneration: task.RunGeneration,
		OwnerStatus: task.Status, RunToken: task.RunToken, WorkerID: task.WorkerID,
		Status: config.StatusWaiting,
		JobInfo: &job.CallbackJobInfo{
			Event: "cancelled", URL: server.URL, Method: http.MethodPost, TimeoutSeconds: 1,
			TimeoutMaxSec: 1, TimeoutMaxNS: int64(time.Second),
			Payload: job.CallbackPayload{Event: "cancelled", Status: string(config.StatusCancelled), TaskID: task.TaskID},
		},
	}

	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	defer cancelRuntime()
	taskCtx, cancelTask := context.WithCancelCause(runtimeCtx)
	taskCtx = context.WithValue(taskCtx, workflowRuntimeContextKey{}, runtimeCtx)
	callbackParent, stopParent := terminalCallbackParentContext(taskCtx, config.StatusCancelled)
	defer stopParent()
	callbackCtx, cancelCallback := context.WithTimeout(callbackParent, time.Second)
	defer cancelCallback()
	done := make(chan error, 1)
	go func() { done <- ctl.runTerminalCallbackJob(callbackCtx, task, callbackJob) }()
	<-requestStarted
	cancelTask(context.Canceled)

	require.NoError(t, <-done)
	require.True(t, task.LeaseExpiresAt.After(time.Now()), "callback should renew the cancelled owner lease")
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
