package workflow

import (
	"context"

	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	approvaltimeout "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/approvaltimeout"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

func TestCancelDelayedVersionTaskForAppSuccess(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-1",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-1", "manual")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.Equal(t, "tester", store.task.TaskRevoker)
	require.Equal(t, config.CancelSourceUser, store.task.CancelSource)
}

func TestCancelDelayedVersionTaskForAppDoesNotOverwriteDispatcherTransition(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-race",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-delay-race",
				Status:       string(config.StatusQueued),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
	}
	store.beforeCAS = func(task *model.WorkflowQueue) {
		task.Status = config.StatusQueued
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-race", "manual")
	require.ErrorIs(t, err, bcode.ErrVersionUpdateTaskNotCancellable)
	require.Equal(t, config.StatusQueued, store.task.Status)
	require.Empty(t, store.task.TaskRevoker)
	require.Empty(t, store.task.CancelSource)
	require.Equal(t, string(config.StatusQueued), store.jobs[0].Status)
	require.Empty(t, store.jobs[0].Error)
	require.Zero(t, store.jobs[0].EndTime)
}

func TestCancelDelayedVersionTaskForAppPrefersStatusConflictWhenCancelSignalUnavailable(t *testing.T) {
	getCount := 0
	var store *statusDataStore
	store = &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-signal-race",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-delay-signal-race",
				Status:       string(config.StatusQueued),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
		onGet: func(_ context.Context, entity datastore.Entity) {
			if _, ok := entity.(*model.WorkflowQueue); !ok {
				return
			}
			getCount++
			if getCount == 2 {
				store.task.Status = config.StatusQueued
			}
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-signal-race", "manual")
	require.ErrorIs(t, err, bcode.ErrVersionUpdateTaskNotCancellable)
	require.Equal(t, 2, getCount)
	require.Equal(t, config.StatusQueued, store.task.Status)
	require.Empty(t, store.task.TaskRevoker)
	require.Empty(t, store.task.CancelSource)
	require.Equal(t, string(config.StatusQueued), store.jobs[0].Status)
	require.Empty(t, store.jobs[0].Error)
	require.Zero(t, store.jobs[0].EndTime)
}

func TestCancelDelayedVersionTaskForAppKeepsSignalUnavailableWhileStillWaiting(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-no-signal",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-no-signal", "manual")
	require.ErrorIs(t, err, bcode.ErrWorkflowCancelSignalUnavailable)
	require.Equal(t, config.StatusWaiting, store.task.Status)
	require.Empty(t, store.task.TaskRevoker)
	require.Empty(t, store.task.CancelSource)
}

func TestCancelDelayedVersionTaskForAppPrefersStatusConflictWhenTaskDisappears(t *testing.T) {
	getCount := 0
	var store *statusDataStore
	store = &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-disappeared",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
		onGet: func(_ context.Context, entity datastore.Entity) {
			if _, ok := entity.(*model.WorkflowQueue); !ok {
				return
			}
			getCount++
			if getCount == 2 {
				store.task = nil
			}
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-disappeared", "manual")
	require.ErrorIs(t, err, bcode.ErrVersionUpdateTaskNotCancellable)
	require.Equal(t, 2, getCount)
	require.Nil(t, store.task)
}

func TestCancelDelayedVersionTaskForAppTerminalizesPrecreatedCleanupJobs(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-cleanup",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-delay-cleanup",
				Status:       string(config.StatusQueued),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-cleanup", "manual")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.Equal(t, string(config.StatusCancelled), store.jobs[0].Status)
	require.Equal(t, "manual", store.jobs[0].Error)
	require.NotZero(t, store.jobs[0].EndTime)
	require.NoError(t, EnsureAppWorkflowIdle(context.Background(), store, "app-1"))
}

func TestCancelDelayedVersionTaskForAppKeepsRunningCleanupJobActive(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-running-cleanup",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-delay-running-cleanup",
				Status:       string(config.StatusRunning),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-running-cleanup", "manual")
	require.NoError(t, err)
	require.Equal(t, string(config.StatusRunning), store.jobs[0].Status)
	require.ErrorIs(t, EnsureAppWorkflowIdle(context.Background(), store, "app-1"), bcode.ErrWorkflowTaskCancelling)
}

func TestCancelDelayedVersionTaskForAppRejectsNonFutureTask(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-2",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(-10 * time.Second).Unix(),
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-2", "manual")
	require.ErrorIs(t, err, bcode.ErrVersionUpdateTaskNotCancellable)
}

func TestCancelDelayedVersionTaskForAppRejectsNonWaitingTask(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-3",
			AppID:     "app-1",
			Status:    config.StatusRunning,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-1", "tester", "task-delay-3", "manual")
	require.ErrorIs(t, err, bcode.ErrVersionUpdateTaskNotCancellable)
}

func TestCancelDelayedVersionTaskForAppRejectsMismatchedApp(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:    "task-delay-4",
			AppID:     "app-1",
			Status:    config.StatusWaiting,
			ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelDelayedVersionTaskForApp(context.Background(), "app-2", "tester", "task-delay-4", "manual")
	require.ErrorIs(t, err, bcode.ErrWorkflowNotExist)
}

func TestCancelAllWorkflowTasksForAppCancelsOnlyActiveAppTasks(t *testing.T) {
	tasks := []*model.WorkflowQueue{
		{TaskID: "task-running", AppID: "app-1", Status: config.StatusRunning},
		{
			TaskID:              "task-approval",
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			ApprovalPending:     true,
			CurrentStep:         1,
			PendingApprovalStep: "manual-approval",
		},
		{TaskID: "task-empty-status", AppID: "app-1"},
		{TaskID: "task-completed", AppID: "app-1", Status: config.StatusCompleted},
		{TaskID: "task-other-app", AppID: "app-2", Status: config.StatusRunning},
	}
	store := &statusDataStore{tasks: tasks}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	cancelledTaskIDs, err := svc.CancelAllWorkflowTasksForApp(context.Background(), "app-1", "tester", "manual cancel")

	require.NoError(t, err)
	require.Equal(t, []string{"task-running", "task-approval", "task-empty-status"}, cancelledTaskIDs)
	for _, idx := range []int{0, 1, 2} {
		require.Equal(t, config.StatusCancelled, tasks[idx].Status)
		require.Equal(t, "tester", tasks[idx].TaskRevoker)
		require.Equal(t, config.CancelSourceUser, tasks[idx].CancelSource)
	}
	require.False(t, tasks[1].ApprovalPending)
	require.Empty(t, tasks[1].PendingApprovalStep)
	require.Equal(t, config.StatusCompleted, tasks[3].Status)
	require.Equal(t, config.StatusRunning, tasks[4].Status)
}

func TestCancelAllWorkflowTasksForAppReturnsSuccessWhenNoTasks(t *testing.T) {
	store := &statusDataStore{}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	cancelledTaskIDs, err := svc.CancelAllWorkflowTasksForApp(context.Background(), "app-1", "tester", "manual cancel")

	require.NoError(t, err)
	require.Empty(t, cancelledTaskIDs)
	require.NotNil(t, cancelledTaskIDs)
}

func TestCancelAllWorkflowTasksForAppResendsCancelledTaskWithActiveJobs(t *testing.T) {
	tasks := []*model.WorkflowQueue{
		{TaskID: "task-cancelling", AppID: "app-1", Status: config.StatusCancelled},
		{TaskID: "task-cancelled-done", AppID: "app-1", Status: config.StatusCancelled},
	}
	store := &statusDataStore{
		tasks: tasks,
		jobs: []*model.JobInfo{
			{TaskID: "task-cancelling", Status: string(config.StatusRunning)},
			{TaskID: "task-cancelled-done", Status: string(config.StatusCompleted)},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	cancelledTaskIDs, err := svc.CancelAllWorkflowTasksForApp(context.Background(), "app-1", "tester", "manual cancel")

	require.NoError(t, err)
	require.Equal(t, []string{"task-cancelling"}, cancelledTaskIDs)
	require.Equal(t, config.StatusCancelled, tasks[0].Status)
	require.Equal(t, "tester", tasks[0].TaskRevoker)
	require.Empty(t, tasks[1].TaskRevoker)
}

func TestCancelAllWorkflowTasksForAppContinuesAfterTerminalRace(t *testing.T) {
	tasks := []*model.WorkflowQueue{
		{TaskID: "task-raced-complete", AppID: "app-1", Status: config.StatusRunning},
		{TaskID: "task-still-running", AppID: "app-1", Status: config.StatusRunning},
	}
	store := &statusDataStore{
		tasks: tasks,
		beforeCAS: func(task *model.WorkflowQueue) {
			task.Status = config.StatusCompleted
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	cancelledTaskIDs, err := svc.CancelAllWorkflowTasksForApp(context.Background(), "app-1", "tester", "manual cancel")

	require.NoError(t, err)
	require.Equal(t, []string{"task-still-running"}, cancelledTaskIDs)
	require.Equal(t, config.StatusCompleted, tasks[0].Status)
	require.Empty(t, tasks[0].TaskRevoker)
	require.Equal(t, config.StatusCancelled, tasks[1].Status)
	require.Equal(t, "tester", tasks[1].TaskRevoker)
}

func TestCancelWorkflowTaskForAppClearsApprovalCheckpointWhenPutDropsZeroValues(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-cancel-sql",
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			CurrentStep:         2,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		putDropsZeroValues: true,
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "approver", "task-cancel-sql", "manual cancel")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)
	require.Equal(t, "approver", store.task.TaskRevoker)
	require.Equal(t, config.CancelSourceUser, store.task.CancelSource)
}

func TestCancelWorkflowTaskForAppApprovalPausedTriggersCallback(t *testing.T) {
	var callbackCount int32
	var callbackMu sync.Mutex
	var callbackMethod string
	var callbackBody string
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		body, _ := io.ReadAll(r.Body)
		callbackMu.Lock()
		callbackMethod = r.Method
		callbackBody = string(body)
		callbackMu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-cancel-approval",
			AppID:               "app-1",
			WorkflowID:          "wf-cancel-approval",
			WorkflowName:        "approval-workflow",
			ProjectID:           "project-1",
			Type:                config.WorkflowTaskTypeWorkflow,
			Status:              config.StatusWaitingApprove,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		workflow: &model.Workflow{
			ID:       "wf-cancel-approval",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
	}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})

	err = svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "approver", "task-cancel-approval", "manual cancel")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)
	require.Equal(t, "approver", store.task.TaskRevoker)
	require.Equal(t, config.CancelSourceUser, store.task.CancelSource)

	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel callback not received for approval-paused task")
	}
	callbackMu.Lock()
	gotMethod := callbackMethod
	gotBody := callbackBody
	callbackMu.Unlock()
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
	require.Equal(t, http.MethodPost, gotMethod)
	require.Contains(t, gotBody, `"event":"cancelled"`)
	require.Contains(t, gotBody, `"status":"cancelled"`)
}

func TestCancelWorkflowTaskForAppApprovalQueuedTriggersCallback(t *testing.T) {
	var callbackCount int32
	var callbackMu sync.Mutex
	var callbackMethod string
	var callbackBody string
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		body, _ := io.ReadAll(r.Body)
		callbackMu.Lock()
		callbackMethod = r.Method
		callbackBody = string(body)
		callbackMu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-cancel-approval-queued",
			AppID:               "app-1",
			WorkflowID:          "wf-cancel-approval-queued",
			WorkflowName:        "approval-workflow",
			ProjectID:           "project-1",
			Type:                config.WorkflowTaskTypeWorkflow,
			Status:              config.StatusQueued,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		workflow: &model.Workflow{
			ID:       "wf-cancel-approval-queued",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
	}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})

	err = svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "approver", "task-cancel-approval-queued", "manual cancel")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)
	require.Equal(t, "approver", store.task.TaskRevoker)
	require.Equal(t, config.CancelSourceUser, store.task.CancelSource)

	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel callback not received for queued approval task")
	}
	callbackMu.Lock()
	gotMethod := callbackMethod
	gotBody := callbackBody
	callbackMu.Unlock()
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
	require.Equal(t, http.MethodPost, gotMethod)
	require.Contains(t, gotBody, `"event":"cancelled"`)
	require.Contains(t, gotBody, `"status":"cancelled"`)
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionAsyncRunsWithCanceledParentContext(t *testing.T) {
	var callbackCount int32
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		workflow: &model.Workflow{
			ID:       "wf-cancelled-parent-ctx",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
		respectContextErr: true,
	}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})
	task := &model.WorkflowQueue{
		TaskID:       "task-cancelled-parent-ctx",
		AppID:        "app-1",
		WorkflowID:   "wf-cancelled-parent-ctx",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, "manual cancel")

	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("callback should still be triggered when parent context is canceled")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionFailsWhenURLSecurityPolicyUnavailable(t *testing.T) {
	var callbackCount int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		workflow: &model.Workflow{
			ID:       "wf-policy-unavailable",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}
	task := &model.WorkflowQueue{
		TaskID:       "task-policy-unavailable",
		AppID:        "app-1",
		WorkflowID:   "wf-policy-unavailable",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	svc.triggerWorkflowTerminalCallbackOnApprovalAction(context.Background(), task, config.StatusCancelled, "manual cancel")

	require.Equal(t, int32(0), atomic.LoadInt32(&callbackCount))
	require.Len(t, store.jobs, 1)
	require.Equal(t, string(config.StatusFailed), store.jobs[0].Status)
	require.Contains(t, store.jobs[0].Error, "load url security policy")
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionFallsBackToAppCallback(t *testing.T) {
	var callbackCount int32
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		app: &model.Applications{
			ID:       "app-1",
			Callback: callback,
		},
		workflow: &model.Workflow{
			ID:    "wf-app-callback",
			AppID: "app-1",
			Name:  "approval-workflow",
		},
	}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})
	task := &model.WorkflowQueue{
		TaskID:       "task-app-callback",
		AppID:        "app-1",
		WorkflowID:   "wf-app-callback",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	svc.triggerWorkflowTerminalCallbackOnApprovalAction(context.Background(), task, config.StatusCancelled, "manual cancel")

	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("callback should be triggered from app callback fallback")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionUsesTaskCallbackBeforeWorkflowCallback(t *testing.T) {
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

	taskCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Cancelled: taskCallbackServer.URL})
	require.NoError(t, err)
	workflowCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Cancelled: workflowCallbackServer.URL})
	require.NoError(t, err)

	store := &statusDataStore{
		workflow: &model.Workflow{
			ID:       "wf-task-callback-priority",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: workflowCallback,
		},
	}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})
	task := &model.WorkflowQueue{
		TaskID:       "task-callback-priority",
		AppID:        "app-1",
		WorkflowID:   "wf-task-callback-priority",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		Callback:     taskCallback,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	svc.triggerWorkflowTerminalCallbackOnApprovalAction(context.Background(), task, config.StatusCancelled, "manual cancel")

	select {
	case <-taskCallbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("task callback should be triggered")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&taskCallbackCount))
	require.Equal(t, int32(0), atomic.LoadInt32(&workflowCallbackCount))
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionUsesTaskCallbackWhenWorkflowMissing(t *testing.T) {
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

	taskCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Cancelled: taskCallbackServer.URL})
	require.NoError(t, err)

	store := &statusDataStore{}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})
	task := &model.WorkflowQueue{
		TaskID:       "task-callback-without-workflow",
		AppID:        "app-1",
		WorkflowID:   "wf-missing",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		Callback:     taskCallback,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	svc.triggerWorkflowTerminalCallbackOnApprovalAction(context.Background(), task, config.StatusCancelled, "manual cancel")

	select {
	case <-taskCallbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("task callback should be triggered without workflow fallback")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&taskCallbackCount))
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionUsesTaskCallbackWithoutWorkflowID(t *testing.T) {
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

	store := &statusDataStore{}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})
	task := &model.WorkflowQueue{
		TaskID:       "task-callback-without-workflow-id",
		AppID:        "app-1",
		WorkflowName: "update-version",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeUpdate,
		Callback:     taskCallback,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	svc.triggerWorkflowTerminalCallbackOnApprovalAction(context.Background(), task, config.StatusCompleted, "")

	select {
	case <-taskCallbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("task callback should be triggered without workflow id")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&taskCallbackCount))
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionAsyncPropagatesParentContextValue(t *testing.T) {
	type ctxKey string
	const traceKey ctxKey = "trace-id"
	const traceValue = "trace-123"

	valueCh := make(chan interface{}, 1)
	store := &statusDataStore{
		workflow: &model.Workflow{
			ID:    "wf-context-value",
			AppID: "app-1",
			Name:  "approval-workflow",
		},
		onGet: func(ctx context.Context, entity datastore.Entity) {
			workflow, ok := entity.(*model.Workflow)
			if !ok || workflow.ID != "wf-context-value" {
				return
			}
			select {
			case valueCh <- ctx.Value(traceKey):
			default:
			}
		},
	}

	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}
	task := &model.WorkflowQueue{
		TaskID:       "task-context-value",
		AppID:        "app-1",
		WorkflowID:   "wf-context-value",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	ctx := context.WithValue(context.Background(), traceKey, traceValue)
	svc.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, "manual cancel")

	select {
	case got := <-valueCh:
		require.Equal(t, traceValue, got)
	case <-time.After(2 * time.Second):
		t.Fatal("workflow callback load did not observe parent context value")
	}
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionAsyncKeepsTimeoutAfterParentCanceled(t *testing.T) {
	deadlineCh := make(chan time.Time, 1)
	errCh := make(chan error, 1)
	store := &statusDataStore{
		workflow: &model.Workflow{
			ID:    "wf-timeout-with-canceled-parent",
			AppID: "app-1",
			Name:  "approval-workflow",
		},
		onGet: func(ctx context.Context, entity datastore.Entity) {
			workflow, ok := entity.(*model.Workflow)
			if !ok || workflow.ID != "wf-timeout-with-canceled-parent" {
				return
			}
			if err := ctx.Err(); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			deadline, ok := ctx.Deadline()
			if !ok {
				deadline = time.Time{}
			}
			select {
			case deadlineCh <- deadline:
			default:
			}
		},
	}

	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}
	task := &model.WorkflowQueue{
		TaskID:       "task-timeout-with-canceled-parent",
		AppID:        "app-1",
		WorkflowID:   "wf-timeout-with-canceled-parent",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	parentCtx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.triggerWorkflowTerminalCallbackOnApprovalActionAsync(parentCtx, task, config.StatusCancelled, "manual cancel")

	select {
	case err := <-errCh:
		t.Fatalf("load context should not inherit canceled parent: %v", err)
	case deadline := <-deadlineCh:
		require.False(t, deadline.IsZero(), "load context should carry timeout deadline")
		require.True(t, time.Until(deadline) > 0, "load context deadline should remain in the future")
	case <-time.After(2 * time.Second):
		t.Fatal("workflow callback load did not execute under canceled parent context")
	}
}

func TestTriggerWorkflowTerminalCallbackOnApprovalActionHonorsCanceledParentContext(t *testing.T) {
	errCh := make(chan error, 1)
	store := &statusDataStore{
		workflow: &model.Workflow{
			ID:    "wf-direct-canceled-parent",
			AppID: "app-1",
			Name:  "approval-workflow",
		},
		onGet: func(ctx context.Context, entity datastore.Entity) {
			workflow, ok := entity.(*model.Workflow)
			if !ok || workflow.ID != "wf-direct-canceled-parent" {
				return
			}
			select {
			case errCh <- ctx.Err():
			default:
			}
		},
		respectContextErr: true,
	}

	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}
	task := &model.WorkflowQueue{
		TaskID:       "task-direct-canceled-parent",
		AppID:        "app-1",
		WorkflowID:   "wf-direct-canceled-parent",
		WorkflowName: "approval-workflow",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		BaseModel: model.BaseModel{
			CreateTime: time.Now(),
		},
	}

	parentCtx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.triggerWorkflowTerminalCallbackOnApprovalAction(parentCtx, task, config.StatusCancelled, "manual cancel")

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("workflow callback load did not observe canceled parent context")
	}
}

func TestCancelWorkflowTaskForAppApprovalPausedIgnoresSignalErrorAndStillTriggersCallback(t *testing.T) {
	var callbackCount int32
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-cancel-approval-signal-fail",
			AppID:               "app-1",
			WorkflowID:          "wf-cancel-approval-signal-fail",
			WorkflowName:        "approval-workflow",
			ProjectID:           "project-1",
			Type:                config.WorkflowTaskTypeWorkflow,
			Status:              config.StatusWaitingApprove,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		workflow: &model.Workflow{
			ID:       "wf-cancel-approval-signal-fail",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  20 * time.Millisecond,
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
		MaxRetries:   0,
	})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})

	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{
		Store: store,
		Cache: cache.NewWithClient(false, cache.CacheTypeMem, redisClient),
		Cfg:   &config.Config{AllowPrivateURLTargets: true},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = svc.CancelWorkflowTaskForApp(ctx, "app-1", "approver", "task-cancel-approval-signal-fail", "manual cancel")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)

	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel callback not received when signal publish failed")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
}

func TestCancelWorkflowTaskForAppApprovalQueuedIgnoresSignalErrorAndStillTriggersCallback(t *testing.T) {
	var callbackCount int32
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-cancel-approval-queued-signal-fail",
			AppID:               "app-1",
			WorkflowID:          "wf-cancel-approval-queued-signal-fail",
			WorkflowName:        "approval-workflow",
			ProjectID:           "project-1",
			Type:                config.WorkflowTaskTypeWorkflow,
			Status:              config.StatusQueued,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		workflow: &model.Workflow{
			ID:       "wf-cancel-approval-queued-signal-fail",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  20 * time.Millisecond,
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
		MaxRetries:   0,
	})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})

	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{
		Store: store,
		Cache: cache.NewWithClient(false, cache.CacheTypeMem, redisClient),
		Cfg:   &config.Config{AllowPrivateURLTargets: true},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = svc.CancelWorkflowTaskForApp(ctx, "app-1", "approver", "task-cancel-approval-queued-signal-fail", "manual cancel")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)

	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel callback not received for queued approval task when signal publish failed")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
}

func TestCancelWorkflowTaskForAppRetriesAfterApprovalResume(t *testing.T) {
	var callbackCount int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-cancel-approval-race",
			AppID:               "app-1",
			WorkflowID:          "wf-cancel-approval-race",
			WorkflowName:        "approval-workflow",
			ProjectID:           "project-1",
			Type:                config.WorkflowTaskTypeWorkflow,
			Status:              config.StatusWaitingApprove,
			CurrentStep:         2,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		workflow: &model.Workflow{
			ID:       "wf-cancel-approval-race",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
		beforeCAS: func(task *model.WorkflowQueue) {
			// Simulate approval continue winning the race before cancel CAS.
			task.Status = config.StatusWaiting
			task.CurrentStep = 3
			task.ApprovalPending = false
			task.PendingApprovalStep = ""
		},
	}
	svc := &workflowServiceImpl{
		Store: store,
		Cache: newTestWorkflowCancelSignalCache(t),
		Cfg:   &config.Config{AllowPrivateURLTargets: true},
	}

	err = svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "approver", "task-cancel-approval-race", "manual cancel")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.Equal(t, 3, store.task.CurrentStep)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)
	require.Equal(t, "approver", store.task.TaskRevoker)
	require.Equal(t, int32(0), atomic.LoadInt32(&callbackCount))
}

func TestCancelWorkflowTaskForAppNonApprovalTaskDoesNotTriggerCallback(t *testing.T) {
	var callbackCount int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:          "task-cancel-running",
			AppID:           "app-1",
			WorkflowID:      "wf-cancel-running",
			WorkflowName:    "running-workflow",
			ProjectID:       "project-1",
			Type:            config.WorkflowTaskTypeWorkflow,
			Status:          config.StatusRunning,
			ApprovalPending: false,
		},
		workflow: &model.Workflow{
			ID:       "wf-cancel-running",
			AppID:    "app-1",
			Name:     "running-workflow",
			Callback: callback,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t), Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err = svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "approver", "task-cancel-running", "manual cancel")
	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)

	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int32(0), atomic.LoadInt32(&callbackCount))
}

func TestCancelWorkflowTaskForAppRejectsHistoricalTerminalStatus(t *testing.T) {
	tests := []struct {
		name   string
		status config.Status
	}{
		{name: "completed", status: config.StatusCompleted},
		{name: "failed", status: config.StatusFailed},
		{name: "timeout", status: config.StatusTimeout},
		{name: "reject", status: config.StatusReject},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &statusDataStore{task: &model.WorkflowQueue{
				TaskID: "task-terminal-" + tt.name,
				AppID:  "app-1",
				Status: tt.status,
			}}
			svc := &workflowServiceImpl{Store: store}

			err := svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "operator", store.task.TaskID, "manual cancel")

			require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotCancellable)
			require.Equal(t, tt.status, store.task.Status)
			require.Empty(t, store.task.TaskRevoker)
			require.Empty(t, store.task.CancelSource)
		})
	}
}

func TestCancelWorkflowTaskForAppLosesCASAgainstTerminalTransition(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID: "task-cancel-complete-race",
			AppID:  "app-1",
			Status: config.StatusRunning,
		},
		beforeCAS: func(task *model.WorkflowQueue) {
			task.Status = config.StatusCompleted
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t)}

	err := svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "operator", store.task.TaskID, "manual cancel")

	require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotCancellable)
	require.Equal(t, config.StatusCompleted, store.task.Status)
	require.Empty(t, store.task.TaskRevoker)
	require.Empty(t, store.task.CancelSource)
}

func TestCancelWorkflowTaskForAppRetriesActiveStatusTransition(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID: "task-cancel-active-race",
			AppID:  "app-1",
			Status: config.StatusWaiting,
		},
		beforeCAS: func(task *model.WorkflowQueue) {
			task.Status = config.StatusQueued
		},
	}
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t)}

	err := svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "operator", store.task.TaskID, "manual cancel")

	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.Equal(t, "operator", store.task.TaskRevoker)
	require.Equal(t, config.CancelSourceUser, store.task.CancelSource)
}

func TestCancelWorkflowTaskForAppReturnsConflictAfterActiveStatusRetries(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID: "task-cancel-active-contention",
			AppID:  "app-1",
			Status: config.StatusRunning,
		},
	}
	statuses := []config.Status{config.StatusWaiting, config.StatusQueued, config.StatusRunning}
	var transition func(*model.WorkflowQueue)
	transition = func(task *model.WorkflowQueue) {
		task.Status = statuses[0]
		statuses = statuses[1:]
		if len(statuses) > 0 {
			store.beforeCAS = transition
		}
	}
	store.beforeCAS = transition
	svc := &workflowServiceImpl{Store: store, Cache: newTestWorkflowCancelSignalCache(t)}

	err := svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "operator", store.task.TaskID, "manual cancel")

	require.ErrorIs(t, err, bcode.ErrWorkflowTaskCancelConflict)
	require.Equal(t, config.StatusRunning, store.task.Status)
	require.Empty(t, store.task.TaskRevoker)
	require.Empty(t, store.task.CancelSource)
}

func TestCancelWorkflowTaskForAppFailsBeforeStateChangeWithoutCancelSignal(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:          "task-cancel-no-signal",
			AppID:           "app-1",
			Status:          config.StatusRunning,
			ApprovalPending: false,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	err := svc.CancelWorkflowTaskForApp(context.Background(), "app-1", "approver", "task-cancel-no-signal", "manual cancel")
	require.ErrorIs(t, err, bcode.ErrWorkflowCancelSignalUnavailable)
	require.Equal(t, config.StatusRunning, store.task.Status)
	require.Empty(t, store.task.TaskRevoker)
	require.Empty(t, store.task.CancelSource)
}

func TestApproveWorkflowTaskCancel(t *testing.T) {
	var timeoutCancelled int32
	timerID := approvaltimeout.Register("task-approve-2", func() {
		atomic.AddInt32(&timeoutCancelled, 1)
	})
	require.NotZero(t, timerID)
	t.Cleanup(func() {
		approvaltimeout.Cancel("task-approve-2")
	})

	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-approve-2",
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			CurrentStep:         2,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	resp, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-2", "cancel", "approver", "reject")
	require.NoError(t, err)
	require.Equal(t, "cancel", resp.Action)
	require.Equal(t, string(config.StatusCancelled), resp.Status)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.False(t, store.task.ApprovalPending)
	require.Equal(t, "", store.task.PendingApprovalStep)
	require.Equal(t, "approver", store.task.TaskRevoker)
	require.Equal(t, config.CancelSourceUser, store.task.CancelSource)
	require.Equal(t, int32(1), atomic.LoadInt32(&timeoutCancelled))
}

func TestApproveWorkflowTaskCancelTerminalizesPrecreatedCleanupJobs(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-approve-cleanup",
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			CurrentStep:         2,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-approve-cleanup",
				Status:       string(config.StatusQueued),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	resp, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-cleanup", "cancel", "approver", "reject")
	require.NoError(t, err)
	require.Equal(t, string(config.StatusCancelled), resp.Status)
	require.Equal(t, config.StatusCancelled, store.task.Status)
	require.Equal(t, string(config.StatusCancelled), store.jobs[0].Status)
	require.Equal(t, "reject", store.jobs[0].Error)
	require.NotZero(t, store.jobs[0].EndTime)
	require.NoError(t, EnsureAppWorkflowIdle(context.Background(), store, "app-1"))
}

func TestApproveWorkflowTaskCancelTriggersCallback(t *testing.T) {
	var callbackCount int32
	var callbackMu sync.Mutex
	var callbackMethod string
	var callbackBody string
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		body, _ := io.ReadAll(r.Body)
		callbackMu.Lock()
		callbackMethod = r.Method
		callbackBody = string(body)
		callbackMu.Unlock()
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-approve-callback",
			AppID:               "app-1",
			WorkflowID:          "wf-approval",
			WorkflowName:        "approval-workflow",
			ProjectID:           "project-1",
			Type:                config.WorkflowTaskTypeWorkflow,
			Status:              config.StatusWaitingApprove,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		workflow: &model.Workflow{
			ID:       "wf-approval",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
	}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})

	resp, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-callback", "cancel", "approver", "reject")
	require.NoError(t, err)
	require.Equal(t, "cancel", resp.Action)
	require.Equal(t, string(config.StatusCancelled), resp.Status)
	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("approval cancel callback not received")
	}
	callbackMu.Lock()
	gotMethod := callbackMethod
	gotBody := callbackBody
	callbackMu.Unlock()
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
	require.Equal(t, http.MethodPost, gotMethod)
	require.Contains(t, gotBody, `"event":"cancelled"`)
	require.Contains(t, gotBody, `"status":"cancelled"`)
}

func TestApproveWorkflowTaskCancelReturnsBeforeSlowCallbackCompletes(t *testing.T) {
	allowResponse := make(chan struct{})
	var releaseOnce sync.Once
	releaseCallback := func() {
		releaseOnce.Do(func() {
			close(allowResponse)
		})
	}
	defer releaseCallback()

	var callbackCount int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		<-allowResponse
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Cancelled: callbackServer.URL,
	})
	require.NoError(t, err)
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-approve-callback-slow",
			AppID:               "app-1",
			WorkflowID:          "wf-approval-slow",
			WorkflowName:        "approval-workflow",
			ProjectID:           "project-1",
			Type:                config.WorkflowTaskTypeWorkflow,
			Status:              config.StatusWaitingApprove,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		workflow: &model.Workflow{
			ID:       "wf-approval-slow",
			AppID:    "app-1",
			Name:     "approval-workflow",
			Callback: callback,
		},
	}
	svc := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}})

	resultCh := make(chan struct {
		resp *apis.TaskApprovalResponse
		err  error
	}, 1)
	started := time.Now()
	go func() {
		resp, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-callback-slow", "cancel", "approver", "reject")
		resultCh <- struct {
			resp *apis.TaskApprovalResponse
			err  error
		}{resp: resp, err: err}
	}()

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Equal(t, "cancel", result.resp.Action)
		require.Equal(t, string(config.StatusCancelled), result.resp.Status)
		require.Less(t, time.Since(started), 300*time.Millisecond)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("approve cancel should return before callback completes")
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&callbackCount) == 1
	}, time.Second, 10*time.Millisecond)
	releaseCallback()
}

func TestApproveWorkflowTaskCancelCASConflict(t *testing.T) {
	// Simulate: ApprovalPending was already cleared by timeout goroutine
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:          "task-approve-cas-2",
			AppID:           "app-1",
			Status:          config.StatusWaitingApprove,
			ApprovalPending: false, // Already timed out
			CurrentStep:     2,
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-cas-2", "cancel", "approver", "rejected")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotAwaitingApproval)
}

func TestApproveWorkflowTaskCancelCASRejectsCheckpointDrift(t *testing.T) {
	store := &statusDataStore{
		task: &model.WorkflowQueue{
			TaskID:              "task-approve-race-2",
			AppID:               "app-1",
			Status:              config.StatusWaitingApprove,
			CurrentStep:         3,
			ApprovalPending:     true,
			PendingApprovalStep: "manual-approval",
		},
		beforeCAS: func(task *model.WorkflowQueue) {
			// Simulate stale snapshot: controller already moved to the next approval checkpoint.
			task.CurrentStep = 4
			task.PendingApprovalStep = "manual-approval-next"
		},
	}
	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}

	_, err := svc.ApproveWorkflowTask(context.Background(), "task-approve-race-2", "cancel", "approver", "reject")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskNotAwaitingApproval)
	require.Equal(t, config.StatusWaitingApprove, store.task.Status)
	require.True(t, store.task.ApprovalPending)
	require.Equal(t, "manual-approval-next", store.task.PendingApprovalStep)
	require.Equal(t, 4, store.task.CurrentStep)
}
