package workflow

import (
	"context"

	"errors"

	"net/http"
	"net/http/httptest"

	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	workflowsignal "github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func TestWorkflowRunStopsOnAuthoritativeCancellation(t *testing.T) {
	var successCallbacks int32
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&successCallbacks, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	var cancelledCallbacks int32
	cancelledServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&cancelledCallbacks, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer cancelledServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Success:   successServer.URL,
		Cancelled: cancelledServer.URL,
	})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-run-cancel-race",
		WorkflowID:   "wf-run-cancel-race",
		WorkflowName: "deploy",
		AppID:        "app-1",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		Status:       config.StatusWaiting,
		Callback:     callback,
	}
	store := &controlledWorkflowCASStore{
		controllerTestStore: &controllerTestStore{task: cloneWorkflowQueue(task)},
		falseCalls: map[int]func(*model.WorkflowQueue){
			1: func(persisted *model.WorkflowQueue) {
				persisted.Status = config.StatusCancelled
				persisted.CancelSource = config.CancelSourceUser
			},
		},
	}
	client := kubefake.NewSimpleClientset()
	ctl := newTestWorkflowController(t, task, client, store)

	err = ctl.Run(context.Background(), 1)

	require.NoError(t, err)
	require.Equal(t, config.StatusCancelled, ctl.snapshotTask().Status)
	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusCancelled, persisted.Status)
	require.Empty(t, client.Actions())
	require.Equal(t, int32(0), atomic.LoadInt32(&successCallbacks))
	require.Equal(t, int32(1), atomic.LoadInt32(&cancelledCallbacks))
}

func TestWorkflowRunSuppressesCallbackAfterExecutionOwnershipChanges(t *testing.T) {
	var successCallbacks int32
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&successCallbacks, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: successServer.URL})
	require.NoError(t, err)
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:        "task-generation-fenced-callback",
		WorkflowID:    "wf-generation-fenced-callback",
		WorkflowName:  "deploy",
		AppID:         "app-1",
		ProjectID:     "project-1",
		Type:          config.WorkflowTaskTypeWorkflow,
		Status:        config.StatusWaiting,
		Callback:      callback,
		RunGeneration: 1,
		RunToken:      "run-token-1",
		WorkerID:      "worker-1",
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{ID: task.WorkflowID, Steps: steps},
		task:     cloneWorkflowQueue(task),
		beforeCompareHook: func(persisted *model.WorkflowQueue) {
			persisted.Status = config.StatusCompleted
			persisted.RunGeneration = 2
			persisted.RunToken = "run-token-2"
			persisted.WorkerID = "worker-2"
		},
	}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)

	err = ctl.Run(context.Background(), 1)

	require.NoError(t, err)
	require.True(t, ctl.terminalCallbackSuppressed())
	require.Equal(t, uint64(2), ctl.snapshotTask().RunGeneration)
	require.Equal(t, int32(0), atomic.LoadInt32(&successCallbacks))
}

func TestWorkflowRunDoesNotTerminalizeInfrastructureHandoff(t *testing.T) {
	var successCallbacks int32
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&successCallbacks, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	var cancelledCallbacks int32
	cancelledServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&cancelledCallbacks, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer cancelledServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Success:   successServer.URL,
		Cancelled: cancelledServer.URL,
	})
	require.NoError(t, err)
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{})
	require.NoError(t, err)
	task := &model.WorkflowQueue{
		TaskID:        "task-infrastructure-handoff",
		WorkflowID:    "wf-infrastructure-handoff",
		WorkflowName:  "deploy",
		AppID:         "app-1",
		ProjectID:     "project-1",
		Type:          config.WorkflowTaskTypeWorkflow,
		Status:        config.StatusRunning,
		Callback:      callback,
		RunGeneration: 1,
		RunToken:      "run-token-1",
		WorkerID:      "worker-1",
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{ID: task.WorkflowID, Steps: steps},
		task:     cloneWorkflowQueue(task),
	}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(workflowsignal.ErrInfrastructureStop)

	err = ctl.Run(ctx, 1)

	require.ErrorIs(t, err, workflowsignal.ErrInfrastructureStop)
	require.Equal(t, config.StatusRunning, ctl.snapshotTask().Status)
	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusRunning, persisted.Status)
	require.Equal(t, int32(0), atomic.LoadInt32(&successCallbacks))
	require.Equal(t, int32(0), atomic.LoadInt32(&cancelledCallbacks))
}

func TestStopTaskPersistenceUsesInfrastructureStopForRecoverableStops(t *testing.T) {
	tests := []struct {
		name          string
		uncertain     bool
		ownershipLost bool
		wantHandoff   bool
	}{
		{name: "persistence uncertain", uncertain: true, wantHandoff: true},
		{name: "ownership lost", ownershipLost: true, wantHandoff: true},
		{name: "authoritative user state", wantHandoff: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			ctl := &WorkflowCtl{}
			ctl.registerRunCancel(cancel)

			ctl.stopTaskPersistence(nil, tt.uncertain, tt.ownershipLost)

			require.Error(t, ctx.Err())
			if tt.wantHandoff {
				require.ErrorIs(t, context.Cause(ctx), workflowsignal.ErrInfrastructureStop)
				return
			}
			require.ErrorIs(t, context.Cause(ctx), context.Canceled)
			require.NotErrorIs(t, context.Cause(ctx), workflowsignal.ErrInfrastructureStop)
		})
	}
}

func TestWorkflowRunSuppressesCallbackWhenPersistenceReloadFails(t *testing.T) {
	var successCallbacks int32
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&successCallbacks, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: successServer.URL})
	require.NoError(t, err)
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-persistence-reload-failure",
		WorkflowID:   "wf-persistence-reload-failure",
		WorkflowName: "deploy",
		AppID:        "app-1",
		ProjectID:    "project-1",
		Type:         config.WorkflowTaskTypeWorkflow,
		Status:       config.StatusWaiting,
		Callback:     callback,
	}
	store := &controlledWorkflowCASStore{
		controllerTestStore: &controllerTestStore{
			workflow: &model.Workflow{ID: task.WorkflowID, Steps: steps},
			task:     cloneWorkflowQueue(task),
		},
		falseCalls: map[int]func(*model.WorkflowQueue){
			2: func(persisted *model.WorkflowQueue) {
				persisted.Status = config.StatusCancelled
			},
		},
		failTaskGetAfterCAS: 2,
		taskGetErr:          errors.New("injected workflow task reload failure"),
	}
	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)

	err = ctl.Run(context.Background(), 1)

	require.ErrorIs(t, err, errWorkflowTaskPersistenceUncertain)
	require.Equal(t, config.StatusCompleted, ctl.snapshotTask().Status)
	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusCancelled, persisted.Status)
	require.True(t, ctl.terminalCallbackSuppressed())
	require.Equal(t, int32(0), atomic.LoadInt32(&successCallbacks))
}

func TestWorkflowRunRetriesOriginalStepWhenDistributedCheckpointFails(t *testing.T) {
	properties, err := model.NewJSONStructByStruct(model.Properties{
		StartTime: time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, err)
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "delayed-job",
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
		}},
	})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:        "task-delayed-checkpoint",
		WorkflowID:    "wf-delayed-checkpoint",
		WorkflowName:  "delayed-checkpoint",
		AppID:         "app-delayed-checkpoint",
		ProjectID:     "project-1",
		Type:          config.WorkflowTaskTypeWorkflow,
		Status:        config.StatusWaiting,
		RunGeneration: 1,
		RunToken:      "run-1",
		WorkerID:      "worker-1",
	}
	checkpointErr := errors.New("injected job info persistence failure")
	store := &controllerTestStore{
		application: &model.Applications{ID: task.AppID, Name: "delayed-checkpoint"},
		workflow:    &model.Workflow{ID: task.WorkflowID, Steps: steps},
		task:        cloneWorkflowQueue(task),
		components: []*model.ApplicationComponent{{
			Name:          "delayed-job",
			AppID:         task.AppID,
			Namespace:     "default",
			Image:         "busybox:latest",
			ComponentType: config.InstantJob,
			Properties:    properties,
		}},
		jobInfoAddErr: checkpointErr,
	}
	queue := &fakeAckQueue{}
	redisServer, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(redisServer.Close)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() {
		require.NoError(t, redisClient.Close())
	})
	runtimeCache := cache.NewWithClient(false, cache.CacheTypeMem, redisClient)

	newController := func(t *testing.T, workflowTask *model.WorkflowQueue) *WorkflowCtl {
		t.Helper()
		ctl, err := NewWorkflowController(
			workflowTask,
			kubefake.NewSimpleClientset(),
			nil,
			store,
			&config.Config{AllowPrivateURLTargets: true},
			runtimeCache,
			&spec.URLSecurityPolicySpec{AllowPrivateByDefault: true},
		)
		require.NoError(t, err)
		ctl.DelayQueue = queue
		return ctl
	}

	firstCtl := newController(t, task)
	runErr := firstCtl.Run(context.Background(), 1)

	require.ErrorIs(t, runErr, workflowsignal.ErrInfrastructureStop)
	require.ErrorIs(t, runErr, checkpointErr)
	require.Equal(t, config.StatusRunning, firstCtl.snapshotTask().Status)
	require.Zero(t, firstCtl.snapshotTask().CurrentStep)
	require.Empty(t, queue.enqueued, "queue notification must not precede the durable checkpoint")

	store.mu.Lock()
	persistedStatus := store.task.Status
	persistedStep := store.task.CurrentStep
	recoveredTask := cloneWorkflowQueue(store.task)
	recoveredTask.Status = config.StatusWaiting
	recoveredTask.RunGeneration++
	recoveredTask.RunToken = "run-2"
	recoveredTask.WorkerID = "worker-2"
	store.task = cloneWorkflowQueue(recoveredTask)
	store.jobInfoAddErr = nil
	store.mu.Unlock()
	require.Equal(t, config.StatusRunning, persistedStatus)
	require.Zero(t, persistedStep)

	recoveredCtl := newController(t, recoveredTask)
	require.NoError(t, recoveredCtl.Run(context.Background(), 1))
	require.Equal(t, config.StatusCompleted, recoveredCtl.snapshotTask().Status)
	require.Equal(t, 1, recoveredCtl.snapshotTask().CurrentStep)
	require.Len(t, queue.enqueued, 1)
}

func TestWorkflowRunResumesFromCheckpoint(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check-1",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
			},
			{
				Name:     "manual-check-2",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-2",
		WorkflowID:   "wf-approval-2",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
		CurrentStep:  1,
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:    "wf-approval-2",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
	}

	ctl := newTestWorkflowController(t, task, nil, store)
	err = ctl.Run(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, config.StatusWaitingApprove, task.Status)
	require.True(t, task.ApprovalPending)
	require.Equal(t, "manual-check-2", task.PendingApprovalStep)
	require.Equal(t, 1, task.CurrentStep)
}
