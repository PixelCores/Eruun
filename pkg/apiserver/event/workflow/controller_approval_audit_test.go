package workflow

import (
	"context"

	"errors"

	"io"
	"net/http"
	"net/http/httptest"

	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestApprovalNotificationContextUsesDefaultTimeout(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
	defer parentCancel()

	ctx, cancel := approvalNotificationContext(parent, 0)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(workflowconfig.DefaultWorkflowCallbackTimeout), deadline, 2*time.Second)
}

func TestApprovalNotificationContextUsesProvidedTimeout(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
	defer parentCancel()

	ctx, cancel := approvalNotificationContext(parent, 30*time.Minute)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(30*time.Minute), deadline, 2*time.Second)
}

func TestApprovalNotificationContextFallsBackToBackgroundWhenParentCancelled(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	parentCancel()

	ctx, cancel := approvalNotificationContext(parent, 25*time.Second)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(25*time.Second), deadline, 2*time.Second)
	require.NoError(t, ctx.Err())
}

func TestApprovalNotificationContextNegativeTimeoutUsesDefault(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
	defer parentCancel()

	ctx, cancel := approvalNotificationContext(parent, -time.Second)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(workflowconfig.DefaultWorkflowCallbackTimeout), deadline, 2*time.Second)
}

func TestApprovalUpdateContextDetachedDoesNotInheritCancelledParent(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	ctx, cancel := approvalUpdateContext(parent, approvalUpdateContextDetached)
	defer cancel()

	require.NoError(t, ctx.Err())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(approvalUpdateTimeout), deadline, 2*time.Second)
}

func TestApprovalUpdateContextInheritHonorsParentCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := approvalUpdateContext(parent, approvalUpdateContextInherit)
	defer cancel()

	cancelParent()
	require.Eventually(t, func() bool {
		return ctx.Err() != nil
	}, time.Second, 10*time.Millisecond)
}

func TestApprovalUpdateContextInheritUsesDefaultWhenParentNil(t *testing.T) {
	ctx, cancel := approvalUpdateContext(nil, approvalUpdateContextInherit)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(approvalUpdateTimeout), deadline, 2*time.Second)
}

func TestWorkflowRunPausesAtApprovalStep(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-1",
		WorkflowID:   "wf-approval-1",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:    "wf-approval-1",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
	}

	ctl := newTestWorkflowController(t, task, nil, store)
	err = ctl.Run(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, config.StatusWaitingApprove, task.Status)
	require.True(t, task.ApprovalPending)
	require.Equal(t, "manual-check", task.PendingApprovalStep)
	require.Equal(t, 0, task.CurrentStep)
	require.False(t, isWorkflowTerminal(task.Status))
}

func TestWorkflowRunResendApprovalNotificationWhenPendingCheckpointExists(t *testing.T) {
	var notifyCount int32
	var requestBodyMu sync.Mutex
	var requestBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&notifyCount, 1)
		body, _ := io.ReadAll(r.Body)
		requestBodyMu.Lock()
		requestBody = string(body)
		requestBodyMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: server.URL,
					Method:    "POST",
				},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:              "task-approval-3",
		WorkflowID:          "wf-approval-3",
		WorkflowName:        "approval-workflow",
		AppID:               "app-1",
		Status:              config.StatusWaiting,
		CurrentStep:         0,
		ApprovalPending:     true,
		PendingApprovalStep: "manual-check",
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:    "wf-approval-3",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
	}

	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)
	err = ctl.Run(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, config.StatusWaitingApprove, task.Status)
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&notifyCount) == 1
	}, 2*time.Second, 50*time.Millisecond)
	requestBodyMu.Lock()
	gotRequestBody := requestBody
	requestBodyMu.Unlock()
	require.Contains(t, gotRequestBody, `"approvalPath":"/api/v1/workflow/tasks/task-approval-3/approval"`)
}

func TestWorkflowRunApprovalNotificationIsAsyncAndDoesNotOverwriteExternalDecision(t *testing.T) {
	notifyStarted := make(chan struct{}, 1)
	releaseNotify := make(chan struct{})
	notifyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case notifyStarted <- struct{}{}:
		default:
		}
		<-releaseNotify
		w.WriteHeader(http.StatusOK)
	}))
	defer notifyServer.Close()

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
				Approval: &model.WorkflowStepApproval{
					NotifyURL:      notifyServer.URL,
					TimeoutSeconds: 3600,
				},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-async-notify",
		WorkflowID:   "wf-approval-async-notify",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:    "wf-approval-async-notify",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
	}

	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)
	runDone := make(chan error, 1)
	go func() {
		runDone <- ctl.Run(context.Background(), 1)
	}()

	select {
	case <-notifyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("approval notification was not sent")
	}

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("workflow run was blocked by approval notification")
	}

	store.mu.Lock()
	store.task.Status = config.StatusWaiting
	store.task.ApprovalPending = false
	store.task.PendingApprovalStep = ""
	store.task.CurrentStep = 1
	store.mu.Unlock()

	close(releaseNotify)
	time.Sleep(200 * time.Millisecond)

	store.mu.Lock()
	snapshot := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusWaiting, snapshot.Status)
	require.False(t, snapshot.ApprovalPending)
	require.Equal(t, "", snapshot.PendingApprovalStep)
}

func TestPauseAtApprovalStepDoesNotOverwriteCancelledTask(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:              "task-approval-cancel-race",
		WorkflowID:          "wf-approval-cancel-race",
		WorkflowName:        "approval-workflow",
		AppID:               "app-1",
		Status:              config.StatusRunning,
		CurrentStep:         0,
		ApprovalPending:     false,
		PendingApprovalStep: "",
	}
	store := &controllerTestStore{
		task: cloneWorkflowQueue(task),
	}
	ctl := newTestWorkflowController(t, task, nil, store)

	store.mu.Lock()
	store.task.Status = config.StatusCancelled
	store.task.ApprovalPending = false
	store.task.PendingApprovalStep = ""
	store.mu.Unlock()

	stepExec := &StepExecution{
		Name:     "manual-check",
		StepType: config.WorkflowStepTypeApproval,
		Mode:     config.WorkflowModeStepByStep,
	}
	paused, err := ctl.pauseAtApprovalStep(context.Background(), stepExec, 0)
	require.NoError(t, err)
	require.False(t, paused)

	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusCancelled, persisted.Status)
	require.False(t, persisted.ApprovalPending)
	require.Equal(t, "", persisted.PendingApprovalStep)

	snapshot := ctl.snapshotTask()
	require.Equal(t, config.StatusCancelled, snapshot.Status)
	require.False(t, snapshot.ApprovalPending)
	require.Equal(t, "", snapshot.PendingApprovalStep)
}

func TestPauseAtApprovalStepReturnsErrorOnCASMissWhenTaskStillRunning(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:              "task-approval-cas-miss-running",
		WorkflowID:          "wf-approval-cas-miss-running",
		WorkflowName:        "approval-workflow",
		AppID:               "app-1",
		Status:              config.StatusRunning,
		CurrentStep:         1,
		ApprovalPending:     false,
		PendingApprovalStep: "",
	}
	store := &controllerTestStore{
		task: cloneWorkflowQueue(task),
	}
	// Simulate stale DB checkpoint due prior ack failure:
	// runner is at step 1, but persisted task is still step 0.
	store.task.CurrentStep = 0
	ctl := newTestWorkflowController(t, task, nil, store)

	stepExec := &StepExecution{
		Name:     "manual-check-2",
		StepType: config.WorkflowStepTypeApproval,
		Mode:     config.WorkflowModeStepByStep,
	}
	paused, err := ctl.pauseAtApprovalStep(context.Background(), stepExec, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "approval checkpoint cas miss while task still active")
	require.False(t, paused)

	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusRunning, persisted.Status)
	require.Equal(t, 0, persisted.CurrentStep)
	require.False(t, persisted.ApprovalPending)
	require.Equal(t, "", persisted.PendingApprovalStep)
}

func TestWorkflowRunReturnsErrorWhenApprovalCheckpointPersistFails(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-cas-error",
		WorkflowID:   "wf-approval-cas-error",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:    "wf-approval-cas-error",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
	}
	store.beforeCompareHook = func(*model.WorkflowQueue) {
		store.compareAndSwapWithConditionsErr = errors.New("cas unavailable")
	}

	ctl := newTestWorkflowController(t, task, nil, store)
	runErr := ctl.Run(context.Background(), 1)
	require.Error(t, runErr)
	require.Contains(t, runErr.Error(), "persist approval checkpoint")

	snapshot := ctl.snapshotTask()
	require.Equal(t, config.StatusRunning, snapshot.Status)
	require.False(t, snapshot.ApprovalPending)
	require.Equal(t, "", snapshot.PendingApprovalStep)

	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusRunning, persisted.Status)
	require.False(t, persisted.ApprovalPending)
	require.Equal(t, "", persisted.PendingApprovalStep)
}

func TestWorkflowRunApprovalStepTimesOut(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
				Approval: &model.WorkflowStepApproval{
					TimeoutSeconds: 1,
				},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-timeout",
		WorkflowID:   "wf-approval-timeout",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	removed := &model.ApplicationComponent{
		Name:          "old",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:    "wf-approval-timeout",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
		jobs: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				TaskID:       "task-approval-timeout",
				Status:       string(config.StatusQueued),
				InternalInfo: mustVersionUpdateCleanupInternalInfo(t, removed, 1),
				ServiceName:  "old",
			},
		},
	}

	ctl := newTestWorkflowController(t, task, nil, store)
	err = ctl.Run(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, config.StatusWaitingApprove, task.Status)

	require.Eventually(t, func() bool {
		snapshot := ctl.snapshotTask()
		return snapshot.Status == config.StatusTimeout &&
			!snapshot.ApprovalPending &&
			snapshot.PendingApprovalStep == ""
	}, 3*time.Second, 100*time.Millisecond)

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, string(config.StatusTimeout), store.jobs[0].Status)
	require.Contains(t, store.jobs[0].Error, "manual-check")
	require.NotZero(t, store.jobs[0].EndTime)
}

func TestWorkflowRunApprovalTimeoutCallbackSentOnce(t *testing.T) {
	notifyStarted := make(chan struct{}, 1)
	releaseNotify := make(chan struct{})
	notifyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case notifyStarted <- struct{}{}:
		default:
		}
		<-releaseNotify
		w.WriteHeader(http.StatusOK)
	}))
	defer notifyServer.Close()

	var callbackCount int32
	var releaseOnce sync.Once
	timeoutCallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		releaseOnce.Do(func() {
			close(releaseNotify)
		})
		w.WriteHeader(http.StatusOK)
	}))
	defer timeoutCallbackServer.Close()

	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{
		Timeout: timeoutCallbackServer.URL,
	})
	require.NoError(t, err)

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
				Approval: &model.WorkflowStepApproval{
					NotifyURL:      notifyServer.URL,
					TimeoutSeconds: 5,
				},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-timeout-callback-once",
		WorkflowID:   "wf-approval-timeout-callback-once",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:       "wf-approval-timeout-callback-once",
			Steps:    stepsJSON,
			Callback: callback,
		},
		task: cloneWorkflowQueue(task),
	}

	ctl := newTestWorkflowController(t, task, kubefake.NewSimpleClientset(), store)
	runDone := make(chan error, 1)
	go func() {
		runDone <- ctl.Run(context.Background(), 1)
	}()

	select {
	case <-notifyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("approval notification was not sent")
	}

	ctl.markApprovalTimeout(task.TaskID, "manual-check", 0, time.Second)

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("workflow run did not return")
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&callbackCount) >= 1
	}, 2*time.Second, 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
}

func TestWorkflowRunApprovalTimeoutFromPreviousStepDoesNotAffectCurrentStep(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check-1",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
				Approval: &model.WorkflowStepApproval{
					TimeoutSeconds: 1,
				},
			},
			{
				Name:     "manual-check-2",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
				Approval: &model.WorkflowStepApproval{
					TimeoutSeconds: 4,
				},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-multi-step",
		WorkflowID:   "wf-approval-multi-step",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	store := &controllerTestStore{
		workflow: &model.Workflow{
			ID:    "wf-approval-multi-step",
			Steps: stepsJSON,
		},
		task: cloneWorkflowQueue(task),
	}

	ctl := newTestWorkflowController(t, task, nil, store)
	err = ctl.Run(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, config.StatusWaitingApprove, task.Status)
	require.Equal(t, "manual-check-1", task.PendingApprovalStep)

	task.CurrentStep = 1
	task.Status = config.StatusWaiting
	task.ApprovalPending = false
	task.PendingApprovalStep = ""
	store.mu.Lock()
	store.task = cloneWorkflowQueue(task)
	store.mu.Unlock()

	err = ctl.Run(context.Background(), 1)
	require.NoError(t, err)
	secondStep := ctl.snapshotTask()
	require.Equal(t, config.StatusWaitingApprove, secondStep.Status)
	require.Equal(t, "manual-check-2", secondStep.PendingApprovalStep)

	time.Sleep(1500 * time.Millisecond)

	snapshot := ctl.snapshotTask()
	require.Equal(t, config.StatusWaitingApprove, snapshot.Status)
	require.True(t, snapshot.ApprovalPending)
	require.Equal(t, "manual-check-2", snapshot.PendingApprovalStep)
}

func TestMarkApprovalTimeoutDoesNotAffectReusedStepNameAfterCheckpointTransition(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:              "task-approval-reused-name",
		WorkflowID:          "wf-approval-reused-name",
		WorkflowName:        "approval-workflow",
		AppID:               "app-1",
		Status:              config.StatusWaitingApprove,
		CurrentStep:         0,
		ApprovalPending:     true,
		PendingApprovalStep: "manual-check",
	}
	store := &controllerTestStore{
		task: cloneWorkflowQueue(task),
		beforeCompareHook: func(t *model.WorkflowQueue) {
			// Simulate checkpoint transition between pre-check and CAS:
			// moved to next approval step with reused step name.
			t.CurrentStep = 1
			t.Status = config.StatusWaitingApprove
			t.ApprovalPending = true
			t.PendingApprovalStep = "manual-check"
		},
	}

	ctl := newTestWorkflowController(t, task, nil, store)
	ctl.markApprovalTimeout(task.TaskID, "manual-check", 0, time.Second)

	store.mu.Lock()
	persisted := *store.task
	store.mu.Unlock()
	require.Equal(t, config.StatusWaitingApprove, persisted.Status)
	require.True(t, persisted.ApprovalPending)
	require.Equal(t, 1, persisted.CurrentStep)
	require.Equal(t, "manual-check", persisted.PendingApprovalStep)
}
