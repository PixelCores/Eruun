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
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
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
	//lint:ignore SA1012 Verify the supported nil-context fallback.
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
	err = ctl.run(context.Background(), 1)
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
	err = ctl.run(context.Background(), 1)
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
		runDone <- ctl.run(context.Background(), 1)
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
	runErr := ctl.run(context.Background(), 1)
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

type approvalClockFailureStore struct {
	*controllerTestStore
}

func (s approvalClockFailureStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	return time.Time{}, errors.New("database clock unavailable")
}

func approvalTimeoutTestSteps(t *testing.T, stepName string) *model.JSONStruct {
	t.Helper()
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:     stepName,
			StepType: config.WorkflowStepTypeApproval,
			Mode:     config.WorkflowModeStepByStep,
			Approval: &model.WorkflowStepApproval{TimeoutSeconds: 60},
		}},
	})
	require.NoError(t, err)
	return stepsJSON
}

// expireStoredApprovalDeadline moves the persisted deadline into the past,
// standing in for the wall-clock wait after the pausing Worker has exited.
func expireStoredApprovalDeadline(t *testing.T, store *controllerTestStore) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	require.NotNil(t, store.task.ApprovalDeadline)
	require.WithinDuration(t, time.Now().Add(time.Minute), *store.task.ApprovalDeadline, 5*time.Second)
	expired := time.Now().UTC().Add(-time.Second)
	store.task.ApprovalDeadline = &expired
}

func TestApprovalTimeoutFiresAfterPausingWorkerExits(t *testing.T) {
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
			Steps: approvalTimeoutTestSteps(t, "manual-check"),
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
	require.NoError(t, ctl.run(context.Background(), 1))
	require.Equal(t, config.StatusWaitingApprove, ctl.snapshotTask().Status)
	expireStoredApprovalDeadline(t, store)

	// Only the Leader runtime acts; the Worker that paused the task is gone.
	(&Workflow{Store: store}).expireApprovalDeadlines(context.Background(), 10)

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, config.StatusTimeout, store.task.Status)
	require.False(t, store.task.ApprovalPending)
	require.Empty(t, store.task.PendingApprovalStep)
	require.Empty(t, store.task.RunToken)
	require.Empty(t, store.task.WorkerID)
	require.Equal(t, string(config.StatusTimeout), store.jobs[0].Status)
	require.Contains(t, store.jobs[0].Error, "manual-check")
	require.NotZero(t, store.jobs[0].EndTime)
}

func TestApprovalTimeoutCallbackSentOnceByLeader(t *testing.T) {
	var callbackCount int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callbackCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()
	callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Timeout: callbackServer.URL})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-timeout-callback-once",
		WorkspaceID:  "workspace-1",
		WorkflowID:   "wf-approval-timeout-callback-once",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	store := &controllerTestStore{
		application: &model.Applications{ID: "app-1", WorkspaceID: "workspace-1", Namespace: "default"},
		workflow: &model.Workflow{
			ID:       "wf-approval-timeout-callback-once",
			Steps:    approvalTimeoutTestSteps(t, "manual-check"),
			Callback: callback,
		},
		task: cloneWorkflowQueue(task),
	}
	ctl := newTestWorkflowController(t, task, nil, store)
	require.NoError(t, ctl.run(context.Background(), 1))
	expireStoredApprovalDeadline(t, store)

	runtime := &Workflow{
		Store:                     store,
		Cfg:                       &config.Config{AllowPrivateURLTargets: true, Workflow: workflowconfig.DefaultRuntimeConfig()},
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}),
	}
	runtime.expireApprovalDeadlines(context.Background(), 10)
	runtime.expireApprovalDeadlines(context.Background(), 10)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&callbackCount) >= 1
	}, 3*time.Second, 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(&callbackCount))
}

func TestApprovalTimeoutDoesNotAffectNewerCheckpointWithReusedStepName(t *testing.T) {
	expired := time.Now().UTC().Add(-time.Minute)
	task := &model.WorkflowQueue{
		TaskID:              "task-approval-reused-name",
		WorkflowID:          "wf-approval-reused-name",
		AppID:               "app-1",
		Status:              config.StatusWaitingApprove,
		CurrentStep:         0,
		ApprovalPending:     true,
		PendingApprovalStep: "manual-check",
		ApprovalDeadline:    &expired,
	}
	ensureTestWorkflowExecutionIdentity(task)
	newer := time.Now().UTC().Add(time.Hour)
	store := &controllerTestStore{
		task: cloneWorkflowQueue(task),
		beforeCompareHook: func(current *model.WorkflowQueue) {
			// The task resumed and paused again at a later step with the same name.
			current.CurrentStep = 1
			current.ApprovalDeadline = &newer
		},
	}

	(&Workflow{Store: store}).expireApprovalDeadlines(context.Background(), 10)

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, config.StatusWaitingApprove, store.task.Status)
	require.True(t, store.task.ApprovalPending)
	require.Equal(t, 1, store.task.CurrentStep)
	require.Equal(t, "manual-check", store.task.PendingApprovalStep)
}

func TestPauseAtApprovalStepFailsWithoutDatabaseClock(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:       "task-approval-clock",
		WorkflowID:   "wf-approval-clock",
		WorkflowName: "approval-workflow",
		AppID:        "app-1",
		Status:       config.StatusWaiting,
	}
	ensureTestWorkflowExecutionIdentity(task)
	base := &controllerTestStore{
		workflow: &model.Workflow{ID: "wf-approval-clock", Steps: approvalTimeoutTestSteps(t, "manual-check")},
		task:     cloneWorkflowQueue(task),
	}

	ctl := newTestWorkflowController(t, task, nil, approvalClockFailureStore{base})
	runErr := ctl.run(context.Background(), 1)

	require.ErrorContains(t, runErr, "database clock unavailable")
	base.mu.Lock()
	defer base.mu.Unlock()
	require.Equal(t, config.StatusRunning, base.task.Status)
	require.False(t, base.task.ApprovalPending)
	require.Nil(t, base.task.ApprovalDeadline)
}
