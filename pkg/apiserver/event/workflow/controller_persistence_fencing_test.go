package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
)

func TestWorkflowTaskPersistenceFieldsEqualRequiresOwnedExecution(t *testing.T) {
	current := &model.WorkflowQueue{
		TaskID:              "task-1",
		Status:              config.StatusRunning,
		CurrentStep:         2,
		ApprovalPending:     true,
		PendingApprovalStep: "approve",
		RunGeneration:       7,
		RunToken:            "token-7",
		WorkerID:            "worker-a",
	}

	require.True(t, workflowTaskPersistenceFieldsEqual(current, current))

	for name, mutate := range map[string]func(*model.WorkflowQueue){
		"generation": func(task *model.WorkflowQueue) { task.RunGeneration++ },
		"token":      func(task *model.WorkflowQueue) { task.RunToken = "token-8" },
		"worker":     func(task *model.WorkflowQueue) { task.WorkerID = "worker-b" },
	} {
		t.Run(name, func(t *testing.T) {
			stale := *current
			mutate(&stale)
			require.False(t, workflowTaskPersistenceFieldsEqual(&stale, current))
		})
	}

	legacy := *current
	legacy.RunToken = ""
	require.True(t, workflowTaskPersistenceFieldsEqual(current, &legacy))
}

func TestUpdateWorkflowTaskStopsWithoutPersistingAfterExecutionContextCancellation(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:        "task-heartbeat-failure",
		WorkflowName:  "deploy",
		Status:        config.StatusRunning,
		RunGeneration: 3,
		RunToken:      "token-3",
		WorkerID:      "worker-a",
	}
	store := &controllerTestStore{task: cloneWorkflowQueue(task)}
	ctl := newTestWorkflowController(t, task, nil, store)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(repository.ErrWorkflowLeaseRenewalFailed)
	ctl.ctx = ctx
	ctl.setStatus(config.StatusCancelled)

	ctl.updateWorkflowTask()

	store.mu.Lock()
	persistedStatus := store.task.Status
	store.mu.Unlock()
	require.Equal(t, config.StatusRunning, persistedStatus)
	stopped, persistenceErr := ctl.workflowRunStopResult()
	require.True(t, stopped)
	require.Error(t, persistenceErr)
	require.True(t, errors.Is(persistenceErr, errWorkflowTaskPersistenceUncertain))
}

func TestWorkflowTaskHeartbeatCancelsWithLeaseFailureCause(t *testing.T) {
	store := &controllerTestStore{
		task: &model.WorkflowQueue{
			TaskID:        "task-heartbeat-error",
			Status:        config.StatusRunning,
			RunGeneration: 4,
			RunToken:      "token-4",
			WorkerID:      "worker-a",
		},
		compareAndSwapWithConditionsErr: errors.New("datastore unavailable"),
	}
	cfg := &config.Config{}
	cfg.Workflow.HeartbeatInterval = time.Millisecond
	cfg.Workflow.LeaseDuration = time.Second
	workflow := &Workflow{Store: store, Cfg: cfg}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	done := workflow.startWorkflowTaskHeartbeat(ctx, cancel, store.task)

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("workflow heartbeat did not cancel execution")
	}
	require.ErrorIs(t, context.Cause(ctx), repository.ErrWorkflowLeaseRenewalFailed)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workflow heartbeat did not stop")
	}
}

func TestWorkflowTaskHeartbeatTreatsOwnedUserCancellationAsOrdinaryCancellation(t *testing.T) {
	claimed := &model.WorkflowQueue{
		TaskID:        "task-user-cancel",
		Status:        config.StatusRunning,
		RunGeneration: 5,
		RunToken:      "token-5",
		WorkerID:      "worker-a",
	}
	store := &controllerTestStore{
		task: cloneWorkflowQueue(claimed),
		beforeCompareHook: func(task *model.WorkflowQueue) {
			task.Status = config.StatusCancelled
			task.CancelSource = config.CancelSourceUser
		},
	}
	cfg := &config.Config{}
	cfg.Workflow.HeartbeatInterval = time.Millisecond
	cfg.Workflow.LeaseDuration = time.Second
	workflow := &Workflow{Store: store, Cfg: cfg}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	done := workflow.startWorkflowTaskHeartbeat(ctx, cancel, claimed)

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("workflow heartbeat did not observe user cancellation")
	}
	require.ErrorIs(t, context.Cause(ctx), context.Canceled)
	require.NotErrorIs(t, context.Cause(ctx), repository.ErrWorkflowOwnershipLost)
	require.NotErrorIs(t, context.Cause(ctx), repository.ErrWorkflowLeaseRenewalFailed)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workflow heartbeat did not stop")
	}
}

func TestWorkflowTaskHeartbeatStopsWithoutCancellingOwnedTerminalExecution(t *testing.T) {
	claimed := &model.WorkflowQueue{
		TaskID:        "task-terminal-callback",
		Status:        config.StatusRunning,
		RunGeneration: 6,
		RunToken:      "token-6",
		WorkerID:      "worker-a",
	}
	store := &controllerTestStore{
		task: cloneWorkflowQueue(claimed),
		beforeCompareHook: func(task *model.WorkflowQueue) {
			task.Status = config.StatusCompleted
		},
	}
	cfg := &config.Config{}
	cfg.Workflow.HeartbeatInterval = time.Millisecond
	cfg.Workflow.LeaseDuration = time.Second
	workflow := &Workflow{Store: store, Cfg: cfg}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	done := workflow.startWorkflowTaskHeartbeat(ctx, cancel, claimed)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workflow heartbeat did not stop after terminal persistence")
	}
	require.NoError(t, context.Cause(ctx))
}

func TestWorkflowTaskHeartbeatReportsOwnershipChangeAfterRenewalMiss(t *testing.T) {
	claimed := &model.WorkflowQueue{
		TaskID:        "task-owner-changed",
		Status:        config.StatusRunning,
		RunGeneration: 6,
		RunToken:      "token-6",
		WorkerID:      "worker-a",
	}
	store := &controllerTestStore{
		task: cloneWorkflowQueue(claimed),
		beforeCompareHook: func(task *model.WorkflowQueue) {
			task.RunGeneration = 7
			task.RunToken = "token-7"
			task.WorkerID = "worker-b"
		},
	}
	cfg := &config.Config{}
	cfg.Workflow.HeartbeatInterval = time.Millisecond
	cfg.Workflow.LeaseDuration = time.Second
	workflow := &Workflow{Store: store, Cfg: cfg}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	done := workflow.startWorkflowTaskHeartbeat(ctx, cancel, claimed)

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("workflow heartbeat did not observe ownership change")
	}
	require.ErrorIs(t, context.Cause(ctx), repository.ErrWorkflowOwnershipLost)
	require.NotErrorIs(t, context.Cause(ctx), repository.ErrWorkflowLeaseRenewalFailed)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workflow heartbeat did not stop")
	}
}
