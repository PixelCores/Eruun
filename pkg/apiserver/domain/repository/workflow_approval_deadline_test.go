package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestPersistApprovalCheckpointRecordsDatabaseDeadline(t *testing.T) {
	now := time.Unix(1700000200, 0).UTC()
	store := &repositoryTestStore{casWithConditionsSwapped: true, databaseNow: now}
	owner := &model.WorkflowQueue{TaskID: "task-1", RunGeneration: 3, RunToken: "token-3", WorkerID: "worker-a"}

	persisted, err := PersistApprovalCheckpoint(context.Background(), store, owner, "manual-check", 2, time.Minute)

	require.NoError(t, err)
	require.True(t, persisted)
	require.Equal(t, map[string]interface{}{
		"status": config.StatusRunning, "current_step": 2, "run_generation": uint64(3),
		"run_token": "token-3", "worker_id": "worker-a",
	}, store.casConditions)
	require.Equal(t, map[string]interface{}{
		"status": config.StatusWaitingApprove, "current_step": 2, "approval_pending": true,
		"pending_approval_step": "manual-check", "approval_deadline": now.Add(time.Minute),
	}, store.casUpdates)
}

func TestPersistApprovalCheckpointWithoutTimeoutClearsDeadline(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}
	owner := &model.WorkflowQueue{TaskID: "task-1", RunGeneration: 3, RunToken: "token-3", WorkerID: "worker-a"}

	_, err := PersistApprovalCheckpoint(context.Background(), store, owner, "manual-check", 0, 0)

	require.NoError(t, err)
	require.Zero(t, store.databaseNowCalls)
	require.Contains(t, store.casUpdates, "approval_deadline")
	require.Nil(t, store.casUpdates["approval_deadline"])
}

func TestApprovalDeadlineFailsFastWithoutDatabaseClock(t *testing.T) {
	owner := &model.WorkflowQueue{TaskID: "task-1", RunGeneration: 3, RunToken: "token-3", WorkerID: "worker-a"}

	_, err := PersistApprovalCheckpoint(context.Background(), &repositoryTestStoreNoConditional{}, owner, "manual-check", 0, time.Minute)
	require.ErrorIs(t, err, ErrWorkflowClockUnsupported)
	_, err = ExpireApprovalDeadlines(context.Background(), &repositoryTestStoreNoConditional{}, 10)
	require.ErrorIs(t, err, ErrWorkflowClockUnsupported)

	clockErr := errors.New("clock unavailable")
	store := &repositoryTestStore{casWithConditionsSwapped: true, databaseNowErr: clockErr}
	_, err = PersistApprovalCheckpoint(context.Background(), store, owner, "manual-check", 0, time.Minute)
	require.ErrorIs(t, err, clockErr)
	_, err = ExpireApprovalDeadlines(context.Background(), store, 10)
	require.ErrorIs(t, err, clockErr)
	require.Nil(t, store.casConditions)
	require.Nil(t, store.lastListOpts)
}

func approvalDeadlineTestTask(t *testing.T, store datastore.DataStore, taskID string, deadline time.Time) *model.WorkflowQueue {
	t.Helper()
	task := &model.WorkflowQueue{
		TaskID: taskID, Status: config.StatusWaitingApprove, CurrentStep: 1,
		ApprovalPending: true, PendingApprovalStep: "manual-check", ApprovalDeadline: &deadline,
		RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-gone",
	}
	require.NoError(t, store.Add(context.Background(), task))
	return task
}

func TestExpireApprovalDeadlinesTimesOutOnlyExpiredCheckpoints(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	now, err := currentWorkflowDatabaseTime(ctx, store)
	require.NoError(t, err)
	approvalDeadlineTestTask(t, store, "expired", now.Add(-time.Minute))
	approvalDeadlineTestTask(t, store, "future", now.Add(time.Hour))

	expired, err := ExpireApprovalDeadlines(ctx, store, 10)

	require.NoError(t, err)
	require.Len(t, expired, 1)
	require.Equal(t, "expired", expired[0].TaskID)
	require.Equal(t, "manual-check", expired[0].PendingApprovalStep)
	latest, err := TaskByID(ctx, store, "expired")
	require.NoError(t, err)
	require.Equal(t, config.StatusTimeout, latest.Status)
	require.False(t, latest.ApprovalPending)
	require.Empty(t, latest.PendingApprovalStep)
	require.Empty(t, latest.RunToken)
	require.Empty(t, latest.WorkerID)
	require.Equal(t, uint64(2), latest.RunGeneration)
	future, err := TaskByID(ctx, store, "future")
	require.NoError(t, err)
	require.Equal(t, config.StatusWaitingApprove, future.Status)

	again, err := ExpireApprovalDeadlines(ctx, store, 10)
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestApprovalActionAndDeadlineExpiryRaceHasOneWinner(t *testing.T) {
	ctx := context.Background()
	continueUpdates := map[string]interface{}{
		"approval_pending": false, "pending_approval_step": "", "status": config.StatusWaiting, "current_step": 2,
	}
	condition := ApproveTaskCondition{
		ApprovalPending: true, Status: config.StatusWaitingApprove, CurrentStep: 1, PendingApprovalStep: "manual-check",
	}

	t.Run("approval first", func(t *testing.T) {
		store := newJobSchedulerTestStore(t)
		now, err := currentWorkflowDatabaseTime(ctx, store)
		require.NoError(t, err)
		approvalDeadlineTestTask(t, store, "task", now.Add(-time.Minute))

		approved, err := ApproveTaskCAS(ctx, store, "task", condition, continueUpdates)
		require.NoError(t, err)
		require.True(t, approved)
		expired, err := ExpireApprovalDeadlines(ctx, store, 10)
		require.NoError(t, err)
		require.Empty(t, expired)

		latest, err := TaskByID(ctx, store, "task")
		require.NoError(t, err)
		require.Equal(t, config.StatusWaiting, latest.Status)
		require.Equal(t, 2, latest.CurrentStep)
	})

	t.Run("expiry first", func(t *testing.T) {
		store := newJobSchedulerTestStore(t)
		now, err := currentWorkflowDatabaseTime(ctx, store)
		require.NoError(t, err)
		approvalDeadlineTestTask(t, store, "task", now.Add(-time.Minute))

		expired, err := ExpireApprovalDeadlines(ctx, store, 10)
		require.NoError(t, err)
		require.Len(t, expired, 1)
		approved, err := ApproveTaskCAS(ctx, store, "task", condition, continueUpdates)
		require.NoError(t, err)
		require.False(t, approved)

		latest, err := TaskByID(ctx, store, "task")
		require.NoError(t, err)
		require.Equal(t, config.StatusTimeout, latest.Status)
	})
}

func TestExpireApprovalDeadlinesLosesToNewerCheckpoint(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	now, err := currentWorkflowDatabaseTime(ctx, store)
	require.NoError(t, err)
	stale := approvalDeadlineTestTask(t, store, "task", now.Add(-time.Minute))
	newer := now.Add(time.Hour)
	moved, err := UpdateTaskFieldsIfConditions(ctx, store, "task", map[string]interface{}{
		"status": config.StatusWaitingApprove,
	}, map[string]interface{}{"current_step": 3, "approval_deadline": newer})
	require.NoError(t, err)
	require.True(t, moved)

	// The listed snapshot is stale: its step and deadline no longer match.
	updated, err := compareWorkflowTaskWithConditions(ctx, store, "task", map[string]interface{}{
		"status": config.StatusWaitingApprove, "approval_pending": true, "current_step": stale.CurrentStep,
		"pending_approval_step": stale.PendingApprovalStep, "run_generation": stale.RunGeneration,
		"approval_deadline": *stale.ApprovalDeadline,
	}, map[string]interface{}{"status": config.StatusTimeout})
	require.NoError(t, err)
	require.False(t, updated)
	expired, err := ExpireApprovalDeadlines(ctx, store, 10)
	require.NoError(t, err)
	require.Empty(t, expired)
}
