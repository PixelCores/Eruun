package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type workflowExecutionClaimStore struct {
	*repositoryTestStore
	reloadedTask model.WorkflowQueue
}

func (s *workflowExecutionClaimStore) Get(_ context.Context, entity datastore.Entity) error {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok || task == nil {
		return datastore.ErrEntityInvalid
	}
	*task = s.reloadedTask
	return nil
}

func TestClaimWorkflowTaskForDispatchCreatesGenerationAndLease(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}
	task := &model.WorkflowQueue{TaskID: "task-1", Status: config.StatusWaiting, RunGeneration: 4, DispatchAttempts: 2}

	claimed, updated, err := ClaimWorkflowTaskForDispatch(context.Background(), store, task, 30*time.Second)

	require.NoError(t, err)
	require.True(t, updated)
	require.Equal(t, uint64(5), claimed.RunGeneration)
	require.NotEmpty(t, claimed.RunToken)
	require.NotNil(t, claimed.LeaseExpiresAt)
	require.Equal(t, uint(3), claimed.DispatchAttempts)
	require.Equal(t, map[string]interface{}{
		"status": config.StatusWaiting, "run_generation": uint64(4),
	}, store.casConditions)
}

func TestWorkflowLeaseRejectsIncompleteIdentity(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}

	_, _, err := ClaimWorkflowTaskForDispatch(context.Background(), store, &model.WorkflowQueue{TaskID: "   "}, 30*time.Second)
	require.ErrorIs(t, err, datastore.ErrPrimaryEmpty)

	_, _, err = ClaimWorkflowTaskForExecution(context.Background(), store, "task-1", 1, "token-1", "   ", 30*time.Second)
	require.ErrorContains(t, err, "worker identity")

	_, err = RenewWorkflowTaskLease(context.Background(), store, "task-1", 1, "   ", "worker-1", 30*time.Second)
	require.ErrorContains(t, err, "execution identity")

	_, err = ExpireWorkflowTaskLease(context.Background(), store, "task-1", 0, "token-1", "worker-1")
	require.ErrorContains(t, err, "execution identity")
}

func TestRenewWorkflowTaskLeaseUsesFullOwnershipFence(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}
	renewed, err := RenewWorkflowTaskLease(context.Background(), store, "task-1", 7, "token-7", "worker-a", 30*time.Second)

	require.NoError(t, err)
	require.True(t, renewed)
	require.Equal(t, config.StatusRunning, store.casConditions["status"])
	require.Equal(t, uint64(7), store.casConditions["run_generation"])
	require.Equal(t, "token-7", store.casConditions["run_token"])
	require.Equal(t, "worker-a", store.casConditions["worker_id"])
}

func TestRecoverExpiredWorkflowTasksFencesExactGeneration(t *testing.T) {
	expired := time.Now().Add(-time.Second)
	future := time.Now().Add(time.Minute)
	now := time.Now()
	store := &repositoryTestStore{
		casWithConditionsSwapped: true,
		listEntities: []datastore.Entity{
			&model.WorkflowQueue{
				TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 9,
				RunToken: "token-9", WorkerID: "worker-a", LeaseExpiresAt: &expired,
			},
			&model.WorkflowQueue{
				TaskID: "task-future", Status: config.StatusRunning, RunGeneration: 10,
				RunToken: "token-10", WorkerID: "worker-b", LeaseExpiresAt: &future,
			},
		},
	}

	recovered, err := RecoverExpiredWorkflowTasks(context.Background(), store, now)

	require.NoError(t, err)
	require.Equal(t, 1, recovered)
	require.Equal(t, []datastore.InQueryOption{{
		Key: "status", Values: []string{string(config.StatusQueued), string(config.StatusRunning)},
	}}, store.lastListOpts.FilterOptions.In)
	require.Equal(t, []datastore.ComparisonQueryOption{{Key: "run_token", Value: ""}}, store.lastListOpts.FilterOptions.NotEqual)
	require.Equal(t, []datastore.ComparisonQueryOption{{Key: "lease_expires_at", Value: now}}, store.lastListOpts.FilterOptions.LessThan)
	require.Equal(t, 1, store.lastListOpts.Page)
	require.Equal(t, workflowLeaseRecoveryBatchSize, store.lastListOpts.PageSize)
	require.Equal(t, []datastore.SortOption{{
		Key: "lease_expires_at", Order: datastore.SortOrderAscending,
	}}, store.lastListOpts.SortBy)
	require.Equal(t, uint64(9), store.casConditions["run_generation"])
	require.Equal(t, "token-9", store.casConditions["run_token"])
	require.Equal(t, expired, store.casConditions["lease_expires_at"])
	require.Equal(t, config.StatusWaiting, store.casUpdates["status"])
	require.Nil(t, store.casUpdates["lease_expires_at"])
}

type countingWorkflowLeaseStore struct {
	*repositoryTestStore
	compareAndSwapCalls int
}

func (s *countingWorkflowLeaseStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	s.compareAndSwapCalls++
	return s.repositoryTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func TestRecoverExpiredWorkflowTasksBoundsOneReaperBatch(t *testing.T) {
	now := time.Now()
	expired := now.Add(-time.Minute)
	entities := make([]datastore.Entity, workflowLeaseRecoveryBatchSize+1)
	for i := range entities {
		entities[i] = &model.WorkflowQueue{
			TaskID: fmt.Sprintf("task-%03d", i), Status: config.StatusRunning,
			RunGeneration: 1, RunToken: "token", LeaseExpiresAt: &expired,
		}
	}
	store := &countingWorkflowLeaseStore{repositoryTestStore: &repositoryTestStore{
		casWithConditionsSwapped: true,
		listEntities:             entities,
	}}

	recovered, err := RecoverExpiredWorkflowTasks(context.Background(), store, now)

	require.NoError(t, err)
	require.Equal(t, workflowLeaseRecoveryBatchSize, recovered)
	require.Equal(t, workflowLeaseRecoveryBatchSize, store.compareAndSwapCalls)
}

func TestOwnedUpdateRejectsStoreWithoutConditionalCAS(t *testing.T) {
	store := &repositoryTestStoreNoConditional{}
	updated, err := UpdateTaskFieldsIfOwned(context.Background(), store, &model.WorkflowQueue{
		TaskID: "task-1", RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-2",
	}, config.StatusRunning, map[string]interface{}{"status": config.StatusCompleted})

	require.False(t, updated)
	require.ErrorIs(t, err, ErrWorkflowFencingUnsupported)
}

func TestOwnedUpdateRejectsIncompleteOwnership(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}
	for _, task := range []*model.WorkflowQueue{
		{TaskID: "task-1", RunToken: "token-1", WorkerID: "worker-1"},
		{TaskID: "task-1", RunGeneration: 1, WorkerID: "worker-1"},
		{TaskID: "task-1", RunGeneration: 1, RunToken: "token-1"},
	} {
		updated, err := UpdateTaskFieldsIfOwned(context.Background(), store, task, config.StatusRunning, map[string]interface{}{"status": config.StatusCompleted})
		require.False(t, updated)
		require.ErrorIs(t, err, ErrWorkflowOwnershipRequired)
	}
}

func TestWorkflowLeaseOperationsRejectIncompleteOwnership(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}

	claimed, updated, err := ClaimWorkflowTaskForExecution(context.Background(), store, "task-1", 0, "token-1", "worker-1", 30*time.Second)
	require.Nil(t, claimed)
	require.False(t, updated)
	require.ErrorIs(t, err, ErrWorkflowOwnershipRequired)

	renewed, err := RenewWorkflowTaskLease(context.Background(), store, "task-1", 1, "", "worker-1", 30*time.Second)
	require.False(t, renewed)
	require.ErrorIs(t, err, ErrWorkflowOwnershipRequired)

	released, err := ReleaseWorkflowDispatchClaim(context.Background(), store, &model.WorkflowQueue{TaskID: "task-1"}, "retry")
	require.False(t, released)
	require.ErrorIs(t, err, ErrWorkflowOwnershipRequired)
}
