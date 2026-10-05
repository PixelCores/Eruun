package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type transactionalWorkflowOwnershipStore struct {
	*repositoryTestStore
	transactionCalls int
}

func (s *transactionalWorkflowOwnershipStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	s.transactionCalls++
	return fn(s.repositoryTestStore)
}

var _ datastore.Transactional = (*transactionalWorkflowOwnershipStore)(nil)

type readCommittedWorkflowOwnershipStore struct {
	*transactionalWorkflowOwnershipStore
	readCommittedCalls int
	readCommittedError error
}

func (s *readCommittedWorkflowOwnershipStore) WithReadCommittedTransaction(_ context.Context, fn func(datastore.DataStore) error) error {
	s.readCommittedCalls++
	if s.readCommittedError != nil {
		return s.readCommittedError
	}
	return fn(s.repositoryTestStore)
}

func TestWithWorkflowTaskOwnershipUsesReadCommittedWhenSupported(t *testing.T) {
	task := &model.WorkflowQueue{TaskID: "task", RunGeneration: 1, RunToken: "token", WorkerID: "worker"}
	store := &readCommittedWorkflowOwnershipStore{transactionalWorkflowOwnershipStore: &transactionalWorkflowOwnershipStore{repositoryTestStore: &repositoryTestStore{casWithConditionsSwapped: true}}}
	called := false
	require.NoError(t, WithWorkflowTaskOwnership(context.Background(), store, task, func(datastore.DataStore) error { called = true; return nil }))
	require.True(t, called)
	require.Equal(t, 1, store.readCommittedCalls)
	require.Zero(t, store.transactionCalls)
	store.readCommittedError = ErrWorkflowFencingUnsupported
	called = false
	require.ErrorIs(t, WithWorkflowTaskOwnership(context.Background(), store, task, func(datastore.DataStore) error { called = true; return nil }), ErrWorkflowFencingUnsupported)
	require.False(t, called)
	require.Zero(t, store.transactionCalls, "a failed production read-committed transaction must never downgrade")
}

func TestWithWorkflowTaskOwnershipFencesSideEffectWrites(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:        "task-1",
		RunGeneration: 7,
		RunToken:      "token-7",
		WorkerID:      "worker-a",
	}

	t.Run("owned execution persists in transaction", func(t *testing.T) {
		base := &repositoryTestStore{casWithConditionsSwapped: true}
		store := &transactionalWorkflowOwnershipStore{repositoryTestStore: base}
		persisted := false

		err := WithWorkflowTaskOwnership(context.Background(), store, task, func(tx datastore.DataStore) error {
			persisted = true
			return tx.Add(context.Background(), &model.JobInfo{TaskID: task.TaskID})
		})

		require.NoError(t, err)
		require.True(t, persisted)
		require.Equal(t, 1, store.transactionCalls)
		require.Equal(t, uint64(7), base.casConditions["run_generation"])
		require.Equal(t, "token-7", base.casConditions["run_token"])
		require.Equal(t, "worker-a", base.casConditions["worker_id"])
		require.IsType(t, &model.JobInfo{}, base.addEntity)
	})

	t.Run("ownership loss blocks side effect", func(t *testing.T) {
		base := &repositoryTestStore{casWithConditionsSwapped: false}
		store := &transactionalWorkflowOwnershipStore{repositoryTestStore: base}
		persisted := false

		err := WithWorkflowTaskOwnership(context.Background(), store, task, func(datastore.DataStore) error {
			persisted = true
			return nil
		})

		require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
		require.False(t, persisted)
		require.Equal(t, 1, store.transactionCalls)
	})

	t.Run("missing execution identity never persists", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			task model.WorkflowQueue
			want error
		}{
			{"empty snapshot", model.WorkflowQueue{}, datastore.ErrPrimaryEmpty},
			{"missing token", model.WorkflowQueue{TaskID: "task", RunGeneration: 1, WorkerID: "worker"}, ErrWorkflowOwnershipRequired},
			{"blank token", model.WorkflowQueue{TaskID: "task", RunGeneration: 1, RunToken: " ", WorkerID: "worker"}, ErrWorkflowOwnershipRequired},
			{"missing generation", model.WorkflowQueue{TaskID: "task", RunToken: "token", WorkerID: "worker"}, ErrWorkflowOwnershipRequired},
			{"missing worker", model.WorkflowQueue{TaskID: "task", RunGeneration: 1, RunToken: "token"}, ErrWorkflowOwnershipRequired},
			{"blank worker", model.WorkflowQueue{TaskID: "task", RunGeneration: 1, RunToken: "token", WorkerID: " "}, ErrWorkflowOwnershipRequired},
		} {
			t.Run(tt.name, func(t *testing.T) {
				store := &transactionalWorkflowOwnershipStore{repositoryTestStore: &repositoryTestStore{casWithConditionsSwapped: true}}
				persisted := false
				err := WithWorkflowTaskOwnership(context.Background(), store, &tt.task, func(datastore.DataStore) error {
					persisted = true
					return nil
				})
				require.ErrorIs(t, err, tt.want)
				require.False(t, persisted)
				require.Zero(t, store.transactionCalls)
			})
		}
	})
}

func TestWithWorkflowTaskOwnershipFencesUnclaimedTerminalCallbacks(t *testing.T) {
	for _, status := range []config.Status{config.StatusCompleted, config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject, config.StatusSkipped} {
		t.Run(string(status), func(t *testing.T) {
			task := &model.WorkflowQueue{TaskID: "unclaimed-task", Status: status}
			base := &repositoryTestStore{casWithConditionsSwapped: true}
			store := &transactionalWorkflowOwnershipStore{repositoryTestStore: base}
			persisted := false
			persist := func(datastore.DataStore) error { persisted = true; return nil }
			require.NoError(t, WithWorkflowTaskOwnership(context.Background(), store, task, persist))
			require.True(t, persisted)
			require.Equal(t, 1, store.transactionCalls)
			require.Equal(t, status, base.casConditions["status"])
			require.Equal(t, uint64(0), base.casConditions["run_generation"])
			require.Equal(t, "", base.casConditions["run_token"])
			require.Equal(t, "", base.casConditions["worker_id"])
			base.casWithConditionsSwapped = false
			persisted = false
			require.ErrorIs(t, WithWorkflowTaskOwnership(context.Background(), store, task, persist), ErrWorkflowOwnershipLost)
			require.False(t, persisted)
		})
	}
}
