package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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

	t.Run("legacy execution remains compatible", func(t *testing.T) {
		store := &repositoryTestStore{}
		persisted := false

		err := WithWorkflowTaskOwnership(context.Background(), store, &model.WorkflowQueue{}, func(datastore.DataStore) error {
			persisted = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, persisted)
	})
}
