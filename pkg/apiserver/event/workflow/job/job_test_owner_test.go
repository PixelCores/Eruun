package job

import (
	"context"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// withJobTestOwner supplies the claimed parent row for tests concerned with a
// job's business behavior. Ownership-transfer tests use their own mutable rows.
// All business writes and injected failures remain delegated to the original store.
func withJobTestOwner(store datastore.DataStore, tasks ...*model.JobTask) datastore.DataStore {
	owners := make(map[string]model.WorkflowQueue, len(tasks))
	for _, task := range tasks {
		if task.TaskID == "" {
			task.TaskID = "test-workflow"
		}
		if task.OwnerRunGeneration == 0 {
			task.OwnerRunGeneration = task.RunGeneration
			if task.OwnerRunGeneration == 0 {
				task.OwnerRunGeneration = 1
			}
		}
		if task.RunToken == "" {
			task.RunToken = "test-run-token"
		}
		if task.WorkerID == "" {
			task.WorkerID = "test-worker"
		}
		if task.OwnerStatus == "" {
			task.OwnerStatus = config.StatusRunning
		}
		owners[task.TaskID] = model.WorkflowQueue{TaskID: task.TaskID, Status: task.OwnerStatus,
			RunGeneration: task.OwnerRunGeneration, RunToken: task.RunToken, WorkerID: task.WorkerID}
	}
	return &jobTestOwnerStore{DataStore: store, owners: owners}
}

type jobTestOwnerStore struct {
	datastore.DataStore
	owners map[string]model.WorkflowQueue
}

func (s *jobTestOwnerStore) Get(ctx context.Context, entity datastore.Entity) error {
	if row, ok := entity.(*model.WorkflowQueue); ok {
		owner, exists := s.owners[row.TaskID]
		if !exists {
			return datastore.ErrRecordNotExist
		}
		*row = owner
		return nil
	}
	return s.DataStore.Get(ctx, entity)
}

func (s *jobTestOwnerStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	if tx, ok := s.DataStore.(datastore.Transactional); ok {
		return tx.WithTransaction(ctx, func(store datastore.DataStore) error {
			return fn(&jobTestOwnerStore{DataStore: store, owners: s.owners})
		})
	}
	return fn(s)
}

func (s *jobTestOwnerStore) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	if clock, ok := s.DataStore.(datastore.DatabaseClock); ok {
		return clock.CurrentDatabaseTime(ctx)
	}
	return time.Now().UTC(), nil
}

func (s *jobTestOwnerStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if row, ok := entity.(*model.WorkflowQueue); ok {
		owner, exists := s.owners[row.TaskID]
		if !exists || conditions["status"] != owner.Status || conditions["run_generation"] != owner.RunGeneration ||
			conditions["run_token"] != owner.RunToken || conditions["worker_id"] != owner.WorkerID {
			return false, nil
		}
		if len(updates) != 0 {
			return false, fmt.Errorf("test owner only supports ownership checks")
		}
		return true, nil
	}
	if conditional, ok := s.DataStore.(datastore.ConditionalCompareAndSwap); ok {
		return conditional.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
	}
	return false, fmt.Errorf("test business store does not support conditional compare-and-swap")
}
