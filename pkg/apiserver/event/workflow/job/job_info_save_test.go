package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type jobInfoSaveStore struct {
	noopStore
	existing  []*model.JobInfo
	added     *model.JobInfo
	updated   *model.JobInfo
	beforeCAS func()
}

func (s *jobInfoSaveStore) Add(_ context.Context, entity datastore.Entity) error {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return datastore.ErrEntityInvalid
	}
	copy := *jobInfo
	s.added = &copy
	s.existing = append(s.existing, &copy)
	return nil
}

func (s *jobInfoSaveStore) Put(_ context.Context, entity datastore.Entity) error {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return datastore.ErrEntityInvalid
	}
	copy := *jobInfo
	s.updated = &copy
	return nil
}

func (s *jobInfoSaveStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	entities := make([]datastore.Entity, 0, len(s.existing))
	for _, jobInfo := range s.existing {
		copy := *jobInfo
		entities = append(entities, &copy)
	}
	return entities, nil
}

func (s *jobInfoSaveStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return false, datastore.ErrEntityInvalid
	}
	if s.beforeCAS != nil {
		hook := s.beforeCAS
		s.beforeCAS = nil
		hook()
	}
	for _, current := range s.existing {
		if current == nil || current.ID != jobInfo.ID {
			continue
		}
		if current.Status != conditions["status"] ||
			jobInfoExecutionKey(*current) != conditions["execution_key"] ||
			current.RunGeneration != conditions["run_generation"] ||
			current.Attempt != conditions["attempt"] {
			return false, nil
		}
		current.Status, _ = updates["status"].(string)
		current.Error, _ = updates["error"].(string)
		current.EndTime, _ = updates["end_time"].(int64)
		copy := *current
		s.updated = &copy
		return true, nil
	}
	return false, nil
}

func TestSaveJobInfoUpdatesRecoveredExecutionTerminalState(t *testing.T) {
	executionKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusFailed),
		Error:         "first execution failed",
		ExecutionKey:  &executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusCompleted,
		ExecutionKey:  executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.added)
	require.NotNil(t, store.updated)
	require.Equal(t, 7, store.updated.ID)
	require.Equal(t, string(config.StatusCompleted), store.updated.Status)
	require.Empty(t, store.updated.Error)
}

func TestSaveJobInfoKeepsDistinctExecutionsSeparate(t *testing.T) {
	existingKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusCompleted),
		ExecutionKey:  &existingKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusCompleted,
		ExecutionKey:  "execution-2",
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.updated)
	require.NotNil(t, store.added)
	require.NotNil(t, store.added.ExecutionKey)
	require.Equal(t, "execution-2", *store.added.ExecutionKey)
}

func TestSaveJobInfoPreservesSuccessfulTerminalState(t *testing.T) {
	executionKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusCompleted),
		ExecutionKey:  &executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusDistributed,
		ExecutionKey:  executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.added)
	require.Nil(t, store.updated)
	require.Equal(t, string(config.StatusCompleted), store.existing[0].Status)
}

func TestSaveJobInfoReloadsAfterConcurrentTerminalUpdate(t *testing.T) {
	executionKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusRunning),
		ExecutionKey:  &executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	store.beforeCAS = func() {
		store.existing[0].Status = string(config.StatusCompleted)
	}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusDistributed,
		ExecutionKey:  executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.added)
	require.Nil(t, store.updated)
	require.Equal(t, string(config.StatusCompleted), store.existing[0].Status)
}
