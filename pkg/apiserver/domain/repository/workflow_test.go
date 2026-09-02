package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type waitingTaskStore struct {
	lastOptions *datastore.ListOptions
}

func (w *waitingTaskStore) Add(context.Context, datastore.Entity) error        { return nil }
func (w *waitingTaskStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (w *waitingTaskStore) Put(context.Context, datastore.Entity) error        { return nil }
func (w *waitingTaskStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (w *waitingTaskStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (w *waitingTaskStore) Get(context.Context, datastore.Entity) error { return nil }
func (w *waitingTaskStore) List(ctx context.Context, query datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	w.lastOptions = options
	return []datastore.Entity{
		&model.WorkflowQueue{TaskID: "t1"},
		&model.WorkflowQueue{TaskID: "t2"},
	}, nil
}
func (w *waitingTaskStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (w *waitingTaskStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (w *waitingTaskStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (w *waitingTaskStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return true, nil
}

func TestWaitingTasksUsesFIFOSort(t *testing.T) {
	store := &waitingTaskStore{}
	list, err := WaitingTasks(context.Background(), store)
	require.NoError(t, err)
	require.Len(t, list, 2)

	if store.lastOptions == nil || len(store.lastOptions.SortBy) == 0 {
		t.Fatalf("expected sort options to be set")
	}
	require.Equal(t, "create_time", store.lastOptions.SortBy[0].Key)
	require.Equal(t, datastore.SortOrderAscending, store.lastOptions.SortBy[0].Order)
}

func TestWaitingTasksFiltersFutureExecuteAt(t *testing.T) {
	now := time.Now().Unix()
	store := &waitingTaskFilterStore{
		tasks: []datastore.Entity{
			&model.WorkflowQueue{TaskID: "t1", ExecuteAt: now - 5},
			&model.WorkflowQueue{TaskID: "t2", ExecuteAt: now + 300},
			&model.WorkflowQueue{TaskID: "t3"},
		},
	}

	list, err := WaitingTasks(context.Background(), store)
	require.NoError(t, err)
	require.Len(t, list, 2)
	require.Equal(t, "t1", list[0].TaskID)
	require.Equal(t, "t3", list[1].TaskID)
}

type waitingTaskFilterStore struct {
	tasks []datastore.Entity
}

func (w *waitingTaskFilterStore) Add(context.Context, datastore.Entity) error        { return nil }
func (w *waitingTaskFilterStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (w *waitingTaskFilterStore) Put(context.Context, datastore.Entity) error        { return nil }
func (w *waitingTaskFilterStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (w *waitingTaskFilterStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (w *waitingTaskFilterStore) Get(context.Context, datastore.Entity) error { return nil }
func (w *waitingTaskFilterStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return w.tasks, nil
}
func (w *waitingTaskFilterStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (w *waitingTaskFilterStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (w *waitingTaskFilterStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (w *waitingTaskFilterStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return true, nil
}

type updateTaskFieldStore struct {
	conditionField string
	conditionValue interface{}
	updates        map[string]interface{}
	swapped        bool
}

func (s *updateTaskFieldStore) Add(context.Context, datastore.Entity) error        { return nil }
func (s *updateTaskFieldStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *updateTaskFieldStore) Put(context.Context, datastore.Entity) error        { return nil }
func (s *updateTaskFieldStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (s *updateTaskFieldStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *updateTaskFieldStore) Get(context.Context, datastore.Entity) error { return nil }
func (s *updateTaskFieldStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}
func (s *updateTaskFieldStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (s *updateTaskFieldStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *updateTaskFieldStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *updateTaskFieldStore) CompareAndSwap(_ context.Context, _ datastore.Entity, field string, condition interface{}, updates map[string]interface{}) (bool, error) {
	s.conditionField = field
	s.conditionValue = condition
	s.updates = updates
	return s.swapped, nil
}

func TestUpdateTaskFieldsWritesZeroValuesWithMapUpdates(t *testing.T) {
	store := &updateTaskFieldStore{swapped: true}
	err := UpdateTaskFields(context.Background(), store, "task-1", map[string]interface{}{
		"approval_pending":      false,
		"pending_approval_step": "",
	})
	require.NoError(t, err)
	require.Equal(t, "task_id", store.conditionField)
	require.Equal(t, "task-1", store.conditionValue)
	require.Equal(t, false, store.updates["approval_pending"])
	require.Equal(t, "", store.updates["pending_approval_step"])
}

func TestUpdateTaskFieldsReturnsNotExistWhenCASNotSwapped(t *testing.T) {
	store := &updateTaskFieldStore{swapped: false}
	err := UpdateTaskFields(context.Background(), store, "task-404", map[string]interface{}{
		"status": "cancelled",
	})
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}

func TestUpdateTaskFieldsIfStatusUsesStatusCondition(t *testing.T) {
	store := &updateTaskFieldStore{swapped: true}
	swapped, err := UpdateTaskFieldsIfStatus(context.Background(), store, "task-1", config.StatusWaiting, map[string]interface{}{
		"status": config.StatusCancelled,
	})
	require.NoError(t, err)
	require.True(t, swapped)
	require.Equal(t, "status", store.conditionField)
	require.Equal(t, config.StatusWaiting, store.conditionValue)
	require.Equal(t, config.StatusCancelled, store.updates["status"])
}

func TestUpdateTaskFieldsIfStatusReportsCASConflict(t *testing.T) {
	store := &updateTaskFieldStore{swapped: false}
	swapped, err := UpdateTaskFieldsIfStatus(context.Background(), store, "task-1", config.StatusWaiting, map[string]interface{}{
		"status": config.StatusCancelled,
	})
	require.NoError(t, err)
	require.False(t, swapped)
}

func TestTaskByIdempotencyKey(t *testing.T) {
	key := "workflow-schedule:sch-1:100"
	store := &idempotencyTaskStore{
		tasks: []datastore.Entity{
			&model.Workflow{ID: "wf-invalid"},
			&model.WorkflowQueue{TaskID: "task-1", IdempotencyKey: &key},
		},
	}

	task, err := TaskByIdempotencyKey(context.Background(), store, key)
	require.NoError(t, err)
	require.Equal(t, "task-1", task.TaskID)

	query, ok := store.lastQuery.(*model.WorkflowQueue)
	require.True(t, ok)
	require.NotNil(t, query.IdempotencyKey)
	require.Equal(t, key, *query.IdempotencyKey)
}

func TestTaskByIdempotencyKeyRejectsEmptyKey(t *testing.T) {
	_, err := TaskByIdempotencyKey(context.Background(), &idempotencyTaskStore{}, " ")
	require.ErrorIs(t, err, datastore.ErrIndexInvalid)
}

type idempotencyTaskStore struct {
	tasks     []datastore.Entity
	lastQuery datastore.Entity
}

func (s *idempotencyTaskStore) Add(context.Context, datastore.Entity) error        { return nil }
func (s *idempotencyTaskStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *idempotencyTaskStore) Put(context.Context, datastore.Entity) error        { return nil }
func (s *idempotencyTaskStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (s *idempotencyTaskStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *idempotencyTaskStore) Get(context.Context, datastore.Entity) error { return nil }
func (s *idempotencyTaskStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	s.lastQuery = query
	return s.tasks, nil
}
func (s *idempotencyTaskStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (s *idempotencyTaskStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *idempotencyTaskStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *idempotencyTaskStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return true, nil
}

var _ datastore.DataStore = (*waitingTaskStore)(nil)
var _ datastore.DataStore = (*waitingTaskFilterStore)(nil)
var _ datastore.DataStore = (*updateTaskFieldStore)(nil)
var _ datastore.DataStore = (*idempotencyTaskStore)(nil)
