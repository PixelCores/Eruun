package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestUpdateWorkflowScheduleNilEntity(t *testing.T) {
	err := UpdateWorkflowSchedule(context.Background(), &repositoryTestStore{}, nil)
	require.ErrorIs(t, err, datastore.ErrNilEntity)
}

func TestUpdateWorkflowScheduleFieldsValidation(t *testing.T) {
	store := &repositoryTestStore{}
	err := UpdateWorkflowScheduleFields(context.Background(), store, "", map[string]interface{}{"enabled": true})
	require.ErrorIs(t, err, datastore.ErrPrimaryEmpty)
}

func TestUpdateWorkflowScheduleFieldsNotFound(t *testing.T) {
	store := &repositoryTestStore{casSwapped: false}
	err := UpdateWorkflowScheduleFields(context.Background(), store, "sch-1", map[string]interface{}{"enabled": true})
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}

func TestUpdateWorkflowScheduleFieldsCompareAndSwapError(t *testing.T) {
	expected := errors.New("cas failed")
	store := &repositoryTestStore{casErr: expected}
	err := UpdateWorkflowScheduleFields(context.Background(), store, "sch-1", map[string]interface{}{"enabled": true})
	require.ErrorIs(t, err, expected)
}

func TestFindWorkflowScheduleByAppAndWorkflowID(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.Workflow{ID: "wf-invalid"},
			&model.WorkflowSchedule{ID: "sch-1", AppID: "app-1", WorkflowID: "wf-1"},
		},
	}
	schedule, err := FindWorkflowScheduleByAppAndWorkflowID(context.Background(), store, "app-1", "wf-1")
	require.NoError(t, err)
	require.Equal(t, "sch-1", schedule.ID)
}

func TestFindDueWorkflowSchedulesUsesDispatchQueryShape(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.Workflow{ID: "wf-invalid"},
			&model.WorkflowSchedule{ID: "sch-1", Enabled: true, NextRun: 10},
		},
	}
	const nowUnix = int64(20)
	schedules, err := FindDueWorkflowSchedules(context.Background(), store, nowUnix, 2)
	require.NoError(t, err)
	require.Len(t, schedules, 1)
	require.Equal(t, "sch-1", schedules[0].ID)

	query, ok := store.lastQuery.(*model.WorkflowSchedule)
	require.True(t, ok)
	require.True(t, query.Enabled)
	require.NotNil(t, store.lastListOpts)
	require.Equal(t, 2, store.lastListOpts.Page)
	require.Equal(t, WorkflowScheduleDispatchQueryBatchSize, store.lastListOpts.PageSize)
	require.Equal(t, []datastore.ComparisonQueryOption{
		{Key: "next_run", Value: nowUnix + 1},
	}, store.lastListOpts.FilterOptions.LessThan)
	require.Equal(t, []datastore.SortOption{
		{Key: "next_run", Order: datastore.SortOrderAscending},
		{Key: "id", Order: datastore.SortOrderAscending},
	}, store.lastListOpts.SortBy)
}

func TestFindDueWorkflowSchedulesRejectsInvalidPage(t *testing.T) {
	for _, page := range []int{0, -1} {
		store := &repositoryTestStore{}
		_, err := FindDueWorkflowSchedules(context.Background(), store, 20, page)
		require.ErrorContains(t, err, "page must be positive")
		require.Nil(t, store.lastListOpts)
	}
}

func TestDeleteWorkflowSchedulesByAppID(t *testing.T) {
	store := &repositoryTestStore{}
	err := DeleteWorkflowSchedulesByAppID(context.Background(), store, "")
	require.NoError(t, err)
	require.Nil(t, store.lastDeleteByFilterEntity)

	err = DeleteWorkflowSchedulesByAppID(context.Background(), store, "app-1")
	require.NoError(t, err)
	require.IsType(t, &model.WorkflowSchedule{}, store.lastDeleteByFilterEntity)
}

func TestUpdateWorkflowScheduleNextRun(t *testing.T) {
	store := &repositoryTestStore{casSwapped: true}
	updated, err := UpdateWorkflowScheduleNextRun(context.Background(), store, "sch-1", 1, 2)
	require.NoError(t, err)
	require.True(t, updated)
	require.Equal(t, "next_run", store.casField)
	require.Equal(t, map[string]interface{}{"next_run": int64(2)}, store.casUpdates)
}
