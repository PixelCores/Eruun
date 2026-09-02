package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestWorkflowRepositoryMethods(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.Workflow{ID: "wf-1", AppID: "app-1"},
			&model.ApplicationComponent{ID: 1},
		},
	}
	repo := &workflowRepository{Store: store}

	_, err := repo.FindByID(context.Background(), "wf-1")
	require.NoError(t, err)

	require.NoError(t, repo.Create(context.Background(), &model.Workflow{ID: "wf-2"}))
	require.NoError(t, repo.Update(context.Background(), &model.Workflow{ID: "wf-2"}))
	require.NoError(t, repo.Delete(context.Background(), &model.Workflow{ID: "wf-2"}))

	list, err := repo.FindByAppID(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "wf-1", list[0].ID)
}

func TestDelWorkflowsByAppID(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.Workflow{ID: "wf-1", AppID: "app-1"},
			&model.Workflow{ID: "wf-2", AppID: "app-1"},
		},
	}
	require.NoError(t, DelWorkflowsByAppID(context.Background(), store, "app-1"))
	require.IsType(t, &model.Workflow{}, store.deleteEntity)
}

func TestDelWorkflowsByAppIDSkipsNotFound(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.Workflow{ID: "wf-1", AppID: "app-1"},
		},
		deleteErr: datastore.ErrRecordNotExist,
	}
	require.NoError(t, DelWorkflowsByAppID(context.Background(), store, "app-1"))
}

func TestComponentRepositoryMethods(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.ApplicationComponent{ID: 1, AppID: "app-1", Name: "web"},
			&model.Workflow{ID: "wf-invalid"},
		},
	}
	repo := &componentRepository{Store: store}

	require.NoError(t, repo.Create(context.Background(), &model.ApplicationComponent{ID: 2, AppID: "app-1"}))
	require.NoError(t, repo.Update(context.Background(), &model.ApplicationComponent{ID: 2, AppID: "app-1"}))
	require.NoError(t, repo.Delete(context.Background(), &model.ApplicationComponent{ID: 2, AppID: "app-1"}))

	require.NoError(t, repo.BatchAdd(context.Background(), []*model.ApplicationComponent{
		{ID: 3, AppID: "app-1"},
		{ID: 4, AppID: "app-1"},
	}))

	list, err := repo.FindByAppID(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, list, 1)

	comp, err := repo.FindByName(context.Background(), "app-1", "web")
	require.NoError(t, err)
	require.Equal(t, "web", comp.Name)

	_, err = repo.FindByName(context.Background(), "app-1", "not-found")
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}

func TestUpdateComponentRuntimeFieldsWritesZeroValues(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}

	err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
		ID:    7,
		AppID: "app-1",
		Name:  "web",
	}, map[string]interface{}{
		"status":         string(config.ComponentStatusNotDeploy),
		"ready_replicas": int32(0),
		"last_abnormal":  "",
	})

	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"app_id": "app-1",
		"id":     7,
	}, store.casConditions)
	require.Equal(t, map[string]interface{}{
		"status":         string(config.ComponentStatusNotDeploy),
		"ready_replicas": int32(0),
		"last_abnormal":  "",
	}, store.casUpdates)
}

func TestUpdateComponentRuntimeFieldsUsesNameConditionWithoutID(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}

	err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
		AppID: "app-1",
		Name:  "worker",
	}, map[string]interface{}{
		"status": string(config.ComponentStatusPending),
	})

	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"app_id": "app-1",
		"name":   "worker",
	}, store.casConditions)
	require.Equal(t, map[string]interface{}{
		"status": string(config.ComponentStatusPending),
	}, store.casUpdates)
}

func TestUpdateComponentRuntimeFieldsTreatsNoChangeAsSuccessWhenComponentExists(t *testing.T) {
	store := &repositoryTestStore{
		casWithConditionsSwapped: false,
		isExistByCondValue:       true,
	}

	err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
		ID:    7,
		AppID: "app-1",
		Name:  "web",
	}, map[string]interface{}{
		"status": string(config.ComponentStatusStopped),
	})

	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{
		"app_id": "app-1",
		"id":     7,
	}, store.casConditions)
	require.Equal(t, store.casConditions, store.isExistByCondCond)
	require.Equal(t, (&model.ApplicationComponent{}).TableName(), store.isExistByCondTable)
	require.IsType(t, &model.ApplicationComponent{}, store.isExistByCondDest)
}

func TestUpdateComponentRuntimeFieldsReturnsNotFoundWhenNoChangeAndComponentMissing(t *testing.T) {
	store := &repositoryTestStore{
		casWithConditionsSwapped: false,
		isExistByCondValue:       false,
	}

	err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
		AppID: "app-1",
		Name:  "web",
	}, map[string]interface{}{
		"status": string(config.ComponentStatusPending),
	})

	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	require.Equal(t, map[string]interface{}{
		"app_id": "app-1",
		"name":   "web",
	}, store.isExistByCondCond)
}

func TestUpdateComponentRuntimeFieldsPropagatesExistenceCheckError(t *testing.T) {
	existenceErr := errors.New("exists check failed")
	store := &repositoryTestStore{
		casWithConditionsSwapped: false,
		isExistByCondErr:         existenceErr,
	}

	err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
		ID:    7,
		AppID: "app-1",
		Name:  "web",
	}, map[string]interface{}{
		"status": string(config.ComponentStatusPending),
	})

	require.ErrorIs(t, err, existenceErr)
}

func TestUpdateComponentRuntimeFieldsRejectsUnknownField(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}

	err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
		ID:    7,
		AppID: "app-1",
		Name:  "web",
	}, map[string]interface{}{
		"replicas": int32(0),
	})

	require.ErrorContains(t, err, "unsupported component runtime field")
	require.Nil(t, store.casUpdates)
	require.Nil(t, store.casConditions)
}

func TestUpdateComponentRuntimeFieldsRejectsNilRuntimeValues(t *testing.T) {
	for _, field := range []string{"status", "ready_replicas", "last_abnormal"} {
		t.Run(field, func(t *testing.T) {
			store := &repositoryTestStore{casWithConditionsSwapped: true}
			err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
				ID:    7,
				AppID: "app-1",
				Name:  "web",
			}, map[string]interface{}{field: nil})

			require.EqualError(t, err, fmt.Sprintf("component runtime field %q cannot be nil", field))
			require.Nil(t, store.casConditions)
			require.Nil(t, store.casUpdates)
		})
	}
}

func TestUpdateComponentRuntimeFieldsNoopWhenPatchEmpty(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}

	err := UpdateComponentRuntimeFields(context.Background(), store, &model.ApplicationComponent{
		ID:    7,
		AppID: "app-1",
		Name:  "web",
	}, map[string]interface{}{})

	require.NoError(t, err)
	require.Nil(t, store.casUpdates)
	require.Nil(t, store.casConditions)
}

func TestUpdateComponentRuntimeFieldsIfUnchangedUsesOnlyRuntimeSnapshotConditions(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}
	snapshotTime := time.Date(2026, time.July, 17, 10, 0, 0, 123000000, time.UTC)
	component := &model.ApplicationComponent{
		ID:            7,
		AppID:         "app-1",
		Name:          "web",
		Status:        string(config.ComponentStatusRunning),
		ReadyReplicas: 1,
		LastAbnormal:  "",
		BaseModel: model.BaseModel{
			UpdateTime: snapshotTime,
		},
	}

	updated, err := UpdateComponentRuntimeFieldsIfUnchanged(context.Background(), store, component, map[string]interface{}{
		"status":         string(config.ComponentStatusPending),
		"ready_replicas": int32(0),
		"last_abnormal":  "waiting for pods",
	})

	require.NoError(t, err)
	require.True(t, updated)
	require.Equal(t, map[string]interface{}{
		"app_id":         "app-1",
		"id":             7,
		"status":         string(config.ComponentStatusRunning),
		"ready_replicas": int32(1),
		"last_abnormal":  "",
	}, store.casConditions)
	require.NotContains(t, store.casConditions, "update_time")
	require.Equal(t, map[string]interface{}{
		"status":         string(config.ComponentStatusPending),
		"ready_replicas": int32(0),
		"last_abnormal":  "waiting for pods",
	}, store.casUpdates)
}

func TestUpdateComponentRuntimeFieldsIfUnchangedTreatsNoChangeAsConflictWhenComponentExists(t *testing.T) {
	store := &repositoryTestStore{
		casWithConditionsSwapped: false,
		isExistByCondValue:       true,
	}
	component := &model.ApplicationComponent{
		ID:            7,
		AppID:         "app-1",
		Name:          "web",
		Status:        string(config.ComponentStatusRunning),
		ReadyReplicas: 1,
	}

	updated, err := UpdateComponentRuntimeFieldsIfUnchanged(context.Background(), store, component, map[string]interface{}{
		"status": string(config.ComponentStatusPending),
	})

	require.NoError(t, err)
	require.False(t, updated)
	require.Equal(t, map[string]interface{}{
		"app_id": "app-1",
		"id":     7,
	}, store.isExistByCondCond)
	require.Equal(t, string(config.ComponentStatusRunning), store.casConditions["status"])
	require.Equal(t, int32(1), store.casConditions["ready_replicas"])
	require.Equal(t, "", store.casConditions["last_abnormal"])
	require.NotContains(t, store.casConditions, "update_time")
}

func TestUpdateComponentRuntimeFieldsIfUnchangedReturnsNotFoundWhenComponentMissing(t *testing.T) {
	store := &repositoryTestStore{
		casWithConditionsSwapped: false,
		isExistByCondValue:       false,
	}

	updated, err := UpdateComponentRuntimeFieldsIfUnchanged(context.Background(), store, &model.ApplicationComponent{
		AppID: "app-1",
		Name:  "worker",
	}, map[string]interface{}{
		"status": string(config.ComponentStatusPending),
	})

	require.False(t, updated)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	require.Equal(t, map[string]interface{}{
		"app_id": "app-1",
		"name":   "worker",
	}, store.isExistByCondCond)
}

func TestUpdateComponentRuntimeFieldsIfUnchangedNoopWhenPatchEmpty(t *testing.T) {
	store := &repositoryTestStore{casWithConditionsSwapped: true}

	updated, err := UpdateComponentRuntimeFieldsIfUnchanged(context.Background(), store, &model.ApplicationComponent{
		ID:    7,
		AppID: "app-1",
		Name:  "web",
	}, map[string]interface{}{})

	require.NoError(t, err)
	require.False(t, updated)
	require.Nil(t, store.casUpdates)
	require.Nil(t, store.casConditions)
}

func TestDelComponentsByAppIDSkipsNotFound(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.ApplicationComponent{ID: 1, AppID: "app-1", Name: "web"},
		},
		deleteErr: datastore.ErrRecordNotExist,
	}
	require.NoError(t, DelComponentsByAppID(context.Background(), store, "app-1"))
}

func TestWorkflowQueueRepositoryMethods(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.WorkflowQueue{TaskID: "task-1", Status: config.StatusRunning},
		},
		casSwapped: true,
	}
	repo := &workflowQueueRepository{Store: store}

	require.NoError(t, repo.Create(context.Background(), &model.WorkflowQueue{TaskID: "task-1"}))
	require.NoError(t, repo.Update(context.Background(), &model.WorkflowQueue{TaskID: "task-1"}))
	_, err := repo.FindByID(context.Background(), "task-1")
	require.NoError(t, err)
	_, err = repo.FindWaiting(context.Background())
	require.NoError(t, err)
	_, err = repo.FindRunning(context.Background())
	require.NoError(t, err)
	updated, err := repo.UpdateStatus(context.Background(), "task-1", config.StatusRunning, config.StatusCompleted)
	require.NoError(t, err)
	require.True(t, updated)
}

func TestTaskAndDeleteHelpers(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.WorkflowQueue{TaskID: "task-1"},
			&model.Workflow{ID: "wf-invalid"},
		},
		casSwapped: true,
	}

	_, err := FindWorkflowTasksByAppID(context.Background(), store, "app-1")
	require.NoError(t, err)

	require.NoError(t, DeleteWorkflowTasksByAppID(context.Background(), store, ""))
	require.NoError(t, DeleteWorkflowTasksByAppID(context.Background(), store, "app-1"))
	require.IsType(t, &model.WorkflowQueue{}, store.lastDeleteByFilterEntity)

	require.NoError(t, DeleteJobInfosByAppID(context.Background(), store, "app-1"))
	require.IsType(t, &model.JobInfo{}, store.lastDeleteByFilterEntity)
	require.NotNil(t, store.lastDeleteByFilterOpts)
	require.Len(t, store.lastDeleteByFilterOpts.In, 1)

	_, err = TaskByID(context.Background(), store, "task-1")
	require.NoError(t, err)
}

func TestUpdateTaskStatusFallbackPath(t *testing.T) {
	store := &repositoryTestStore{}
	updated, err := UpdateTaskStatus(context.Background(), store, "task-1", "", config.StatusCompleted)
	require.NoError(t, err)
	require.True(t, updated)
	require.IsType(t, &model.WorkflowQueue{}, store.putEntity)
}

func TestUpdateTaskStatusFallbackNotFound(t *testing.T) {
	store := &repositoryTestStore{getErr: datastore.ErrRecordNotExist}
	updated, err := UpdateTaskStatus(context.Background(), store, "task-1", "", config.StatusCompleted)
	require.NoError(t, err)
	require.False(t, updated)
}

func TestUpdateTaskStatusWithCondition(t *testing.T) {
	store := &repositoryTestStore{casSwapped: true}
	updated, err := UpdateTaskStatus(context.Background(), store, "task-1", config.StatusRunning, config.StatusCompleted)
	require.NoError(t, err)
	require.True(t, updated)
	require.Equal(t, "status", store.casField)
	require.Equal(t, config.StatusRunning, store.casValue)
}

func TestApproveTaskCAS(t *testing.T) {
	t.Run("with conditional CAS", func(t *testing.T) {
		store := &repositoryTestStore{casWithConditionsSwapped: true}
		updated, err := ApproveTaskCAS(context.Background(), store, "task-1", ApproveTaskCondition{
			ApprovalPending:     true,
			Status:              config.StatusWaitingApprove,
			CurrentStep:         2,
			PendingApprovalStep: "approve",
		}, map[string]interface{}{"status": config.StatusQueued})
		require.NoError(t, err)
		require.True(t, updated)
		require.Equal(t, config.StatusWaitingApprove, store.casConditions["status"])
	})

	t.Run("fallback to single-field CAS", func(t *testing.T) {
		baseStore := &repositoryTestStore{casSwapped: true}
		store := &repositoryTestStoreNoConditional{
			DataStore: baseStore,
			store:     baseStore,
		}
		updated, err := ApproveTaskCAS(context.Background(), store, "task-1", ApproveTaskCondition{
			Status: config.StatusWaitingApprove,
		}, map[string]interface{}{"status": config.StatusQueued})
		require.NoError(t, err)
		require.True(t, updated)
		require.Equal(t, "status", store.store.casField)
	})
}

type repositoryTestStoreNoConditional struct {
	datastore.DataStore
	store *repositoryTestStore
}
