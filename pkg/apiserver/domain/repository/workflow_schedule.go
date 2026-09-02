package repository

import (
	"context"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func CreateWorkflowSchedule(ctx context.Context, store datastore.DataStore, schedule *model.WorkflowSchedule) error {
	return store.Add(ctx, schedule)
}

func UpdateWorkflowSchedule(ctx context.Context, store datastore.DataStore, schedule *model.WorkflowSchedule) error {
	if schedule == nil {
		return datastore.ErrNilEntity
	}
	fields := map[string]interface{}{
		"app_id":      schedule.AppID,
		"workflow_id": schedule.WorkflowID,
		"cron":        schedule.Cron,
		"enabled":     schedule.Enabled,
		"next_run":    schedule.NextRun,
		"last_run":    schedule.LastRun,
	}
	return UpdateWorkflowScheduleFields(ctx, store, schedule.ID, fields)
}

func DeleteWorkflowSchedule(ctx context.Context, store datastore.DataStore, schedule *model.WorkflowSchedule) error {
	return store.Delete(ctx, schedule)
}

func FindWorkflowSchedulesByAppID(ctx context.Context, store datastore.DataStore, appID string) ([]*model.WorkflowSchedule, error) {
	entities, err := store.List(ctx, &model.WorkflowSchedule{AppID: appID}, &datastore.ListOptions{
		SortBy: []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderDescending}},
	})
	if err != nil {
		return nil, err
	}
	schedules := make([]*model.WorkflowSchedule, 0, len(entities))
	for _, entity := range entities {
		schedule, ok := entity.(*model.WorkflowSchedule)
		if !ok {
			klog.Warningf("unexpected workflow schedule entity type: %T", entity)
			continue
		}
		schedules = append(schedules, schedule)
	}
	return schedules, nil
}

func FindWorkflowScheduleByAppAndWorkflowID(ctx context.Context, store datastore.DataStore, appID, workflowID string) (*model.WorkflowSchedule, error) {
	entities, err := store.List(ctx, &model.WorkflowSchedule{AppID: appID, WorkflowID: workflowID}, &datastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, entity := range entities {
		schedule, ok := entity.(*model.WorkflowSchedule)
		if !ok {
			klog.Warningf("unexpected workflow schedule entity type: %T", entity)
			continue
		}
		return schedule, nil
	}
	return nil, datastore.ErrRecordNotExist
}

func FindEnabledWorkflowSchedules(ctx context.Context, store datastore.DataStore) ([]*model.WorkflowSchedule, error) {
	entities, err := store.List(ctx, &model.WorkflowSchedule{Enabled: true}, &datastore.ListOptions{
		SortBy: []datastore.SortOption{{Key: "next_run", Order: datastore.SortOrderAscending}},
	})
	if err != nil {
		return nil, err
	}
	schedules := make([]*model.WorkflowSchedule, 0, len(entities))
	for _, entity := range entities {
		schedule, ok := entity.(*model.WorkflowSchedule)
		if !ok {
			klog.Warningf("unexpected workflow schedule entity type: %T", entity)
			continue
		}
		schedules = append(schedules, schedule)
	}
	return schedules, nil
}

func UpdateWorkflowScheduleNextRun(ctx context.Context, store datastore.DataStore, scheduleID string, from, to int64) (bool, error) {
	schedule := &model.WorkflowSchedule{ID: scheduleID}
	updates := map[string]interface{}{
		"next_run": to,
	}
	updated, err := store.CompareAndSwap(ctx, schedule, "next_run", from, updates)
	if err != nil {
		return false, err
	}
	if !updated {
		return false, nil
	}
	return true, nil
}

func UpdateWorkflowScheduleFields(ctx context.Context, store datastore.DataStore, scheduleID string, fields map[string]interface{}) error {
	if scheduleID == "" {
		return datastore.ErrPrimaryEmpty
	}
	schedule := &model.WorkflowSchedule{ID: scheduleID}
	updated, err := store.CompareAndSwap(ctx, schedule, "id", scheduleID, fields)
	if err != nil {
		return err
	}
	if !updated {
		return datastore.ErrRecordNotExist
	}
	return nil
}

func UpdateWorkflowScheduleLastRun(ctx context.Context, store datastore.DataStore, scheduleID string, lastRun int64) error {
	return UpdateWorkflowScheduleFields(ctx, store, scheduleID, map[string]interface{}{
		"last_run": lastRun,
	})
}

func DeleteWorkflowSchedulesByAppID(ctx context.Context, store datastore.DataStore, appID string) error {
	if appID == "" {
		return nil
	}
	return store.DeleteByFilter(ctx, &model.WorkflowSchedule{AppID: appID}, nil)
}
