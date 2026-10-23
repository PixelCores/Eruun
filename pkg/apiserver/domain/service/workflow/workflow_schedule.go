package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

var workflowScheduleParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
)

var errScheduleNotClaimed = errors.New("schedule not claimed")

func (w *workflowServiceImpl) UpsertWorkflowSchedule(ctx context.Context, appID string, req apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	// Request checks run before the lock, so an invalid request stays a 400
	// under lock contention.
	if !spec.ValidAPIName(req.WorkflowID, datastore.PrimaryKeyMaxLength) {
		return nil, bcode.ErrWorkflowConfig
	}
	normalizedCron, err := utils.NormalizeCronSchedule(req.Cron)
	if err != nil {
		return nil, bcode.ErrWorkflowConfig
	}
	req.Cron = normalizedCron
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return nil, err
	}
	var resp *apis.UpsertWorkflowScheduleResponse
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, appID, "upsert-workflow-schedule", false, func(lockCtx context.Context) error {
		var lockErr error
		resp, lockErr = w.upsertWorkflowScheduleUnlocked(lockCtx, appID, req)
		return lockErr
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// upsertWorkflowScheduleUnlocked expects the request checked and the cron
// normalized by UpsertWorkflowSchedule.
func (w *workflowServiceImpl) upsertWorkflowScheduleUnlocked(ctx context.Context, appID string, req apis.UpsertWorkflowScheduleRequest) (*apis.UpsertWorkflowScheduleResponse, error) {
	if _, err := repository.ApplicationByID(ctx, w.Store, appID); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	workflow, err := repository.WorkflowByID(ctx, w.Store, req.WorkflowID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrWorkflowNotExist
		}
		return nil, err
	}
	if workflow.AppID == "" || workflow.AppID != appID {
		return nil, bcode.ErrWorkflowNotExist
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	var nextRun int64
	if enabled {
		nextRun, err = NextScheduleRun(req.Cron, time.Now().UTC())
		if err != nil {
			return nil, bcode.ErrWorkflowConfig
		}
	}

	schedule, err := repository.FindWorkflowScheduleByAppAndWorkflowID(ctx, w.Store, appID, req.WorkflowID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	if schedule == nil {
		schedule = &model.WorkflowSchedule{
			ID:         utils.RandStringByNumLowercase(24),
			AppID:      appID,
			WorkflowID: req.WorkflowID,
			Cron:       req.Cron,
			Enabled:    enabled,
			NextRun:    nextRun,
			LastRun:    0,
		}
		if err := repository.CreateWorkflowSchedule(ctx, w.Store, schedule); err != nil {
			return nil, err
		}
	} else {
		schedule.Cron = req.Cron
		schedule.Enabled = enabled
		if enabled {
			schedule.NextRun = nextRun
		} else {
			schedule.NextRun = 0
		}
		if err := repository.UpdateWorkflowSchedule(ctx, w.Store, schedule); err != nil {
			return nil, err
		}
	}

	dto := workflowScheduleToDTO(schedule, workflow)
	return &apis.UpsertWorkflowScheduleResponse{Schedule: dto}, nil
}

func (w *workflowServiceImpl) ListWorkflowSchedules(ctx context.Context, appID string) ([]apis.WorkflowSchedule, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if _, err := repository.ApplicationByID(ctx, w.Store, appID); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}
	schedules, err := repository.FindWorkflowSchedulesByAppID(ctx, w.Store, appID)
	if err != nil {
		return nil, err
	}
	if len(schedules) == 0 {
		return []apis.WorkflowSchedule{}, nil
	}
	workflows, err := repository.FindWorkflowsByAppID(ctx, w.Store, appID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	workflowByID := make(map[string]*model.Workflow, len(workflows))
	for _, wf := range workflows {
		if wf == nil {
			continue
		}
		workflowByID[wf.ID] = wf
	}
	resp := make([]apis.WorkflowSchedule, 0, len(schedules))
	for _, schedule := range schedules {
		if schedule == nil {
			continue
		}
		resp = append(resp, workflowScheduleToDTO(schedule, workflowByID[schedule.WorkflowID]))
	}
	return resp, nil
}

func (w *workflowServiceImpl) DeleteWorkflowSchedule(ctx context.Context, appID, workflowID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return bcode.ErrApplicationNotExist
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return err
	}
	return schedulelock.WithAppScheduleLock(ctx, lockProvider, appID, "delete-workflow-schedule", false, func(lockCtx context.Context) error {
		return w.deleteWorkflowScheduleUnlocked(lockCtx, appID, workflowID)
	})
}

func (w *workflowServiceImpl) deleteWorkflowScheduleUnlocked(ctx context.Context, appID, workflowID string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return bcode.ErrApplicationNotExist
	}
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		return bcode.ErrWorkflowConfig
	}
	schedule, err := repository.FindWorkflowScheduleByAppAndWorkflowID(ctx, w.Store, appID, workflowID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return bcode.ErrWorkflowNotExist
		}
		return err
	}
	return repository.DeleteWorkflowSchedule(ctx, w.Store, schedule)
}

func (w *workflowServiceImpl) DispatchWorkflowSchedules(ctx context.Context) (int, error) {
	w.scheduleDispatchMu.Lock()
	defer w.scheduleDispatchMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	txStore, ok := w.Store.(datastore.Transactional)
	if !ok {
		return 0, fmt.Errorf("workflow schedule dispatch requires transactional datastore")
	}
	if w.scheduleDispatchPage == 0 {
		w.scheduleDispatchPage = 1
	}
	now := time.Now().UTC()
	schedules, err := repository.FindDueWorkflowSchedules(ctx, w.Store, now.Unix(), w.scheduleDispatchPage)
	if err != nil {
		return 0, err
	}
	if len(schedules) == 0 {
		w.scheduleDispatchPage = 1
		return 0, nil
	}
	processed := 0
	var dispatchErrs []error
	for _, schedule := range schedules {
		queued, err := w.dispatchWorkflowSchedule(ctx, txStore, schedule, now)
		if err != nil {
			if queued {
				processed++
			}
			scheduleID := "<nil>"
			if schedule != nil {
				scheduleID = schedule.ID
			}
			scheduleErr := fmt.Errorf("dispatch workflow schedule %s: %w", scheduleID, err)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return processed, scheduleErr
			}
			dispatchErrs = append(dispatchErrs, scheduleErr)
			continue
		}
		if queued {
			processed++
		}
	}
	// Advance even when a full batch failed or was skipped. Successful dispatches
	// can shrink the candidate set; wrapping at the end revisits any rows shifted
	// into earlier pages, without changing their next_run or idempotency keys.
	if len(schedules) == repository.WorkflowScheduleDispatchQueryBatchSize {
		w.scheduleDispatchPage++
	} else {
		w.scheduleDispatchPage = 1
	}
	if len(dispatchErrs) > 0 {
		return processed, errors.Join(dispatchErrs...)
	}
	return processed, nil
}

func (w *workflowServiceImpl) dispatchWorkflowSchedule(ctx context.Context, txStore datastore.Transactional, schedule *model.WorkflowSchedule, now time.Time) (bool, error) {
	if schedule == nil || !schedule.Enabled {
		return false, nil
	}
	if schedule.Cron == "" {
		return false, nil
	}
	nowUnix := now.Unix()
	if schedule.NextRun <= 0 {
		nextRun, err := NextScheduleRun(schedule.Cron, now)
		if err != nil {
			return false, err
		}
		err = txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
			claimed, err := repository.UpdateWorkflowScheduleNextRun(ctx, tx, schedule.ID, schedule.NextRun, nextRun)
			if err != nil {
				return err
			}
			if !claimed {
				return errScheduleNotClaimed
			}
			return nil
		})
		if errors.Is(err, errScheduleNotClaimed) {
			return false, nil
		}
		return false, err
	}
	if schedule.NextRun > nowUnix {
		return false, nil
	}
	claimedNextRun := schedule.NextRun
	nextRun, err := NextScheduleRun(schedule.Cron, now)
	if err != nil {
		return false, err
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return false, err
	}

	queued := false
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, schedule.AppID, "dispatch-workflow-schedule", true, func(lockCtx context.Context) error {
		return repository.WithApplicationSchedulingTransaction(lockCtx, w.Store, schedule.AppID, func(tx datastore.DataStore) error {
			claimed, err := repository.UpdateWorkflowScheduleNextRun(lockCtx, tx, schedule.ID, schedule.NextRun, nextRun)
			if err != nil {
				return err
			}
			if !claimed {
				return errScheduleNotClaimed
			}
			if err := EnsureAppWorkflowIdle(lockCtx, tx, schedule.AppID); err != nil {
				if errors.Is(err, bcode.ErrWorkflowTaskRunning) || errors.Is(err, bcode.ErrWorkflowTaskCancelling) {
					return nil
				}
				return err
			}
			if err := EnsureNoPendingStatefulSetCleanup(lockCtx, tx, schedule.AppID); err != nil {
				if errors.Is(err, bcode.ErrApplicationConfig) ||
					errors.Is(err, bcode.ErrWorkflowTaskRunning) ||
					errors.Is(err, bcode.ErrWorkflowTaskCancelling) {
					return nil
				}
				return err
			}
			workflow, err := repository.WorkflowByID(lockCtx, tx, schedule.WorkflowID)
			if err != nil {
				return err
			}
			idempotencyKey := workflowScheduleIdempotencyKey(schedule.ID, claimedNextRun)
			if _, err := w.enqueueWorkflowTaskWithStoreAndIdempotencyKey(lockCtx, tx, workflow, 0, idempotencyKey); err != nil {
				return err
			}
			if err := repository.UpdateWorkflowScheduleLastRun(lockCtx, tx, schedule.ID, nowUnix); err != nil {
				return err
			}
			queued = true
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, errScheduleNotClaimed) || errors.Is(err, bcode.ErrApplicationOperationLocked) {
			return false, nil
		}
		return false, err
	}
	return queued, nil
}

// NextScheduleRun calculates the next five-field cron occurrence in UTC.
func NextScheduleRun(spec string, base time.Time) (int64, error) {
	next, err := workflowScheduleParser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return next.Next(base.UTC()).Unix(), nil
}

func workflowScheduleIdempotencyKey(scheduleID string, nextRun int64) string {
	return fmt.Sprintf("workflow-schedule:%s:%d", strings.TrimSpace(scheduleID), nextRun)
}

func workflowScheduleToDTO(schedule *model.WorkflowSchedule, workflow *model.Workflow) apis.WorkflowSchedule {
	dto := apis.WorkflowSchedule{
		ID:         schedule.ID,
		AppID:      schedule.AppID,
		WorkflowID: schedule.WorkflowID,
		Cron:       schedule.Cron,
		Enabled:    schedule.Enabled,
		NextRun:    schedule.NextRun,
		LastRun:    schedule.LastRun,
		CreateTime: schedule.CreateTime,
		UpdateTime: schedule.UpdateTime,
	}
	if workflow != nil {
		dto.WorkflowName = workflow.Name
		dto.WorkflowAlias = workflow.Alias
	}
	return dto
}
