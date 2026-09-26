package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/cancelsignal"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
)

func (c *applicationsServiceImpl) DeleteApplicationCascade(ctx context.Context, appID string, req apisv1.DeleteApplicationRequest) (*apisv1.DeleteApplicationResponse, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if req.WaitSeconds != nil && *req.WaitSeconds < 0 {
		return nil, bcode.ErrApplicationConfig
	}
	if c.Store == nil {
		return nil, fmt.Errorf("datastore is nil")
	}
	if c.AppRepo == nil {
		return nil, fmt.Errorf("app repository is nil")
	}

	app, err := c.AppRepo.FindByID(ctx, appID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrApplicationNotExist
		}
		return nil, err
	}

	txStore, err := c.transactionalDeleteStore()
	if err != nil {
		return nil, err
	}

	waitSeconds := config.DefaultDeleteApplicationWaitSeconds
	if req.WaitSeconds != nil {
		waitSeconds = *req.WaitSeconds
	}

	lockProvider, err := c.appScheduleLocker()
	if err != nil {
		return nil, err
	}

	var resp *apisv1.DeleteApplicationResponse
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, app.ID, "delete-application", true, func(lockCtx context.Context) error {
		lockCtx = context.WithValue(lockCtx, applicationMutationLockContextKey{}, app.ID)
		lockedApp, err := c.AppRepo.FindByID(lockCtx, app.ID)
		if err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return bcode.ErrApplicationNotExist
			}
			return err
		}
		app = lockedApp
		counts, err := c.countApplicationMetadata(lockCtx, app.ID)
		if err != nil {
			return err
		}

		resp = &apisv1.DeleteApplicationResponse{
			AppID:             app.ID,
			DeletedCounts:     counts,
			ResourcesRetained: app.EffectiveManagementMode() != domainspec.ManagementModeNative,
		}

		nativeApplication := app.EffectiveManagementMode() == domainspec.ManagementModeNative
		if nativeApplication {
			if err := c.deleteApplicationSchedulesTx(lockCtx, txStore, app.ID); err != nil {
				return err
			}
		}

		cancelReason := fmt.Sprintf("application %s is being deleted", app.ID)
		cancelledTaskIDs, activeTaskIDs, warnings, err := c.cancelAndWaitAppTasks(lockCtx, app.ID, waitSeconds, cancelReason, "pre-cleanup")
		if err != nil {
			return err
		}
		resp.CancelledTaskIDs = append(resp.CancelledTaskIDs, cancelledTaskIDs...)
		resp.ActiveTaskIDs = append(resp.ActiveTaskIDs, activeTaskIDs...)
		resp.Warnings = append(resp.Warnings, warnings...)

		if len(activeTaskIDs) > 0 {
			return fmt.Errorf("refusing to detach %s application while workflow tasks remain active: %s",
				app.EffectiveManagementMode(), strings.Join(uniqueStrings(activeTaskIDs), ","))
		}

		if nativeApplication {
			// Cascading deletion is the terminal application override: it already owns
			// the application lock and has cancelled in-flight tasks, so it intentionally
			// bypasses the idle/pending migration guards without acquiring a nested lock.
			cleanupResp, cleanupErr := c.cleanupApplicationResourcesUnlocked(lockCtx, app.ID, true)
			if cleanupResp != nil {
				resp.DeletedResources = append(resp.DeletedResources, cleanupResp.DeletedResources...)
				resp.FailedResources = append(resp.FailedResources, cleanupResp.FailedResources...)
			}
			if cleanupErr != nil {
				resp.Warnings = append(resp.Warnings, fmt.Sprintf("cleanup resources reported failures: %v", cleanupErr))
			}
		} else if err := c.deleteApplicationSchedulesTx(lockCtx, txStore, app.ID); err != nil {
			return err
		}

		cancelledTaskIDs, activeTaskIDs, warnings, err = c.cancelAndWaitAppTasks(lockCtx, app.ID, waitSeconds, cancelReason, "post-cleanup")
		if err != nil {
			return err
		}
		resp.CancelledTaskIDs = append(resp.CancelledTaskIDs, cancelledTaskIDs...)
		resp.ActiveTaskIDs = append(resp.ActiveTaskIDs, activeTaskIDs...)
		resp.Warnings = append(resp.Warnings, warnings...)
		if len(activeTaskIDs) > 0 {
			return fmt.Errorf("refusing to detach %s application while workflow tasks remain active: %s",
				app.EffectiveManagementMode(), strings.Join(uniqueStrings(activeTaskIDs), ","))
		}

		if err := c.deleteApplicationMetadataTx(lockCtx, txStore, app); err != nil {
			return err
		}

		resp.CancelledTaskIDs = uniqueStrings(resp.CancelledTaskIDs)
		resp.ActiveTaskIDs = uniqueStrings(resp.ActiveTaskIDs)
		resp.DeletedResources = uniqueStrings(resp.DeletedResources)
		resp.FailedResources = uniqueStrings(resp.FailedResources)
		resp.Warnings = uniqueStrings(resp.Warnings)
		hasIncompleteCleanup := len(resp.Warnings) > 0 || len(resp.FailedResources) > 0 || len(resp.ActiveTaskIDs) > 0

		c.invalidateApplicationListCaches(ctx)
		c.invalidateApplicationComponentsCache(ctx, app.ID)

		if hasIncompleteCleanup {
			return fmt.Errorf("application deleted with warnings")
		}
		return nil
	})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *applicationsServiceImpl) filterActiveTasks(ctx context.Context, tasks []*model.WorkflowQueue) ([]*model.WorkflowQueue, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	now := time.Now().Unix()
	active := make([]*model.WorkflowQueue, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		status := task.Status
		if status == "" || isWorkflowActiveStatus(status) {
			active = append(active, task)
			continue
		}
		if status == config.StatusWaiting && task.ExecuteAt > now {
			active = append(active, task)
			continue
		}
		if status != config.StatusCancelled {
			continue
		}
		if wf.IsTerminalCallbackPending(task.SchedulingReason) {
			active = append(active, task)
			continue
		}
		hasJobs, err := taskHasActiveJobs(ctx, c.Store, task.TaskID)
		if err != nil {
			return nil, err
		}
		if hasJobs {
			active = append(active, task)
		}
	}
	return active, nil
}

func (c *applicationsServiceImpl) cancelTaskForAppDelete(ctx context.Context, task *model.WorkflowQueue, reason string) error {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return nil
	}
	redisClient, err := cancelsignal.RedisClientForCancelSignal(ctx, c.RedisClient)
	if err != nil {
		return err
	}
	callbackPending, err := c.cancelledTaskCallbackConfigured(ctx, task)
	if err != nil {
		return err
	}
	callbackState := wf.TerminalCallbackReconciledReason
	if callbackPending {
		callbackState = wf.TerminalCallbackPendingReason(reason)
	}
	current := *task
	for attempt := 0; attempt < 3; attempt++ {
		if current.Status == config.StatusCancelled {
			cancelReason := strings.TrimSpace(reason)
			if wf.IsTerminalCallbackPending(current.SchedulingReason) {
				cancelReason = wf.TerminalCallbackReason(current.SchedulingReason)
			}
			*task = current
			return cancelsignal.PublishWorkflowCancelSignal(ctx, task.TaskID, cancelReason, redisClient)
		}
		if current.Status != "" && !isWorkflowActiveStatus(current.Status) {
			return nil
		}
		conditions := map[string]interface{}{
			"status": current.Status, "run_generation": current.RunGeneration,
			"run_token": current.RunToken, "worker_id": current.WorkerID,
		}
		updates := map[string]interface{}{
			"task_revoker": config.DefaultTaskRevoker, "status": config.StatusCancelled,
			"cancel_source": config.CancelSourceSystem, "approval_pending": false,
			"pending_approval_step": "", "scheduling_reason": callbackState,
		}
		transactional, ok := c.Store.(datastore.Transactional)
		if !ok {
			return fmt.Errorf("cancel application task: datastore does not support transactions")
		}
		updated := false
		err := transactional.WithTransaction(ctx, func(tx datastore.DataStore) error {
			var updateErr error
			updated, updateErr = repository.UpdateTaskFieldsIfConditions(ctx, tx, current.TaskID, conditions, updates)
			if updateErr != nil || !updated {
				return updateErr
			}
			return terminalizeUnclaimedAppDeleteTaskJobs(ctx, tx, &current, reason)
		})
		if err != nil {
			return err
		}
		if updated {
			current.TaskRevoker = config.DefaultTaskRevoker
			current.Status = config.StatusCancelled
			current.CancelSource = config.CancelSourceSystem
			current.ApprovalPending = false
			current.PendingApprovalStep = ""
			current.SchedulingReason = callbackState
			*task = current
			return cancelsignal.PublishWorkflowCancelSignal(ctx, task.TaskID, reason, redisClient)
		}
		latest, err := repository.TaskByID(ctx, c.Store, current.TaskID)
		if err != nil {
			return err
		}
		current = *latest
	}
	return repository.ErrWorkflowOwnershipLost
}

func terminalizeUnclaimedAppDeleteTaskJobs(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue, reason string) error {
	if task == nil || task.Status != config.StatusWaiting || task.DispatchAttempts != 0 || task.RunToken != "" || task.WorkerID != "" {
		return nil
	}
	return repository.TerminalizeCancelledWorkflowJobs(
		ctx, store, task.TaskID, strings.TrimSpace(reason),
		workflowjob.TerminalCallbackExecutionKey(task.TaskID, task.RunGeneration, "cancelled"),
	)
}

func (c *applicationsServiceImpl) cancelledTaskCallbackConfigured(ctx context.Context, task *model.WorkflowQueue) (bool, error) {
	if task == nil {
		return false, nil
	}
	var workflow *model.Workflow
	if task.WorkflowID != "" {
		workflow = &model.Workflow{ID: task.WorkflowID}
		if err := c.Store.Get(ctx, workflow); err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
			return false, fmt.Errorf("load workflow cancellation callback: %w", err)
		}
	}
	var app *model.Applications
	if task.AppID != "" {
		app = &model.Applications{ID: task.AppID}
		if err := c.Store.Get(ctx, app); err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
			return false, fmt.Errorf("load application cancellation callback: %w", err)
		}
	}
	source := model.WorkflowCallbackSource(task, workflow, app)
	if source == nil {
		return false, nil
	}
	data, err := json.Marshal(source)
	if err != nil {
		return false, err
	}
	var callback model.WorkflowCallback
	if err := json.Unmarshal(data, &callback); err != nil {
		return false, err
	}
	return strings.TrimSpace(callback.Cancelled) != "" || strings.TrimSpace(callback.Failure) != "", nil
}

func (c *applicationsServiceImpl) waitForAppTasksTermination(ctx context.Context, appID string, wait time.Duration) ([]string, error) {
	remaining, err := c.activeTaskIDs(ctx, appID)
	if err != nil || len(remaining) == 0 || wait <= 0 {
		return remaining, err
	}

	timer := time.NewTimer(wait)
	ticker := time.NewTicker(config.DeleteApplicationTaskPollInterval)
	defer timer.Stop()
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return remaining, ctx.Err()
		case <-timer.C:
			return remaining, nil
		case <-ticker.C:
			remaining, err = c.activeTaskIDs(ctx, appID)
			if err != nil || len(remaining) == 0 {
				return remaining, err
			}
		}
	}
}

func (c *applicationsServiceImpl) activeTaskIDs(ctx context.Context, appID string) ([]string, error) {
	tasks, err := repository.FindWorkflowTasksByAppID(ctx, c.Store, appID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	active, err := c.filterActiveTasks(ctx, tasks)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(active))
	for _, task := range active {
		if task == nil || strings.TrimSpace(task.TaskID) == "" {
			continue
		}
		ids = append(ids, task.TaskID)
	}
	return uniqueStrings(ids), nil
}

func (c *applicationsServiceImpl) cancelAndWaitAppTasks(ctx context.Context, appID string, waitSeconds int64, cancelReason, phase string) ([]string, []string, []string, error) {
	tasks, err := repository.FindWorkflowTasksByAppID(ctx, c.Store, appID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, nil, nil, err
	}

	activeTasks, err := c.filterActiveTasks(ctx, tasks)
	if err != nil {
		return nil, nil, nil, err
	}

	cancelledTaskIDs := make([]string, 0, len(activeTasks))
	warnings := make([]string, 0)
	for _, task := range activeTasks {
		if task == nil || strings.TrimSpace(task.TaskID) == "" {
			continue
		}
		if err := c.cancelTaskForAppDelete(ctx, task, cancelReason); err != nil {
			warnings = append(warnings, fmt.Sprintf("cancel task %s failed during %s: %v", task.TaskID, phase, err))
			continue
		}
		cancelledTaskIDs = append(cancelledTaskIDs, task.TaskID)
	}

	remainingTaskIDs, err := c.waitForAppTasksTermination(ctx, appID, time.Duration(waitSeconds)*time.Second)
	if err != nil {
		return cancelledTaskIDs, nil, warnings, err
	}
	if len(remainingTaskIDs) > 0 {
		warnings = append(warnings,
			fmt.Sprintf("active workflow tasks remain after %s wait %ds: %s", phase, waitSeconds, strings.Join(remainingTaskIDs, ",")))
	}

	return cancelledTaskIDs, remainingTaskIDs, warnings, nil
}

func (c *applicationsServiceImpl) transactionalDeleteStore() (datastore.Transactional, error) {
	txStore, ok := c.Store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf("datastore does not support transactional delete")
	}
	return txStore, nil
}

func (c *applicationsServiceImpl) countApplicationMetadata(ctx context.Context, appID string) (apisv1.DeleteResourceCount, error) {
	var counts apisv1.DeleteResourceCount
	if strings.TrimSpace(appID) == "" {
		return counts, bcode.ErrApplicationNotExist
	}

	scheduleCount, err := c.Store.Count(ctx, &model.WorkflowSchedule{AppID: appID}, nil)
	if err != nil {
		return counts, err
	}
	workflowCount, err := c.Store.Count(ctx, &model.Workflow{AppID: appID}, nil)
	if err != nil {
		return counts, err
	}
	componentCount, err := c.Store.Count(ctx, &model.ApplicationComponent{AppID: appID}, nil)
	if err != nil {
		return counts, err
	}
	taskCount, err := c.Store.Count(ctx, &model.WorkflowQueue{AppID: appID}, nil)
	if err != nil {
		return counts, err
	}
	jobCount, err := c.Store.Count(ctx, &model.JobInfo{}, &datastore.FilterOptions{
		In: []datastore.InQueryOption{
			{Key: "app_id", Values: []string{appID}},
		},
	})
	if err != nil {
		return counts, err
	}

	counts = apisv1.DeleteResourceCount{
		Schedules:  scheduleCount,
		Workflows:  workflowCount,
		Components: componentCount,
		Tasks:      taskCount,
		Jobs:       jobCount,
		Apps:       1,
	}
	return counts, nil
}

func (c *applicationsServiceImpl) deleteApplicationSchedulesTx(ctx context.Context, txStore datastore.Transactional, appID string) error {
	if strings.TrimSpace(appID) == "" {
		return bcode.ErrApplicationNotExist
	}
	if txStore == nil {
		return fmt.Errorf("transactional datastore is nil")
	}
	return txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
		return repository.DeleteWorkflowSchedulesByAppID(ctx, tx, appID)
	})
}

func (c *applicationsServiceImpl) deleteApplicationMetadataTx(ctx context.Context, txStore datastore.Transactional, app *model.Applications) error {
	if app == nil || strings.TrimSpace(app.ID) == "" {
		return bcode.ErrApplicationNotExist
	}
	if txStore == nil {
		return fmt.Errorf("transactional datastore is nil")
	}

	return txStore.WithTransaction(ctx, func(tx datastore.DataStore) error {
		if err := repository.DeleteWorkflowSchedulesByAppID(ctx, tx, app.ID); err != nil {
			return err
		}
		if err := repository.DelWorkflowsByAppID(ctx, tx, app.ID); err != nil {
			return err
		}
		if err := repository.DelComponentsByAppID(ctx, tx, app.ID); err != nil {
			return err
		}
		if err := repository.DeleteWorkflowTasksByAppID(ctx, tx, app.ID); err != nil {
			return err
		}
		if err := repository.DeleteJobInfosByAppID(ctx, tx, app.ID); err != nil {
			return err
		}
		if err := tx.Delete(ctx, app); err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
			return err
		}
		return nil
	})
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
