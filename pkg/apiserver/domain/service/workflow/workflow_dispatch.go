package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// ClaimTaskForDispatch rechecks delayed application tasks under the same lock
// used by enqueue and version updates, then creates the fenced dispatch lease.
func (w *workflowServiceImpl) ClaimTaskForDispatch(ctx context.Context, task *model.WorkflowQueue, leaseDuration time.Duration) (*model.WorkflowQueue, bool, error) {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return nil, false, datastore.ErrPrimaryEmpty
	}
	// Independent Jobs have no application, so neither the application lock nor
	// the StatefulSet cleanup fence applies, including for delayed execution.
	if task.Type == config.WorkflowTaskTypeJob {
		return repository.ClaimWorkflowTaskForDispatch(ctx, w.Store, task, leaseDuration)
	}
	// Immediate and cron tasks stay active for idle checks from enqueue onward.
	// Only delayed tasks can outlive the safety decision made when enqueued.
	if task.ExecuteAt <= 0 {
		return repository.ClaimWorkflowTaskForDispatch(ctx, w.Store, task, leaseDuration)
	}
	lockProvider, err := w.appScheduleLocker()
	if err != nil {
		return nil, false, err
	}
	var queued *model.WorkflowQueue
	var failed *model.WorkflowQueue
	var claimed bool
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, task.AppID, "dispatch-workflow-task", true, func(lockCtx context.Context) error {
		current, err := repository.TaskByID(lockCtx, w.Store, task.TaskID)
		if err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return nil
			}
			return err
		}
		if current.Status != config.StatusWaiting || current.RunGeneration != task.RunGeneration {
			return nil
		}
		if current.AppID == "" || current.AppID != task.AppID {
			return fmt.Errorf("workflow task %s changed application from %q to %q", task.TaskID, task.AppID, current.AppID)
		}
		_, migrationTask, err := statefulSetCleanupInfoForFence(current)
		if err != nil {
			return err
		}
		// The v2/v3 migration itself resolves the fence. Its cleanup contract was
		// validated while the version update held this same application lock.
		if !migrationTask {
			if err := EnsureNoPendingStatefulSetCleanup(lockCtx, w.Store, current.AppID); err != nil {
				if !errors.Is(err, errPendingStatefulSetCleanup) {
					return err
				}
				reason := "delayed workflow dispatch rejected: unfinished StatefulSet cleanup; resume the migration with the version update API"
				updated, err := repository.UpdateTaskFieldsIfConditions(lockCtx, w.Store, current.TaskID, map[string]interface{}{
					"status":         config.StatusWaiting,
					"run_generation": current.RunGeneration,
				}, map[string]interface{}{
					"status":            config.StatusFailed,
					"scheduling_reason": reason,
				})
				if err != nil {
					return fmt.Errorf("fail delayed workflow task after cleanup fence: %w", err)
				}
				if updated {
					current.Status = config.StatusFailed
					current.SchedulingReason = reason
					failed = current
				}
				return nil
			}
		}
		queued, claimed, err = repository.ClaimWorkflowTaskForDispatch(lockCtx, w.Store, current, leaseDuration)
		return err
	})
	if errors.Is(err, bcode.ErrApplicationOperationLocked) {
		// Lock contention keeps the task Waiting for the next dispatch pass.
		klog.V(2).InfoS("defer delayed workflow task dispatch", "taskID", task.TaskID, "appID", task.AppID, "reason", err)
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if failed != nil {
		w.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, failed, config.StatusFailed, failed.SchedulingReason)
	}
	return queued, claimed, nil
}
