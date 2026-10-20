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
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/cancelsignal"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
)

const cancelWorkflowTaskCASMaxAttempts = 3

const (
	workflowApprovalActionContinue = "continue"
	workflowApprovalActionCancel   = "cancel"
)

func (w *workflowServiceImpl) CancelWorkflowTask(ctx context.Context, userName, taskID, reason string) error {
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		return err
	}
	return w.cancelWorkflowTask(ctx, task, userName, reason)
}

func (w *workflowServiceImpl) CancelWorkflowTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error {
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		return err
	}
	if task.AppID == "" || task.AppID != appID {
		return bcode.ErrWorkflowNotExist
	}
	return w.cancelWorkflowTask(ctx, task, userName, reason)
}

func (w *workflowServiceImpl) CancelAllWorkflowTasksForApp(ctx context.Context, appID, userName, reason string) ([]string, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	userName = strings.TrimSpace(userName)
	if userName == "" {
		userName = config.DefaultTaskRevoker
	}

	tasks, err := repository.FindWorkflowTasksByAppID(ctx, w.Store, appID)
	if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	cancelledTaskIDs := make([]string, 0, len(tasks))
	for _, task := range tasks {
		shouldCancel, err := w.shouldCancelAppTask(ctx, appID, task)
		if err != nil {
			return cancelledTaskIDs, err
		}
		if !shouldCancel {
			continue
		}
		if err := w.cancelWorkflowTask(ctx, task, userName, reason); err != nil {
			if errors.Is(err, bcode.ErrWorkflowTaskNotCancellable) {
				continue
			}
			return cancelledTaskIDs, fmt.Errorf("cancel workflow task %s: %w", task.TaskID, err)
		}
		cancelledTaskIDs = append(cancelledTaskIDs, task.TaskID)
	}
	return cancelledTaskIDs, nil
}

func (w *workflowServiceImpl) shouldCancelAppTask(ctx context.Context, appID string, task *model.WorkflowQueue) (bool, error) {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return false, nil
	}
	if task.AppID == "" || task.AppID != appID {
		return false, nil
	}
	return w.shouldCancelTask(ctx, task)
}

func (w *workflowServiceImpl) shouldCancelTask(ctx context.Context, task *model.WorkflowQueue) (bool, error) {
	return w.shouldCancelTaskWithStore(ctx, w.Store, task)
}

func (w *workflowServiceImpl) shouldCancelTaskWithStore(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue) (bool, error) {
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return false, nil
	}
	if task.Status == "" || model.IsWorkflowActiveStatus(task.Status) {
		return true, nil
	}
	if task.Status != config.StatusCancelled {
		return false, nil
	}
	return taskHasActiveJobs(ctx, store, task.TaskID)
}

func (w *workflowServiceImpl) CancelDelayedVersionTaskForApp(ctx context.Context, appID, userName, taskID, reason string) error {
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		return err
	}
	if task.AppID == "" || task.AppID != appID {
		return bcode.ErrWorkflowNotExist
	}
	now := time.Now().Unix()
	if task.Status != config.StatusWaiting || task.ExecuteAt <= now {
		return bcode.ErrVersionUpdateTaskNotCancellable
	}
	return w.cancelWorkflowTaskIfStatus(ctx, task, userName, reason, config.StatusWaiting, bcode.ErrVersionUpdateTaskNotCancellable)
}

func (w *workflowServiceImpl) ApproveWorkflowTask(ctx context.Context, taskID, action, userName, reason string) (*apis.TaskApprovalResponse, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, bcode.ErrWorkflowTaskNotExist
	}
	normalizedAction := strings.ToLower(strings.TrimSpace(action))
	if normalizedAction != workflowApprovalActionContinue && normalizedAction != workflowApprovalActionCancel {
		return nil, bcode.ErrWorkflowApprovalActionInvalid
	}
	task, err := repository.TaskByID(ctx, w.Store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrWorkflowTaskNotExist
		}
		return nil, err
	}
	if !task.ApprovalPending {
		return nil, bcode.ErrWorkflowTaskNotAwaitingApproval
	}
	if task.Status != config.StatusWaitingApprove && task.Status != config.StatusWaiting {
		return nil, bcode.ErrWorkflowTaskNotAwaitingApproval
	}
	expectedCondition := repository.ApproveTaskCondition{
		ApprovalPending:     true,
		Status:              task.Status,
		CurrentStep:         task.CurrentStep,
		PendingApprovalStep: task.PendingApprovalStep,
	}

	if normalizedAction == workflowApprovalActionContinue {
		nextStep := task.CurrentStep
		if nextStep < 0 {
			nextStep = 0
		}
		nextStep++
		updates := map[string]interface{}{
			"approval_pending":      false,
			"pending_approval_step": "",
			"status":                config.StatusWaiting,
			"current_step":          nextStep,
		}
		swapped, err := repository.ApproveTaskCAS(ctx, w.Store, taskID, expectedCondition, updates)
		if err != nil {
			return nil, err
		}
		if !swapped {
			return nil, bcode.ErrWorkflowTaskNotAwaitingApproval
		}
		return &apis.TaskApprovalResponse{
			TaskID: task.TaskID,
			Action: normalizedAction,
			Status: string(config.StatusWaiting),
		}, nil
	}

	user := strings.TrimSpace(userName)
	if user == "" {
		user = config.DefaultTaskRevoker
	}
	cancelReason := strings.TrimSpace(reason)
	if cancelReason == "" {
		cancelReason = fmt.Sprintf("cancelled by %s", user)
	}
	cancelUpdates := map[string]interface{}{
		"approval_pending":      false,
		"pending_approval_step": "",
		"status":                config.StatusCancelled,
		"task_revoker":          user,
		"cancel_source":         config.CancelSourceUser,
		"run_token":             "",
		"worker_id":             "",
		"heartbeat_at":          nil,
		"lease_expires_at":      nil,
		"scheduling_reason":     wf.TerminalCallbackPendingReason(cancelReason),
	}
	if err := runCancelledTaskStoreUpdate(ctx, w.Store, taskID, cancelReason, func(store datastore.DataStore) error {
		swapped, err := repository.ApproveTaskCAS(ctx, store, taskID, expectedCondition, cancelUpdates)
		if err != nil {
			return err
		}
		if !swapped {
			return bcode.ErrWorkflowTaskNotAwaitingApproval
		}
		return repository.TerminalizeCancelledWorkflowJobs(
			ctx, store, task.TaskID, cancelReason,
			workflowjob.TerminalCallbackExecutionKey(task.TaskID, task.RunGeneration, "cancelled"),
		)
	}); err != nil {
		return nil, err
	}
	task.Status = config.StatusCancelled
	task.ApprovalPending = false
	task.PendingApprovalStep = ""
	task.TaskRevoker = user
	task.CancelSource = config.CancelSourceUser
	task.RunToken = ""
	task.WorkerID = ""
	task.HeartbeatAt = nil
	task.LeaseExpiresAt = nil
	task.SchedulingReason = wf.TerminalCallbackPendingReason(cancelReason)
	w.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, cancelReason)
	return &apis.TaskApprovalResponse{
		TaskID: task.TaskID,
		Action: normalizedAction,
		Status: string(config.StatusCancelled),
	}, nil
}

func (w *workflowServiceImpl) cancelWorkflowTask(ctx context.Context, task *model.WorkflowQueue, userName, reason string) error {
	return w.cancelWorkflowTaskIfStatus(ctx, task, userName, reason, "", nil)
}

func (w *workflowServiceImpl) cancelWorkflowTaskIfStatus(ctx context.Context, task *model.WorkflowQueue, userName, reason string, expectedStatus config.Status, conflictErr error) error {
	// Audit log: record cancel operation with full context
	klog.Infof("AUDIT: cancel workflow task taskID=%s workflowID=%s workflowName=%s user=%s reason=%s prevStatus=%s",
		task.TaskID, task.WorkflowID, task.WorkflowName, userName, reason, task.Status)
	cancellable, err := w.shouldCancelTask(ctx, task)
	if err != nil {
		return err
	}
	if !cancellable {
		klog.Warningf("AUDIT: cancel workflow task rejected due terminal status taskID=%s user=%s status=%s", task.TaskID, userName, task.Status)
		return bcode.ErrWorkflowTaskNotCancellable
	}
	callbackWithoutWorker := false
	if expectedStatus == "" {
		callbackWithoutWorker, err = shouldTriggerTerminalCallbackWithoutWorker(ctx, w.Store, task)
		if err != nil {
			return err
		}
	}
	redisClient, cancelSignalErr := cancelsignal.RedisClientForCancelSignal(ctx, w.RedisClient)
	if cancelSignalErr != nil && !callbackWithoutWorker {
		if expectedStatus != "" && conflictErr != nil {
			current, stateErr := repository.TaskByID(ctx, w.Store, task.TaskID)
			switch {
			case errors.Is(stateErr, datastore.ErrRecordNotExist):
				klog.Warningf("AUDIT: cancel workflow task rejected because task disappeared during cancel signal preflight taskID=%s user=%s", task.TaskID, userName)
				return conflictErr
			case stateErr == nil && (current == nil || current.Status != expectedStatus):
				klog.Warningf("AUDIT: cancel workflow task rejected due status drift during cancel signal preflight taskID=%s user=%s expectedStatus=%s", task.TaskID, userName, expectedStatus)
				return conflictErr
			case stateErr != nil:
				klog.Warningf("AUDIT: cancel workflow task state recheck failed after cancel signal backend error taskID=%s user=%s error=%v", task.TaskID, userName, stateErr)
			}
		}
		klog.Errorf("AUDIT: cancel workflow task missing cancel signal backend taskID=%s user=%s error=%v", task.TaskID, userName, cancelSignalErr)
		return cancelSignalErr
	}
	if reason == "" {
		reason = fmt.Sprintf("cancelled by %s", userName)
	}
	pendingReason := wf.TerminalCallbackPendingReason(reason)
	updates := map[string]interface{}{
		"task_revoker":          userName,
		"status":                config.StatusCancelled,
		"cancel_source":         config.CancelSourceUser,
		"approval_pending":      false,
		"pending_approval_step": "",
		"scheduling_reason":     pendingReason,
	}
	cancelledWithoutWorker := callbackWithoutWorker
	if err := runCancelledTaskStoreUpdate(ctx, w.Store, task.TaskID, reason, func(store datastore.DataStore) error {
		if expectedStatus != "" {
			expectedUpdates := cloneWorkflowTaskUpdates(updates)
			if callbackWithoutWorker {
				clearWorkflowExecutionOwner(expectedUpdates)
			}
			swapped, err := repository.UpdateTaskFieldsIfStatus(ctx, store, task.TaskID, expectedStatus, expectedUpdates)
			if err != nil {
				return err
			}
			if !swapped {
				if conflictErr != nil {
					return conflictErr
				}
				return datastore.ErrRecordNotExist
			}
			if callbackWithoutWorker {
				return repository.TerminalizeCancelledWorkflowJobs(
					ctx, store, task.TaskID, reason,
					workflowjob.TerminalCallbackExecutionKey(task.TaskID, task.RunGeneration, "cancelled"),
				)
			}
			return nil
		}
		var err error
		cancelledWithoutWorker, err = w.cancelWorkflowTaskState(ctx, store, task, userName, updates, cancelSignalErr)
		return err
	}); err != nil {
		klog.Errorf("AUDIT: cancel workflow task failed taskID=%s user=%s error=%v", task.TaskID, userName, err)
		return err
	}
	callbackWithoutWorker = cancelledWithoutWorker
	task.TaskRevoker = userName
	task.Status = config.StatusCancelled
	task.CancelSource = config.CancelSourceUser
	task.ApprovalPending = false
	task.PendingApprovalStep = ""
	task.SchedulingReason = pendingReason
	if callbackWithoutWorker {
		task.RunToken = ""
		task.WorkerID = ""
		task.HeartbeatAt = nil
		task.LeaseExpiresAt = nil
	}
	if redisClient == nil {
		var err error
		redisClient, err = cancelsignal.RedisClientForCancelSignal(ctx, w.RedisClient)
		if err != nil {
			// Tasks cancelled before a worker claim are terminal in storage. A missing
			// signal backend must not suppress their only terminal callback path.
			klog.Warningf("AUDIT: signal cancel skipped for unclaimed task taskID=%s user=%s error=%v", task.TaskID, userName, err)
			if callbackWithoutWorker {
				w.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, reason)
			}
			klog.Infof("AUDIT: cancel workflow task completed taskID=%s user=%s", task.TaskID, userName)
			return nil
		}
	}
	if err := cancelsignal.PublishWorkflowCancelSignal(ctx, task.TaskID, reason, redisClient); err != nil {
		if callbackWithoutWorker {
			// Tasks cancelled before a worker claim are already terminal in storage.
			klog.Warningf("AUDIT: signal cancel skipped for unclaimed task taskID=%s user=%s error=%v", task.TaskID, userName, err)
		} else {
			klog.Errorf("AUDIT: signal cancel failed taskID=%s user=%s error=%v", task.TaskID, userName, err)
			return err
		}
	}
	if callbackWithoutWorker {
		w.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, config.StatusCancelled, reason)
	}

	klog.Infof("AUDIT: cancel workflow task completed taskID=%s user=%s", task.TaskID, userName)
	return nil
}

func (w *workflowServiceImpl) cancelWorkflowTaskState(
	ctx context.Context,
	store datastore.DataStore,
	task *model.WorkflowQueue,
	userName string,
	updates map[string]interface{},
	cancelSignalErr error,
) (bool, error) {
	current := task
	for attempt := 0; attempt < cancelWorkflowTaskCASMaxAttempts; attempt++ {
		cancellable, err := w.shouldCancelTaskWithStore(ctx, store, current)
		if err != nil {
			return false, err
		}
		if !cancellable {
			return false, bcode.ErrWorkflowTaskNotCancellable
		}
		if workflowTaskCancellationMatches(current, userName) {
			*task = *current
			return false, nil
		}

		callbackWithoutWorker, err := shouldTriggerTerminalCallbackWithoutWorker(ctx, store, current)
		if err != nil {
			return false, err
		}
		if !callbackWithoutWorker && cancelSignalErr != nil {
			return false, cancelSignalErr
		}

		attemptUpdates := cloneWorkflowTaskUpdates(updates)
		if callbackWithoutWorker {
			clearWorkflowExecutionOwner(attemptUpdates)
		}
		var swapped bool
		if current.ApprovalPending {
			expectedCondition := repository.ApproveTaskCondition{
				ApprovalPending:     true,
				Status:              current.Status,
				CurrentStep:         current.CurrentStep,
				PendingApprovalStep: current.PendingApprovalStep,
			}
			swapped, err = repository.ApproveTaskCAS(ctx, store, current.TaskID, expectedCondition, attemptUpdates)
		} else {
			swapped, err = repository.UpdateTaskFieldsIfStatus(ctx, store, current.TaskID, current.Status, attemptUpdates)
		}
		if err != nil {
			return false, err
		}
		if swapped {
			if callbackWithoutWorker {
				if err := repository.TerminalizeCancelledWorkflowJobs(
					ctx, store, current.TaskID, wf.TerminalCallbackReason(attemptUpdates["scheduling_reason"].(string)),
					workflowjob.TerminalCallbackExecutionKey(current.TaskID, current.RunGeneration, "cancelled"),
				); err != nil {
					return false, err
				}
			}
			return callbackWithoutWorker, nil
		}

		latest, err := repository.TaskByID(ctx, store, current.TaskID)
		if err != nil {
			return false, err
		}
		*task = *latest
		current = task

		cancellable, err = w.shouldCancelTaskWithStore(ctx, store, current)
		if err != nil {
			return false, err
		}
		if !cancellable {
			return false, bcode.ErrWorkflowTaskNotCancellable
		}
		if workflowTaskCancellationMatches(current, userName) {
			return false, nil
		}
		if attempt == cancelWorkflowTaskCASMaxAttempts-1 {
			return false, bcode.ErrWorkflowTaskCancelConflict
		}
	}
	return false, bcode.ErrWorkflowTaskCancelConflict
}

func clearWorkflowExecutionOwner(updates map[string]interface{}) {
	updates["run_token"] = ""
	updates["worker_id"] = ""
	updates["heartbeat_at"] = nil
	updates["lease_expires_at"] = nil
}

func cloneWorkflowTaskUpdates(updates map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(updates)+4)
	for key, value := range updates {
		cloned[key] = value
	}
	return cloned
}

func workflowTaskCancellationMatches(task *model.WorkflowQueue, userName string) bool {
	return task != nil &&
		task.Status == config.StatusCancelled &&
		task.TaskRevoker == userName &&
		task.CancelSource == config.CancelSourceUser &&
		!task.ApprovalPending &&
		task.PendingApprovalStep == ""
}

func runCancelledTaskStoreUpdate(ctx context.Context, store datastore.DataStore, taskID, reason string, updateTask func(datastore.DataStore) error) error {
	run := func(tx datastore.DataStore) error {
		if updateTask != nil {
			if err := updateTask(tx); err != nil {
				return err
			}
		}
		if err := TerminalizePrecreatedVersionUpdateCleanupJobs(ctx, tx, taskID, config.StatusCancelled, reason); err != nil {
			return fmt.Errorf("terminalize version update cleanup jobs: %w", err)
		}
		return nil
	}
	txStore, ok := store.(datastore.Transactional)
	if !ok {
		return fmt.Errorf("cancel workflow task requires transactional datastore")
	}
	return txStore.WithTransaction(ctx, run)
}

func shouldTriggerTerminalCallbackWithoutWorker(ctx context.Context, store datastore.DataStore, task *model.WorkflowQueue) (bool, error) {
	if task == nil {
		return false, nil
	}
	switch task.Status {
	case config.StatusWaitingApprove:
		return task.ApprovalPending, nil
	case config.StatusQueued:
		if task.WorkerID != "" {
			return false, nil
		}
	case config.StatusWaiting:
		if task.DispatchAttempts > 0 || task.RunToken != "" || task.WorkerID != "" {
			return false, nil
		}
	default:
		return false, nil
	}
	active, err := hasNonterminalWorkflowJobs(ctx, store, task.TaskID)
	return !active, err
}

func hasNonterminalWorkflowJobs(ctx context.Context, store datastore.DataStore, taskID string) (bool, error) {
	entities, err := store.List(ctx, &model.JobInfo{TaskID: taskID}, &datastore.ListOptions{})
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("list workflow jobs before cancellation: %w", err)
	}
	for _, entity := range entities {
		jobInfo, ok := entity.(*model.JobInfo)
		if !ok || jobInfo == nil {
			return false, datastore.ErrEntityInvalid
		}
		switch config.Status(jobInfo.Status) {
		case config.StatusCompleted, config.StatusPassed, config.StatusSkipped, config.StatusFailed,
			config.StatusTimeout, config.StatusCancelled, config.StatusReject, config.StatusNotRun:
			continue
		default:
			return true, nil
		}
	}
	return false, nil
}
