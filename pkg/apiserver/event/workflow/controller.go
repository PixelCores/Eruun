package workflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	workflowservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	signal "github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

const (
	approvalEventName          = "approval_required"
	approvalPathTemplate       = "/api/v1/workflow/tasks/%s/approval"
	approvalUpdateTimeout      = 5 * time.Second
	approvalNotifyTimeout      = workflowconfig.DefaultWorkflowCallbackTimeout
	taskPersistenceCASAttempts = 2
	approvalTimeoutReasonFmt   = "approval step %q timed out"
)

var errWorkflowTaskPersistenceUncertain = errors.New("workflow task persistence state is uncertain")

type WorkflowCtl struct {
	accountConfig            *spec.AccountConfig
	workspaceManager         *workspace.Manager
	workspace                *model.Workspace
	workflowTask             *model.WorkflowQueue
	workflowTaskMutex        sync.RWMutex
	persistedTaskStatus      config.Status
	taskPersistenceStopped   bool
	taskOwnershipLost        bool
	taskPersistenceUncertain bool
	terminalCallbackOnce     sync.Once
	Client                   kubernetes.Interface
	KubeConfig               *rest.Config
	Store                    datastore.DataStore
	RedisClient              *redis.Client
	Cache                    cache.ICache
	DelayQueue               msg.Queue
	ResourceWaiter           informer.ComponentReadyObserver
	prefix                   string
	ack                      func()
	defaultJobTimeoutSeconds int64
	callbackTimeoutMax       time.Duration
	terminalReason           string
	urlSecurityPolicy        *spec.URLSecurityPolicySpec
	runtimeConfig            *config.Config
	runCancel                context.CancelCauseFunc
	// ctx holds the workflow execution context for use in callbacks like updateWorkflowTask.
	// This avoids using context.Background() which would break tracing and cancellation.
	ctx context.Context
}

func NewWorkflowController(workflowTask *model.WorkflowQueue, client kubernetes.Interface, kubeConfig *rest.Config, store datastore.DataStore, cfg *config.Config, redisClient *redis.Client, cache cache.ICache, urlSecurityPolicy *spec.URLSecurityPolicySpec) (*WorkflowCtl, error) {
	if workflowTask == nil {
		return nil, fmt.Errorf("workflow task is nil")
	}
	if urlSecurityPolicy == nil {
		return nil, fmt.Errorf("url security policy is required")
	}
	if cfg == nil {
		return nil, fmt.Errorf("server config is required")
	}
	if store == nil {
		return nil, fmt.Errorf("datastore is required")
	}
	ctl := &WorkflowCtl{
		workflowTask:             workflowTask,
		runtimeConfig:            cfg,
		persistedTaskStatus:      workflowTask.Status,
		Store:                    store,
		Client:                   client,
		KubeConfig:               kubeConfig,
		RedisClient:              redisClient,
		Cache:                    cache,
		prefix:                   fmt.Sprintf("workflowctl-%s-%s", workflowTask.WorkflowName, workflowTask.TaskID),
		defaultJobTimeoutSeconds: resolveDefaultJobTimeout(cfg),
		callbackTimeoutMax:       workflowconfig.ResolveWorkflowCallbackTimeoutMax(cfg.Workflow),
		urlSecurityPolicy:        urlSecurityPolicy,
		accountConfig:            cfg.Accounts,
	}
	ctl.ack = ctl.updateWorkflowTask
	return ctl, nil
}

// 更改工作流的状态或信息
func (w *WorkflowCtl) updateWorkflowTask() {
	taskSnapshot, expectedStatus, stopped := w.snapshotTaskPersistence()
	if stopped {
		return
	}
	// Use the stored context to preserve tracing and cancellation signals.
	// Falls back to context.Background() if ctx is not set (e.g., during initialization).
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if cause := context.Cause(ctx); ctx.Err() != nil &&
		(signal.IsInfrastructureStop(ctx) ||
			errors.Is(cause, repository.ErrWorkflowLeaseRenewalFailed) ||
			errors.Is(cause, repository.ErrWorkflowOwnershipLost)) {
		w.stopTaskPersistence(nil, true, false)
		klog.V(2).InfoS("skip workflow task persistence after execution context stopped",
			"workflow", taskSnapshot.WorkflowName,
			"taskID", taskSnapshot.TaskID,
			"cause", cause)
		return
	}
	persistCtx := ctx
	cancel := func() {}
	if ctx.Err() != nil {
		persistCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	}
	defer cancel()
	updates := map[string]interface{}{
		"status":                taskSnapshot.Status,
		"current_step":          taskSnapshot.CurrentStep,
		"approval_pending":      taskSnapshot.ApprovalPending,
		"pending_approval_step": taskSnapshot.PendingApprovalStep,
	}
	if taskSnapshot.Status == config.StatusCancelled && wf.IsTerminalCallbackPending(taskSnapshot.SchedulingReason) {
		updates["scheduling_reason"] = taskSnapshot.SchedulingReason
	}
	authoritativeTask, err := w.persistWorkflowTaskSnapshot(persistCtx, taskSnapshot, expectedStatus, updates)
	if err != nil {
		w.stopTaskPersistence(nil, true, false)
		klog.ErrorS(err, "update workflow task persistence state is uncertain",
			"workflow", taskSnapshot.WorkflowName,
			"taskID", taskSnapshot.TaskID,
			"expectedStatus", expectedStatus,
			"targetStatus", taskSnapshot.Status)
		return
	}
	if authoritativeTask != nil {
		ownershipLost := !workflowTaskExecutionIdentityEqual(authoritativeTask, &taskSnapshot)
		w.stopTaskPersistence(authoritativeTask, false, ownershipLost)
		klog.InfoS("stop workflow execution after authoritative task state changed",
			"workflow", taskSnapshot.WorkflowName,
			"taskID", taskSnapshot.TaskID,
			"expectedStatus", expectedStatus,
			"authoritativeStatus", authoritativeTask.Status)
		return
	}
	w.workflowTaskMutex.Lock()
	if !w.taskPersistenceStopped && w.persistedTaskStatus == expectedStatus {
		w.persistedTaskStatus = taskSnapshot.Status
	}
	w.workflowTaskMutex.Unlock()
	w.terminalizePrecreatedVersionUpdateCleanupJobs(persistCtx, taskSnapshot.TaskID, taskSnapshot.Status, w.snapshotTerminalReason())
}

func (w *WorkflowCtl) persistWorkflowTaskSnapshot(
	ctx context.Context,
	taskSnapshot model.WorkflowQueue,
	expectedStatus config.Status,
	updates map[string]interface{},
) (*model.WorkflowQueue, error) {
	for attempt := 0; attempt < taskPersistenceCASAttempts; attempt++ {
		persisted, err := repository.UpdateTaskFieldsIfOwned(ctx, w.Store, &taskSnapshot, expectedStatus, updates)
		if err != nil {
			return nil, errors.Join(
				errWorkflowTaskPersistenceUncertain,
				fmt.Errorf("compare and swap workflow task: %w", err),
			)
		}
		if persisted {
			return nil, nil
		}

		latest, err := w.loadWorkflowTaskAfterPersistenceMiss(taskSnapshot.TaskID)
		if err != nil {
			return nil, errors.Join(
				errWorkflowTaskPersistenceUncertain,
				fmt.Errorf("reload workflow task after compare and swap miss: %w", err),
			)
		}
		if !workflowTaskExecutionIdentityEqual(latest, &taskSnapshot) {
			return latest, nil
		}
		if workflowTaskPersistenceFieldsEqual(latest, &taskSnapshot) {
			return nil, nil
		}
		if latest.Status != expectedStatus {
			return latest, nil
		}
	}
	return nil, fmt.Errorf("%w: compare and swap did not converge after %d attempts", errWorkflowTaskPersistenceUncertain, taskPersistenceCASAttempts)
}

func workflowTaskExecutionIdentityEqual(left, right *model.WorkflowQueue) bool {
	return left != nil &&
		right != nil &&
		left.RunGeneration == right.RunGeneration &&
		left.RunToken == right.RunToken &&
		left.WorkerID == right.WorkerID
}

func workflowTaskPersistenceFieldsEqual(left, right *model.WorkflowQueue) bool {
	fieldsEqual := left != nil &&
		right != nil &&
		left.Status == right.Status &&
		left.CurrentStep == right.CurrentStep &&
		left.ApprovalPending == right.ApprovalPending &&
		left.PendingApprovalStep == right.PendingApprovalStep
	if !fieldsEqual {
		return false
	}
	if right.RunToken == "" {
		return true
	}
	return left.RunGeneration == right.RunGeneration &&
		left.RunToken == right.RunToken &&
		left.WorkerID == right.WorkerID
}

func (w *WorkflowCtl) terminalizePrecreatedVersionUpdateCleanupJobs(ctx context.Context, taskID string, status config.Status, reason string) {
	if !shouldTerminalizeSkippedCleanupForWorkflowStatus(status) {
		return
	}
	if err := workflowservice.TerminalizePrecreatedVersionUpdateCleanupJobs(ctx, w.Store, taskID, status, reason); err != nil {
		klog.Errorf("terminalize cleanup jobs for workflow task %s status=%s failed: %v", taskID, status, err)
	}
}

func shouldTerminalizeSkippedCleanupForWorkflowStatus(status config.Status) bool {
	switch status {
	case config.StatusCancelled, config.StatusFailed, config.StatusTimeout, config.StatusReject:
		return true
	default:
		return false
	}
}

func (w *WorkflowCtl) updateWorkflowStatus(_ context.Context) {
	w.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = config.StatusCompleted
		task.ApprovalPending = false
		task.PendingApprovalStep = ""
	})
	w.updateWorkflowTask()
}

func resolveDefaultJobTimeout(cfg *config.Config) int64 {
	timeout := cfg.Workflow.DefaultJobTimeout
	seconds := int64(timeout / time.Second)
	// JobTask and Kubernetes deadlines use whole seconds. Round fractional
	// values up, but keep conversion back to time.Duration within its range.
	if timeout%time.Second != 0 && seconds < math.MaxInt64/int64(time.Second) {
		seconds++
	}
	return seconds
}

func (w *WorkflowCtl) mutateTask(mut func(task *model.WorkflowQueue)) {
	w.workflowTaskMutex.Lock()
	defer w.workflowTaskMutex.Unlock()
	mut(w.workflowTask)
}

func (w *WorkflowCtl) snapshotTask() model.WorkflowQueue {
	w.workflowTaskMutex.RLock()
	defer w.workflowTaskMutex.RUnlock()
	return *w.workflowTask
}

func (w *WorkflowCtl) snapshotTaskPersistence() (model.WorkflowQueue, config.Status, bool) {
	w.workflowTaskMutex.RLock()
	defer w.workflowTaskMutex.RUnlock()
	return *w.workflowTask, w.persistedTaskStatus, w.taskPersistenceStopped
}

func (w *WorkflowCtl) resetTaskPersistenceForRun() {
	w.workflowTaskMutex.Lock()
	defer w.workflowTaskMutex.Unlock()
	w.persistedTaskStatus = w.workflowTask.Status
	w.taskPersistenceStopped = false
	w.taskPersistenceUncertain = false
	w.runCancel = nil
	w.taskOwnershipLost = false
}

func (w *WorkflowCtl) registerRunCancel(cancel context.CancelCauseFunc) {
	w.workflowTaskMutex.Lock()
	defer w.workflowTaskMutex.Unlock()
	w.runCancel = cancel
}

func (w *WorkflowCtl) clearRunCancel() {
	w.workflowTaskMutex.Lock()
	defer w.workflowTaskMutex.Unlock()
	w.runCancel = nil
}

func (w *WorkflowCtl) stopTaskPersistence(authoritativeTask *model.WorkflowQueue, uncertain, ownershipLost bool) {
	w.workflowTaskMutex.Lock()
	w.taskOwnershipLost = w.taskOwnershipLost || ownershipLost
	if w.taskPersistenceStopped {
		if w.taskPersistenceUncertain && authoritativeTask != nil {
			*w.workflowTask = *authoritativeTask
			w.persistedTaskStatus = authoritativeTask.Status
			w.taskPersistenceUncertain = false
		}
		cancel := w.runCancel
		cause := workflowStopCause(uncertain, w.taskOwnershipLost)
		w.workflowTaskMutex.Unlock()
		if cancel != nil {
			cancel(cause)
		}
		return
	}
	if authoritativeTask != nil {
		*w.workflowTask = *authoritativeTask
		w.persistedTaskStatus = authoritativeTask.Status
	}
	w.taskPersistenceStopped = true
	w.taskPersistenceUncertain = uncertain
	cancel := w.runCancel
	cause := workflowStopCause(uncertain, w.taskOwnershipLost)
	w.workflowTaskMutex.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

func workflowStopCause(uncertain, ownershipLost bool) error {
	if uncertain || ownershipLost {
		return signal.ErrInfrastructureStop
	}
	return nil
}

func (w *WorkflowCtl) workflowRunStopResult() (bool, error) {
	w.workflowTaskMutex.RLock()
	defer w.workflowTaskMutex.RUnlock()
	if !w.taskPersistenceStopped {
		return false, nil
	}
	if w.taskPersistenceUncertain {
		return true, errWorkflowTaskPersistenceUncertain
	}
	return true, nil
}

func (w *WorkflowCtl) terminalCallbackSuppressed() bool {
	w.workflowTaskMutex.RLock()
	defer w.workflowTaskMutex.RUnlock()
	return w.taskPersistenceStopped && (w.taskPersistenceUncertain || w.taskOwnershipLost)
}

func (w *WorkflowCtl) setTerminalStatus(status config.Status, reason string) {
	w.workflowTaskMutex.Lock()
	defer w.workflowTaskMutex.Unlock()
	w.workflowTask.Status = status
	w.terminalReason = strings.TrimSpace(reason)
	if status == config.StatusCancelled {
		w.workflowTask.SchedulingReason = wf.TerminalCallbackPendingReason(reason)
	}
}

func (w *WorkflowCtl) snapshotTerminalReason() string {
	w.workflowTaskMutex.RLock()
	defer w.workflowTaskMutex.RUnlock()
	return w.terminalReason
}

func (w *WorkflowCtl) setCurrentStep(step int) {
	w.mutateTask(func(task *model.WorkflowQueue) {
		task.CurrentStep = step
	})
}

func (w *WorkflowCtl) loadWorkflowTaskAfterPersistenceMiss(taskID string) (*model.WorkflowQueue, error) {
	updateCtx, cancel := approvalUpdateContext(context.Background(), approvalUpdateContextDetached)
	defer cancel()
	task, err := repository.TaskByID(updateCtx, w.Store, taskID)
	if err != nil {
		return nil, err
	}
	return task, nil
}

func isWorkflowTerminal(status config.Status) bool {
	return status == config.StatusCompleted ||
		status == config.StatusPassed ||
		status == config.StatusFailed ||
		status == config.StatusTimeout ||
		status == config.StatusReject ||
		status == config.StatusCancelled
}
