package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	approvaltimeout "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/approvaltimeout"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/importsecret"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	signal "github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

const (
	approvalEventName          = "approval_required"
	approvalPathTemplate       = "/api/v1/workflow/tasks/%s/approval"
	approvalUpdateTimeout      = 5 * time.Second
	approvalProbeInterval      = 2 * time.Second
	approvalNotifyTimeout      = config.DefaultWorkflowCallbackTimeout
	taskPersistenceCASAttempts = 2
	approvalTimeoutReasonFmt   = "approval step %q timed out after %s"
	approvalTimeoutStepReason  = "approval step timed out after %s"
)

var errWorkflowTaskPersistenceUncertain = errors.New("workflow task persistence state is uncertain")

type approvalUpdateContextMode int

const (
	approvalUpdateContextInherit approvalUpdateContextMode = iota
	approvalUpdateContextDetached
)

type WorkflowCtl struct {
	workflowTask             *model.WorkflowQueue
	workflowTaskMutex        sync.RWMutex
	persistedTaskStatus      config.Status
	taskPersistenceStopped   bool
	taskOwnershipLost        bool
	taskPersistenceUncertain bool
	terminalCallbackBlocked  bool
	terminalCallbackOnce     sync.Once
	Client                   kubernetes.Interface
	KubeConfig               *rest.Config
	Store                    datastore.DataStore
	Cache                    cache.ICache
	DelayQueue               msg.Queue
	ResourceWaiter           informer.ComponentReadyObserver
	prefix                   string
	ack                      func()
	defaultJobTimeoutSeconds int64
	callbackTimeoutMax       time.Duration
	terminalReason           string
	urlSecurityPolicy        *spec.URLSecurityPolicySpec
	importSecretKeyring      *importsecret.Keyring
	runCancel                context.CancelCauseFunc
	// ctx holds the workflow execution context for use in callbacks like updateWorkflowTask.
	// This avoids using context.Background() which would break tracing and cancellation.
	ctx context.Context
}

func NewWorkflowController(workflowTask *model.WorkflowQueue, client kubernetes.Interface, kubeConfig *rest.Config, store datastore.DataStore, cfg *config.Config, cache cache.ICache, urlSecurityPolicy *spec.URLSecurityPolicySpec) (*WorkflowCtl, error) {
	if workflowTask == nil {
		return nil, fmt.Errorf("workflow task is nil")
	}
	if urlSecurityPolicy == nil {
		return nil, fmt.Errorf("url security policy is required")
	}
	var importSecretKeyring *importsecret.Keyring
	if cfg != nil {
		var err error
		importSecretKeyring, err = importsecret.Load(cfg.ImportSecretKeyring, cfg.ImportSecretKeyringFile)
		if err != nil {
			return nil, fmt.Errorf("load workflow import secret keyring: %w", err)
		}
	}
	ctl := &WorkflowCtl{
		workflowTask:             workflowTask,
		persistedTaskStatus:      workflowTask.Status,
		Store:                    store,
		Client:                   client,
		KubeConfig:               kubeConfig,
		Cache:                    cache,
		prefix:                   fmt.Sprintf("workflowctl-%s-%s", workflowTask.WorkflowName, workflowTask.TaskID),
		defaultJobTimeoutSeconds: resolveDefaultJobTimeout(cfg),
		callbackTimeoutMax:       config.ResolveWorkflowCallbackTimeoutMax(cfg),
		urlSecurityPolicy:        urlSecurityPolicy,
		importSecretKeyring:      importSecretKeyring,
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
	if w == nil || w.Store == nil {
		return
	}
	if err := service.TerminalizePrecreatedVersionUpdateCleanupJobs(ctx, w.Store, taskID, status, reason); err != nil {
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

func (w *WorkflowCtl) Run(ctx context.Context, concurrency int) error {
	w.resetTaskPersistenceForRun()
	// 1. Start a new trace for this workflow execution
	tracer := otel.Tracer("workflow-runner")
	taskMeta := w.snapshotTask()
	workflowName := taskMeta.WorkflowName
	ctx, span := tracer.Start(ctx, workflowName, trace.WithAttributes(
		attribute.String("workflow.name", workflowName),
		attribute.String("workflow.task_id", taskMeta.TaskID),
	))
	defer span.End()

	// 2. TmpCreate a logger with the traceID and put it in the context
	logger := klog.FromContext(ctx).WithValues(
		"traceID", span.SpanContext().TraceID().String(),
		"workflowName", workflowName,
		"taskID", taskMeta.TaskID,
	)
	ctx = klog.NewContext(ctx, logger)
	ctx = job.WithTaskMetadata(ctx, taskMeta.TaskID)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	w.registerRunCancel(cancel)
	defer w.clearRunCancel()

	// Store context for use in callbacks (e.g., updateWorkflowTask)
	w.ctx = ctx
	failureReason := ""
	skipExitAck := false
	suppressTerminalCallback := false
	stopForInfrastructureStop := func() (bool, error) {
		if !signal.IsInfrastructureStop(ctx) {
			return false, nil
		}
		cause := context.Cause(ctx)
		skipExitAck = true
		suppressTerminalCallback = true
		span.RecordError(cause)
		span.SetStatus(codes.Error, "Workflow stopped for infrastructure stop")
		logger.Info("Stopping workflow for infrastructure stop")
		return true, cause
	}
	stopForJobInfrastructure := func(err error) error {
		skipExitAck = true
		suppressTerminalCallback = true
		failureReason = err.Error()
		cancel(err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Workflow job persistence stopped for infrastructure retry")
		return err
	}
	stopAfterPersistence := func() (bool, error) {
		if ctx.Err() != nil {
			w.stopTaskPersistence(nil, true, false)
		}
		stopped, err := w.workflowRunStopResult()
		if !stopped {
			return false, nil
		}
		skipExitAck = true
		if err != nil {
			failureReason = err.Error()
			span.RecordError(err)
			span.SetStatus(codes.Error, "Workflow persistence state is uncertain")
		}
		return true, err
	}
	defer func() {
		if suppressTerminalCallback || w.terminalCallbackSuppressed() {
			return
		}
		status := w.snapshotTask().Status
		w.triggerWorkflowCallbackOnce(ctx, status, failureReason)
	}()
	if stopped, err := stopForInfrastructureStop(); stopped {
		return err
	}

	// 将工作流的状态更改为运行中
	w.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = config.StatusRunning
		if task.CreateTime.IsZero() {
			task.CreateTime = time.Now()
		}
	})
	w.ack()
	if stopped, err := stopAfterPersistence(); stopped {
		return err
	}
	if stopped, err := stopForInfrastructureStop(); stopped {
		return err
	}
	logger.Info("Starting workflow", "status", w.snapshotTask().Status)

	defer func() {
		finalTask := w.snapshotTask()
		logger.Info("Finished workflow", "status", finalTask.Status)
		if skipExitAck {
			return
		}
		// Approval checkpoint is already persisted in pauseAtApprovalStep.
		// Skip exit ack here to avoid overwriting an approval action that may
		// have been applied by API while notification was in-flight.
		if finalTask.Status == config.StatusWaitingApprove && finalTask.ApprovalPending {
			return
		}
		w.ack()
	}()

	taskForGeneration := w.snapshotTask()
	stepExecutions, err := GenerateJobTasks(ctx, &taskForGeneration, w.Store, w.defaultJobTimeoutSeconds)
	if err != nil {
		runErr := errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("restore workflow job executions: %w", err))
		skipExitAck = true
		span.RecordError(runErr)
		span.SetStatus(codes.Error, "Failed to restore workflow job executions")
		logger.Error(runErr, "Failed to restore workflow job executions")
		failureReason = runErr.Error()
		return runErr
	}
	seqLimit := 1
	if concurrency > 0 {
		seqLimit = concurrency
	}
	startStep := taskForGeneration.CurrentStep
	if startStep < 0 {
		startStep = 0
	}
	if startStep >= len(stepExecutions) {
		if stopped, err := stopForInfrastructureStop(); stopped {
			return err
		}
		span.SetStatus(codes.Ok, "Workflow completed successfully")
		w.updateWorkflowStatus(ctx)
		if stopped, err := stopAfterPersistence(); stopped {
			return err
		}
		skipExitAck = true
		return nil
	}

	for stepIdx := startStep; stepIdx < len(stepExecutions); stepIdx++ {
		if stopped, err := stopForInfrastructureStop(); stopped {
			return err
		}
		stepExec := stepExecutions[stepIdx]
		if stepExec.StepType == config.WorkflowStepTypeApproval {
			paused, pauseErr := w.pauseAtApprovalStep(ctx, &stepExec, stepIdx)
			if pauseErr != nil {
				skipExitAck = true
				span.RecordError(pauseErr)
				span.SetStatus(codes.Error, "Failed to persist approval checkpoint")
				failureReason = pauseErr.Error()
				return pauseErr
			}
			if paused {
				span.SetStatus(codes.Ok, "Workflow paused waiting for approval")
			} else {
				// CAS condition mismatch means task state changed concurrently (e.g. cancelled).
				// Skip exit ack to avoid writing stale in-memory snapshot back to store.
				skipExitAck = true
			}
			return nil
		}
		if stepExec.Jobs == nil {
			if stopped, err := stopForInfrastructureStop(); stopped {
				return err
			}
			w.setCurrentStep(stepIdx + 1)
			w.ack()
			if stopped, err := stopAfterPersistence(); stopped {
				return err
			}
			continue
		}
		priorities := sortedPriorities(stepExec.Jobs)
		for _, priority := range priorities {
			tasksInPriority := stepExec.Jobs[priority]
			if len(tasksInPriority) == 0 {
				continue
			}
			stepConcurrency := determineStepConcurrency(stepExec.Mode, len(tasksInPriority), seqLimit)
			// Fix: StepByStep mode should stop on first failure (stopOnFailure=true)
			// Parallel mode continues all jobs even if some fail (stopOnFailure=false)
			stopOnFailure := !stepExec.Mode.IsParallel()
			logger.Info("Executing workflow step", "workflowName", workflowName, "step", stepExec.Name, "mode", stepExec.Mode, "priority", priority, "jobCount", len(tasksInPriority), "concurrency", stepConcurrency, "stopOnFailure", stopOnFailure)

			if err := job.RunJobs(ctx, tasksInPriority, stepConcurrency, w.Client, w.KubeConfig, w.Store, w.ack, stopOnFailure, w.Cache, w.urlSecurityPolicy, w.DelayQueue, w.ResourceWaiter, w.importSecretKeyring); err != nil {
				logger.Error(err, "Stopping workflow after job persistence failure", "step", stepExec.Name, "priority", priority)
				return stopForJobInfrastructure(err)
			}
			if stopped, err := stopAfterPersistence(); stopped {
				return err
			}
			if stopped, err := stopForInfrastructureStop(); stopped {
				return err
			}

			cleanupTrigger := workflowFailureCleanupTrigger(stepExec.FailurePolicy, tasksInPriority)
			for _, task := range tasksInPriority {
				if !isJobSuccessStatus(task) {
					reason := workflowFailureReason(workflowName, task, cleanupTrigger)
					if cleanupTrigger != nil {
						cleanupErr := w.runWorkflowFailureCleanup(ctx, logger)
						if stopped, err := stopAfterPersistence(); stopped {
							return err
						}
						if cleanupErr != nil {
							if errors.Is(cleanupErr, signal.ErrInfrastructureStop) {
								logger.Error(cleanupErr, "Stopping workflow after cleanup job persistence failure")
								return stopForJobInfrastructure(cleanupErr)
							}
							reason = fmt.Sprintf("%s; cleanup_all failed: %v", reason, cleanupErr)
						}
					}
					err := errors.New(reason)
					logger.Error(err, "Workflow failed at job, aborting.", "step", stepExec.Name, "priority", priority, "jobName", task.Name, "jobStatus", task.Status)
					if task.Status == config.StatusCancelled {
						w.setTerminalStatus(config.StatusCancelled, reason)
						span.SetStatus(codes.Error, "Workflow cancelled")
					} else {
						w.setTerminalStatus(config.StatusFailed, reason)
						span.SetStatus(codes.Error, "Workflow failed")
					}
					span.RecordError(err)
					failureReason = w.snapshotTerminalReason()
					return err
				}
			}
		}
		if stopped, err := stopForInfrastructureStop(); stopped {
			return err
		}
		w.setCurrentStep(stepIdx + 1)
		w.ack()
		if stopped, err := stopAfterPersistence(); stopped {
			return err
		}
		logger.Info("Workflow step completed successfully", "workflowName", workflowName, "step", stepExec.Name)
	}

	if stopped, err := stopForInfrastructureStop(); stopped {
		return err
	}
	span.SetStatus(codes.Ok, "Workflow completed successfully")
	w.updateWorkflowStatus(ctx)
	if stopped, err := stopAfterPersistence(); stopped {
		return err
	}
	skipExitAck = true
	return nil
}

func isJobSuccessStatus(task *model.JobTask) bool {
	if task == nil {
		return false
	}
	switch task.Status {
	case config.StatusCompleted, config.StatusSkipped, config.StatusPassed:
		return true
	}
	if task.JobType == string(config.JobDeployScheduled) || task.JobType == string(config.JobDeployInstant) {
		switch task.Status {
		case config.StatusDistributed, config.StatusQueued, config.StatusWaiting:
			return true
		}
	}
	return false
}

func shouldCleanupAllOnWorkflowFailure(policy config.WorkflowFailurePolicy, task *model.JobTask) bool {
	if task == nil {
		return false
	}
	effectivePolicy := policy
	if override, ok := config.NormalizeJobFailurePolicy(task.FailurePolicy); ok && override != "" {
		effectivePolicy = override
	}
	normalized, ok := config.NormalizeWorkflowFailurePolicy(effectivePolicy)
	if !ok || normalized != config.WorkflowFailurePolicyCleanupAll {
		return false
	}
	if task.Status != config.StatusFailed && task.Status != config.StatusTimeout {
		return false
	}
	return isWorkflowFailureCleanupTriggerJob(config.JobType(task.JobType))
}

func workflowFailureCleanupTrigger(policy config.WorkflowFailurePolicy, tasks []*model.JobTask) *model.JobTask {
	for _, task := range tasks {
		if shouldCleanupAllOnWorkflowFailure(policy, task) {
			return task
		}
	}
	return nil
}

func workflowFailureReason(workflowName string, primaryFailure, cleanupTrigger *model.JobTask) string {
	reason := fmt.Sprintf("workflow %s failed at job %s (status=%s)", workflowName, primaryFailure.Name, primaryFailure.Status)
	if cleanupTrigger != nil && cleanupTrigger != primaryFailure {
		reason = fmt.Sprintf("%s; cleanup_all triggered by job %s (status=%s)", reason, cleanupTrigger.Name, cleanupTrigger.Status)
	}
	return reason
}

func isWorkflowFailureCleanupTriggerJob(jobType config.JobType) bool {
	switch jobType {
	case config.JobDeploy,
		config.JobDeployService,
		config.JobDeployStore,
		config.JobDeployPVC,
		config.JobDeployConfigMap,
		config.JobDeploySecret,
		config.JobDeployIngress,
		config.JobDeployServiceAccount,
		config.JobDeployRole,
		config.JobDeployRoleBinding,
		config.JobDeployClusterRole,
		config.JobDeployClusterRoleBinding,
		config.JobDeployPodDisruptionBudget,
		config.JobDeployNetworkPolicy,
		config.JobDeployInstant,
		config.JobDeployScheduled,
		config.JobDeployCloud:
		return true
	default:
		return false
	}
}

func (w *WorkflowCtl) runWorkflowFailureCleanup(ctx context.Context, logger klog.Logger) error {
	task := w.snapshotTask()
	cleanupJobs, err := buildWorkflowFailureCleanupJobs(ctx, &task, w.Store, w.defaultJobTimeoutSeconds)
	if err != nil {
		return err
	}
	if len(cleanupJobs) == 0 {
		logger.Info("No workflow failure cleanup jobs generated", "appID", task.AppID, "taskID", task.TaskID)
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cleanupCtx := klog.NewContext(ctx, logger.WithValues("failurePolicy", config.WorkflowFailurePolicyCleanupAll))
	cleanupCtx = job.WithTaskMetadata(cleanupCtx, task.TaskID)
	logger.Info("Running workflow failure cleanup jobs", "appID", task.AppID, "taskID", task.TaskID, "jobCount", len(cleanupJobs))
	if err := job.RunJobs(cleanupCtx, cleanupJobs, 1, w.Client, w.KubeConfig, w.Store, w.ack, false, w.Cache, w.urlSecurityPolicy, w.DelayQueue, w.ResourceWaiter, w.importSecretKeyring); err != nil {
		return err
	}
	return workflowFailureCleanupError(cleanupJobs)
}

func workflowFailureCleanupError(cleanupJobs []*model.JobTask) error {
	var errs []error
	for _, cleanupJob := range cleanupJobs {
		if isJobSuccessStatus(cleanupJob) {
			continue
		}
		message := fmt.Sprintf("cleanup job %s status=%s", cleanupJob.Name, cleanupJob.Status)
		if detail := strings.TrimSpace(cleanupJob.Error); detail != "" {
			message = fmt.Sprintf("%s: %s", message, detail)
		}
		errs = append(errs, errors.New(message))
	}
	return errors.Join(errs...)
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
	if cfg != nil && cfg.Workflow.DefaultJobTimeout > 0 {
		seconds := int64(cfg.Workflow.DefaultJobTimeout / time.Second)
		if seconds > 0 {
			return seconds
		}
	}
	return int64(config.DefaultJobTaskTimeout)
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
	w.terminalCallbackBlocked = false
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

func (w *WorkflowCtl) setStatus(status config.Status) {
	w.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = status
	})
}

func (w *WorkflowCtl) setTerminalStatus(status config.Status, reason string) {
	w.workflowTaskMutex.Lock()
	defer w.workflowTaskMutex.Unlock()
	w.workflowTask.Status = status
	w.terminalReason = strings.TrimSpace(reason)
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

func (w *WorkflowCtl) pauseAtApprovalStep(ctx context.Context, stepExec *StepExecution, stepIndex int) (bool, error) {
	if stepExec == nil {
		return false, nil
	}
	stepName := strings.TrimSpace(stepExec.Name)
	if stepName == "" {
		stepName = fmt.Sprintf("step-%d", stepIndex+1)
	}

	taskID := strings.TrimSpace(w.snapshotTask().TaskID)
	if taskID == "" || w.Store == nil {
		return false, fmt.Errorf("invalid workflow task context for approval checkpoint")
	}

	persisted, err := w.persistApprovalCheckpoint(taskID, stepName, stepIndex)
	if err != nil {
		return false, fmt.Errorf("persist approval checkpoint: %w", err)
	}
	if !persisted {
		if err := w.reloadTaskSnapshot(taskID); err != nil {
			return false, fmt.Errorf("reload workflow task snapshot: %w", err)
		}
		snapshot := w.snapshotTask()
		if isApprovalCheckpointPersisted(snapshot, stepName, stepIndex) {
			klog.Infof("approval checkpoint already persisted by concurrent actor task=%s step=%s", taskID, stepName)
			return false, nil
		}
		if shouldFailOnApprovalCheckpointMiss(snapshot, stepIndex) {
			return false, fmt.Errorf("approval checkpoint cas miss while task still active: task=%s status=%s currentStep=%d", taskID, snapshot.Status, snapshot.CurrentStep)
		}
		klog.Infof("skip approval checkpoint write due concurrent status change task=%s step=%s status=%s currentStep=%d approvalPending=%t",
			taskID, stepName, snapshot.Status, snapshot.CurrentStep, snapshot.ApprovalPending)
		return false, nil
	}

	w.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = config.StatusWaitingApprove
		task.CurrentStep = stepIndex
		task.ApprovalPending = true
		task.PendingApprovalStep = stepName
	})
	w.scheduleApprovalTimeout(stepExec, stepName, stepIndex)
	w.triggerApprovalNotification(ctx, stepExec, stepName)
	return true, nil
}

func (w *WorkflowCtl) persistApprovalCheckpoint(taskID, stepName string, stepIndex int) (bool, error) {
	updates := map[string]interface{}{
		"status":                config.StatusWaitingApprove,
		"current_step":          stepIndex,
		"approval_pending":      true,
		"pending_approval_step": stepName,
	}
	updateCtx, cancel := approvalUpdateContext(nil, approvalUpdateContextDetached)
	defer cancel()
	taskSnapshot := w.snapshotTask()
	if taskSnapshot.RunGeneration == 0 || taskSnapshot.RunToken == "" || taskSnapshot.WorkerID == "" {
		return false, repository.ErrWorkflowOwnershipRequired
	}
	casStore, ok := w.Store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return false, repository.ErrWorkflowFencingUnsupported
	}
	conditions := map[string]interface{}{
		"status":         config.StatusRunning,
		"current_step":   stepIndex,
		"run_generation": taskSnapshot.RunGeneration,
		"run_token":      taskSnapshot.RunToken,
		"worker_id":      taskSnapshot.WorkerID,
	}
	return casStore.CompareAndSwapWithConditions(
		updateCtx,
		&model.WorkflowQueue{TaskID: taskID},
		conditions,
		updates,
	)
}

func (w *WorkflowCtl) reloadTaskSnapshot(taskID string) error {
	updateCtx, cancel := approvalUpdateContext(nil, approvalUpdateContextDetached)
	defer cancel()
	task, err := repository.TaskByID(updateCtx, w.Store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil
		}
		return err
	}
	if task == nil {
		return nil
	}
	w.mutateTask(func(current *model.WorkflowQueue) {
		*current = *task
	})
	return nil
}

func (w *WorkflowCtl) loadWorkflowTaskAfterPersistenceMiss(taskID string) (*model.WorkflowQueue, error) {
	updateCtx, cancel := approvalUpdateContext(nil, approvalUpdateContextDetached)
	defer cancel()
	task, err := repository.TaskByID(updateCtx, w.Store, taskID)
	if err != nil {
		return nil, err
	}
	return task, nil
}

func isApprovalCheckpointPersisted(task model.WorkflowQueue, stepName string, stepIndex int) bool {
	if !task.ApprovalPending {
		return false
	}
	if task.CurrentStep != stepIndex {
		return false
	}
	if strings.TrimSpace(task.PendingApprovalStep) != strings.TrimSpace(stepName) {
		return false
	}
	return task.Status == config.StatusWaitingApprove || task.Status == config.StatusWaiting
}

func shouldFailOnApprovalCheckpointMiss(task model.WorkflowQueue, stepIndex int) bool {
	if isWorkflowTerminal(task.Status) {
		return false
	}
	if task.ApprovalPending {
		return false
	}
	if task.CurrentStep > stepIndex {
		return false
	}
	switch task.Status {
	case config.StatusRunning, config.StatusQueued, config.StatusWaiting:
		return true
	default:
		return false
	}
}

func (w *WorkflowCtl) triggerApprovalNotification(ctx context.Context, stepExec *StepExecution, stepName string) {
	if stepExec == nil || stepExec.Approval == nil {
		return
	}
	approval := stepExec.Approval
	targetURL := strings.TrimSpace(approval.NotifyURL)
	if targetURL == "" {
		return
	}
	method := strings.ToUpper(strings.TrimSpace(approval.Method))
	if method == "" {
		method = "POST"
	}
	task := w.snapshotTask()
	payload := job.CallbackPayload{
		Event:        approvalEventName,
		Status:       string(config.StatusWaitingApprove),
		AppID:        task.AppID,
		WorkflowID:   task.WorkflowID,
		WorkflowName: task.WorkflowName,
		TaskID:       task.TaskID,
		WorkflowType: task.Type,
		StepName:     stepName,
		Message:      strings.TrimSpace(approval.Message),
		ApprovalPath: fmt.Sprintf(approvalPathTemplate, task.TaskID),
		StartTime:    task.CreateTime.Unix(),
	}
	callbackName := fmt.Sprintf("workflow-approval-notify-%s", task.TaskID)
	callbackJob := &model.JobTask{
		Name:          callbackName,
		WorkflowID:    task.WorkflowID,
		ProjectID:     task.ProjectID,
		AppID:         task.AppID,
		TaskID:        task.TaskID,
		JobType:       string(config.JobDeployCallback),
		ExecutionKey:  workflowJobExecutionKey(&task, -1, 0, 0, callbackName+"|"+stepName, string(config.JobDeployCallback)),
		RunGeneration: task.RunGeneration,
		OwnerStatus:   task.Status,
		RunToken:      task.RunToken,
		WorkerID:      task.WorkerID,
		JobInfo: &job.CallbackJobInfo{
			Event:          approvalEventName,
			URL:            targetURL,
			Method:         method,
			Headers:        approval.Headers,
			TimeoutSeconds: int64(approvalNotifyTimeout / time.Second),
			TimeoutMaxSec:  config.ResolveWorkflowCallbackTimeoutMaxSeconds(approvalNotifyTimeout),
			TimeoutMaxNS:   int64(approvalNotifyTimeout),
			Payload:        payload,
		},
		Status: config.StatusWaiting,
	}
	job.ApplyExecutionIdentity(callbackJob)
	callbackCtx, cancel := approvalNotificationContext(context.Background(), approvalNotifyTimeout)
	go func() {
		defer cancel()
		// Notification jobs should not mutate workflow queue state.
		job.RunJobs(callbackCtx, []*model.JobTask{callbackJob}, 1, w.Client, w.KubeConfig, w.Store, func() {}, false, w.Cache, w.urlSecurityPolicy, w.DelayQueue, w.ResourceWaiter, w.importSecretKeyring)
	}()
}

func (w *WorkflowCtl) scheduleApprovalTimeout(stepExec *StepExecution, stepName string, stepIndex int) {
	if stepExec == nil || stepExec.Approval == nil || w == nil || w.Store == nil {
		return
	}
	if stepExec.Approval.TimeoutSeconds <= 0 {
		return
	}
	task := w.snapshotTask()
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return
	}
	timeout := time.Duration(stepExec.Approval.TimeoutSeconds) * time.Second
	timerCtx, stopTimer := context.WithCancel(context.Background())
	timerID := approvaltimeout.Register(taskID, stopTimer)
	go func() {
		defer approvaltimeout.Unregister(taskID, timerID)
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		probeEvery := approvalProbeInterval
		if timeout < probeEvery {
			probeEvery = timeout
		}
		probeTicker := time.NewTicker(probeEvery)
		defer probeTicker.Stop()
		for {
			select {
			case <-timerCtx.Done():
				return
			case <-timer.C:
				w.markApprovalTimeout(taskID, stepName, stepIndex, timeout)
				return
			case <-probeTicker.C:
				if !w.isApprovalCheckpointPending(taskID, stepName, stepIndex) {
					return
				}
			}
		}
	}()
}

func (w *WorkflowCtl) isApprovalCheckpointPending(taskID, stepName string, stepIndex int) bool {
	if w == nil || w.Store == nil {
		return false
	}
	updateCtx, cancel := approvalUpdateContext(nil, approvalUpdateContextDetached)
	defer cancel()

	task, err := repository.TaskByID(updateCtx, w.Store, taskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return false
		}
		klog.Warningf("probe approval task %s failed: %v", taskID, err)
		return true
	}
	if task == nil {
		return false
	}
	return task.Status == config.StatusWaitingApprove &&
		task.ApprovalPending &&
		task.CurrentStep == stepIndex &&
		strings.TrimSpace(task.PendingApprovalStep) == strings.TrimSpace(stepName)
}

func (w *WorkflowCtl) markApprovalTimeout(taskID, stepName string, stepIndex int, timeout time.Duration) {
	updateCtx, cancel := approvalUpdateContext(nil, approvalUpdateContextDetached)
	defer cancel()

	task, err := repository.TaskByID(updateCtx, w.Store, taskID)
	if err != nil || task == nil {
		if err != nil {
			klog.Warningf("load approval task %s for timeout failed: %v", taskID, err)
		}
		return
	}
	if task.Status != config.StatusWaitingApprove || !task.ApprovalPending {
		return
	}
	if task.CurrentStep != stepIndex {
		return
	}
	if strings.TrimSpace(task.PendingApprovalStep) != strings.TrimSpace(stepName) {
		return
	}

	updates := map[string]interface{}{
		"status":                config.StatusTimeout,
		"approval_pending":      false,
		"pending_approval_step": "",
	}
	updated, err := w.compareAndSwapApprovalTimeoutCheckpoint(updateCtx, taskID, stepName, stepIndex, updates)
	if err != nil {
		klog.Warningf("mark approval timeout failed task=%s step=%s: %v", taskID, stepName, err)
		return
	}
	if !updated {
		return
	}
	approvaltimeout.Cancel(taskID)

	reason := fmt.Sprintf(approvalTimeoutStepReason, timeout)
	if strings.TrimSpace(stepName) != "" {
		reason = fmt.Sprintf(approvalTimeoutReasonFmt, stepName, timeout)
	}
	w.terminalizePrecreatedVersionUpdateCleanupJobs(updateCtx, taskID, config.StatusTimeout, reason)

	w.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = config.StatusTimeout
		task.ApprovalPending = false
		task.PendingApprovalStep = ""
	})
	w.triggerWorkflowCallbackOnce(context.Background(), config.StatusTimeout, reason)
}

func (w *WorkflowCtl) compareAndSwapApprovalTimeoutCheckpoint(
	ctx context.Context,
	taskID, stepName string,
	stepIndex int,
	updates map[string]interface{},
) (bool, error) {
	task := &model.WorkflowQueue{TaskID: taskID}
	normalizedStep := strings.TrimSpace(stepName)
	if casStore, ok := w.Store.(datastore.ConditionalCompareAndSwap); ok {
		conditions := map[string]interface{}{
			"status":                config.StatusWaitingApprove,
			"approval_pending":      true,
			"current_step":          stepIndex,
			"pending_approval_step": normalizedStep,
		}
		return casStore.CompareAndSwapWithConditions(ctx, task, conditions, updates)
	}
	// Fallback for stores without multi-condition CAS support.
	return w.Store.CompareAndSwap(ctx, task, "status", config.StatusWaitingApprove, updates)
}

func (w *WorkflowCtl) triggerWorkflowCallbackOnce(ctx context.Context, status config.Status, reason string) {
	if !isWorkflowTerminal(status) {
		return
	}
	w.terminalCallbackOnce.Do(func() {
		w.triggerWorkflowCallback(ctx, status, reason)
	})
}

func isWorkflowTerminal(status config.Status) bool {
	return status == config.StatusCompleted ||
		status == config.StatusPassed ||
		status == config.StatusFailed ||
		status == config.StatusTimeout ||
		status == config.StatusReject ||
		status == config.StatusCancelled
}

func (w *WorkflowCtl) triggerWorkflowCallback(ctx context.Context, status config.Status, reason string) {
	if w == nil || w.Store == nil {
		return
	}
	if !isWorkflowTerminal(status) {
		return
	}
	task := w.snapshotTask()
	workflowID := strings.TrimSpace(task.WorkflowID)
	if workflowID == "" {
		return
	}
	callbackSource := model.WorkflowCallbackSource(&task, nil, nil)
	if callbackSource == nil {
		workflow := &model.Workflow{ID: workflowID}
		loadCtx := ctx
		if loadCtx == nil || loadCtx.Err() != nil {
			loadCtx = context.Background()
		}
		loadCtx, loadCancel := context.WithTimeout(loadCtx, 5*time.Second)
		defer loadCancel()
		if err := w.Store.Get(loadCtx, workflow); err != nil {
			klog.Errorf("load workflow %s for callback failed: %v", workflowID, err)
			return
		}
		callbackSource = model.WorkflowCallbackSource(nil, workflow, nil)
		if callbackSource == nil && strings.TrimSpace(task.AppID) != "" {
			app := &model.Applications{ID: task.AppID}
			if err := w.Store.Get(loadCtx, app); err != nil {
				klog.Errorf("load application %s for callback failed: %v", task.AppID, err)
				return
			}
			callbackSource = model.WorkflowCallbackSource(nil, workflow, app)
		}
	}
	if callbackSource == nil {
		return
	}
	var callback model.WorkflowCallback
	if err := decodeWorkflowCallback(callbackSource, &callback); err != nil {
		klog.Errorf("decode workflow %s callback failed: %v", workflowID, err)
		return
	}
	event, targetURL := callbackTargetForStatus(&callback, status)
	if targetURL == "" {
		return
	}
	method := callbackMethodForEvent(&callback, event)
	payload := job.CallbackPayload{
		Event:        event,
		Status:       string(status),
		AppID:        task.AppID,
		WorkflowID:   task.WorkflowID,
		WorkflowName: task.WorkflowName,
		TaskID:       task.TaskID,
		WorkflowType: task.Type,
		StartTime:    task.CreateTime.Unix(),
		EndTime:      time.Now().Unix(),
		Reason:       strings.TrimSpace(reason),
	}
	callbackName := fmt.Sprintf("workflow-callback-%s", task.TaskID)
	callbackJob := &model.JobTask{
		Name:          callbackName,
		WorkflowID:    task.WorkflowID,
		ProjectID:     task.ProjectID,
		AppID:         task.AppID,
		TaskID:        task.TaskID,
		JobType:       string(config.JobDeployCallback),
		ExecutionKey:  workflowJobExecutionKey(&task, -1, 0, 0, callbackName+"|"+event, string(config.JobDeployCallback)),
		RunGeneration: task.RunGeneration,
		OwnerStatus:   status,
		RunToken:      task.RunToken,
		WorkerID:      task.WorkerID,
		JobInfo: &job.CallbackJobInfo{
			Event:          event,
			URL:            targetURL,
			Method:         method,
			Headers:        callback.Headers,
			TimeoutSeconds: config.ClampWorkflowCallbackTimeoutSeconds(callback.TimeoutSeconds, w.callbackTimeoutMax),
			TimeoutMaxSec:  config.ResolveWorkflowCallbackTimeoutMaxSeconds(w.callbackTimeoutMax),
			TimeoutMaxNS:   int64(w.callbackTimeoutMax),
			Payload:        payload,
		},
		Status: config.StatusWaiting,
	}
	job.ApplyExecutionIdentity(callbackJob)
	callbackCtx, cancel := callbackContext(ctx, callback.TimeoutSeconds, w.callbackTimeoutMax)
	defer cancel()
	job.RunJobs(callbackCtx, []*model.JobTask{callbackJob}, 1, w.Client, w.KubeConfig, w.Store, func() {}, false, w.Cache, w.urlSecurityPolicy, w.DelayQueue, w.ResourceWaiter, w.importSecretKeyring)
}

func approvalUpdateContext(parent context.Context, mode approvalUpdateContextMode) (context.Context, context.CancelFunc) {
	switch mode {
	case approvalUpdateContextInherit:
		if parent == nil {
			parent = context.Background()
		}
		return context.WithTimeout(parent, approvalUpdateTimeout)
	default:
		return context.WithTimeout(context.Background(), approvalUpdateTimeout)
	}
}

func approvalNotificationContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = approvalNotifyTimeout
	}
	if ctx != nil && ctx.Err() == nil {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithTimeout(context.Background(), timeout)
}

func callbackContext(ctx context.Context, timeoutSeconds int64, timeoutMax time.Duration) (context.Context, context.CancelFunc) {
	if ctx != nil && ctx.Err() == nil {
		return ctx, func() {}
	}
	timeout := config.ResolveWorkflowCallbackTimeout(timeoutSeconds, timeoutMax)
	return context.WithTimeout(context.Background(), timeout)
}

func decodeWorkflowCallback(raw *model.JSONStruct, target interface{}) error {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func callbackTargetForStatus(callback *model.WorkflowCallback, status config.Status) (string, string) {
	if callback == nil {
		return "", ""
	}
	switch status {
	case config.StatusCompleted, config.StatusPassed:
		return "success", strings.TrimSpace(callback.Success)
	case config.StatusCancelled:
		if callback.Cancelled != "" {
			return "cancelled", strings.TrimSpace(callback.Cancelled)
		}
		return "failure", strings.TrimSpace(callback.Failure)
	case config.StatusTimeout:
		if callback.Timeout != "" {
			return "timeout", strings.TrimSpace(callback.Timeout)
		}
		return "failure", strings.TrimSpace(callback.Failure)
	case config.StatusReject:
		if callback.Reject != "" {
			return "reject", strings.TrimSpace(callback.Reject)
		}
		return "failure", strings.TrimSpace(callback.Failure)
	default:
		return "failure", strings.TrimSpace(callback.Failure)
	}
}

func callbackMethodForEvent(callback *model.WorkflowCallback, event string) string {
	if callback == nil || len(callback.Methods) == 0 || event == "" {
		return ""
	}
	method, ok := callback.Methods[strings.ToLower(event)]
	if !ok {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(method))
}
