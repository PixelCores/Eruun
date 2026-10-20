package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobcleanup"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	evaluationjobs "github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	signal "github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

// Run establishes persisted workspace ownership before entering the state machine.
func (w *WorkflowCtl) Run(ctx context.Context, concurrency int) error {
	scopedCtx, err := w.prepareWorkspace(ctx)
	if err != nil {
		w.resetTaskPersistenceForRun()
		w.ctx = ctx
		w.mutateTask(func(task *model.WorkflowQueue) {
			task.Status = config.StatusFailed
		})
		w.ack()
		return fmt.Errorf("prepare workflow workspace: %w", err)
	}
	return w.run(scopedCtx, concurrency)
}

// run is the workflow state machine, independently exercised by its unit tests.
func (w *WorkflowCtl) run(ctx context.Context, concurrency int) error {
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
	ctx = jobexec.WithTaskMetadata(ctx, taskMeta.TaskID)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	w.registerRunCancel(cancel)
	defer w.clearRunCancel()

	// Store context for use in callbacks (e.g., updateWorkflowTask)
	w.ctx = ctx
	run := &workflowRun{WorkflowCtl: w, ctx: ctx, span: span, cancel: cancel}
	return run.run(concurrency)
}

type workflowRun struct {
	*WorkflowCtl
	ctx                      context.Context
	span                     trace.Span
	cancel                   context.CancelCauseFunc
	failureReason            string
	skipExitAck              bool
	suppressTerminalCallback bool
}

func (r *workflowRun) stopForInfrastructureStop() (bool, error) {
	logger := klog.FromContext(r.ctx)
	if !signal.IsInfrastructureStop(r.ctx) {
		return false, nil
	}
	cause := context.Cause(r.ctx)
	r.skipExitAck = true
	r.suppressTerminalCallback = true
	r.span.RecordError(cause)
	r.span.SetStatus(codes.Error, "Workflow stopped for infrastructure stop")
	logger.Info("Stopping workflow for infrastructure stop")
	return true, cause
}

func (r *workflowRun) stopForJobInfrastructure(err error) error {
	r.skipExitAck = true
	r.suppressTerminalCallback = true
	r.failureReason = err.Error()
	r.cancel(err)
	r.span.RecordError(err)
	r.span.SetStatus(codes.Error, "Workflow job persistence stopped for infrastructure retry")
	return err
}

func (r *workflowRun) stopAfterPersistence() (bool, error) {
	if r.ctx.Err() != nil {
		r.stopTaskPersistence(nil, true, false)
	}
	stopped, err := r.workflowRunStopResult()
	if !stopped {
		return false, nil
	}
	r.skipExitAck = true
	if err != nil {
		r.failureReason = err.Error()
		r.span.RecordError(err)
		r.span.SetStatus(codes.Error, "Workflow persistence state is uncertain")
	}
	return true, err
}

func (r *workflowRun) run(concurrency int) error {
	ctx, span := r.ctx, r.span
	logger := klog.FromContext(ctx)
	defer func() {
		if r.suppressTerminalCallback || r.terminalCallbackSuppressed() {
			return
		}
		status := r.snapshotTask().Status
		r.triggerWorkflowCallbackOnce(ctx, status, r.failureReason)
	}()
	if stopped, err := r.stopForInfrastructureStop(); stopped {
		return err
	}

	// 将工作流的状态更改为运行中
	r.mutateTask(func(task *model.WorkflowQueue) {
		task.Status = config.StatusRunning
		if task.CreateTime.IsZero() {
			task.CreateTime = time.Now()
		}
	})
	r.ack()
	if stopped, err := r.stopAfterPersistence(); stopped {
		return err
	}
	if stopped, err := r.stopForInfrastructureStop(); stopped {
		return err
	}
	logger.Info("Starting workflow", "status", r.snapshotTask().Status)

	defer func() {
		finalTask := r.snapshotTask()
		logger.Info("Finished workflow", "status", finalTask.Status)
		if r.skipExitAck {
			return
		}
		// Approval checkpoint is already persisted in pauseAtApprovalStep.
		// Skip exit ack here to avoid overwriting an approval action that may
		// have been applied by API while notification was in-flight.
		if finalTask.Status == config.StatusWaitingApprove && finalTask.ApprovalPending {
			return
		}
		r.ack()
	}()

	taskForGeneration := r.snapshotTask()
	stepExecutions, err := GenerateJobTasks(ctx, &taskForGeneration, r.Store, r.defaultJobTimeoutSeconds, r.runtimeConfig)
	if err != nil {
		runErr := errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("restore workflow job executions: %w", err))
		r.skipExitAck = true
		span.RecordError(runErr)
		span.SetStatus(codes.Error, "Failed to restore workflow job executions")
		logger.Error(runErr, "Failed to restore workflow job executions")
		r.failureReason = runErr.Error()
		return runErr
	}
	if r.workspaceManager != nil {
		for _, step := range stepExecutions {
			if step.generationError != nil {
				continue
			}
			for _, tasks := range step.Jobs {
				for _, task := range tasks {
					if _, err := r.prepareJobTask(task, taskForGeneration.AppID); err != nil {
						r.failureReason = err.Error()
						r.suppressTerminalCallback = true
						r.mutateTask(func(t *model.WorkflowQueue) {
							t.Status = config.StatusFailed
						})
						return err
					}
				}
			}
		}
	}
	return r.runSteps(taskForGeneration, stepExecutions, concurrency)
}

func (r *workflowRun) runSteps(taskForGeneration model.WorkflowQueue, stepExecutions []StepExecution, concurrency int) error {
	ctx, span := r.ctx, r.span
	// Generation failure replaces the plan, so it precedes any saved checkpoint
	// and must keep its cause instead of running a synthetic cleanup payload.
	if len(stepExecutions) == 1 && stepExecutions[0].generationError != nil {
		err := stepExecutions[0].generationError
		for _, task := range stepExecutions[0].Jobs[config.JobPriorityLow] {
			controller := jobcleanup.NewCleanupResourcesJobCtl(task, &jobexec.Runtime{Client: r.Client, Store: r.Store})
			if persistErr := controller.SaveInfo(ctx); persistErr != nil {
				return r.stopForJobInfrastructure(fmt.Errorf("persist workflow generation failure: %w", persistErr))
			}
		}
		r.setTerminalStatus(config.StatusFailed, err.Error())
		r.failureReason = r.snapshotTerminalReason()
		span.RecordError(err)
		span.SetStatus(codes.Error, "Workflow generation failed")
		return err
	}
	for _, execution := range stepExecutions {
		for _, tasks := range execution.Jobs {
			for _, task := range tasks {
				if task.JobType == string(config.JobEval) && task.EvaluationInfo != "" && r.runtimeConfig != nil && r.runtimeConfig.Jobs != nil {
					ctx = workspace.WithEvaluationRunner(ctx, task.Name, r.runtimeConfig.Jobs.RunnerImage)
				}
			}
		}
	}
	logger := klog.FromContext(ctx)
	workflowName := taskForGeneration.WorkflowName
	namespaceReady := false
	seqLimit := max(1, concurrency)
	startStep := max(0, taskForGeneration.CurrentStep)
	if startStep >= len(stepExecutions) {
		if stopped, err := r.stopForInfrastructureStop(); stopped {
			return err
		}
		span.SetStatus(codes.Ok, "Workflow completed successfully")
		r.updateWorkflowStatus(ctx)
		if stopped, err := r.stopAfterPersistence(); stopped {
			return err
		}
		r.skipExitAck = true
		return nil
	}

	for stepIdx := startStep; stepIdx < len(stepExecutions); stepIdx++ {
		if stopped, err := r.stopForInfrastructureStop(); stopped {
			return err
		}
		stepExec := stepExecutions[stepIdx]
		if stepExec.StepType == config.WorkflowStepTypeApproval {
			paused, pauseErr := r.pauseAtApprovalStep(ctx, &stepExec, stepIdx)
			if pauseErr != nil {
				r.skipExitAck = true
				span.RecordError(pauseErr)
				span.SetStatus(codes.Error, "Failed to persist approval checkpoint")
				r.failureReason = pauseErr.Error()
				return pauseErr
			}
			if paused {
				span.SetStatus(codes.Ok, "Workflow paused waiting for approval")
			} else {
				// CAS condition mismatch means task state changed concurrently (e.g. cancelled).
				// Skip exit ack to avoid writing stale in-memory snapshot back to store.
				r.skipExitAck = true
			}
			return nil
		}
		if stepExec.Jobs == nil {
			if stopped, err := r.stopForInfrastructureStop(); stopped {
				return err
			}
			r.setCurrentStep(stepIdx + 1)
			r.ack()
			if stopped, err := r.stopAfterPersistence(); stopped {
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
			if !namespaceReady {
				var err error
				namespaceReady, err = r.ensureWorkspaceForJobs(tasksInPriority, taskForGeneration.AppID)
				if err != nil {
					return err
				}
			}
			// Evaluation may occur after ordinary deployment steps have already
			// initialized the namespace.
			if err := r.ensureEvaluationRunner(tasksInPriority); err != nil {
				return err
			}
			stepConcurrency := determineStepConcurrency(stepExec.Mode, len(tasksInPriority), seqLimit)
			// Fix: StepByStep mode should stop on first failure (stopOnFailure=true)
			// Parallel mode continues all jobs even if some fail (stopOnFailure=false)
			stopOnFailure := !stepExec.Mode.IsParallel()
			logger.Info("Executing workflow step", "workflowName", workflowName, "step", stepExec.Name, "mode", stepExec.Mode, "priority", priority, "jobCount", len(tasksInPriority), "concurrency", stepConcurrency, "stopOnFailure", stopOnFailure)

			recoveryCtx := job.WithEvaluationRecovery(ctx, func(recoveryCtx context.Context, task *model.JobTask) (bool, error) {
				svc, err := evaluationjobs.New(r.Store, r.Client, r.runtimeConfig)
				if err != nil {
					return false, err
				}
				if r.KubeConfig != nil {
					svc.SandboxClient, err = dynamic.NewForConfig(r.KubeConfig)
					if err != nil {
						return false, err
					}
				}
				return svc.RecoverEvaluation(recoveryCtx, task)
			})
			if err := r.runJobs(recoveryCtx, tasksInPriority, stepConcurrency, r.ack, stopOnFailure); err != nil {
				logger.Error(err, "Stopping workflow after job persistence failure", "step", stepExec.Name, "priority", priority)
				return r.stopForJobInfrastructure(err)
			}
			if stopped, err := r.stopAfterPersistence(); stopped {
				return err
			}
			if stopped, err := r.stopForInfrastructureStop(); stopped {
				return err
			}

			cleanupTrigger := workflowFailureCleanupTrigger(stepExec.FailurePolicy, tasksInPriority)
			for _, task := range tasksInPriority {
				if !isJobSuccessStatus(task) {
					reason := workflowFailureReason(workflowName, task, cleanupTrigger)
					if cleanupTrigger != nil {
						cleanupErr := r.runWorkflowFailureCleanup(ctx, logger)
						if stopped, err := r.stopAfterPersistence(); stopped {
							return err
						}
						if cleanupErr != nil {
							if errors.Is(cleanupErr, signal.ErrInfrastructureStop) {
								logger.Error(cleanupErr, "Stopping workflow after cleanup job persistence failure")
								return r.stopForJobInfrastructure(cleanupErr)
							}
							reason = fmt.Sprintf("%s; cleanup_all failed: %v", reason, cleanupErr)
						}
					}
					err := errors.New(reason)
					logger.Error(err, "Workflow failed at job, aborting.", "step", stepExec.Name, "priority", priority, "jobName", task.Name, "jobStatus", task.Status)
					if task.Status == config.StatusCancelled {
						r.setTerminalStatus(config.StatusCancelled, reason)
						span.SetStatus(codes.Error, "Workflow cancelled")
					} else {
						r.setTerminalStatus(config.StatusFailed, reason)
						span.SetStatus(codes.Error, "Workflow failed")
					}
					span.RecordError(err)
					r.failureReason = r.snapshotTerminalReason()
					return err
				}
			}
		}
		if stopped, err := r.stopForInfrastructureStop(); stopped {
			return err
		}
		r.setCurrentStep(stepIdx + 1)
		r.ack()
		if stopped, err := r.stopAfterPersistence(); stopped {
			return err
		}
		logger.Info("Workflow step completed successfully", "workflowName", workflowName, "step", stepExec.Name)
	}

	if stopped, err := r.stopForInfrastructureStop(); stopped {
		return err
	}
	span.SetStatus(codes.Ok, "Workflow completed successfully")
	r.updateWorkflowStatus(ctx)
	if stopped, err := r.stopAfterPersistence(); stopped {
		return err
	}
	r.skipExitAck = true
	return nil
}

func (r *workflowRun) ensureWorkspaceForJobs(tasks []*model.JobTask, appID string) (bool, error) {
	if r.workspaceManager == nil {
		return false, nil
	}
	requiresNamespace := false
	for _, task := range tasks {
		deploy, _ := r.prepareJobTask(task, appID)
		requiresNamespace = requiresNamespace || deploy
	}
	if !requiresNamespace {
		return false, nil
	}
	if err := r.workspaceManager.Ensure(r.ctx, r.workspace); err != nil {
		r.failureReason = err.Error()
		r.suppressTerminalCallback = true
		r.mutateTask(func(t *model.WorkflowQueue) { t.Status = config.StatusFailed })
		return false, fmt.Errorf("initialize workspace before deployment: %w", err)
	}
	return true, nil
}

func (r *workflowRun) ensureEvaluationRunner(tasks []*model.JobTask) error {
	if r.workspaceManager == nil {
		return nil
	}
	for _, task := range tasks {
		if task.JobType != string(config.JobEval) || isWorkflowTerminal(task.Status) || task.Status == config.StatusSkipped {
			continue
		}
		if r.runtimeConfig == nil || r.runtimeConfig.Jobs == nil {
			return fmt.Errorf("evaluation runner configuration is required")
		}
		if err := r.workspaceManager.EnsureEvaluationRunner(r.ctx, r.workspace, r.runtimeConfig.Jobs.RunnerEgress...); err != nil {
			r.failureReason = err.Error()
			r.suppressTerminalCallback = true
			r.mutateTask(func(t *model.WorkflowQueue) { t.Status = config.StatusFailed })
			return fmt.Errorf("initialize evaluation runner: %w", err)
		}
		break
	}
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

func shouldCleanupAllOnWorkflowFailure(policy workflowconfig.WorkflowFailurePolicy, task *model.JobTask) bool {
	if task == nil {
		return false
	}
	effectivePolicy := policy
	if override, ok := workflowconfig.NormalizeJobFailurePolicy(task.FailurePolicy); ok && override != "" {
		effectivePolicy = override
	}
	normalized, ok := workflowconfig.NormalizeWorkflowFailurePolicy(effectivePolicy)
	if !ok || normalized != workflowconfig.WorkflowFailurePolicyCleanupAll {
		return false
	}
	if task.Status != config.StatusFailed && task.Status != config.StatusTimeout {
		return false
	}
	return isWorkflowFailureCleanupTriggerJob(config.JobType(task.JobType))
}

func workflowFailureCleanupTrigger(policy workflowconfig.WorkflowFailurePolicy, tasks []*model.JobTask) *model.JobTask {
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
		config.JobDeployInstant,
		config.JobDeployScheduled,
		config.JobDeployCloud:
		return true
	default:
		return false
	}
}

func (w *WorkflowCtl) runJobs(ctx context.Context, tasks []*model.JobTask, concurrency int, ack func(), stopOnFailure bool) error {
	return job.RunJobs(ctx, tasks, &jobexec.Runtime{
		Concurrency:       concurrency,
		Client:            w.Client,
		Store:             w.Store,
		Ack:               ack,
		StopOnFailure:     stopOnFailure,
		RedisClient:       w.RedisClient,
		Cache:             w.Cache,
		URLSecurityPolicy: w.urlSecurityPolicy,
		DelayQueue:        w.DelayQueue,
		ResourceWaiter:    w.ResourceWaiter,
		KubeConfig:        w.KubeConfig,
	})
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
	cleanupCtx := klog.NewContext(ctx, logger.WithValues("failurePolicy", workflowconfig.WorkflowFailurePolicyCleanupAll))
	cleanupCtx = jobexec.WithTaskMetadata(cleanupCtx, task.TaskID)
	logger.Info("Running workflow failure cleanup jobs", "appID", task.AppID, "taskID", task.TaskID, "jobCount", len(cleanupJobs))
	if err := w.runJobs(cleanupCtx, cleanupJobs, 1, w.ack, false); err != nil {
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

func (w *WorkflowCtl) prepareWorkspace(ctx context.Context) (context.Context, error) {
	if w.accountConfig == nil {
		return ctx, fmt.Errorf("workspace configuration is required")
	}
	task := w.snapshotTask()
	workspaceID := ""
	expectedNamespace := ""
	if task.Type == config.WorkflowTaskTypeJob {
		if task.AppID != "" || task.WorkflowID != "" {
			return ctx, fmt.Errorf("workspace Job has application or workflow ownership")
		}
		workspaceID = strings.TrimSpace(task.WorkspaceID)
	} else {
		app := &model.Applications{ID: task.AppID}
		if err := w.Store.Get(ctx, app); err != nil {
			return ctx, err
		}
		if app.WorkspaceID == "" {
			return ctx, fmt.Errorf("application has no workspace")
		}
		workspaceID = app.WorkspaceID
		expectedNamespace = app.Namespace
	}
	if workspaceID == "" {
		return ctx, fmt.Errorf("workflow task has no workspace")
	}
	space := &model.Workspace{ID: workspaceID}
	if err := w.Store.Get(ctx, space); err != nil {
		return ctx, err
	}
	if task.Type == config.WorkflowTaskTypeJob {
		expectedNamespace = space.Namespace
	}
	if expectedNamespace == "" || expectedNamespace != space.Namespace {
		return ctx, fmt.Errorf("workflow task namespace does not match workspace")
	}
	manager := w.workspaceManager
	if manager == nil {
		manager = &workspace.Manager{Client: w.Client, RESTConfig: w.KubeConfig, Config: w.accountConfig.Workspace}
	}
	w.workspaceManager, w.workspace = manager, space
	// Ordinary workflow queues predate explicit workspace IDs. Resolve the
	// canonical application workspace before generating any Job or callback.
	w.mutateTask(func(task *model.WorkflowQueue) { task.WorkspaceID = space.ID })
	client, restConfig, err := manager.TenantClient(space)
	if err != nil {
		return ctx, err
	}
	w.Client, w.KubeConfig = client, restConfig
	w.Store = access.NewStore(w.Store)
	ctx = access.WithScope(ctx, access.ForWorkspace(space))
	if task.Type == config.WorkflowTaskTypeJob {
		var definition spec.JobSpec
		if err := spec.DecodeJobJSON([]byte(task.JobSpec), &definition); err != nil {
			return ctx, fmt.Errorf("decode workspace Job definition: %w", err)
		}
		if err := definition.Normalize(); err != nil {
			return ctx, fmt.Errorf("validate workspace Job definition: %w", err)
		}
		for _, stage := range definition.Stages {
			for _, step := range stage.Steps {
				if step.Traits.Evaluation != nil && (w.runtimeConfig == nil || w.runtimeConfig.Jobs == nil || w.runtimeConfig.Jobs.RunnerImage == "") {
					return ctx, fmt.Errorf("evaluation runner configuration is required for stage %q step %q", stage.Name, step.Name)
				}
			}
		}
	}
	return ctx, nil
}

func (w *WorkflowCtl) prepareJobTask(task *model.JobTask, appID string) (bool, error) {
	if task.JobType == string(config.JobEval) {
		if isWorkflowTerminal(task.Status) || task.Status == config.StatusSkipped {
			return false, nil
		}
		if w.runtimeConfig == nil || w.runtimeConfig.Jobs == nil {
			return false, fmt.Errorf("evaluation runner configuration is required")
		}
		return true, evaluationjobs.PrepareEvaluationTask(task, w.workspace, w.accountConfig.Workspace, w.runtimeConfig.Jobs.RunnerImage)
	}
	return jobexec.PrepareTask(task, appID, w.workspace, w.accountConfig.Workspace)
}
