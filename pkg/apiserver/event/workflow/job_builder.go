package workflow

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type StepExecution struct {
	Name          string
	Mode          config.WorkflowMode
	StepType      config.WorkflowStepType
	FailurePolicy workflowconfig.WorkflowFailurePolicy
	Approval      *ApprovalExecution
	Jobs          map[int][]*model.JobTask
}

type ApprovalExecution struct {
	NotifyURL      string
	Message        string
	Method         string
	Headers        map[string]string
	TimeoutSeconds int64
}

const versionUpdateCleanupStepName = "cleanup-removed-components"

func GenerateJobTasks(ctx context.Context, task *model.WorkflowQueue, ds datastore.DataStore, defaultJobTimeoutSeconds int64) ([]StepExecution, error) {
	logger := klog.FromContext(ctx)
	workflowSteps, componentMap, err := loadWorkflowJobTaskInputs(ctx, task, ds)
	if err != nil {
		logger.Error(err, "Failed to prepare workflow job tasks", "workflowID", task.WorkflowID, "appID", task.AppID)
		return []StepExecution{*failedWorkflowGenerationExecution(task, defaultJobTimeoutSeconds, err)}, nil
	}
	if versionUpdateCleanupOnlyFromTask(task) {
		workflowSteps = leadingApprovalWorkflowSteps(workflowSteps)
	}
	resourceActionInfo, hasResourceActions, err := versionUpdateResourceActionInfoFromTask(task)
	if err != nil {
		logger.Error(err, "Failed to prepare workflow resource actions", "workflowID", task.WorkflowID, "appID", task.AppID)
		return []StepExecution{*failedWorkflowGenerationExecution(task, defaultJobTimeoutSeconds, err)}, nil
	}
	if hasResourceActions && resourceActionInfo.RestartOnly {
		workflowSteps = leadingApprovalWorkflowSteps(workflowSteps)
	}
	if hasResourceActions {
		workflowSteps, err = filterVersionUpdateExecutionScopeWorkflowSteps(workflowSteps, resourceActionInfo)
		if err != nil {
			logger.Error(err, "Failed to prepare workflow execution scope", "workflowID", task.WorkflowID, "appID", task.AppID)
			return []StepExecution{*failedWorkflowGenerationExecution(task, defaultJobTimeoutSeconds, err)}, nil
		}
	}
	failurePolicy, ok := workflowconfig.NormalizeWorkflowFailurePolicy(workflowSteps.FailurePolicy)
	if !ok {
		err := fmt.Errorf("workflow %s has unsupported failurePolicy %q", task.WorkflowID, workflowSteps.FailurePolicy)
		logger.Error(err, "Failed to prepare workflow job tasks", "workflowID", task.WorkflowID, "appID", task.AppID)
		return []StepExecution{*failedWorkflowGenerationExecution(task, defaultJobTimeoutSeconds, err)}, nil
	}

	stepGroups := buildWorkflowStepExecutionGroups(ctx, workflowSteps, componentMap, task, defaultJobTimeoutSeconds)
	stepGroups, err = augmentAdoptedDependencyJobs(ctx, stepGroups, task, ds, defaultJobTimeoutSeconds)
	if err != nil {
		logger.Error(err, "Failed to prepare adopted dependency jobs", "workflowID", task.WorkflowID, "appID", task.AppID)
		return []StepExecution{*failedWorkflowGenerationExecution(task, defaultJobTimeoutSeconds, err)}, nil
	}
	cleanupExecutions := buildPersistedCleanupExecutions(ctx, task, ds, defaultJobTimeoutSeconds, len(workflowSteps.Steps), logger)
	executions, totalJobs := mergeStepExecutionsWithCleanup(stepGroups, cleanupExecutions, task, logger)
	if hasResourceActions {
		restartExecution, err := buildVersionUpdateRestartExecution(resourceActionInfo, componentMap, task, defaultJobTimeoutSeconds)
		if err != nil {
			logger.Error(err, "Failed to prepare version restart jobs", "workflowID", task.WorkflowID, "appID", task.AppID)
			return []StepExecution{*failedWorkflowGenerationExecution(task, defaultJobTimeoutSeconds, err)}, nil
		}
		if restartExecution != nil && !bucketsEmpty(restartExecution.Jobs) {
			executions = append(executions, *restartExecution)
			totalJobs += countJobs(restartExecution.Jobs)
			logGeneratedJobs(logger, task.WorkflowName, restartExecution.Name, restartExecution.Mode, restartExecution.Jobs)
		}
	}
	applyWorkflowFailurePolicyToExecutions(executions, failurePolicy)
	applyWorkflowExecutionIdentity(executions, task)
	if err := restoreCommittedJobExecutions(ctx, executions, task, ds); err != nil {
		logger.Error(err, "Failed to restore committed job executions", "workflowID", task.WorkflowID, "appID", task.AppID)
		return nil, err
	}

	logger.Info("Generated total jobs for workflow", "totalJobs", totalJobs, "workflowName", task.WorkflowName)
	return executions, nil
}

func applyWorkflowExecutionIdentity(executions []StepExecution, task *model.WorkflowQueue) {
	if task == nil {
		return
	}
	for stepIndex := range executions {
		for priority, jobs := range executions[stepIndex].Jobs {
			applyWorkflowJobExecutionIdentity(jobs, task, stepIndex, priority)
		}
	}
}

func applyWorkflowJobExecutionIdentity(jobs []*model.JobTask, task *model.WorkflowQueue, stepIndex, priority int) {
	if task == nil {
		return
	}
	for jobIndex, jobTask := range jobs {
		if jobTask == nil {
			continue
		}
		jobTask.ExecutionKey = workflowJobExecutionKey(task, stepIndex, priority, jobIndex, jobTask.Name, jobTask.JobType)
		jobTask.RunGeneration = task.RunGeneration
		jobTask.OwnerRunGeneration = task.RunGeneration
		jobTask.OwnerStatus = task.Status
		jobTask.RunToken = task.RunToken
		jobTask.WorkerID = task.WorkerID
		jobTask.Attempt = uint(jobTask.RetryCount + 1)
		workflowjob.ApplyExecutionIdentity(jobTask)
	}
}

func workflowJobExecutionKey(task *model.WorkflowQueue, stepIndex, priority, jobIndex int, name, jobType string) string {
	if task == nil {
		return ""
	}
	return workflowExecutionKey(task.TaskID, task.RunGeneration, stepIndex, priority, jobIndex, name, jobType)
}

func workflowExecutionKey(taskID string, generation uint64, stepIndex, priority, jobIndex int, name, jobType string) string {
	identity := fmt.Sprintf("%s|%d|%d|%d|%d|%s|%s", taskID, generation, stepIndex, priority, jobIndex, name, jobType)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
}

func restoreCommittedJobExecutions(ctx context.Context, executions []StepExecution, task *model.WorkflowQueue, ds datastore.DataStore) error {
	if task == nil || task.RunGeneration == 0 || task.TaskID == "" {
		return nil
	}
	if ds == nil {
		return fmt.Errorf("restore committed job executions: datastore is nil")
	}
	entities, err := ds.List(ctx, &model.JobInfo{TaskID: task.TaskID}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{{
				Key: "status",
				Values: []string{
					string(config.StatusDistributed), string(config.StatusCompleted),
					string(config.StatusPassed), string(config.StatusSkipped),
					string(config.StatusFailed), string(config.StatusTimeout),
					string(config.StatusCancelled), string(config.StatusReject),
				},
			}},
		},
	})
	if err != nil {
		return fmt.Errorf("list committed jobs: %w", err)
	}
	for stepIndex := range executions {
		for priority, jobs := range executions[stepIndex].Jobs {
			for jobIndex, jobTask := range jobs {
				if jobTask == nil {
					continue
				}
				var selected *model.JobInfo
				for _, entity := range entities {
					jobInfo, ok := entity.(*model.JobInfo)
					if !ok || jobInfo == nil {
						return datastore.ErrEntityInvalid
					}
					status := config.Status(strings.TrimSpace(jobInfo.Status))
					if !isRestorableCommittedJobStatus(jobTask, status) || jobInfo.ExecutionKey == nil ||
						jobInfo.RunGeneration == 0 || jobInfo.RunGeneration > task.RunGeneration {
						continue
					}
					expectedKey := workflowExecutionKey(task.TaskID, jobInfo.RunGeneration, stepIndex, priority, jobIndex, jobTask.Name, jobTask.JobType)
					if *jobInfo.ExecutionKey != expectedKey || (selected != nil && selected.RunGeneration >= jobInfo.RunGeneration) {
						continue
					}
					selected = jobInfo
				}
				if selected != nil {
					jobTask.ExecutionKey = *selected.ExecutionKey
					jobTask.RunGeneration = selected.RunGeneration
					jobTask.Status = config.Status(strings.TrimSpace(selected.Status))
					jobTask.StartTime = selected.StartTime
					jobTask.EndTime = selected.EndTime
					jobTask.Info = selected.Info
					jobTask.InternalInfo = selected.InternalInfo
					jobTask.Error = selected.Error
					jobTask.DelayState = selected.DelayState
					jobTask.DelayExecuteAt = selected.DelayExecuteAt
					jobTask.DelayPayload = selected.DelayPayload
					if selected.Attempt > 0 {
						jobTask.Attempt = selected.Attempt
					}
				}
			}
		}
	}
	return nil
}

func isRestorableCommittedJobStatus(jobTask *model.JobTask, status config.Status) bool {
	switch status {
	case config.StatusCompleted,
		config.StatusPassed,
		config.StatusSkipped,
		config.StatusFailed,
		config.StatusTimeout,
		config.StatusCancelled,
		config.StatusReject:
		return true
	case config.StatusDistributed:
		if jobTask == nil {
			return false
		}
		jobType := config.JobType(jobTask.JobType)
		return jobType == config.JobDeployInstant || jobType == config.JobDeployScheduled
	default:
		return false
	}
}

func applyWorkflowFailurePolicyToExecutions(executions []StepExecution, policy workflowconfig.WorkflowFailurePolicy) {
	for idx := range executions {
		executions[idx].FailurePolicy = policy
	}
}

func leadingApprovalWorkflowSteps(steps *model.WorkflowSteps) *model.WorkflowSteps {
	if steps == nil {
		return &model.WorkflowSteps{}
	}
	filtered := &model.WorkflowSteps{
		FailurePolicy: steps.FailurePolicy,
		Steps:         make([]*model.WorkflowStep, 0, len(steps.Steps)),
	}
	for _, step := range steps.Steps {
		if step == nil {
			continue
		}
		if config.ParseWorkflowStepType(string(step.StepType)) != config.WorkflowStepTypeApproval {
			break
		}
		filtered.Steps = append(filtered.Steps, step)
	}
	return filtered
}

func mergeStepExecutionsWithCleanup(stepGroups [][]StepExecution, cleanupExecutions map[int]*StepExecution, task *model.WorkflowQueue, logger klog.Logger) ([]StepExecution, int) {
	var executions []StepExecution
	totalJobs := 0
	appendExecution := func(execution StepExecution) {
		executions = append(executions, execution)
		if bucketsEmpty(execution.Jobs) {
			return
		}
		totalJobs += countJobs(execution.Jobs)
		logGeneratedJobs(logger, task.WorkflowName, execution.Name, execution.Mode, execution.Jobs)
	}
	appendCleanupExecution := func(index int) {
		cleanupExecution := cleanupExecutions[index]
		if cleanupExecution == nil || bucketsEmpty(cleanupExecution.Jobs) {
			return
		}
		appendExecution(*cleanupExecution)
	}

	for stepIndex, group := range stepGroups {
		appendCleanupExecution(stepIndex)
		for _, execution := range group {
			appendExecution(execution)
		}
	}
	appendCleanupExecution(len(stepGroups))
	return executions, totalJobs
}

func logGeneratedJobs(logger klog.Logger, workflowName, stepName string, mode config.WorkflowMode, buckets map[int][]*model.JobTask) {
	for priority, jobs := range buckets {
		if len(jobs) == 0 {
			continue
		}
		logger.Info("Generated jobs for workflow step", "workflowName", workflowName, "step", stepName, "mode", mode, "priority", priority, "jobCount", len(jobs))
		for _, j := range jobs {
			logger.Info("Generated job details", "workflowName", workflowName, "step", stepName, "jobName", j.Name, "jobType", j.JobType, "priority", priority)
		}
	}
}
