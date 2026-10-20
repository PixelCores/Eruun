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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type approvalUpdateContextMode int

const (
	approvalUpdateContextInherit approvalUpdateContextMode = iota
	approvalUpdateContextDetached
)

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

	var timeout time.Duration
	if stepExec.Approval != nil && stepExec.Approval.TimeoutSeconds > 0 {
		timeout = time.Duration(stepExec.Approval.TimeoutSeconds) * time.Second
	}
	persisted, err := w.persistApprovalCheckpoint(taskID, stepName, stepIndex, timeout)
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
	w.triggerApprovalNotification(ctx, stepExec, stepName)
	return true, nil
}

func (w *WorkflowCtl) persistApprovalCheckpoint(taskID, stepName string, stepIndex int, timeout time.Duration) (bool, error) {
	updateCtx, cancel := approvalUpdateContext(context.Background(), approvalUpdateContextDetached)
	defer cancel()
	taskSnapshot := w.snapshotTask()
	taskSnapshot.TaskID = taskID
	return repository.PersistApprovalCheckpoint(updateCtx, w.Store, &taskSnapshot, stepName, stepIndex, timeout)
}

func (w *WorkflowCtl) reloadTaskSnapshot(taskID string) error {
	updateCtx, cancel := approvalUpdateContext(context.Background(), approvalUpdateContextDetached)
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
		Name:            callbackName,
		SchedulingClass: stepExec.SchedulingClass,
		WorkspaceID:     task.WorkspaceID,
		WorkflowID:      task.WorkflowID,
		ProjectID:       task.ProjectID,
		AppID:           task.AppID,
		TaskID:          task.TaskID,
		JobType:         string(config.JobDeployCallback),
		ExecutionKey:    workflowJobExecutionKey(&task, -1, 0, 0, callbackName+"|"+stepName, string(config.JobDeployCallback)),
		RunGeneration:   task.RunGeneration,
		OwnerStatus:     task.Status,
		RunToken:        task.RunToken,
		WorkerID:        task.WorkerID,
		JobInfo: &job.CallbackJobInfo{
			Event:          approvalEventName,
			URL:            targetURL,
			Method:         method,
			Headers:        approval.Headers,
			TimeoutSeconds: int64(approvalNotifyTimeout / time.Second),
			TimeoutMaxNS:   int64(approvalNotifyTimeout),
			Payload:        payload,
		},
		Status: config.StatusWaiting,
	}
	if w.workspace != nil {
		callbackJob.Namespace = w.workspace.Namespace
	}
	job.ApplyExecutionIdentity(callbackJob)
	callbackCtx, cancel := approvalNotificationContext(job.WithTaskMetadata(context.WithoutCancel(ctx), ""), approvalNotifyTimeout)
	go func() {
		defer cancel()
		// Notification jobs should not mutate workflow queue state.
		if err := w.runJobs(callbackCtx, []*model.JobTask{callbackJob}, 1, func() {}, false); err != nil {
			klog.ErrorS(err, "workflow callback execution failed", "taskID", callbackJob.TaskID, "jobName", callbackJob.Name)
		}
	}()
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
