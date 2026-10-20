package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	signal "github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func (w *WorkflowCtl) triggerWorkflowCallbackOnce(ctx context.Context, status config.Status, reason string) {
	if !isWorkflowTerminal(status) {
		return
	}
	w.terminalCallbackOnce.Do(func() {
		w.triggerWorkflowCallback(ctx, status, reason)
	})
}

func (w *WorkflowCtl) triggerWorkflowCallback(ctx context.Context, status config.Status, reason string) {
	if w == nil || w.Store == nil {
		return
	}
	if !isWorkflowTerminal(status) {
		return
	}
	task := w.snapshotTask()
	if status == config.StatusCancelled && strings.TrimSpace(reason) == "" && wf.IsTerminalCallbackPending(task.SchedulingReason) {
		reason = wf.TerminalCallbackReason(task.SchedulingReason)
	}
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
	event, targetURL, method := callback.TerminalTarget(status)
	if targetURL == "" {
		return
	}
	payload := jobinfo.CallbackPayload{
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
		WorkspaceID:   task.WorkspaceID,
		WorkflowID:    task.WorkflowID,
		ProjectID:     task.ProjectID,
		AppID:         task.AppID,
		TaskID:        task.TaskID,
		JobType:       string(config.JobDeployCallback),
		ExecutionKey:  job.TerminalCallbackExecutionKey(task.TaskID, task.RunGeneration, job.TerminalCallbackKeyEvent(status, event)),
		RunGeneration: task.RunGeneration,
		OwnerStatus:   status,
		RunToken:      task.RunToken,
		WorkerID:      task.WorkerID,
		JobInfo: &jobinfo.CallbackJobInfo{
			Event:          event,
			URL:            targetURL,
			Method:         method,
			Headers:        callback.Headers,
			TimeoutSeconds: workflowconfig.ClampWorkflowCallbackTimeoutSeconds(callback.TimeoutSeconds, w.callbackTimeoutMax),
			TimeoutMaxNS:   int64(w.callbackTimeoutMax),
			Payload:        payload,
		},
		Status: config.StatusWaiting,
	}
	if w.workspace != nil {
		callbackJob.Namespace = w.workspace.Namespace
	}
	jobinfo.ApplyExecutionIdentity(callbackJob)
	callbackParent, stopCallbackParent := terminalCallbackParentContext(ctx, status)
	defer stopCallbackParent()
	callbackCtx, cancel := callbackContext(callbackParent, callback.TimeoutSeconds, w.callbackTimeoutMax)
	defer cancel()
	if err := w.runTerminalCallbackJob(callbackCtx, &task, callbackJob); err != nil {
		klog.ErrorS(err, "workflow callback execution failed", "taskID", callbackJob.TaskID, "jobName", callbackJob.Name)
	}
}

func terminalCallbackParentContext(ctx context.Context, status config.Status) (context.Context, context.CancelFunc) {
	if status != config.StatusCancelled || ctx == nil {
		return ctx, func() {}
	}
	runtimeCtx, ok := ctx.Value(workflowRuntimeContextKey{}).(context.Context)
	if !ok || runtimeCtx == nil {
		return ctx, func() {}
	}
	parent, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		select {
		case <-runtimeCtx.Done():
			cancel(context.Cause(runtimeCtx))
		case <-done:
		}
	}()
	return parent, func() {
		close(done)
		cancel(nil)
	}
}

func (w *WorkflowCtl) runTerminalCallbackJob(ctx context.Context, task *model.WorkflowQueue, callbackJob *model.JobTask) error {
	run := func(runCtx context.Context) error {
		return w.runJobs(runCtx, []*model.JobTask{callbackJob}, 1, func() {}, false)
	}
	if task == nil || task.Status != config.StatusCancelled || task.RunGeneration == 0 || task.RunToken == "" || task.WorkerID == "" {
		return run(ctx)
	}
	leaseDuration := workflowconfig.DefaultWorkflowLeaseDuration
	if w.runtimeConfig != nil && w.runtimeConfig.Workflow.LeaseDuration > 0 {
		leaseDuration = w.runtimeConfig.Workflow.LeaseDuration
	}
	renew := func(renewCtx context.Context) error {
		renewed, err := repository.RenewCancelledWorkflowTaskLease(
			renewCtx, w.Store, task.TaskID, task.RunGeneration, task.RunToken, task.WorkerID, leaseDuration,
		)
		if err != nil {
			return err
		}
		if !renewed {
			return repository.ErrWorkflowOwnershipLost
		}
		return nil
	}
	if err := renew(ctx); err != nil {
		return fmt.Errorf("renew cancelled workflow callback lease: %w", err)
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan error, 1)
	interval := leaseDuration / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(runCtx, config.TaskStateTransitionTimeout)
				err := renew(renewCtx)
				renewCancel()
				if err != nil {
					wrapped := fmt.Errorf("renew cancelled workflow callback lease: %w", err)
					cancel(errors.Join(signal.ErrInfrastructureStop, wrapped))
					done <- wrapped
					return
				}
			}
		}
	}()
	runErr := run(runCtx)
	cancel(nil)
	return errors.Join(runErr, <-done)
}

func callbackContext(ctx context.Context, timeoutSeconds int64, timeoutMax time.Duration) (context.Context, context.CancelFunc) {
	timeout := workflowconfig.ResolveWorkflowCallbackTimeout(timeoutSeconds, timeoutMax)
	if ctx != nil && ctx.Err() == nil {
		return context.WithTimeout(ctx, timeout)
	}
	parent := context.Background()
	if ctx != nil {
		parent = jobexec.WithTaskMetadata(context.WithoutCancel(ctx), "")
	}
	return context.WithTimeout(parent, timeout)
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
