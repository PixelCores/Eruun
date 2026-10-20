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
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func (w *workflowServiceImpl) triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx context.Context, task *model.WorkflowQueue, status config.Status, reason string) {
	if w == nil || task == nil {
		return
	}
	taskSnapshot := *task
	callbackCtx := detachWorkflowCallbackParentContext(ctx)
	go func() {
		if _, err := w.reconcileWorkflowTerminalCallback(callbackCtx, &taskSnapshot, status, reason); err != nil {
			klog.ErrorS(err, "reconcile terminal workflow callback", "taskID", taskSnapshot.TaskID)
		}
	}()
}

func TriggerWorkflowTerminalCallbackAsync(ctx context.Context, store datastore.DataStore, cfg *config.Config, provider *urlpolicy.Provider, task *model.WorkflowQueue, status config.Status, reason string) {
	svc := &workflowServiceImpl{
		Store:                     store,
		Cfg:                       cfg,
		URLSecurityPolicyProvider: provider,
	}
	svc.triggerWorkflowTerminalCallbackOnApprovalActionAsync(ctx, task, status, reason)
}

func (w *workflowServiceImpl) triggerWorkflowTerminalCallbackOnApprovalAction(ctx context.Context, task *model.WorkflowQueue, status config.Status, reason string) error {
	if w == nil || w.Store == nil || task == nil {
		return nil
	}
	callback, err := w.loadWorkflowTerminalCallback(ctx, task)
	if err != nil || callback == nil {
		return err
	}
	return w.runWorkflowTerminalCallback(ctx, task, status, reason, callback)
}

// loadWorkflowTerminalCallback resolves the callback configuration once for sending.
func (w *workflowServiceImpl) loadWorkflowTerminalCallback(ctx context.Context, task *model.WorkflowQueue) (*model.WorkflowCallback, error) {
	workflowID := strings.TrimSpace(task.WorkflowID)
	parentCtx := inheritWorkflowCallbackParentContext(ctx)
	callbackSource := model.WorkflowCallbackSource(task, nil, nil)
	if callbackSource == nil {
		if workflowID == "" {
			return nil, nil
		}
		workflow := &model.Workflow{ID: workflowID}
		loadCtx, cancel := inheritWorkflowCallbackTimeout(parentCtx, 5*time.Second)
		defer cancel()
		if err := w.Store.Get(loadCtx, workflow); err != nil {
			klog.Errorf("load workflow %s for callback failed: %v", workflowID, err)
			return nil, fmt.Errorf("load workflow %s for callback: %w", workflowID, err)
		}
		callbackSource = model.WorkflowCallbackSource(nil, workflow, nil)
		if callbackSource == nil && strings.TrimSpace(task.AppID) != "" {
			app := &model.Applications{ID: task.AppID}
			if err := w.Store.Get(loadCtx, app); err != nil {
				klog.Errorf("load application %s for callback failed: %v", task.AppID, err)
				return nil, fmt.Errorf("load application %s for callback: %w", task.AppID, err)
			}
			callbackSource = model.WorkflowCallbackSource(nil, workflow, app)
		}
	}
	if callbackSource == nil {
		return nil, nil
	}
	var callback model.WorkflowCallback
	if err := decodeWorkflowCallbackForTerminal(callbackSource, &callback); err != nil {
		klog.Errorf("decode workflow %s callback failed: %v", workflowID, err)
		return nil, nil
	}
	return &callback, nil
}

func (w *workflowServiceImpl) runWorkflowTerminalCallback(ctx context.Context, task *model.WorkflowQueue, status config.Status, reason string, callback *model.WorkflowCallback) error {
	parentCtx := inheritWorkflowCallbackParentContext(ctx)
	event, targetURL, method := callback.TerminalTarget(status)
	if targetURL == "" {
		return nil
	}
	// API-side callbacks also execute under a persisted parent. An approval
	// cancellation may have no active Worker, including generation zero, but
	// must still match the terminal state that actually committed.
	loadCtx, loadCancel := inheritWorkflowCallbackTimeout(parentCtx, 5*time.Second)
	defer loadCancel()
	parent := &model.WorkflowQueue{TaskID: task.TaskID}
	if err := w.Store.Get(loadCtx, parent); err != nil {
		klog.ErrorS(err, "load terminal callback workflow", "taskID", task.TaskID)
		return fmt.Errorf("load terminal callback workflow %s: %w", task.TaskID, err)
	}
	if parent.Status != status || parent.RunGeneration != task.RunGeneration || parent.AppID != task.AppID {
		klog.ErrorS(repository.ErrWorkflowOwnershipLost, "terminal callback workflow changed", "taskID", task.TaskID)
		return repository.ErrWorkflowOwnershipLost
	}
	app := &model.Applications{ID: parent.AppID}
	if err := w.Store.Get(loadCtx, app); err != nil {
		klog.ErrorS(err, "load terminal callback application", "taskID", task.TaskID, "appID", parent.AppID)
		return fmt.Errorf("load terminal callback application %s: %w", parent.AppID, err)
	}
	if app.WorkspaceID == "" || app.Namespace == "" || (parent.WorkspaceID != "" && parent.WorkspaceID != app.WorkspaceID) {
		klog.ErrorS(repository.ErrWorkflowOwnershipLost, "terminal callback application workspace is invalid", "taskID", task.TaskID, "appID", parent.AppID)
		return repository.ErrWorkflowOwnershipLost
	}
	task = parent
	payload := workflowjob.CallbackPayload{
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
	callbackTimeoutMax := workflowconfig.ResolveWorkflowCallbackTimeoutMax(w.Cfg.WorkflowRuntime())
	callbackTimeoutSeconds := workflowconfig.ClampWorkflowCallbackTimeoutSeconds(callback.TimeoutSeconds, callbackTimeoutMax)
	callbackJob := &model.JobTask{
		Name:          fmt.Sprintf("workflow-callback-%s", task.TaskID),
		Namespace:     app.Namespace,
		WorkspaceID:   app.WorkspaceID,
		WorkflowID:    task.WorkflowID,
		ProjectID:     task.ProjectID,
		AppID:         task.AppID,
		TaskID:        task.TaskID,
		JobType:       string(config.JobDeployCallback),
		ExecutionKey:  workflowjob.TerminalCallbackExecutionKey(task.TaskID, task.RunGeneration, workflowjob.TerminalCallbackKeyEvent(status, event)),
		RunGeneration: task.RunGeneration,
		OwnerStatus:   task.Status,
		RunToken:      task.RunToken,
		WorkerID:      task.WorkerID,
		JobInfo: &workflowjob.CallbackJobInfo{
			Event:          event,
			URL:            targetURL,
			Method:         method,
			Headers:        callback.Headers,
			TimeoutSeconds: callbackTimeoutSeconds,
			TimeoutMaxNS:   int64(callbackTimeoutMax),
			Payload:        payload,
		},
		Status: config.StatusWaiting,
	}
	callbackTimeout := workflowconfig.ResolveWorkflowCallbackTimeout(callback.TimeoutSeconds, callbackTimeoutMax)
	runCtx, runCancel := inheritWorkflowCallbackTimeout(parentCtx, callbackTimeout)
	defer runCancel()
	callbackJob.StartTime = time.Now().Unix()
	urlPolicy, err := urlpolicy.ResolvePolicy(parentCtx, w.URLSecurityPolicyProvider)
	if err != nil {
		callbackJob.Status = config.StatusFailed
		callbackJob.Error = fmt.Sprintf("load url security policy: %v", err)
		callbackJob.EndTime = time.Now().Unix()
		callbackCtl := workflowjob.NewCallbackJobCtl(callbackJob, w.Store, nil)
		if callbackCtl != nil {
			if saveErr := callbackCtl.SaveInfo(parentCtx); saveErr != nil {
				klog.Warningf("save callback job info failed for workflow %s task %s: %v", task.WorkflowID, task.TaskID, saveErr)
				return fmt.Errorf("save callback policy failure: %w", saveErr)
			}
		}
		return nil
	}
	// The cancellation signal applies to application work, not to its terminal
	// notification. The callback keeps its own bounded timeout and parent fence.
	runCtx = workflowjob.WithTaskMetadata(runCtx, "")
	if err := workflowjob.RunJobs(runCtx, []*model.JobTask{callbackJob}, &workflowjob.Runtime{
		Concurrency:       1,
		Store:             w.Store,
		Ack:               func() {},
		RedisClient:       w.RedisClient,
		Cache:             w.Cache,
		URLSecurityPolicy: urlPolicy,
	}); err != nil {
		klog.ErrorS(err, "run terminal workflow callback", "taskID", task.TaskID, "jobName", callbackJob.Name)
		return err
	}
	return nil
}

func (w *workflowServiceImpl) reconcileWorkflowTerminalCallback(ctx context.Context, task *model.WorkflowQueue, status config.Status, reason string) (bool, error) {
	if status != config.StatusCancelled || task == nil || !wf.IsTerminalCallbackPending(task.SchedulingReason) {
		return true, w.triggerWorkflowTerminalCallbackOnApprovalAction(ctx, task, status, reason)
	}
	current := &model.WorkflowQueue{TaskID: task.TaskID}
	if err := w.Store.Get(ctx, current); err != nil {
		return false, fmt.Errorf("load pending terminal callback task: %w", err)
	}
	if current.Status != config.StatusCancelled ||
		current.RunGeneration != task.RunGeneration ||
		!wf.IsTerminalCallbackPending(current.SchedulingReason) {
		return false, nil
	}
	if current.RunToken != "" || current.WorkerID != "" {
		return false, nil
	}
	callbackReason := wf.TerminalCallbackReason(current.SchedulingReason)
	callback, err := w.loadWorkflowTerminalCallback(ctx, current)
	if err != nil {
		return false, err
	}
	callbackKey := workflowjob.TerminalCallbackExecutionKey(current.TaskID, current.RunGeneration, "cancelled")
	active, err := workflowHasActiveChildExecution(ctx, w.Store, current.TaskID)
	if err != nil {
		return false, err
	}
	if active {
		return false, nil
	}
	if err := repository.TerminalizeCancelledWorkflowJobs(ctx, w.Store, current.TaskID, callbackReason, callbackKey); err != nil {
		return false, err
	}
	settled, err := terminalCallbackJobSettled(ctx, w.Store, current.TaskID, current.RunGeneration, callbackKey)
	if err != nil {
		return false, err
	}
	if !settled && callback != nil {
		if err := w.runWorkflowTerminalCallback(ctx, current, status, callbackReason, callback); err != nil {
			return false, err
		}
	}
	updated, err := repository.UpdateTaskFieldsIfConditions(ctx, w.Store, current.TaskID, map[string]interface{}{
		"status":            config.StatusCancelled,
		"run_generation":    current.RunGeneration,
		"run_token":         "",
		"worker_id":         "",
		"scheduling_reason": current.SchedulingReason,
	}, map[string]interface{}{
		"scheduling_reason": wf.TerminalCallbackReconciledReason,
	})
	if err != nil {
		return false, fmt.Errorf("complete terminal callback marker: %w", err)
	}
	if !updated {
		return false, repository.ErrWorkflowOwnershipLost
	}
	return true, nil
}

// ReconcileWorkflowTerminalCallbacks replays durable cancellation callbacks
// whose worker cleanup has completed or expired.
func ReconcileWorkflowTerminalCallbacks(ctx context.Context, store datastore.DataStore, cfg *config.Config, provider *urlpolicy.Provider) (int, error) {
	tasks, err := PendingWorkflowTerminalCallbacks(ctx, store, 1, 100)
	if err != nil {
		return 0, err
	}
	reconciled := 0
	var reconcileErr error
	for _, task := range tasks {
		done, err := ReconcileWorkflowTerminalCallback(ctx, store, cfg, provider, task)
		if err != nil {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("task %s: %w", task.TaskID, err))
			continue
		}
		if done {
			reconciled++
		}
	}
	return reconciled, reconcileErr
}

// PendingWorkflowTerminalCallbacks returns one stable page for the controller
// replay loop. The caller rotates pages so one failing callback cannot starve
// newer cancellation intents.
func PendingWorkflowTerminalCallbacks(ctx context.Context, store datastore.DataStore, page, pageSize int) ([]*model.WorkflowQueue, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}
	entities, err := store.List(ctx, &model.WorkflowQueue{Status: config.StatusCancelled}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{Queries: []datastore.FuzzyQueryOption{{
			Key: "scheduling_reason", Query: wf.TerminalCallbackPendingReason(""),
		}}},
		Page:     page,
		PageSize: pageSize,
		SortBy: []datastore.SortOption{{
			Key: "update_time", Order: datastore.SortOrderAscending,
		}},
	})
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, nil
		}
		return nil, err
	}
	tasks := make([]*model.WorkflowQueue, 0, len(entities))
	for _, entity := range entities {
		task, ok := entity.(*model.WorkflowQueue)
		if !ok || task == nil || !wf.IsTerminalCallbackPending(task.SchedulingReason) {
			continue
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// ReconcileWorkflowTerminalCallback replays one durable cancellation callback.
func ReconcileWorkflowTerminalCallback(ctx context.Context, store datastore.DataStore, cfg *config.Config, provider *urlpolicy.Provider, task *model.WorkflowQueue) (bool, error) {
	if task == nil || task.RunToken != "" || task.WorkerID != "" {
		return false, nil
	}
	svc := &workflowServiceImpl{Store: store, Cfg: cfg, URLSecurityPolicyProvider: provider}
	return svc.reconcileWorkflowTerminalCallback(ctx, task, config.StatusCancelled, "")
}

func workflowHasActiveChildExecution(ctx context.Context, store datastore.DataStore, taskID string) (bool, error) {
	entities, err := store.List(ctx, &model.JobInfo{TaskID: taskID}, &datastore.ListOptions{})
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("list cancelled workflow jobs before callback: %w", err)
	}
	for _, entity := range entities {
		jobInfo, ok := entity.(*model.JobInfo)
		if !ok || jobInfo == nil {
			return false, datastore.ErrEntityInvalid
		}
		if jobInfo.Type == string(config.JobDeployCallback) {
			continue
		}
		if workflowjob.IsCancelledJobCleanupPending(jobInfo) {
			return true, nil
		}
		if config.Status(jobInfo.Status) == config.StatusRunning || jobInfo.SchedulingState == workflowconfig.JobSchedulingAdmitted {
			return true, nil
		}
	}
	return false, nil
}

// terminalCallbackJobSettled reports whether the callback has a durable final
// disposition. Callback delivery remains single-attempt: a persisted failure
// is observable, but recovery does not turn it into an automatic retry loop.
func terminalCallbackJobSettled(ctx context.Context, store datastore.DataStore, taskID string, generation uint64, executionKey string) (bool, error) {
	entities, err := store.List(ctx, &model.JobInfo{TaskID: taskID}, &datastore.ListOptions{
		Page: 1, PageSize: 2,
		FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{
			Key: "execution_key", Values: []string{executionKey},
		}}},
	})
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("load terminal callback job: %w", err)
	}
	for _, entity := range entities {
		jobInfo, ok := entity.(*model.JobInfo)
		if !ok || jobInfo == nil {
			return false, datastore.ErrEntityInvalid
		}
		if jobInfo.RunGeneration != generation || jobInfo.ExecutionKey == nil || *jobInfo.ExecutionKey != executionKey {
			continue
		}
		switch config.Status(jobInfo.Status) {
		case config.StatusCompleted, config.StatusPassed, config.StatusSkipped, config.StatusFailed,
			config.StatusTimeout, config.StatusCancelled, config.StatusReject, config.StatusNotRun:
			return true, nil
		}
	}
	return false, nil
}

func inheritWorkflowCallbackParentContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func detachWorkflowCallbackParentContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func inheritWorkflowCallbackTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(inheritWorkflowCallbackParentContext(ctx), timeout)
}

func decodeWorkflowCallbackForTerminal(raw *model.JSONStruct, target interface{}) error {
	if raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
