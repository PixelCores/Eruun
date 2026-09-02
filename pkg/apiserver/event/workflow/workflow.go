package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	"github.com/PixelCores/Eruun/pkg/apiserver/security/urlpolicy"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	signal "github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type workflowRuntimeService interface {
	WaitingTasks(context.Context) ([]*model.WorkflowQueue, error)
	UpdateTask(context.Context, *model.WorkflowQueue) bool
	MarkTaskStatus(context.Context, string, config.Status, config.Status) (bool, error)
	DispatchWorkflowSchedules(context.Context) (int, error)
}

type Workflow struct {
	KubeClient                kubernetes.Interface            `inject:"kubeClient"`
	KubeConfig                *rest.Config                    `inject:"kubeConfig"`
	Store                     datastore.DataStore             `inject:"datastore"`
	WorkflowService           workflowRuntimeService          `inject:""`
	Queue                     msg.Queue                       `inject:"queue"`
	DelayQueue                msg.Queue                       `inject:"delayQueue"`
	ResultQueue               msg.Queue                       `inject:"resultQueue"`
	Cfg                       *config.Config                  `inject:""`
	Cache                     cache.ICache                    `inject:"cache"`
	ResourceWaiter            informer.ComponentReadyObserver `inject:"resourceObserver"`
	URLSecurityPolicyProvider *urlpolicy.Provider             `inject:""`
	TaskRunLocker             locker.Locker                   `inject:"workflowTaskRunLocker"`
	controllerLifecycleMu     sync.Mutex
	schedulerLifecycleMu      sync.Mutex
	workerLimiterOnce         sync.Once
	workflowLimiter           *semaphore.Weighted
	errChan                   chan error
	taskRunLocker             locker.Locker
	taskRunLockerErr          error
	taskRunLockOnce           sync.Once
}

type workflowWorkerRun struct {
	executionCtx context.Context
	taskGroup    *errgroup.Group
	limiter      *semaphore.Weighted
}

func newWorkflowWorkerRun(executionCtx context.Context, limiter *semaphore.Weighted) *workflowWorkerRun {
	if executionCtx == nil {
		executionCtx = context.Background()
	}
	return &workflowWorkerRun{
		executionCtx: executionCtx,
		taskGroup:    &errgroup.Group{},
		limiter:      limiter,
	}
}

func (r *workflowWorkerRun) wait() error {
	if r == nil || r.taskGroup == nil {
		return nil
	}
	return r.taskGroup.Wait()
}

func (w *Workflow) workerConcurrencyLimiter() *semaphore.Weighted {
	w.workerLimiterOnce.Do(func() {
		if max := w.maxWorkflowConcurrency(); max > 0 {
			w.workflowLimiter = semaphore.NewWeighted(max)
		}
	})
	return w.workflowLimiter
}

// StartController runs the controller-owned coordination loops. It intentionally
// excludes queue admission and worker message consumption.
func (w *Workflow) StartController(ctx context.Context, errChan chan error) {
	if ctx == nil {
		ctx = context.Background()
	}
	w.controllerLifecycleMu.Lock()
	defer w.controllerLifecycleMu.Unlock()
	var wg sync.WaitGroup
	w.startDelayDispatcher(ctx, &wg)
	w.startResultDispatcher(ctx, &wg)
	w.startResultOutboxDispatcher(ctx, &wg)
	<-ctx.Done()
	wg.Wait()
}

// StartScheduler runs queue admission, schedule dispatch, and database lease recovery.
// The ready callback is invoked only after synchronous startup recovery succeeds
// and all scheduler loops have been launched.
func (w *Workflow) StartScheduler(ctx context.Context, errChan chan error, ready func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	w.schedulerLifecycleMu.Lock()
	defer w.schedulerLifecycleMu.Unlock()
	var wg sync.WaitGroup
	w.startScheduleDispatcher(ctx, &wg)
	w.startLeaseReaper(ctx, &wg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Dispatcher(ctx)
	}()
	if ready != nil {
		ready()
	}
	<-ctx.Done()
	wg.Wait()
}

func (w *Workflow) startLeaseReaper(ctx context.Context, wg *sync.WaitGroup) {
	run := func() {
		ticker := time.NewTicker(w.workflowLeaseReaperInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			reaperCtx, cancel := context.WithTimeout(ctx, config.TaskStateTransitionTimeout)
			recovered, err := repository.RecoverExpiredWorkflowTasks(reaperCtx, w.Store, time.Now().UTC())
			cancel()
			if err != nil {
				klog.ErrorS(err, "recover expired workflow execution leases")
				continue
			}
			if recovered > 0 {
				klog.InfoS("recovered expired workflow execution leases", "count", recovered)
			}
		}
	}
	if wg == nil {
		go run()
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		run()
	}()
}

func (w *Workflow) startScheduleDispatcher(ctx context.Context, wg *sync.WaitGroup) {
	if w.WorkflowService == nil {
		return
	}
	if wg == nil {
		go w.workflowScheduleDispatcher(ctx)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.workflowScheduleDispatcher(ctx)
	}()
}

func (w *Workflow) workflowScheduleDispatcher(ctx context.Context) {
	ticker := time.NewTicker(w.dispatchPollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			klog.V(3).Info("workflow schedule dispatcher stopped: context cancelled")
			return
		case <-ticker.C:
		}
		processed, err := w.WorkflowService.DispatchWorkflowSchedules(ctx)
		if err != nil {
			klog.Errorf("dispatch workflow schedules failed: %v", err)
			continue
		}
		if processed > 0 {
			klog.Infof("dispatched %d workflow schedules", processed)
		}
	}
}

func (w *Workflow) startDelayDispatcher(ctx context.Context, wg *sync.WaitGroup) {
	if w.DelayQueue == nil {
		return
	}
	consumer := w.delayConsumerName()
	dispatcher := job.NewDelayDispatcher(w.DelayQueue, w.KubeClient, w.Store, config.DelayQueueGroup, consumer)
	if wg == nil {
		dispatcher.Start(ctx)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Run(ctx)
	}()
}

func (w *Workflow) startResultDispatcher(ctx context.Context, wg *sync.WaitGroup) {
	if w.ResultQueue == nil {
		return
	}
	consumer := w.resultConsumerName()
	dispatcher := job.NewResultDispatcher(w.ResultQueue, w.KubeClient, w.Store, config.ResultQueueGroup, consumer)
	if wg == nil {
		dispatcher.Start(ctx)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Run(ctx)
	}()
}

func (w *Workflow) startResultOutboxDispatcher(ctx context.Context, wg *sync.WaitGroup) {
	if w.ResultQueue == nil {
		return
	}
	dispatcher := job.NewResultOutboxDispatcher(w.ResultQueue, w.KubeClient, w.Store)
	if wg == nil {
		dispatcher.Start(ctx)
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Run(ctx)
	}()
}

func (w *Workflow) runWorkflowTask(ctx context.Context, workerRun *workflowWorkerRun, task *model.WorkflowQueue, concurrency int) (bool, error) {
	if task == nil || task.TaskID == "" {
		return false, fmt.Errorf("invalid workflow task")
	}
	if task.RunGeneration == 0 || task.RunToken == "" || task.WorkerID == "" {
		return false, repository.ErrWorkflowOwnershipRequired
	}
	runnerCtx := ctx
	var taskGroup *errgroup.Group
	var workflowLimiter *semaphore.Weighted
	if workerRun != nil {
		runnerCtx = workerRun.executionCtx
		taskGroup = workerRun.taskGroup
		workflowLimiter = workerRun.limiter
	}
	taskCtx, cancelTask := context.WithCancelCause(runnerCtx)
	heartbeatDone := w.startWorkflowTaskHeartbeat(taskCtx, cancelTask, task)
	stopHeartbeat := func() {
		cancelTask(nil)
		<-heartbeatDone
	}
	runnerCtx = taskCtx

	lease, leaseAcquired, err := w.tryAcquireTaskRunLease(ctx, runnerCtx, task.TaskID)
	if err != nil {
		stopHeartbeat()
		return false, fmt.Errorf("acquire workflow task run lease: %w", err)
	}
	if !leaseAcquired {
		stopHeartbeat()
		return false, errTaskRunLeaseHeld
	}

	urlPolicy, err := urlpolicy.ResolvePolicy(ctx, w.URLSecurityPolicyProvider)
	if err != nil {
		runErr := fmt.Errorf("load url security policy: %w", err)
		if !isContextCancellationError(runErr) {
			w.markTaskRunStartFailure(ctx, task, runErr)
		}
		lease.release()
		stopHeartbeat()
		return false, runErr
	}

	acquired := false
	if workflowLimiter != nil {
		if err := workflowLimiter.Acquire(runnerCtx, 1); err != nil {
			lease.release()
			stopHeartbeat()
			return false, fmt.Errorf("acquire workflow slot: %w", err)
		}
		acquired = true
	}
	controller, err := NewWorkflowController(task, w.KubeClient, w.KubeConfig, w.Store, w.Cfg, w.Cache, urlPolicy)
	if err != nil {
		runErr := fmt.Errorf("init workflow controller: %w", err)
		w.markTaskRunStartFailure(ctx, task, runErr)
		if acquired {
			workflowLimiter.Release(1)
		}
		lease.release()
		stopHeartbeat()
		return false, runErr
	}
	runController := func() error {
		defer stopHeartbeat()
		defer lease.release()
		if acquired {
			defer workflowLimiter.Release(1)
		}
		err := w.runWorkflowControllerWithPersistenceRecovery(runnerCtx, controller, concurrency)
		if err != nil {
			w.reportTaskError(err)
		}
		return err
	}
	if taskGroup != nil {
		taskGroup.Go(runController)
		return true, nil
	}
	go func() {
		_ = runController()
	}()
	return true, nil
}

func (w *Workflow) runClaimedWorkflowTask(ctx context.Context, workerRun *workflowWorkerRun, task *model.WorkflowQueue, concurrency int) error {
	sequentialConcurrency := concurrency
	if w.Cfg != nil && w.Cfg.Workflow.SequentialMaxConcurrency > 0 {
		sequentialConcurrency = w.Cfg.Workflow.SequentialMaxConcurrency
	}
	started, err := w.runWorkflowTask(ctx, workerRun, task, sequentialConcurrency)
	if err != nil {
		return err
	}
	if !started {
		return errTaskRunLeaseHeld
	}
	return nil
}

func (w *Workflow) startWorkflowTaskHeartbeat(ctx context.Context, cancel context.CancelCauseFunc, task *model.WorkflowQueue) <-chan struct{} {
	done := make(chan struct{})
	if task == nil || task.RunGeneration == 0 || task.RunToken == "" || task.WorkerID == "" {
		close(done)
		return done
	}
	taskID := task.TaskID
	runGeneration := task.RunGeneration
	runToken := task.RunToken
	workerID := task.WorkerID
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.workflowHeartbeatInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				expireCtx, expireCancel := context.WithTimeout(context.Background(), config.TaskStateTransitionTimeout)
				if _, err := repository.ExpireWorkflowTaskLease(expireCtx, w.Store, taskID, runGeneration, runToken, workerID); err != nil {
					klog.V(2).InfoS("expire workflow execution lease after stop failed", "taskID", taskID, "generation", runGeneration, "error", err)
				}
				expireCancel()
				return
			case <-ticker.C:
			}
			renewCtx, renewCancel := context.WithTimeout(ctx, config.TaskStateTransitionTimeout)
			renewed, err := repository.RenewWorkflowTaskLease(
				renewCtx, w.Store, taskID, runGeneration, runToken, workerID, w.workflowLeaseDuration(),
			)
			renewCancel()
			if err != nil {
				klog.ErrorS(err, "renew workflow execution lease", "taskID", taskID, "generation", runGeneration)
				cancel(errors.Join(signal.ErrInfrastructureStop, repository.ErrWorkflowLeaseRenewalFailed, err))
				return
			}
			if !renewed {
				expected := &model.WorkflowQueue{
					TaskID: taskID, RunGeneration: runGeneration, RunToken: runToken, WorkerID: workerID,
				}
				cause := w.workflowLeaseRenewalMissCause(ctx, expected)
				if cause == nil {
					klog.V(4).InfoS("workflow execution lease ended after task reached a terminal checkpoint", "taskID", taskID, "generation", runGeneration)
					return
				}
				klog.Warningf("workflow execution lease renewal rejected: taskID=%s generation=%d cause=%v", taskID, runGeneration, cause)
				if errors.Is(cause, context.Canceled) && !errors.Is(cause, repository.ErrWorkflowOwnershipLost) && !errors.Is(cause, repository.ErrWorkflowLeaseRenewalFailed) {
					cancel(cause)
				} else {
					cancel(errors.Join(signal.ErrInfrastructureStop, cause))
				}
				return
			}
		}
	}()
	return done
}

func (w *Workflow) workflowLeaseRenewalMissCause(ctx context.Context, expected *model.WorkflowQueue) error {
	if expected == nil {
		return errors.Join(repository.ErrWorkflowLeaseRenewalFailed, fmt.Errorf("reload workflow task after lease renewal miss: task is nil"))
	}
	reloadBase := context.Background()
	if ctx != nil {
		reloadBase = context.WithoutCancel(ctx)
	}
	reloadCtx, cancel := context.WithTimeout(reloadBase, config.TaskStateTransitionTimeout)
	defer cancel()
	authoritative, err := repository.TaskByID(reloadCtx, w.Store, expected.TaskID)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return errors.Join(repository.ErrWorkflowOwnershipLost, fmt.Errorf("reload workflow task %s after lease renewal miss: %w", expected.TaskID, err))
		}
		return errors.Join(repository.ErrWorkflowLeaseRenewalFailed, fmt.Errorf("reload workflow task %s after lease renewal miss: %w", expected.TaskID, err))
	}
	if !sameWorkflowTaskOwnership(expected, authoritative) {
		return errors.Join(
			repository.ErrWorkflowOwnershipLost,
			fmt.Errorf("workflow task %s ownership changed from generation %d worker %s to generation %d worker %s", expected.TaskID, expected.RunGeneration, expected.WorkerID, authoritative.RunGeneration, authoritative.WorkerID),
		)
	}
	if authoritative.Status == config.StatusCancelled && strings.EqualFold(strings.TrimSpace(authoritative.CancelSource), config.CancelSourceUser) {
		return context.Canceled
	}
	if authoritative.Status == config.StatusWaitingApprove || (isWorkflowTerminal(authoritative.Status) && authoritative.Status != config.StatusCancelled) {
		return nil
	}
	return errors.Join(
		repository.ErrWorkflowLeaseRenewalFailed,
		fmt.Errorf("workflow task %s rejected lease renewal while ownership remained unchanged in status %s", expected.TaskID, authoritative.Status),
	)
}

// The dispatch message is acknowledged after the controller starts, not after it
// finishes. Keep the task lease while recovering an uncertain persistence write
// so a transient database failure cannot strand or concurrently rerun the task.
func (w *Workflow) runWorkflowControllerWithPersistenceRecovery(ctx context.Context, controller *WorkflowCtl, concurrency int) error {
	current := controller
	for {
		current.DelayQueue = w.DelayQueue
		current.ResourceWaiter = w.ResourceWaiter
		runErr := current.Run(ctx, concurrency)
		if stopped, persistenceErr := current.workflowRunStopResult(); stopped {
			if persistenceErr == nil {
				return nil
			}
			if !errors.Is(runErr, persistenceErr) {
				runErr = errors.Join(runErr, persistenceErr)
			}
		}
		if errors.Is(runErr, signal.ErrInfrastructureStop) && !errors.Is(runErr, errWorkflowTaskPersistenceUncertain) {
			return nil
		}
		if !errors.Is(runErr, errWorkflowTaskPersistenceUncertain) {
			if errors.Is(runErr, repository.ErrWorkflowOwnershipRequired) || errors.Is(runErr, repository.ErrWorkflowFencingUnsupported) {
				return runErr
			}
			return runErr
		}

		expectedOwner := current.snapshotTask()
		taskID := expectedOwner.TaskID
		klog.Warningf("workflow task persistence is uncertain; retaining lease while waiting for recovery: taskID=%s", taskID)
		recovered, err := w.waitForWorkflowTaskPersistenceRecovery(ctx, taskID)
		if err != nil {
			return errors.Join(runErr, err)
		}
		if !sameWorkflowTaskOwnership(&expectedOwner, recovered) {
			return errors.Join(runErr, fmt.Errorf(
				"%w: task %s expected generation %d worker %s, recovered generation %d worker %s",
				repository.ErrWorkflowOwnershipLost,
				taskID,
				expectedOwner.RunGeneration,
				expectedOwner.WorkerID,
				recovered.RunGeneration,
				recovered.WorkerID,
			))
		}
		if recovered.Status != config.StatusQueued && recovered.Status != config.StatusRunning {
			klog.Infof("workflow task persistence recovery resolved to non-runnable state: taskID=%s status=%s", taskID, recovered.Status)
			return nil
		}

		next, err := NewWorkflowController(
			recovered,
			w.KubeClient,
			w.KubeConfig,
			w.Store,
			w.Cfg,
			w.Cache,
			current.urlSecurityPolicy,
		)
		if err != nil {
			return errors.Join(
				runErr,
				fmt.Errorf("recreate workflow controller after persistence recovery: %w", err),
			)
		}
		klog.Infof("resume workflow task after persistence recovery: taskID=%s status=%s currentStep=%d", recovered.TaskID, recovered.Status, recovered.CurrentStep)
		current = next
	}
}

func sameWorkflowTaskOwnership(expected, recovered *model.WorkflowQueue) bool {
	if expected == nil || recovered == nil {
		return false
	}
	return expected.TaskID == recovered.TaskID &&
		expected.RunGeneration == recovered.RunGeneration &&
		expected.RunToken == recovered.RunToken &&
		expected.WorkerID == recovered.WorkerID
}

func (w *Workflow) waitForWorkflowTaskPersistenceRecovery(ctx context.Context, taskID string) (*model.WorkflowQueue, error) {
	minBackoff := w.workerBackoffMin()
	maxBackoff := w.workerBackoffMax()
	delay := minBackoff
	for {
		reloadCtx, cancel := context.WithTimeout(ctx, config.TaskStateTransitionTimeout)
		task, err := repository.TaskByID(reloadCtx, w.Store, taskID)
		cancel()
		if err == nil {
			return task, nil
		}
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, errors.Join(
				errWorkflowTaskPersistenceUncertain,
				fmt.Errorf("reload workflow task %s for persistence recovery: %w", taskID, err),
			)
		}
		if ctx.Err() != nil {
			return nil, errors.Join(
				errWorkflowTaskPersistenceUncertain,
				fmt.Errorf("wait for workflow task %s persistence recovery: %w", taskID, ctx.Err()),
			)
		}

		klog.Warningf("reload workflow task for persistence recovery failed; retrying: taskID=%s retryIn=%s error=%v", taskID, delay, err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(
				errWorkflowTaskPersistenceUncertain,
				fmt.Errorf("wait for workflow task %s persistence recovery: %w", taskID, ctx.Err()),
			)
		case <-timer.C:
		}
		delay = w.workerBackoffDelay(delay, minBackoff, maxBackoff)
	}
}

// reportTaskError logs workflow task errors.
// Note: Workflow task failures are expected business errors (e.g., deployment failures,
// validation errors) and should NOT cause the server to exit. Only infrastructure errors
// (e.g., Redis connection failures, database errors) should trigger service termination.
// Therefore, we only log the error instead of sending it to errChan.
func (w *Workflow) reportTaskError(err error) {
	if err == nil {
		return
	}
	klog.Errorf("workflow task error: %v", err)
}

func (w *Workflow) markTaskRunStartFailure(ctx context.Context, task *model.WorkflowQueue, runErr error) {
	if task == nil || w == nil || w.Store == nil {
		return
	}
	expectedStatus := task.Status
	task.Status = config.StatusFailed
	baseCtx := ctx
	if baseCtx == nil || baseCtx.Err() != nil {
		baseCtx = context.Background()
	}
	persistCtx, cancel := context.WithTimeout(baseCtx, config.TaskStateTransitionTimeout)
	defer cancel()
	updated, err := repository.UpdateTaskFieldsIfOwned(persistCtx, w.Store, task, expectedStatus, map[string]interface{}{
		"status": config.StatusFailed,
	})
	if err != nil {
		klog.Errorf("mark workflow task %s failed before run: %v (cause: %v)", task.TaskID, err, runErr)
		return
	}
	if !updated {
		klog.V(2).Infof("skip workflow task %s start failure write after ownership changed", task.TaskID)
		return
	}
	reason := ""
	if runErr != nil {
		reason = runErr.Error()
	}
	if err := service.TerminalizePrecreatedVersionUpdateCleanupJobs(persistCtx, w.Store, task.TaskID, config.StatusFailed, reason); err != nil {
		klog.Errorf("terminalize cleanup jobs for failed workflow task %s before run: %v", task.TaskID, err)
	}
}

func (w *Workflow) maxWorkflowConcurrency() int64 {
	if w.Cfg != nil && w.Cfg.Workflow.MaxConcurrentWorkflows > 0 {
		return int64(w.Cfg.Workflow.MaxConcurrentWorkflows)
	}
	return int64(config.DefaultMaxConcurrentWorkflows)
}
