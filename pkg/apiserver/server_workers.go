package apiserver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	importruntime "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/runtime"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type workerRun struct {
	ctx             context.Context
	cancel          context.CancelFunc
	executionCancel context.CancelCauseFunc
	done            chan struct{}
	wg              sync.WaitGroup
}

func newWorkerRun(parent context.Context) *workerRun {
	if parent == nil {
		panic("create worker run: nil context")
	}
	ctx, cancel := context.WithCancel(parent)
	return &workerRun{
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

func (r *workerRun) start(fn func(context.Context)) {
	if r == nil || fn == nil {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		fn(r.ctx)
	}()
}

func (r *workerRun) markStarted() {
	if r == nil {
		return
	}
	go func() {
		r.wg.Wait()
		close(r.done)
	}()
}

func (r *workerRun) stop() {
	if r == nil {
		return
	}
	r.cancel()
}

func (r *workerRun) stopExecution() {
	if r == nil {
		return
	}
	if r.executionCancel != nil {
		r.executionCancel(signal.ErrInfrastructureStop)
		return
	}
	r.stop()
}

func (r *workerRun) wait() {
	if r == nil {
		return
	}
	<-r.done
}

func (r *workerRun) waitUntil(ctx context.Context) bool {
	if r == nil {
		return true
	}
	select {
	case <-r.done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *restServer) startWorkers(ctx context.Context, errChan chan error) {
	if ctx == nil {
		panic("start workers: nil context")
	}
	s.workersMu.Lock()
	if ctx.Err() != nil {
		s.workersMu.Unlock()
		return
	}
	if s.workersStarted {
		s.workersMu.Unlock()
		return
	}
	// Consumer cancellation stops intake only. In-flight execution is cancelled
	// explicitly with ErrInfrastructureStop after the configured drain window.
	executionCtx, cancelExecution := context.WithCancelCause(context.WithoutCancel(ctx))
	stopParentCancellation := context.AfterFunc(ctx, func() {
		cancelExecution(signal.ErrInfrastructureStop)
	})
	run := newWorkerRun(executionCtx)
	run.executionCancel = func(cause error) {
		stopParentCancellation()
		cancelExecution(cause)
	}
	workers := append([]event.Worker(nil), s.eventWorkers...)
	subscribers := make([]event.WorkerSubscriber, 0, len(workers))
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		if subscriber, ok := worker.(event.WorkerSubscriber); ok {
			subscribers = append(subscribers, subscriber)
		}
	}
	s.workersStarted = true
	s.workersReady = false
	s.workersCancel = run.cancel
	s.workersRun = run
	readySubscribers := 0
	for _, subscriber := range subscribers {
		subscriber := subscriber
		run.start(func(runCtx context.Context) {
			active := true
			var readyOnce, stoppedOnce sync.Once
			markStopped := func() {
				stoppedOnce.Do(func() {
					s.workersMu.Lock()
					active = false
					if s.workersRun == run {
						s.workersReady = false
					}
					s.workersMu.Unlock()
				})
			}
			defer markStopped()
			subscriber.StartWorker(runCtx, executionCtx, errChan, func() {
				readyOnce.Do(func() {
					s.workersMu.Lock()
					defer s.workersMu.Unlock()
					if !active || s.workersRun != run || !s.workersStarted {
						return
					}
					readySubscribers++
					if readySubscribers == len(subscribers) {
						s.workersReady = true
					}
				})
			}, markStopped)
		})
	}
	run.markStarted()
	go s.observeWorkerRun(run)
	s.workersMu.Unlock()
}

func (s *restServer) observeWorkerRun(run *workerRun) {
	if run == nil {
		return
	}
	<-run.done
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	if s.workersRun != run {
		return
	}
	s.workersStarted = false
	s.workersReady = false
	s.workersCancel = nil
	s.workersRun = nil
}

func (s *restServer) stopWorkers(ctx context.Context) {
	if ctx == nil {
		panic("stop workers: nil context")
	}
	s.workersMu.Lock()
	if !s.workersStarted {
		s.workersMu.Unlock()
		return
	}
	run := s.workersRun
	cancel := s.workersCancel
	s.workersStarted = false
	s.workersReady = false
	s.workersCancel = nil
	s.workersRun = nil
	s.workersMu.Unlock()
	if run != nil {
		run.stop()
		if run.waitUntil(ctx) {
			run.stopExecution()
			return
		}
		run.stopExecution()
		s.trackDrainingWorkerRun(run)
		return
	}
	if cancel != nil {
		cancel()
	}
}

func (s *restServer) trackDrainingWorkerRun(run *workerRun) {
	if run == nil {
		return
	}
	s.workersMu.Lock()
	if s.drainingWorkerRuns == nil {
		s.drainingWorkerRuns = make(map[*workerRun]struct{})
	}
	s.drainingWorkerRuns[run] = struct{}{}
	s.workersMu.Unlock()
	go func() {
		run.wait()
		s.workersMu.Lock()
		delete(s.drainingWorkerRuns, run)
		s.workersMu.Unlock()
	}()
}

func (s *restServer) finishWorkerDrain(ctx context.Context, run *workerRun) {
	if run == nil {
		return
	}
	if !run.waitUntil(ctx) {
		run.stopExecution()
	}
	run.stopExecution()
	run.wait()
	s.workersMu.Lock()
	delete(s.drainingWorkerRuns, run)
	s.workersMu.Unlock()
}

func (s *restServer) stopDrainingWorkers() {
	s.workersMu.Lock()
	runs := make([]*workerRun, 0, len(s.drainingWorkerRuns))
	for run := range s.drainingWorkerRuns {
		runs = append(runs, run)
	}
	s.workersMu.Unlock()

	for _, run := range runs {
		run.stopExecution()
	}
}

func reportableInformerStartError(ctx context.Context, err error) error {
	if err == nil || (ctx != nil && ctx.Err() != nil) {
		return nil
	}
	return fmt.Errorf("start informer manager: %w", err)
}

func (s *restServer) ensureQueueGroup(ctx context.Context) error {
	if s.Queue == nil {
		return nil
	}
	if err := s.Queue.EnsureGroup(ctx, config.WorkflowWorkerQueueGroup); err != nil {
		if (ctx != nil && ctx.Err() != nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		failures := s.ensureQueueGroupFailures.Add(1)
		klog.Warningf("ensure queue group failed group=%s error=%v failure_count=%d", config.WorkflowWorkerQueueGroup, err, failures)
		return err
	}
	return nil
}

func (s *restServer) startControllerEventWorkers(run *workerRun, errChan chan error) {
	if run == nil {
		return
	}
	for _, worker := range append([]event.Worker(nil), s.eventWorkers...) {
		controllerWorker, ok := worker.(event.ControllerWorker)
		if !ok || controllerWorker == nil {
			continue
		}
		run.start(func(runCtx context.Context) {
			controllerWorker.StartController(runCtx, errChan)
		})
	}
}

func (s *restServer) startSchedulerEventWorkers(run *workerRun, errChan chan error) (int, <-chan bool) {
	startupResults := make(chan bool, len(s.eventWorkers))
	if run == nil {
		return 0, startupResults
	}
	workerCount := 0
	for _, worker := range append([]event.Worker(nil), s.eventWorkers...) {
		schedulerWorker, ok := worker.(event.SchedulerWorker)
		if !ok || schedulerWorker == nil {
			continue
		}
		workerCount++
		run.start(func(runCtx context.Context) {
			var startupOnce sync.Once
			reportStartup := func(ready bool) {
				startupOnce.Do(func() {
					startupResults <- ready
				})
			}
			defer reportStartup(false)
			schedulerWorker.StartScheduler(runCtx, errChan, func() {
				reportStartup(true)
			})
		})
	}
	return workerCount, startupResults
}

func (s *restServer) beginControllerRun(ctx context.Context) *workerRun {
	s.workersMu.Lock()
	previous := s.controllerRun
	s.controllerRun = nil
	s.workersMu.Unlock()
	if previous != nil {
		previous.stop()
		previous.wait()
	}
	if s.InformerManager != nil {
		s.InformerManager.Stop()
	}
	run := newWorkerRun(ctx)
	s.workersMu.Lock()
	s.controllerRun = run
	s.workersMu.Unlock()
	return run
}

func (s *restServer) stopControllerRun() {
	s.controllerReady.Store(false)
	s.controllerLeading.Store(false)
	s.workersMu.Lock()
	run := s.controllerRun
	s.controllerRun = nil
	s.workersMu.Unlock()
	if run != nil {
		run.stop()
	}
	if s.InformerManager != nil {
		s.InformerManager.Stop()
	}
	if run != nil {
		run.wait()
	}
}

func (s *restServer) beginSchedulerRun(ctx context.Context) *workerRun {
	s.workersMu.Lock()
	previous := s.schedulerRun
	s.schedulerRun = nil
	s.workersMu.Unlock()
	if previous != nil {
		previous.stop()
		previous.wait()
	}
	run := newWorkerRun(ctx)
	s.workersMu.Lock()
	s.schedulerRun = run
	s.workersMu.Unlock()
	return run
}

func (s *restServer) stopSchedulerRun() {
	s.schedulerReady.Store(false)
	s.schedulerLeading.Store(false)
	s.workersMu.Lock()
	run := s.schedulerRun
	s.schedulerRun = nil
	s.workersMu.Unlock()
	if run != nil {
		run.stop()
		run.wait()
	}
}

func (s *restServer) onStartedControllerLeading(ctx context.Context, errChan chan error) {
	s.controllerLeading.Store(true)
	s.controllerReady.Store(false)
	run := s.beginControllerRun(ctx)
	if s.InformerManager != nil {
		if err := s.InformerManager.Start(run.ctx); err != nil {
			if reportable := reportableInformerStartError(run.ctx, err); reportable != nil && errChan != nil {
				errChan <- reportable
			}
			run.markStarted()
			return
		}
	}
	s.startControllerEventWorkers(run, errChan)
	if s.KubeClient != nil && s.dataStore != nil {
		coordinator := importruntime.NewPodCoordinator(s.KubeClient, importruntime.NewDataStoreBindingLoader(s.dataStore))
		run.start(coordinator.Run)
	}
	run.markStarted()
	s.controllerReady.Store(true)
}

func (s *restServer) onStartedSchedulerLeading(ctx context.Context, errChan chan error) {
	s.schedulerLeading.Store(true)
	s.schedulerReady.Store(false)
	run := s.beginSchedulerRun(ctx)
	if err := s.ensureQueueGroup(run.ctx); err != nil {
		if run.ctx.Err() == nil && errChan != nil {
			errChan <- fmt.Errorf("ensure queue group %s: %w", config.WorkflowWorkerQueueGroup, err)
		}
		run.markStarted()
		return
	}
	workerCount, startupResults := s.startSchedulerEventWorkers(run, errChan)
	run.start(s.runQueueMetrics)
	run.markStarted()
	for range workerCount {
		select {
		case ready := <-startupResults:
			if !ready {
				return
			}
		case <-run.ctx.Done():
			return
		}
	}
	s.schedulerReady.Store(true)
}

func (s *restServer) startQueueMetrics(ctx context.Context) {
	go s.runQueueMetrics(ctx)
}

func (s *restServer) runQueueMetrics(ctx context.Context) {
	if s.Queue == nil {
		return
	}
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if s.Queue == nil {
				continue
			}
			if bl, pd, err := s.Queue.Stats(ctx, config.WorkflowWorkerQueueGroup); err == nil {
				klog.Infof("queue stats stream=%s backlog=%d pending=%d", s.dispatchTopic(), bl, pd)
			} else {
				klog.V(4).Infof("queue stats error: %v", err)
			}
		}
	}
}

func (s *restServer) loadPodRestartMonitorConfig(ctx context.Context) (informer.PodRestartMonitorConfig, error) {
	if ctx == nil {
		return informer.PodRestartMonitorConfig{}, fmt.Errorf("context is nil")
	}
	if s.dataStore == nil {
		return informer.PodRestartMonitorConfig{}, fmt.Errorf("datastore is not initialized")
	}
	setting := &model.SystemSetting{Type: model.SystemSettingTypePodRestartMonitor}
	if err := s.dataStore.Get(ctx, setting); err != nil {
		return informer.PodRestartMonitorConfig{}, fmt.Errorf("load podRestartMonitor setting: %w", err)
	}
	cfg, err := spec.ParsePodRestartMonitorSetting(setting.Value)
	if err != nil {
		return informer.PodRestartMonitorConfig{}, fmt.Errorf("parse podRestartMonitor setting: %w", err)
	}
	return informer.PodRestartMonitorConfig{
		Enabled:   cfg.Enabled,
		Window:    time.Duration(cfg.WindowSeconds) * time.Second,
		Threshold: cfg.Threshold,
	}, nil
}

func (s *restServer) handleDeploymentPodRestartThresholdExceeded(event informer.DeploymentPodRestartEvent) {
	klog.InfoS("deployment pod restart monitor threshold exceeded",
		"namespace", event.Namespace,
		"pod", event.PodName,
		"appID", event.AppID,
		"component", event.ComponentName,
		"componentID", event.ComponentID,
		"window", event.Window.String(),
		"threshold", event.Threshold,
		"restartCount", event.RestartCount,
		"occurredAt", event.OccurredAt.Format(time.RFC3339),
	)
}
