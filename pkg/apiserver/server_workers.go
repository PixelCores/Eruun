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
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
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
	if s.workersRun != nil {
		s.workersMu.Unlock()
		return
	}
	if s.workflow == nil {
		s.workersMu.Unlock()
		reportWorkerStartupError(ctx, errChan, fmt.Errorf("workflow runtime is not configured"))
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
	s.workersReady = false
	s.workersRun = run
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
		s.workflow.StartWorker(runCtx, executionCtx, func() {
			readyOnce.Do(func() {
				s.workersMu.Lock()
				defer s.workersMu.Unlock()
				if active && s.workersRun == run {
					s.workersReady = true
				}
			})
		}, markStopped)
	})
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
	s.workersReady = false
	s.workersRun = nil
}

func (s *restServer) stopWorkers(ctx context.Context) {
	if ctx == nil {
		panic("stop workers: nil context")
	}
	s.workersMu.Lock()
	run := s.workersRun
	if run == nil {
		s.workersMu.Unlock()
		return
	}
	s.workersReady = false
	s.workersRun = nil
	s.workersMu.Unlock()
	run.stop()
	if run.waitUntil(ctx) {
		run.stopExecution()
		return
	}
	run.stopExecution()
	s.trackDrainingWorkerRun(run)
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
		run.stopExecution()
		s.workersMu.Lock()
		delete(s.drainingWorkerRuns, run)
		s.workersMu.Unlock()
	}()
}

func reportableInformerStartError(ctx context.Context, err error) error {
	if err == nil || (ctx != nil && ctx.Err() != nil) {
		return nil
	}
	return fmt.Errorf("start informer manager: %w", err)
}

func reportWorkerStartupError(ctx context.Context, errChan chan error, err error) {
	if err == nil || errChan == nil {
		return
	}
	select {
	case errChan <- err:
	case <-ctx.Done():
	}
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
