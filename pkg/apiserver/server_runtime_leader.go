package apiserver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
)

func (s *restServer) setupRuntimeLeaderElection(ctx, runtimeCtx context.Context, errChan chan error) (leaderelection.LeaderElectionConfig, error) {
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return leaderelection.LeaderElectionConfig{}, fmt.Errorf("load kubernetes config for leader election: %w", err)
	}
	lock, err := resourcelock.NewFromKubeconfig(resourcelock.LeasesResourceLock,
		s.cfg.LeaderConfig.Namespace, s.cfg.LeaderConfig.LockName,
		resourcelock.ResourceLockConfig{Identity: s.cfg.LeaderConfig.ID}, restConfig, 10*time.Second)
	if err != nil {
		return leaderelection.LeaderElectionConfig{}, fmt.Errorf("create runtime leader lock: %w", err)
	}
	return s.buildRuntimeLeaderElectionConfig(ctx, runtimeCtx, lock, errChan), nil
}

func (s *restServer) buildRuntimeLeaderElectionConfig(ctx, runtimeCtx context.Context, lock resourcelock.Interface, errChan chan error) leaderelection.LeaderElectionConfig {
	return leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: s.cfg.LeaderConfig.Duration,
		RenewDeadline: renewDeadlineForLeaseDuration(s.cfg.LeaderConfig.Duration), RetryPeriod: leaderElectionRetryPeriod,
		ReleaseOnCancel: false, Name: "eruun-runtime",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				if ctx.Err() != nil || leaderCtx.Err() != nil {
					return
				}
				s.startLeader(leaderCtx, errChan)
			},
			OnStoppedLeading: func() {
				s.stopLeader()
				releaseLeaderLock(lock, leaderElectionReleaseTimeout)
				if ctx.Err() == nil {
					s.startWorkers(runtimeCtx, errChan)
				}
			},
			OnNewLeader: func(identity string) { klog.InfoS("runtime leader observed", "identity", identity) },
		},
	}
}

func (s *restServer) startLeader(parent context.Context, errChan chan error) {
	ctx, cancel := context.WithCancel(parent)
	ready := false
	defer func() {
		if !ready {
			cancel()
		}
	}()
	s.leading.Store(true)
	s.pauseWorkerIntake()
	s.onStartedControllerLeading(ctx, errChan)
	if ctx.Err() != nil || !s.controllerReady.Load() {
		return
	}
	s.onStartedSchedulerLeading(ctx, errChan)
	if ctx.Err() != nil || !s.schedulerReady.Load() {
		return
	}
	s.leaderMu.Lock()
	s.leaderCtx, s.leaderCancel = ctx, cancel
	s.leaderMu.Unlock()
	publishCtx, finishPublish := context.WithTimeout(ctx, leaderElectionReleaseTimeout)
	err := s.publishLeaderService(publishCtx)
	finishPublish()
	if err != nil {
		cancel()
		reportWorkerStartupError(parent, errChan, fmt.Errorf("publish Leader API endpoint: %w", err))
		return
	}
	if s.cfg.LeaderConfig.ServiceName != "" {
		done := make(chan struct{})
		s.leaderServiceDone = done
		go func() {
			defer close(done)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				reconcileCtx, finish := context.WithTimeout(ctx, leaderElectionReleaseTimeout)
				err := s.publishLeaderService(reconcileCtx)
				finish()
				if err != nil && ctx.Err() == nil {
					klog.ErrorS(err, "reconcile Leader API Service")
				}
			}
		}()
	}
	ready = true
	klog.InfoS("runtime Leader ready", "identity", s.cfg.LeaderConfig.ID)
}

func (s *restServer) stopLeader() {
	s.leaderMu.Lock()
	if s.leaderCancel != nil {
		s.leaderCancel()
	}
	s.leaderCtx, s.leaderCancel = nil, nil
	s.leaderMu.Unlock()
	if s.leaderServiceDone != nil {
		<-s.leaderServiceDone
		s.leaderServiceDone = nil
	}
	if s.leading.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), leaderElectionReleaseTimeout)
		if err := s.withdrawLeaderService(ctx); err != nil {
			klog.ErrorS(err, "withdraw Leader API endpoint")
		}
		cancel()
	}
	s.stopSchedulerRun()
	s.stopControllerRun()
	s.leading.Store(false)
}

// client-go starts OnStartedLeading asynchronously. Serialize complete terms so
// a late startup cannot resurrect work after loss or overlap the next term.
func (s *restServer) runRuntimeLeaderElection(ctx context.Context, election leaderelection.LeaderElectionConfig) {
	for ctx.Err() == nil {
		cfg := election
		callbacks := cfg.Callbacks
		var mu sync.Mutex
		started, stopped := false, false
		startedDone := make(chan struct{})
		cfg.Callbacks.OnStartedLeading = func(leaderCtx context.Context) {
			mu.Lock()
			if stopped {
				mu.Unlock()
				return
			}
			started = true
			mu.Unlock()
			defer close(startedDone)
			if callbacks.OnStartedLeading != nil {
				callbacks.OnStartedLeading(leaderCtx)
			}
		}
		cfg.Callbacks.OnStoppedLeading = func() {
			mu.Lock()
			stopped = true
			wait := started
			mu.Unlock()
			if wait {
				<-startedDone
			}
			if callbacks.OnStoppedLeading != nil {
				callbacks.OnStoppedLeading()
			}
		}
		runLeaderElector(ctx, cfg)
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(leaderElectionRetryDelay):
		}
	}
}

// Promotion stops intake without cancelling executions that already own a task.
// They retain their database leases and the shared concurrency budget.
func (s *restServer) pauseWorkerIntake() {
	s.workersMu.Lock()
	run := s.workersRun
	s.workersRun = nil
	s.workersStarted, s.workersReady = false, false
	s.workersCancel = nil
	if run != nil {
		if s.drainingWorkerRuns == nil {
			s.drainingWorkerRuns = make(map[*workerRun]struct{})
		}
		s.drainingWorkerRuns[run] = struct{}{}
	}
	s.workersMu.Unlock()
	if run != nil {
		run.stop()
		s.trackDrainingWorkerRun(run)
	}
}

func (s *restServer) drainPromotedWorkers(ctx context.Context) {
	s.workersMu.Lock()
	runs := make([]*workerRun, 0, len(s.drainingWorkerRuns))
	for run := range s.drainingWorkerRuns {
		runs = append(runs, run)
	}
	s.workersMu.Unlock()
	for _, run := range runs {
		run.waitUntil(ctx)
		run.stopExecution()
	}
}
