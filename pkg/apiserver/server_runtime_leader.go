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

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	importruntime "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/resourceimport/runtime"
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

func (s *restServer) startLeader(parent context.Context, errChan chan error) bool {
	run := newWorkerRun(parent)
	s.leaderMu.Lock()
	s.leaderRun = run
	s.leaderMu.Unlock()
	ready := false
	defer func() {
		// Seal only after every goroutine for this term has been registered.
		run.markStarted()
		if !ready {
			s.stopLeaderRun()
		}
	}()
	ctx := run.ctx
	s.leading.Store(true)
	s.pauseWorkerIntake()
	if s.InformerManager != nil {
		if err := s.InformerManager.Start(ctx); err != nil {
			reportWorkerStartupError(ctx, errChan, reportableInformerStartError(ctx, err))
			return false
		}
	}
	if err := s.ensureQueueGroup(ctx); err != nil {
		if ctx.Err() == nil {
			reportWorkerStartupError(ctx, errChan, fmt.Errorf("ensure queue group %s: %w", config.WorkflowWorkerQueueGroup, err))
		}
		return false
	}
	if s.workflow == nil {
		reportWorkerStartupError(ctx, errChan, fmt.Errorf("workflow runtime is not configured"))
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	if s.jobs != nil {
		run.start(s.jobs.Maintain)
	}
	if s.accounts != nil {
		run.start(s.accounts.RunSessionCleanup)
	}
	if s.KubeClient != nil && s.dataStore != nil {
		coordinator := importruntime.NewPodCoordinator(s.KubeClient, importruntime.NewDataStoreBindingLoader(s.dataStore))
		run.start(coordinator.Run)
	}
	run.start(s.runQueueMetrics)
	startup := make(chan bool, 1)
	run.start(func(ctx context.Context) {
		var once sync.Once
		report := func(ready bool) { once.Do(func() { startup <- ready }) }
		defer report(false)
		s.workflow.StartLeader(ctx, func() { report(true) })
	})
	select {
	case started := <-startup:
		if !started {
			return false
		}
	case <-ctx.Done():
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	s.leaderMu.Lock()
	s.leaderCtx = ctx
	s.leaderMu.Unlock()
	publishCtx, finishPublish := context.WithTimeout(ctx, leaderElectionReleaseTimeout)
	err := s.publishLeaderService(publishCtx)
	finishPublish()
	if err != nil {
		run.stop()
		reportWorkerStartupError(parent, errChan, fmt.Errorf("publish Leader API endpoint: %w", err))
		return false
	}
	if s.cfg.LeaderConfig.ServiceName != "" {
		run.start(func(ctx context.Context) {
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
		})
	}
	ready = true
	klog.InfoS("runtime Leader ready", "identity", s.cfg.LeaderConfig.ID)
	return true
}

// Startup and shutdown are serialized by runRuntimeLeaderElection. Failed startup
// also uses this path, so partial initialization never outlives its term.
func (s *restServer) stopLeaderRun() {
	s.leaderMu.Lock()
	run := s.leaderRun
	s.leaderRun, s.leaderCtx = nil, nil
	s.leaderMu.Unlock()
	run.stop()
	if s.InformerManager != nil {
		s.InformerManager.Stop()
	}
	run.wait()
}

func (s *restServer) stopLeader() {
	s.stopLeaderRun()
	if s.leading.Load() {
		ctx, cancel := context.WithTimeout(context.Background(), leaderElectionReleaseTimeout)
		if err := s.withdrawLeaderService(ctx); err != nil {
			klog.ErrorS(err, "withdraw Leader API endpoint")
		}
		cancel()
	}
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
	s.workersReady = false
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
