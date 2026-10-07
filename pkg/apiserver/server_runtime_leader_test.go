package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestRuntimeLeaderElectionUsesOneLease(t *testing.T) {
	cfg := config.NewConfig()
	s := &restServer{cfg: *cfg}
	lock := &testLeaderElectionLock{identity: cfg.LeaderConfig.ID}
	election := s.buildRuntimeLeaderElectionConfig(context.Background(), context.Background(), lock, nil)
	require.Equal(t, "eruun-runtime", election.Name)
	require.Equal(t, cfg.LeaderConfig.Duration, election.LeaseDuration)
	require.Same(t, lock, election.Lock)
	require.False(t, election.ReleaseOnCancel)
	require.NotNil(t, election.Callbacks.OnStartedLeading)
	require.NotNil(t, election.Callbacks.OnStoppedLeading)
}

func TestPromotionPreservesRunningExecutionAndDemotionResumesIntake(t *testing.T) {
	worker := &contextTrackingServerWorker{contexts: make(chan testWorkerContexts, 2), waitForExecution: true}
	s := &restServer{cfg: *config.NewConfig(), eventWorkers: []event.Worker{worker}}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startWorkers(runtimeCtx, nil)
	first := <-worker.contexts
	require.Eventually(t, func() bool { ready, _ := s.RuntimeReady(); return ready }, time.Second, time.Millisecond)
	election := s.buildRuntimeLeaderElectionConfig(runtimeCtx, runtimeCtx, &testLeaderElectionLock{identity: s.cfg.LeaderConfig.ID}, nil)
	leaderCtx, lose := context.WithCancel(runtimeCtx)
	election.Callbacks.OnStartedLeading(leaderCtx)
	require.ErrorIs(t, first.consumer.Err(), context.Canceled)
	require.NoError(t, first.execution.Err(), "promotion must not interrupt an owned task")
	require.Equal(t, "leader", s.RuntimeRole())
	ready, reason := s.RuntimeReady()
	require.True(t, ready, reason)
	request, release, ok := s.requestLeadership(context.Background())
	require.True(t, ok)
	defer release()
	lose()
	require.Eventually(t, func() bool { return request.Err() != nil }, time.Second, time.Millisecond)
	election.Callbacks.OnStoppedLeading()
	var second testWorkerContexts
	select {
	case second = <-worker.contexts:
	case <-time.After(time.Second):
		t.Fatal("demotion did not restart intake")
	}
	require.Equal(t, "worker", s.RuntimeRole())
	require.NoError(t, second.consumer.Err())
	require.NoError(t, first.execution.Err(), "leadership loss must not cancel an earlier worker task")
	stopped, cancelStop := context.WithCancel(context.Background())
	cancelStop()
	s.stopWorkers(stopped)
	s.drainPromotedWorkers(stopped)
	require.Eventually(t, func() bool { return first.execution.Err() != nil && second.execution.Err() != nil }, time.Second, time.Millisecond)
}

func TestLeaderStopsBothControlLoopsBeforeLeaseRelease(t *testing.T) {
	old := releaseLeaderLock
	defer func() { releaseLeaderLock = old }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &restServer{cfg: *config.NewConfig()}
	controller := newWorkerRun(context.Background())
	scheduler := newWorkerRun(context.Background())
	for _, run := range []*workerRun{controller, scheduler} {
		run.start(func(ctx context.Context) { <-ctx.Done() })
		run.markStarted()
	}
	s.controllerRun, s.schedulerRun = controller, scheduler
	apiCtx, apiCancel := context.WithCancel(context.Background())
	s.leaderCtx, s.leaderCancel = apiCtx, apiCancel
	releaseLeaderLock = func(resourcelock.Interface, time.Duration) bool {
		require.ErrorIs(t, apiCtx.Err(), context.Canceled)
		for _, run := range []*workerRun{controller, scheduler} {
			select {
			case <-run.done:
			default:
				t.Fatal("released Lease before control loop stopped")
			}
		}
		return true
	}
	election := s.buildRuntimeLeaderElectionConfig(ctx, ctx, &testLeaderElectionLock{}, nil)
	election.Callbacks.OnStoppedLeading()
	require.False(t, s.workersStarted, "shutdown must not restart workers")
}

func TestLeaderIgnoresStartupAfterShutdown(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	runtimeCtx, finish := newRuntimeLifecycleContext(parent)
	defer finish()
	s := &restServer{cfg: *config.NewConfig()}
	election := s.buildRuntimeLeaderElectionConfig(parent, runtimeCtx, &testLeaderElectionLock{}, nil)
	cancel()
	require.NoError(t, runtimeCtx.Err())
	election.Callbacks.OnStartedLeading(context.Background())
	require.False(t, s.leading.Load())
	require.Nil(t, s.controllerRun)
}

func TestLeaderWaitsForInitializationBeforeExposingAPI(t *testing.T) {
	s := &restServer{cfg: *config.NewConfig(), Queue: &testServerQueue{ensureGroupErr: context.DeadlineExceeded}}
	errors := make(chan error, 1)
	s.startLeader(context.Background(), errors)
	defer s.stopLeader()
	ready, _ := s.RuntimeReady()
	require.False(t, ready)
	_, _, ok := s.requestLeadership(context.Background())
	require.False(t, ok)
	select {
	case err := <-errors:
		require.Error(t, err)
	default:
		t.Fatal("startup failure not reported")
	}
}

func TestLeaderElectionSerializesLateStartupAndStop(t *testing.T) {
	old := runLeaderElector
	defer func() { runLeaderElector = old }()
	started := make(chan struct{})
	release := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runLeaderElector = func(ctx context.Context, cfg leaderelection.LeaderElectionConfig) {
		leaderCtx, lose := context.WithCancel(ctx)
		go cfg.Callbacks.OnStartedLeading(leaderCtx)
		<-started
		lose()
		cfg.Callbacks.OnStoppedLeading()
		cancel()
	}
	s := &restServer{}
	election := leaderelection.LeaderElectionConfig{Callbacks: leaderelection.LeaderCallbacks{
		OnStartedLeading: func(context.Context) { close(started); <-release },
		OnStoppedLeading: func() { close(stopped) },
	}}
	done := make(chan struct{})
	go func() { defer close(done); s.runRuntimeLeaderElection(ctx, election) }()
	<-started
	select {
	case <-stopped:
		t.Fatal("stop ran while startup was still in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("election did not stop")
	}
	<-stopped
}

func TestLeaderElectionRejectsStartDeliveredAfterStop(t *testing.T) {
	old := runLeaderElector
	defer func() { runLeaderElector = old }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runLeaderElector = func(ctx context.Context, cfg leaderelection.LeaderElectionConfig) {
		cfg.Callbacks.OnStoppedLeading()
		cfg.Callbacks.OnStartedLeading(ctx)
		cancel()
	}
	s := &restServer{}
	starts, stops := 0, 0
	s.runRuntimeLeaderElection(ctx, leaderelection.LeaderElectionConfig{Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: func(context.Context) { starts++ }, OnStoppedLeading: func() { stops++ }}})
	require.Zero(t, starts)
	require.Equal(t, 1, stops)
}

func TestLeaderElectionRetriesAfterLoss(t *testing.T) {
	oldRunner, oldDelay := runLeaderElector, leaderElectionRetryDelay
	defer func() { runLeaderElector = oldRunner; leaderElectionRetryDelay = oldDelay }()
	leaderElectionRetryDelay = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runs := 0
	runLeaderElector = func(context.Context, leaderelection.LeaderElectionConfig) {
		runs++
		if runs == 2 {
			cancel()
		}
	}
	(&restServer{}).runRuntimeLeaderElection(ctx, leaderelection.LeaderElectionConfig{})
	require.Equal(t, 2, runs)
}

func TestRuntimeReadinessTracksCurrentDuty(t *testing.T) {
	s := &restServer{}
	ready, _ := s.RuntimeReady()
	require.False(t, ready)
	s.workersStarted, s.workersReady = true, true
	ready, _ = s.RuntimeReady()
	require.True(t, ready)
	s.leading.Store(true)
	ready, _ = s.RuntimeReady()
	require.False(t, ready)
	ctx, cancel := context.WithCancel(context.Background())
	s.leaderCtx = ctx
	ready, _ = s.RuntimeReady()
	require.True(t, ready)
	cancel()
	ready, _ = s.RuntimeReady()
	require.False(t, ready)
	s.leading.Store(false)
	ready, _ = s.RuntimeReady()
	require.True(t, ready, "a healthy Worker remains ready")
}
