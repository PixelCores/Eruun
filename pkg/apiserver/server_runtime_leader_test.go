package apiserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
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
	s := &restServer{cfg: *config.NewConfig(), workflow: worker, Queue: &testServerQueue{}}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startWorkers(runtimeCtx)
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
	lock := &testLeaderElectionLock{identity: "self", record: &resourcelock.LeaderElectionRecord{HolderIdentity: "self"}}
	lock.getFunc = func(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
		require.ErrorIs(t, apiCtx.Err(), context.Canceled)
		for _, run := range []*workerRun{controller, scheduler} {
			select {
			case <-run.done:
			default:
				t.Fatal("released Lease before control loop stopped")
			}
		}
		record := *lock.record
		return &record, nil, nil
	}
	election := s.buildRuntimeLeaderElectionConfig(ctx, ctx, lock, nil)
	election.Callbacks.OnStoppedLeading()
	require.Len(t, lock.updates, 1, "the Lease must be released")
	require.Nil(t, s.workersRun, "shutdown must not restart workers")
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

func TestLeaderTermSerializesLateStartupAndStop(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	stopped := make(chan struct{})
	term := serializeLeaderTerm(leaderelection.LeaderCallbacks{
		OnStartedLeading: func(context.Context) { close(started); <-release },
		OnStoppedLeading: func() { close(stopped) },
	})
	go term.OnStartedLeading(context.Background())
	<-started
	go term.OnStoppedLeading()
	select {
	case <-stopped:
		t.Fatal("stop ran while startup was still in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not run after startup finished")
	}
}

func TestLeaderTermRejectsStartDeliveredAfterStop(t *testing.T) {
	starts, stops := 0, 0
	term := serializeLeaderTerm(leaderelection.LeaderCallbacks{OnStartedLeading: func(context.Context) { starts++ }, OnStoppedLeading: func() { stops++ }})
	term.OnStoppedLeading()
	term.OnStartedLeading(context.Background())
	require.Zero(t, starts)
	require.Equal(t, 1, stops)
}

func TestLeaderElectionRetriesAfterLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lock := &renewFailingLeaderLock{identity: "self"}
	stopped := make(chan time.Time, 1)
	election := leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: 400 * time.Millisecond, RenewDeadline: 200 * time.Millisecond, RetryPeriod: 50 * time.Millisecond,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) {},
			OnStoppedLeading: func() { stopped <- time.Now() },
		},
	}
	done := make(chan struct{})
	go func() { defer close(done); (&restServer{}).runRuntimeLeaderElection(ctx, election) }()

	lostAt := <-stopped
	getsAtLoss := lock.gets()
	require.Eventually(t, func() bool { return lock.gets() > getsAtLoss }, 3*leaderElectionRetryPeriod, 10*time.Millisecond, "election was not retried")
	require.GreaterOrEqual(t, time.Since(lostAt), leaderElectionRetryPeriod, "retry must wait for the retry period")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("election did not stop")
	}
}

// renewFailingLeaderLock grants the Lease once and rejects every renewal, so a
// real elector loses each term after RenewDeadline.
type renewFailingLeaderLock struct {
	identity string
	mu       sync.Mutex
	record   *resourcelock.LeaderElectionRecord
	getCalls int
}

func (l *renewFailingLeaderLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.getCalls++
	if l.record == nil {
		return nil, nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "test")
	}
	record := *l.record
	return &record, nil, nil
}

func (l *renewFailingLeaderLock) Create(_ context.Context, record resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.record = &record
	return nil
}

func (l *renewFailingLeaderLock) Update(context.Context, resourcelock.LeaderElectionRecord) error {
	return errors.New("renewal rejected")
}

func (l *renewFailingLeaderLock) RecordEvent(string) {}

func (l *renewFailingLeaderLock) Identity() string { return l.identity }

func (l *renewFailingLeaderLock) Describe() string { return "renew-failing-lock" }

func (l *renewFailingLeaderLock) gets() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.getCalls
}

func TestRuntimeReadinessTracksCurrentDuty(t *testing.T) {
	s := &restServer{}
	ready, _ := s.RuntimeReady()
	require.False(t, ready)
	s.workersRun, s.workersReady = &workerRun{}, true
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
