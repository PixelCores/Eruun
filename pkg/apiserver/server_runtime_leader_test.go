package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestBuildRuntimeLeaderElectionConfigUsesIndependentScope(t *testing.T) {
	cfg := config.NewConfig()
	server := &restServer{cfg: *cfg}
	lock := &testLeaderElectionLock{identity: cfg.LeaderConfig.ID}

	controller := server.buildRuntimeLeaderElectionConfig(context.Background(), controllerLeaderScope, lock, nil)
	scheduler := server.buildRuntimeLeaderElectionConfig(context.Background(), schedulerLeaderScope, lock, nil)

	require.False(t, controller.ReleaseOnCancel)
	require.False(t, scheduler.ReleaseOnCancel)
	require.Equal(t, "eruun-controller", controller.Name)
	require.Equal(t, "eruun-scheduler", scheduler.Name)
	require.Equal(t, cfg.LeaderConfig.Duration, controller.LeaseDuration)
	require.Equal(t, cfg.LeaderConfig.Duration, scheduler.LeaseDuration)
	require.NotNil(t, controller.Callbacks.OnStartedLeading)
	require.NotNil(t, controller.Callbacks.OnStoppedLeading)
	require.NotNil(t, scheduler.Callbacks.OnStartedLeading)
	require.NotNil(t, scheduler.Callbacks.OnStoppedLeading)
}

func TestRuntimeLeaderScopes(t *testing.T) {
	testCases := []struct {
		name       string
		role       config.RuntimeRole
		wantScopes []runtimeLeaderScope
	}{
		{name: "controller", role: config.RuntimeRoleController, wantScopes: []runtimeLeaderScope{controllerLeaderScope}},
		{name: "scheduler", role: config.RuntimeRoleScheduler, wantScopes: []runtimeLeaderScope{schedulerLeaderScope}},
		{name: "api", role: config.RuntimeRoleAPI},
		{name: "worker", role: config.RuntimeRoleWorker},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Role = tc.role
			var got []runtimeLeaderScope
			for _, scope := range runtimeLeaderScopes(*cfg) {
				if scope.enabled {
					got = append(got, scope.scope)
				}
			}
			require.Equal(t, tc.wantScopes, got)
		})
	}
}

func TestRuntimeLeaderStopsScopeBeforeReleasingLease(t *testing.T) {
	oldRelease := releaseLeaderLock
	t.Cleanup(func() {
		releaseLeaderLock = oldRelease
	})

	run := newWorkerRun(context.Background())
	stopped := make(chan struct{})
	run.start(func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond)
		close(stopped)
	})
	run.markStarted()
	server := &restServer{
		cfg:           *config.NewConfig(),
		controllerRun: run,
	}
	releaseLeaderLock = func(resourcelock.Interface, time.Duration) bool {
		select {
		case <-stopped:
		default:
			t.Fatal("controller work was not stopped before lease release")
		}
		return true
	}

	leaderConfig := server.buildRuntimeLeaderElectionConfig(
		context.Background(),
		controllerLeaderScope,
		&testLeaderElectionLock{identity: server.cfg.LeaderConfig.ID},
		nil,
	)
	leaderConfig.Callbacks.OnStoppedLeading()
}

func TestRuntimeLeaderReportsLossWhenExitEnabled(t *testing.T) {
	cfg := config.NewConfig()
	cfg.ExitOnLostLeader = true
	errChan := make(chan error, 1)
	server := &restServer{cfg: *cfg}
	leaderConfig := server.buildRuntimeLeaderElectionConfig(
		context.Background(),
		controllerLeaderScope,
		&testLeaderElectionLock{identity: cfg.LeaderConfig.ID},
		errChan,
	)

	leaderConfig.Callbacks.OnStoppedLeading()

	select {
	case err := <-errChan:
		require.ErrorContains(t, err, "controller leadership lost")
		require.ErrorContains(t, err, cfg.LeaderConfig.ID)
	default:
		t.Fatal("expected runtime leader loss to be reported")
	}
}

func TestRuntimeLeaderDoesNotReportShutdownAsLeaderLoss(t *testing.T) {
	cfg := config.NewConfig()
	cfg.ExitOnLostLeader = true
	errChan := make(chan error, 1)
	server := &restServer{cfg: *cfg}
	ctx, cancel := context.WithCancel(context.Background())
	leaderConfig := server.buildRuntimeLeaderElectionConfig(
		ctx,
		schedulerLeaderScope,
		&testLeaderElectionLock{identity: cfg.LeaderConfig.ID},
		errChan,
	)
	cancel()

	leaderConfig.Callbacks.OnStoppedLeading()

	select {
	case err := <-errChan:
		t.Fatalf("shutdown reported as leader loss: %v", err)
	default:
	}
}

func TestRunRuntimeLeaderElectionDoesNotRetryWhenExitEnabled(t *testing.T) {
	oldRunner := runLeaderElector
	t.Cleanup(func() {
		runLeaderElector = oldRunner
	})
	runs := 0
	runLeaderElector = func(context.Context, leaderelection.LeaderElectionConfig) {
		runs++
	}
	server := &restServer{cfg: config.Config{ExitOnLostLeader: true}}

	server.runRuntimeLeaderElection(context.Background(), runtimeLeaderElection{})

	require.Equal(t, 1, runs)
}

func TestRunRuntimeLeaderElectionRetriesWhenExitDisabled(t *testing.T) {
	oldRunner := runLeaderElector
	t.Cleanup(func() {
		runLeaderElector = oldRunner
	})
	ctx, cancel := context.WithCancel(context.Background())
	runs := 0
	runLeaderElector = func(context.Context, leaderelection.LeaderElectionConfig) {
		runs++
		if runs == 2 {
			cancel()
		}
	}
	server := &restServer{cfg: config.Config{ExitOnLostLeader: false}}

	server.runRuntimeLeaderElection(ctx, runtimeLeaderElection{})

	require.Equal(t, 2, runs)
}

func TestRuntimeReadyAllowsStandbyAndRejectsInitializingLeader(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Role = config.RuntimeRoleController
	server := &restServer{cfg: *cfg}

	ready, reason := server.RuntimeReady()
	require.True(t, ready)
	require.Empty(t, reason)

	server.controllerLeading.Store(true)
	ready, reason = server.RuntimeReady()
	require.False(t, ready)
	require.Contains(t, reason, "controller leader")

	server.controllerReady.Store(true)
	ready, reason = server.RuntimeReady()
	require.True(t, ready)
	require.Empty(t, reason)
}

func TestRuntimeReadyRequiresWorkerSubscriber(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Role = config.RuntimeRoleWorker
	server := &restServer{cfg: *cfg}

	ready, reason := server.RuntimeReady()
	require.False(t, ready)
	require.Contains(t, reason, "worker subscriber")
}
