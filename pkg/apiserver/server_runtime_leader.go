package apiserver

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

type runtimeLeaderScope string

const (
	controllerLeaderScope runtimeLeaderScope = "controller"
	schedulerLeaderScope  runtimeLeaderScope = "scheduler"
)

type runtimeLeaderScopeConfig struct {
	enabled  bool
	scope    runtimeLeaderScope
	lockName string
}

type runtimeLeaderElection struct {
	scope  runtimeLeaderScope
	config leaderelection.LeaderElectionConfig
}

func runtimeLeaderScopes(cfg config.Config) []runtimeLeaderScopeConfig {
	return []runtimeLeaderScopeConfig{
		{enabled: cfg.RunsController(), scope: controllerLeaderScope, lockName: cfg.LeaderConfig.ControllerLockName},
		{enabled: cfg.RunsScheduler(), scope: schedulerLeaderScope, lockName: cfg.LeaderConfig.SchedulerLockName},
	}
}

func (s *restServer) setupRuntimeLeaderElections(ctx context.Context, errChan chan error) ([]runtimeLeaderElection, error) {
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config for leader election: %w", err)
	}
	namespace := s.cfg.LeaderConfig.Namespace
	if namespace == "" {
		namespace = config.NAMESPACE
	}
	scopes := runtimeLeaderScopes(s.cfg)
	elections := make([]runtimeLeaderElection, 0, len(scopes))
	for _, item := range scopes {
		if !item.enabled {
			continue
		}
		if item.lockName == "" {
			return nil, fmt.Errorf("%s leader election lock name cannot be empty", item.scope)
		}
		lock, lockErr := resourcelock.NewFromKubeconfig(
			resourcelock.LeasesResourceLock,
			namespace,
			item.lockName,
			resourcelock.ResourceLockConfig{Identity: s.cfg.LeaderConfig.ID},
			restConfig,
			10*time.Second,
		)
		if lockErr != nil {
			return nil, fmt.Errorf("create %s leader lock: %w", item.scope, lockErr)
		}
		elections = append(elections, runtimeLeaderElection{
			scope:  item.scope,
			config: s.buildRuntimeLeaderElectionConfig(ctx, item.scope, lock, errChan),
		})
	}
	return elections, nil
}

func (s *restServer) buildRuntimeLeaderElectionConfig(ctx context.Context, scope runtimeLeaderScope, lock resourcelock.Interface, errChan chan error) leaderelection.LeaderElectionConfig {
	return leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   s.cfg.LeaderConfig.Duration,
		RenewDeadline:   renewDeadlineForLeaseDuration(s.cfg.LeaderConfig.Duration),
		RetryPeriod:     leaderElectionRetryPeriod,
		ReleaseOnCancel: false,
		Name:            "eruun-" + string(scope),
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				switch scope {
				case controllerLeaderScope:
					s.onStartedControllerLeading(leaderCtx, errChan)
				case schedulerLeaderScope:
					s.onStartedSchedulerLeading(leaderCtx, errChan)
				}
			},
			OnStoppedLeading: func() {
				s.stopRuntimeLeaderScope(scope)
				releaseLeaderLock(lock, leaderElectionReleaseTimeout)
				if s.cfg.ExitOnLostLeader && ctx.Err() == nil {
					err := fmt.Errorf("%s leadership lost for %s", scope, s.cfg.LeaderConfig.ID)
					if errChan != nil {
						select {
						case errChan <- err:
						case <-ctx.Done():
						}
					}
					return
				}
				klog.Warningf("%s leadership lost; instance remains ready as standby", scope)
			},
			OnNewLeader: func(identity string) {
				klog.InfoS("runtime leader observed", "scope", scope, "identity", identity)
			},
		},
	}
}

func (s *restServer) runRuntimeLeaderElection(ctx context.Context, election runtimeLeaderElection) {
	for ctx.Err() == nil {
		runLeaderElector(ctx, election.config)
		if ctx.Err() != nil {
			return
		}
		if s.cfg.ExitOnLostLeader {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(leaderElectionRetryDelay):
		}
	}
}

func (s *restServer) stopRuntimeLeaderScope(scope runtimeLeaderScope) {
	switch scope {
	case controllerLeaderScope:
		s.stopControllerRun()
	case schedulerLeaderScope:
		s.stopSchedulerRun()
	}
}
