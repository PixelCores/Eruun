package apiserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	workflowevent "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

func TestBuildRuntimeQueuesBuildsOnlyRoleQueues(t *testing.T) {
	for _, tc := range []struct {
		role                    config.RuntimeRole
		dispatch, delay, result bool
	}{
		{role: config.RuntimeRoleAPI},
		{role: config.RuntimeRoleController, delay: true, result: true},
		{role: config.RuntimeRoleScheduler, dispatch: true},
		{role: config.RuntimeRoleWorker, dispatch: true, delay: true},
	} {
		t.Run(string(tc.role), func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Role = tc.role
			cfg.Messaging.Type = config.KAFKA
			cfg.Messaging.KafkaBrokers = []string{"127.0.0.1:9092"}
			server := &restServer{cfg: *cfg}

			queues, err := server.buildRuntimeQueues(nil)

			require.NoError(t, err)
			require.Equal(t, tc.dispatch, queues.Dispatch != nil)
			require.Equal(t, tc.delay, queues.Delay != nil)
			require.Equal(t, tc.result, queues.Result != nil)
			for _, queue := range []msg.Queue{queues.Dispatch, queues.Delay, queues.Result} {
				if queue != nil {
					require.NoError(t, queue.Close(context.Background()))
				}
			}
		})
	}
}

func TestInitRoleObserversBuildsOnlyOwnedObserver(t *testing.T) {
	for _, tc := range []struct {
		role                      config.RuntimeRole
		wantManager, wantObserver bool
	}{
		{role: config.RuntimeRoleAPI},
		{role: config.RuntimeRoleController, wantManager: true},
		{role: config.RuntimeRoleScheduler},
		{role: config.RuntimeRoleWorker, wantObserver: true},
	} {
		t.Run(string(tc.role), func(t *testing.T) {
			server := &restServer{cfg: config.Config{Role: tc.role}}

			server.initRoleObservers(fake.NewSimpleClientset())

			require.Equal(t, tc.wantManager, server.InformerManager != nil)
			require.Equal(t, tc.wantObserver, server.resourceObserver != nil)
		})
	}
}

func TestConfigureWorkflowEventWorkersAssignsRoleDependencies(t *testing.T) {
	dispatch := &testServerQueue{}
	delay := &testServerQueue{}
	result := &testServerQueue{}
	observer := informer.NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	worker := &workflowevent.Workflow{}
	workers := []event.Worker{worker}

	configureWorkflowEventWorkers(workers, &msg.RuntimeQueues{
		Dispatch: dispatch,
		Delay:    delay,
		Result:   result,
	}, observer)

	require.Same(t, dispatch, worker.Queue)
	require.Same(t, delay, worker.DelayQueue)
	require.Same(t, result, worker.ResultQueue)
	require.Same(t, observer, worker.ResourceWaiter)
}
