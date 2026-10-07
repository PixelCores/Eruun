package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type leaderPreparationQueue struct {
	testServerQueue
	prepare func(context.Context) error
}

func (q *leaderPreparationQueue) EnsureGroup(ctx context.Context, _ string) error {
	return q.prepare(ctx)
}

type leaderLifecycleRuntime struct {
	testServerWorker
	start func(context.Context, func())
}

func (w *leaderLifecycleRuntime) StartLeader(ctx context.Context, ready func()) { w.start(ctx, ready) }

func TestLeaderStartupWaitsForInformerQueueAndRuntime(t *testing.T) {
	client := fake.NewSimpleClientset()
	listEntered, listRelease := make(chan struct{}), make(chan struct{})
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		close(listEntered)
		<-listRelease
		return true, &corev1.PodList{}, nil
	})
	manager := informer.NewManager(client)
	queueEntered, queueRelease := make(chan struct{}), make(chan struct{})
	queue := &leaderPreparationQueue{prepare: func(ctx context.Context) error {
		close(queueEntered)
		select {
		case <-queueRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	runtimeReady := make(chan func(), 1)
	worker := &leaderLifecycleRuntime{start: func(ctx context.Context, ready func()) {
		runtimeReady <- ready
		<-ctx.Done()
	}}
	server := &restServer{InformerManager: manager, Queue: queue, workflow: worker}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t.Cleanup(func() { server.stopLeader(); manager.GetWaiter().Close() })
	result := make(chan bool, 1)
	go func() { result <- server.startLeader(ctx, nil) }()
	select {
	case <-listEntered:
	case <-ctx.Done():
		t.Fatal("informer list did not start")
	}
	select {
	case <-queueEntered:
		t.Fatal("queue preparation preceded informer snapshot")
	default:
	}
	close(listRelease)
	select {
	case <-queueEntered:
	case <-ctx.Done():
		t.Fatal("queue preparation did not start")
	}
	select {
	case <-runtimeReady:
		t.Fatal("runtime started before queue preparation")
	default:
	}
	close(queueRelease)
	var ready func()
	select {
	case ready = <-runtimeReady:
	case <-ctx.Done():
		t.Fatal("runtime did not start")
	}
	_, _, admitted := server.requestLeadership(context.Background())
	require.False(t, admitted, "API must wait for runtime readiness")
	ready()
	select {
	case started := <-result:
		require.True(t, started)
	case <-ctx.Done():
		t.Fatal("startup did not settle")
	}
	readyNow, reason := server.RuntimeReady()
	require.True(t, readyNow, reason)
}

func TestLeaderCancellationDuringPreparationCleansPartialStartup(t *testing.T) {
	manager := informer.NewManager(fake.NewSimpleClientset())
	entered := make(chan struct{})
	queue := &leaderPreparationQueue{prepare: func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}
	worker := &testServerWorker{}
	server := &restServer{InformerManager: manager, Queue: queue, workflow: worker}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() { server.stopLeader(); manager.GetWaiter().Close() })
	result := make(chan bool, 1)
	go func() { result <- server.startLeader(ctx, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("queue preparation did not start")
	}
	require.True(t, manager.IsStarted())
	cancel()
	select {
	case started := <-result:
		require.False(t, started)
	case <-time.After(time.Second):
		t.Fatal("startup did not cancel")
	}
	require.False(t, manager.IsStarted())
	require.Nil(t, server.leaderRun)
	require.Zero(t, worker.starts.Load())
	_, _, admitted := server.requestLeadership(context.Background())
	require.False(t, admitted)
}

func TestLeaderPublishFailureWaitsForStartedRuntimeToExit(t *testing.T) {
	cancelled, allowExit, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	worker := &leaderLifecycleRuntime{start: func(ctx context.Context, ready func()) {
		ready()
		<-ctx.Done()
		close(cancelled)
		<-allowExit
		close(exited)
	}}
	manager := informer.NewManager(fake.NewSimpleClientset())
	cfg := config.NewConfig()
	cfg.LeaderConfig.ServiceName = "api"
	server := &restServer{cfg: *cfg, workflow: worker, InformerManager: manager}
	t.Cleanup(func() { server.stopLeader(); manager.GetWaiter().Close() })
	errors := make(chan error, 1)
	result := make(chan bool, 1)
	go func() { result <- server.startLeader(context.Background(), errors) }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("publication failure did not cancel runtime")
	}
	select {
	case <-result:
		t.Fatal("startup returned before the failed term exited")
	default:
	}
	_, _, admitted := server.requestLeadership(context.Background())
	require.False(t, admitted)
	close(allowExit)
	select {
	case started := <-result:
		require.False(t, started)
	case <-time.After(time.Second):
		t.Fatal("startup did not finish cleanup")
	}
	select {
	case <-exited:
	default:
		t.Fatal("runtime remained active")
	}
	require.Nil(t, server.leaderRun)
	require.False(t, manager.IsStarted())
	require.ErrorContains(t, <-errors, "publish Leader API endpoint")
}
