package informer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
)

func TestManagerCanRestartAfterStop(t *testing.T) {
	manager := NewManager(fake.NewSimpleClientset())
	waiter := manager.GetWaiter()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, manager.Start(ctx))
	require.True(t, manager.IsStarted())

	pod := newDeploymentTestPod("default", "stale-demo", "app-1", "api", 7, 0)
	waiter.OnPodAdd(pod)
	require.NoError(t, waiter.WaitForComponentReady(ctx, "app-1", "api", 1, 100*time.Millisecond))

	manager.Stop()
	require.False(t, manager.IsStarted())
	require.Same(t, waiter, manager.GetWaiter())

	require.NoError(t, manager.Start(ctx))
	require.True(t, manager.IsStarted())
	require.Same(t, waiter, manager.GetWaiter())
	require.Equal(t, 0, podSnapshotCount(waiter))
	require.Equal(t, 0, podRestartSnapshotCount(waiter))

	err := waiter.WaitForComponentReady(ctx, "app-1", "api", 1, 50*time.Millisecond)
	require.Error(t, err)

	manager.Stop()
	require.False(t, manager.IsStarted())
}

func TestManagerRejectsPreviousGenerationHandlersAfterRestart(t *testing.T) {
	manager := NewManager(fake.NewSimpleClientset())
	t.Cleanup(manager.Stop)
	waiter := manager.GetWaiter()

	require.NoError(t, manager.Start(context.Background()))
	oldGeneration := manager.waiterGeneration
	require.NotZero(t, oldGeneration)
	manager.Stop()

	require.NoError(t, manager.Start(context.Background()))
	currentGeneration := manager.waiterGeneration
	require.NotZero(t, currentGeneration)
	require.NotEqual(t, oldGeneration, currentGeneration)

	manager.stopGeneration(oldGeneration)
	require.True(t, manager.IsStarted(), "a late stop from the old runtime must not stop the current runtime")
	require.Equal(t, currentGeneration, manager.waiterGeneration)

	stalePod := newDeploymentTestPod("default", "stale", "app-1", "api", 7, 0)
	waiter.onPodAddForGeneration(oldGeneration, stalePod)
	require.Equal(t, 0, podSnapshotCount(waiter))
	require.Equal(t, 0, podRestartSnapshotCount(waiter))

	currentPod := newDeploymentTestPod("default", "current", "app-1", "api", 7, 0)
	waiter.onPodAddForGeneration(currentGeneration, currentPod)
	require.Equal(t, 1, podSnapshotCount(waiter))
	require.Equal(t, 1, podRestartSnapshotCount(waiter))
}

func TestManagerStopWaitsForHandlerAndClearsItsSnapshot(t *testing.T) {
	manager := NewManager(fake.NewSimpleClientset())
	waiter := manager.GetWaiter()
	waiter.SetStatusSyncFunc(func(*ComponentStatusUpdate) {})
	require.NoError(t, manager.Start(context.Background()))
	generation := manager.waiterGeneration

	var statusUnlockOnce sync.Once
	waiter.statusSyncMu.Lock()
	unlockStatus := func() { statusUnlockOnce.Do(waiter.statusSyncMu.Unlock) }
	t.Cleanup(func() {
		unlockStatus()
		manager.Stop()
	})

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		pod := newDeploymentTestPod("default", "in-flight", "app-1", "api", 7, 0)
		waiter.onPodAddForGeneration(generation, pod)
	}()
	require.Eventually(t, func() bool {
		return podSnapshotCount(waiter) == 1
	}, time.Second, 5*time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		manager.Stop()
		close(stopDone)
	}()
	requirePodGenerationWriteFencePending(t, waiter)
	select {
	case <-stopDone:
		t.Fatal("manager stop returned before the active handler left its generation")
	default:
	}

	unlockStatus()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("generation handler did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("manager stop did not finish after the handler exited")
	}

	require.False(t, manager.IsStarted())
	require.Equal(t, 0, podSnapshotCount(waiter))
	require.Equal(t, 0, podRestartSnapshotCount(waiter))
}

func TestManagerStopWaitsForCurrentGenerationStatusCallback(t *testing.T) {
	manager := NewManager(fake.NewSimpleClientset())
	waiter := manager.GetWaiter()
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseCallback) })
		manager.Stop()
		waiter.Close()
	})

	callbackStarted := make(chan struct{}, 2)
	waiter.SetStatusSyncFunc(func(*ComponentStatusUpdate) {
		callbackStarted <- struct{}{}
		<-releaseCallback
	})
	require.NoError(t, manager.Start(context.Background()))
	waiter.statusSyncMu.Lock()
	epoch := waiter.statusSyncEpoch
	waiter.statusSyncMu.Unlock()
	update := &ComponentStatusUpdate{AppID: "app-1", ComponentID: 7}
	callbackDone := make(chan struct{})
	go func() {
		waiter.executeStatusSyncIfCurrent(update, epoch)
		close(callbackDone)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("current generation status callback did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		manager.Stop()
		close(stopDone)
	}()
	requirePodGenerationWriteFencePending(t, waiter)
	select {
	case <-stopDone:
		t.Fatal("manager stop returned while the current generation callback was still running")
	default:
	}

	releaseOnce.Do(func() { close(releaseCallback) })
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("current generation status callback did not finish")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("manager stop did not finish after the callback left its generation")
	}

	waiter.executeStatusSyncIfCurrent(update, epoch)
	select {
	case <-callbackStarted:
		t.Fatal("previous generation callback started after manager stop returned")
	case <-time.After(100 * time.Millisecond):
	}
}
