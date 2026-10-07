package informer

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestResourceReadyWaiterPodAbnormalUpdates(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	updates := make(chan *model.ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		updates <- update
	})

	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off",
		},
	}, false)
	waiter.OnPodUpdate(nil, pod)

	first := readUpdate(t, updates)
	require.NotNil(t, first.Status)
	require.Equal(t, config.ComponentStatusFailed, *first.Status)
	require.NotNil(t, first.LastAbnormal)
	require.Contains(t, *first.LastAbnormal, "CrashLoopBackOff")

	podNormal := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	waiter.OnPodUpdate(pod, podNormal)

	second := readUpdate(t, updates)
	require.NotNil(t, second.Status)
	require.Equal(t, config.ComponentStatusRunning, *second.Status)
	require.NotNil(t, second.LastAbnormal)
	require.Equal(t, "", *second.LastAbnormal)
}

func TestResourceReadyWaiterIdenticalPodSnapshotDoesNotSyncTwice(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	updates := make(chan *model.ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		updates <- cloneStatusUpdate(update)
	})

	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	waiter.OnPodAdd(pod)

	first := readUpdate(t, updates)
	require.NotNil(t, first.Status)
	require.Equal(t, config.ComponentStatusRunning, *first.Status)

	waiter.OnPodUpdate(pod, pod.DeepCopy())
	select {
	case update := <-updates:
		t.Fatalf("identical pod snapshot triggered a second status sync: %+v", update)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestResourceReadyWaiterRecoveredRunningReadyClearsLastTerminatedError(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	updates := make(chan *model.ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		updates <- update
	})

	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off",
		},
	}, false)
	waiter.OnPodUpdate(nil, pod)

	first := readUpdate(t, updates)
	require.NotNil(t, first.Status)
	require.Equal(t, config.ComponentStatusFailed, *first.Status)
	require.NotNil(t, first.LastAbnormal)
	require.Contains(t, *first.LastAbnormal, "CrashLoopBackOff")

	podRecovered := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	podRecovered.Status.ContainerStatuses[0].Ready = true
	podRecovered.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			Reason:   "Error",
			ExitCode: 1,
		},
	}
	waiter.OnPodUpdate(pod, podRecovered)

	second := readUpdate(t, updates)
	require.NotNil(t, second.Status)
	require.Equal(t, config.ComponentStatusRunning, *second.Status)
	require.NotNil(t, second.LastAbnormal)
	require.Equal(t, "", *second.LastAbnormal)
}

func TestResourceReadyWaiterRecoveredInitContainerClearsLastTerminatedError(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	updates := make(chan *model.ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		updates <- update
	})

	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, false)
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "init-db",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  "CrashLoopBackOff",
					Message: "back-off",
				},
			},
		},
	}
	waiter.OnPodUpdate(nil, pod)

	first := readUpdate(t, updates)
	require.NotNil(t, first.Status)
	require.Equal(t, config.ComponentStatusFailed, *first.Status)
	require.NotNil(t, first.LastAbnormal)
	require.Contains(t, *first.LastAbnormal, "CrashLoopBackOff")

	podRecovered := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	podRecovered.Status.ContainerStatuses[0].Ready = true
	podRecovered.Status.InitContainerStatuses = []corev1.ContainerStatus{
		{
			Name: "init-db",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Reason:   "Completed",
					ExitCode: 0,
				},
			},
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Reason:   "Error",
					ExitCode: 1,
				},
			},
		},
	}
	waiter.OnPodUpdate(pod, podRecovered)

	second := readUpdate(t, updates)
	require.NotNil(t, second.Status)
	require.Equal(t, config.ComponentStatusRunning, *second.Status)
	require.NotNil(t, second.LastAbnormal)
	require.Equal(t, "", *second.LastAbnormal)
}

func TestResourceReadyWaiterResetPodSnapshotsClearsReadySnapshot(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	pod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 0)
	waiter.OnPodAdd(pod)

	require.Equal(t, 1, podSnapshotCount(waiter))
	require.Equal(t, 1, podRestartSnapshotCount(waiter))

	waiter.ResetPodSnapshots()
	require.Equal(t, 0, podSnapshotCount(waiter))
	require.Equal(t, 0, podRestartSnapshotCount(waiter))

	relistedPod := newDeploymentTestPod("default", "demo-relisted", "app-1", "api", 7, 0)
	waiter.OnPodAdd(relistedPod)
	require.Equal(t, 1, podSnapshotCount(waiter))
	require.Equal(t, 1, podRestartSnapshotCount(waiter))
}

func TestDeploymentPodRestartThresholdTriggersOncePerWindow(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	now := time.Unix(1700000000, 0)
	waiter.now = func() time.Time { return now }
	waiter.SetPodRestartMonitorConfigFunc(func(context.Context) (PodRestartMonitorConfig, error) {
		return PodRestartMonitorConfig{
			Enabled:   true,
			Window:    30 * time.Minute,
			Threshold: 3,
		}, nil
	})

	events := make(chan DeploymentPodRestartEvent, 2)
	waiter.SetDeploymentPodRestartTriggerFunc(func(event DeploymentPodRestartEvent) {
		events <- event
	})

	oldPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 0)
	waiter.OnPodAdd(oldPod)
	for i := int32(1); i <= 3; i++ {
		now = now.Add(time.Minute)
		newPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, i)
		waiter.OnPodUpdate(oldPod, newPod)
		oldPod = newPod
	}

	event := readRestartEvent(t, events)
	require.Equal(t, "default", event.Namespace)
	require.Equal(t, "demo", event.PodName)
	require.Equal(t, "app-1", event.AppID)
	require.Equal(t, "api", event.ComponentName)
	require.Equal(t, 7, event.ComponentID)
	require.Equal(t, 3, event.RestartCount)
	require.Equal(t, 3, event.Threshold)
	require.Equal(t, 30*time.Minute, event.Window)

	now = now.Add(time.Minute)
	newPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 4)
	waiter.OnPodUpdate(oldPod, newPod)
	assertNoRestartEvent(t, events, 100*time.Millisecond)
}

func TestDeploymentPodRestartThresholdCanTriggerAfterWindowExpires(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	now := time.Unix(1700000000, 0)
	waiter.now = func() time.Time { return now }
	waiter.SetPodRestartMonitorConfigFunc(func(context.Context) (PodRestartMonitorConfig, error) {
		return PodRestartMonitorConfig{
			Enabled:   true,
			Window:    30 * time.Minute,
			Threshold: 3,
		}, nil
	})

	events := make(chan DeploymentPodRestartEvent, 2)
	waiter.SetDeploymentPodRestartTriggerFunc(func(event DeploymentPodRestartEvent) {
		events <- event
	})

	oldPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 0)
	waiter.OnPodAdd(oldPod)
	for i := int32(1); i <= 3; i++ {
		now = now.Add(time.Minute)
		newPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, i)
		waiter.OnPodUpdate(oldPod, newPod)
		oldPod = newPod
	}
	readRestartEvent(t, events)

	now = now.Add(31 * time.Minute)
	for i := int32(4); i <= 6; i++ {
		newPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, i)
		waiter.OnPodUpdate(oldPod, newPod)
		oldPod = newPod
		now = now.Add(time.Minute)
	}
	event := readRestartEvent(t, events)
	require.Equal(t, 3, event.RestartCount)
	require.Equal(t, "demo", event.PodName)
}

func TestDeploymentPodRestartMonitorDisabledDoesNotTrigger(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	waiter.SetPodRestartMonitorConfigFunc(func(context.Context) (PodRestartMonitorConfig, error) {
		return PodRestartMonitorConfig{
			Enabled:   false,
			Window:    30 * time.Minute,
			Threshold: 3,
		}, nil
	})

	events := make(chan DeploymentPodRestartEvent, 1)
	waiter.SetDeploymentPodRestartTriggerFunc(func(event DeploymentPodRestartEvent) {
		events <- event
	})

	oldPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 0)
	waiter.OnPodAdd(oldPod)
	newPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 3)
	waiter.OnPodUpdate(oldPod, newPod)
	assertNoRestartEvent(t, events, 100*time.Millisecond)
}

func TestDeploymentPodRestartMonitorIgnoresNonDeploymentPodAndAddHistory(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	waiter.SetPodRestartMonitorConfigFunc(func(context.Context) (PodRestartMonitorConfig, error) {
		return PodRestartMonitorConfig{
			Enabled:   true,
			Window:    30 * time.Minute,
			Threshold: 3,
		}, nil
	})

	events := make(chan DeploymentPodRestartEvent, 1)
	waiter.SetDeploymentPodRestartTriggerFunc(func(event DeploymentPodRestartEvent) {
		events <- event
	})

	historicalPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 3)
	waiter.OnPodAdd(historicalPod)
	assertNoRestartEvent(t, events, 100*time.Millisecond)

	oldPod := newTestPod("default", "stateful", "app-1", "db", 8, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	oldPod.Status.ContainerStatuses[0].RestartCount = 0
	oldPod.OwnerReferences = []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db"}}
	newPod := oldPod.DeepCopy()
	newPod.Status.ContainerStatuses[0].RestartCount = 3
	waiter.OnPodUpdate(oldPod, newPod)
	assertNoRestartEvent(t, events, 100*time.Millisecond)
}

func TestDeploymentPodRestartMonitorDeleteClearsWindow(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	waiter.SetPodRestartMonitorConfigFunc(func(context.Context) (PodRestartMonitorConfig, error) {
		return PodRestartMonitorConfig{
			Enabled:   true,
			Window:    30 * time.Minute,
			Threshold: 3,
		}, nil
	})

	events := make(chan DeploymentPodRestartEvent, 1)
	waiter.SetDeploymentPodRestartTriggerFunc(func(event DeploymentPodRestartEvent) {
		events <- event
	})

	oldPod := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 0)
	waiter.OnPodAdd(oldPod)
	oneRestart := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 1)
	waiter.OnPodUpdate(oldPod, oneRestart)
	waiter.OnPodDelete(oneRestart)

	recreated := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 1)
	waiter.OnPodAdd(recreated)
	twoRestarts := newDeploymentTestPod("default", "demo", "app-1", "api", 7, 3)
	waiter.OnPodUpdate(recreated, twoRestarts)
	assertNoRestartEvent(t, events, 100*time.Millisecond)
}

func TestStatusSyncSerializesAndCoalescesLatestForSameComponent(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	firstStarted := make(chan struct{}, 1)
	runningStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	updates := make(chan *model.ComponentStatusUpdate, 4)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		if update.Status != nil && *update.Status == config.ComponentStatusPending {
			firstStarted <- struct{}{}
			<-releaseFirst
		}
		if update.Status != nil && *update.Status == config.ComponentStatusRunning {
			runningStarted <- struct{}{}
		}
		updates <- cloneStatusUpdate(update)
	})

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "api-old",
		componentID:   7,
		readyCount:    0,
		totalCount:    1,
	})
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("pending status sync did not start")
	}

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "api-failed",
		componentID:   7,
		readyCount:    0,
		totalCount:    1,
		lastAbnormal:  "CrashLoopBackOff",
	})
	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "api-new",
		componentID:   7,
		readyCount:    1,
		totalCount:    1,
	})

	select {
	case <-runningStarted:
		t.Fatal("same component lane executed running concurrently with pending")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseFirst) })
	first := readUpdate(t, updates)
	second := readUpdate(t, updates)
	require.NotNil(t, first.Status)
	require.NotNil(t, second.Status)
	require.Equal(t, config.ComponentStatusPending, *first.Status)
	require.Equal(t, config.ComponentStatusRunning, *second.Status)
	require.Equal(t, "api-new", second.ComponentName)
	select {
	case extra := <-updates:
		t.Fatalf("intermediate status should have been coalesced, got %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestStatusSyncKeepsDifferentComponentLanesParallel(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	firstStarted := make(chan struct{}, 1)
	secondStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		switch update.ComponentID {
		case 7:
			firstStarted <- struct{}{}
			<-releaseFirst
		case 8:
			secondStarted <- struct{}{}
		}
	})

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "api",
		componentID:   7,
		readyCount:    1,
		totalCount:    1,
	})
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first component lane did not start")
	}
	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "api",
		componentID:   8,
		readyCount:    1,
		totalCount:    1,
	})
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("different component lane should execute in parallel")
	}
	releaseOnce.Do(func() { close(releaseFirst) })
}

func TestStatusSyncBacklogCoalescesLatestWithTwoWorkers(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	started := make(chan struct{}, 2)
	const components = 300
	updates := make(chan *model.ComponentStatusUpdate, components)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		if update.ComponentID <= 2 {
			started <- struct{}{}
			<-release
			return
		}
		updates <- cloneStatusUpdate(update)
	})
	for id := 1; id <= 2; id++ {
		waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: id, totalCount: 1})
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("both status workers did not start")
		}
	}

	// Backlog beyond the old executor capacity must keep only the latest value
	// for each component without blocking the Pod event handler.
	submitted := make(chan struct{})
	go func() {
		defer close(submitted)
		for id := 3; id < components+3; id++ {
			waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: id, totalCount: 1})
			waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: id, totalCount: 1, readyCount: 1})
		}
	}()
	select {
	case <-submitted:
	case <-time.After(time.Second):
		t.Fatal("status submission blocked behind running callbacks")
	}
	select {
	case update := <-updates:
		t.Fatalf("more than two callbacks ran concurrently: %+v", update)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	seen := make(map[int]bool, components)
	for range components {
		update := readUpdate(t, updates)
		require.NotNil(t, update.Status)
		require.Equal(t, config.ComponentStatusRunning, *update.Status)
		require.False(t, seen[update.ComponentID], "component was processed more than once")
		seen[update.ComponentID] = true
	}
	select {
	case update := <-updates:
		t.Fatalf("backlog was not coalesced: %+v", update)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestResetPodSnapshotsDropsQueuedPreviousGenerationStatus(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	firstStarted := make(chan struct{}, 2)
	oldGeneration := make(chan *model.ComponentStatusUpdate, 1)
	marker := make(chan struct{}, 1)
	currentGeneration := make(chan *model.ComponentStatusUpdate, 1)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		switch update.ComponentID {
		case 1, 4:
			firstStarted <- struct{}{}
			<-releaseFirst
		case 2:
			if update.Status != nil && *update.Status == config.ComponentStatusPending {
				oldGeneration <- cloneStatusUpdate(update)
				return
			}
			currentGeneration <- cloneStatusUpdate(update)
		case 3:
			marker <- struct{}{}
		}
	})

	for _, id := range []int{1, 4} {
		waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "blocker", componentID: id, readyCount: 1, totalCount: 1})
	}
	for range 2 {
		select {
		case <-firstStarted:
		case <-time.After(time.Second):
			t.Fatal("blocking status sync did not start")
		}
	}
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "api", componentID: 2, readyCount: 0, totalCount: 1})
	resetDone := make(chan struct{})
	go func() {
		waiter.ResetPodSnapshots()
		close(resetDone)
	}()
	requirePodGenerationWriteFencePending(t, waiter)
	select {
	case <-resetDone:
		t.Fatal("snapshot reset returned while the previous generation callback was still running")
	default:
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("snapshot reset did not finish after the running callback exited")
	}
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "marker", componentID: 3, readyCount: 1, totalCount: 1})
	select {
	case <-marker:
	case <-time.After(time.Second):
		t.Fatal("current generation marker did not execute")
	}
	select {
	case update := <-oldGeneration:
		t.Fatalf("queued previous generation status executed after reset: %+v", update)
	case <-time.After(100 * time.Millisecond):
	}

	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "api", componentID: 2, readyCount: 1, totalCount: 1})
	update := readUpdate(t, currentGeneration)
	require.NotNil(t, update.Status)
	require.Equal(t, config.ComponentStatusRunning, *update.Status)
}

func TestResetPodSnapshotsPreservesSameKeyUpdateAfterOldWorkerFinishes(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	started := make(chan struct{}, 2)
	called := make(chan *model.ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		if update.ComponentID <= 2 {
			started <- struct{}{}
			<-release
			return
		}
		called <- cloneStatusUpdate(update)
	})
	for id := 1; id <= 2; id++ {
		waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: id, totalCount: 1})
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("both status workers did not start")
		}
	}

	// Hold the same key between taking its old payload and finishing processing.
	// Both real workers are occupied, making this reset interleaving deterministic.
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: 7, totalCount: 1})
	key, shutdown := waiter.statusSyncQueue.Get()
	require.False(t, shutdown)
	require.Equal(t, 7, key.componentID)
	taken, epoch, ok := waiter.takeLatestStatusSync(key)
	require.True(t, ok)
	resetDone := make(chan struct{})
	go func() {
		waiter.ResetPodSnapshots()
		close(resetDone)
	}()
	requirePodGenerationWriteFencePending(t, waiter)
	releaseOnce.Do(func() { close(release) })
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("snapshot reset did not finish")
	}

	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: 7, totalCount: 1, readyCount: 1})
	waiter.executeStatusSyncIfCurrent(taken, epoch)
	select {
	case update := <-called:
		t.Fatalf("previous epoch or overlapping same-key callback executed: %+v", update)
	case <-time.After(100 * time.Millisecond):
	}
	waiter.statusSyncQueue.Done(key)
	current := readUpdate(t, called)
	require.Equal(t, 7, current.ComponentID)
	require.NotNil(t, current.Status)
	require.Equal(t, config.ComponentStatusRunning, *current.Status)
}

func TestResetPodSnapshotsWaitsForCurrentEpochCallbackFence(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)

	callbackStarted := make(chan struct{}, 2)
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseCallback) }) })
	waiter.SetStatusSyncFunc(func(*model.ComponentStatusUpdate) {
		callbackStarted <- struct{}{}
		<-releaseCallback
	})

	waiter.statusSyncMu.Lock()
	epoch := waiter.statusSyncEpoch
	waiter.statusSyncMu.Unlock()
	update := &model.ComponentStatusUpdate{AppID: "app-1", ComponentID: 7}
	callbackDone := make(chan struct{})
	go func() {
		waiter.executeStatusSyncIfCurrent(update, epoch)
		close(callbackDone)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("current epoch callback did not start")
	}

	resetDone := make(chan struct{})
	go func() {
		waiter.ResetPodSnapshots()
		close(resetDone)
	}()
	requirePodGenerationWriteFencePending(t, waiter)
	select {
	case <-resetDone:
		t.Fatal("snapshot reset returned while the previous epoch callback was still running")
	default:
	}

	releaseOnce.Do(func() { close(releaseCallback) })
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("current epoch callback did not finish")
	}
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("snapshot reset did not finish after the callback left its generation")
	}

	waiter.executeStatusSyncIfCurrent(update, epoch)
	select {
	case <-callbackStarted:
		t.Fatal("previous epoch callback started after snapshot reset returned")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCloseDropsQueuedStatusSyncCallbacks(t *testing.T) {
	waiter := NewResourceReadyWaiter()

	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFirst) })
		waiter.Close()
	})
	firstStarted := make(chan struct{}, 2)
	queuedCalled := make(chan struct{}, 1)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		if update.ComponentID == 1 || update.ComponentID == 3 {
			firstStarted <- struct{}{}
			<-releaseFirst
			return
		}
		queuedCalled <- struct{}{}
	})

	for _, id := range []int{1, 3} {
		waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "blocker", componentID: id, readyCount: 1, totalCount: 1})
	}
	for range 2 {
		select {
		case <-firstStarted:
		case <-time.After(time.Second):
			t.Fatal("blocking status sync did not start")
		}
	}
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "queued", componentID: 2, readyCount: 1, totalCount: 1})

	closed := make(chan struct{})
	go func() {
		waiter.Close()
		close(closed)
	}()
	select {
	case <-waiter.statusSyncStop:
	case <-time.After(time.Second):
		t.Fatal("waiter close did not stop status sync")
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("waiter close did not finish")
	}
	select {
	case <-queuedCalled:
		t.Fatal("queued status callback executed after close started")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCloseRejectsConcurrentStatusSyncSubmissions(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		waiter.Close()
	})
	started := make(chan int, 3)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		started <- update.ComponentID
		<-release
	})
	for id := 1; id <= 2; id++ {
		waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: id, totalCount: 1})
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("both status workers did not start")
		}
	}
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: 3, totalCount: 1})

	var submitters sync.WaitGroup
	for n := range 8 {
		submitters.Add(1)
		go func() {
			defer submitters.Done()
			for id := 4; id < 204; id++ {
				waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: id + n*200, totalCount: 1})
			}
		}()
	}
	submitted := make(chan struct{})
	go func() { submitters.Wait(); close(submitted) }()
	closed := make(chan struct{})
	go func() { waiter.Close(); close(closed) }()
	select {
	case <-waiter.statusSyncStop:
	case <-time.After(time.Second):
		t.Fatal("waiter close did not stop status sync")
	}
	select {
	case <-submitted:
	case <-time.After(time.Second):
		t.Fatal("close left status submitters blocked")
	}
	select {
	case <-closed:
		t.Fatal("close returned while callbacks were still running")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close deadlocked with concurrent status submission")
	}
	select {
	case id := <-started:
		t.Fatalf("queued callback ran after close started: component %d", id)
	default:
	}
}

func TestStatusSyncPanicPreservesSameKeyFollowup(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	started := make(chan struct{}, 1)
	called := make(chan *model.ComponentStatusUpdate, 1)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		if *update.Status == config.ComponentStatusPending {
			started <- struct{}{}
			<-release
			panic("status callback failed")
		}
		called <- cloneStatusUpdate(update)
	})
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: 7, totalCount: 1})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("panicking callback did not start")
	}
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentID: 7, totalCount: 1, readyCount: 1})
	releaseOnce.Do(func() { close(release) })
	update := readUpdate(t, called)
	require.Equal(t, config.ComponentStatusRunning, *update.Status)
}

func TestResourceReadyWaiterCloseIsIdempotent(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	called := make(chan struct{}, 1)
	waiter.SetStatusSyncFunc(func(update *model.ComponentStatusUpdate) {
		called <- struct{}{}
	})

	waiter.Close()
	waiter.Close()

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "api",
		componentID:   7,
		readyCount:    1,
		totalCount:    1,
	})

	select {
	case <-called:
		t.Fatal("status sync callback should not run after waiter close")
	case <-time.After(100 * time.Millisecond):
	}
}

func newTestPod(namespace, name, appID, componentName string, componentID int, state corev1.ContainerState, ready bool) *corev1.Pod {
	conditions := []corev1.PodCondition{}
	if ready {
		conditions = append(conditions, corev1.PodCondition{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				config.LabelAppID:         appID,
				config.LabelComponentName: componentName,
				config.LabelComponentID:   strconv.Itoa(componentID),
			},
		},
		Status: corev1.PodStatus{
			Conditions: conditions,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "app",
					State: state,
				},
			},
		},
	}
}

func newDeploymentTestPod(namespace, name, appID, componentName string, componentID int, restartCount int32) *corev1.Pod {
	pod := newTestPod(namespace, name, appID, componentName, componentID, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	pod.OwnerReferences = []metav1.OwnerReference{{Kind: podOwnerKindReplicaSet, Name: name + "-rs"}}
	pod.Status.ContainerStatuses[0].RestartCount = restartCount
	return pod
}

func readUpdate(t *testing.T, updates <-chan *model.ComponentStatusUpdate) *model.ComponentStatusUpdate {
	t.Helper()
	select {
	case update := <-updates:
		return update
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for status update")
		return nil
	}
}

func requirePodGenerationWriteFencePending(t *testing.T, waiter *ResourceReadyWaiter) {
	t.Helper()
	require.Eventually(t, func() bool {
		if waiter.podGenerationMu.TryRLock() {
			waiter.podGenerationMu.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond, "generation write fence did not begin")
}

func readRestartEvent(t *testing.T, events <-chan DeploymentPodRestartEvent) DeploymentPodRestartEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for restart event")
		return DeploymentPodRestartEvent{}
	}
}

func assertNoRestartEvent(t *testing.T, events <-chan DeploymentPodRestartEvent, wait time.Duration) {
	t.Helper()
	select {
	case event := <-events:
		t.Fatalf("unexpected restart event: %+v", event)
	case <-time.After(wait):
	}
}

func podSnapshotCount(waiter *ResourceReadyWaiter) int {
	waiter.pods.mu.Lock()
	defer waiter.pods.mu.Unlock()
	return len(waiter.pods.pods)
}

func podRestartSnapshotCount(waiter *ResourceReadyWaiter) int {
	waiter.podRestarts.mu.Lock()
	defer waiter.podRestarts.mu.Unlock()
	return len(waiter.podRestarts.pods)
}

func cloneStatusUpdate(update *model.ComponentStatusUpdate) *model.ComponentStatusUpdate {
	if update == nil {
		return nil
	}
	cloned := *update
	if update.Status != nil {
		status := *update.Status
		cloned.Status = &status
	}
	if update.ReadyReplicas != nil {
		ready := *update.ReadyReplicas
		cloned.ReadyReplicas = &ready
	}
	if update.Replicas != nil {
		replicas := *update.Replicas
		cloned.Replicas = &replicas
	}
	if update.LastAbnormal != nil {
		lastAbnormal := *update.LastAbnormal
		cloned.LastAbnormal = &lastAbnormal
	}
	return &cloned
}
