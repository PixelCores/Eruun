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
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/async"
)

func TestResourceReadyWaiterPodAbnormalUpdates(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	updates := make(chan *ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
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
	updates := make(chan *ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
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
	updates := make(chan *ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
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
	updates := make(chan *ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
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
	updates := make(chan *ComponentStatusUpdate, 4)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
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
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
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

func TestStatusSyncOverflowRetriesThroughSameExecutorLane(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	waiter.statusSyncExecutor.Close()
	waiter.statusSyncExecutor = async.NewBoundedExecutor("test-status-sync-timeout", 1, 1)
	t.Cleanup(waiter.Close)

	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	firstStarted := make(chan struct{}, 1)
	overflowStarted := make(chan struct{}, 1)
	overflowUpdates := make(chan *ComponentStatusUpdate, 2)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
		switch update.ComponentID {
		case 1:
			firstStarted <- struct{}{}
			<-releaseFirst
		case 3:
			overflowStarted <- struct{}{}
			overflowUpdates <- cloneStatusUpdate(update)
		}
	})

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "blocker",
		componentID:   1,
		readyCount:    1,
		totalCount:    1,
	})
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking status sync did not start")
	}

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "queued",
		componentID:   2,
		readyCount:    1,
		totalCount:    1,
	})

	start := time.Now()
	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "overflow",
		componentID:   3,
		readyCount:    0,
		totalCount:    1,
	})
	elapsed := time.Since(start)
	require.Less(t, elapsed, statusSyncSubmitTimeout+200*time.Millisecond)

	select {
	case <-overflowStarted:
		t.Fatal("overflow status sync bypassed the saturated executor")
	case <-time.After(100 * time.Millisecond):
	}

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "overflow",
		componentID:   3,
		readyCount:    1,
		totalCount:    1,
	})
	releaseOnce.Do(func() { close(releaseFirst) })

	overflow := readUpdate(t, overflowUpdates)
	require.NotNil(t, overflow.Status)
	require.Equal(t, config.ComponentStatusRunning, *overflow.Status)
	select {
	case extra := <-overflowUpdates:
		t.Fatalf("overflow lane should execute only its latest status, got %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestResetPodSnapshotsDropsQueuedPreviousGenerationStatus(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	waiter.statusSyncExecutor.Close()
	waiter.statusSyncExecutor = async.NewBoundedExecutor("test-status-sync-reset", 1, 2)
	t.Cleanup(waiter.Close)

	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	firstStarted := make(chan struct{}, 1)
	oldGeneration := make(chan *ComponentStatusUpdate, 1)
	marker := make(chan struct{}, 1)
	currentGeneration := make(chan *ComponentStatusUpdate, 1)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
		switch update.ComponentID {
		case 1:
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

	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "blocker", componentID: 1, readyCount: 1, totalCount: 1})
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking status sync did not start")
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

func TestResetPodSnapshotsRejectsPreviousEpochAfterLaneTakesUpdate(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	called := make(chan *ComponentStatusUpdate, 1)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
		called <- cloneStatusUpdate(update)
	})

	previous := buildStatusUpdate(componentSnapshot{
		appID:         "app-1",
		componentName: "api",
		componentID:   7,
		readyCount:    0,
		totalCount:    1,
	})
	key, schedule := waiter.enqueueStatusSync(previous)
	require.True(t, schedule)
	taken, epoch, ok := waiter.takeLatestStatusSync(key)
	require.True(t, ok)
	require.Same(t, previous, taken)

	waiter.ResetPodSnapshots()
	waiter.executeStatusSyncIfCurrent(taken, epoch)
	select {
	case update := <-called:
		t.Fatalf("previous epoch update executed after reset: %+v", update)
	case <-time.After(100 * time.Millisecond):
	}
	waiter.drainStatusSyncLane(key)

	waiter.syncComponentSnapshot(componentSnapshot{
		appID:         "app-1",
		componentName: "api",
		componentID:   7,
		readyCount:    1,
		totalCount:    1,
	})
	current := readUpdate(t, called)
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
	waiter.SetStatusSyncFunc(func(*ComponentStatusUpdate) {
		callbackStarted <- struct{}{}
		<-releaseCallback
	})

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
	waiter.statusSyncExecutor.Close()
	waiter.statusSyncExecutor = async.NewBoundedExecutor("test-status-sync-close", 1, 1)

	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFirst) })
		waiter.Close()
	})
	firstStarted := make(chan struct{}, 1)
	queuedCalled := make(chan struct{}, 1)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
		if update.ComponentID == 1 {
			firstStarted <- struct{}{}
			<-releaseFirst
			return
		}
		queuedCalled <- struct{}{}
	})

	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "blocker", componentID: 1, readyCount: 1, totalCount: 1})
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking status sync did not start")
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

func TestCloseUnblocksDeferredStatusSyncSubmit(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	waiter.statusSyncExecutor.Close()
	waiter.statusSyncExecutor = async.NewBoundedExecutor("test-status-sync-close-deferred", 1, 1)

	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFirst) })
		waiter.Close()
	})
	firstStarted := make(chan struct{}, 1)
	deferredCalled := make(chan struct{}, 1)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
		switch update.ComponentID {
		case 1:
			firstStarted <- struct{}{}
			<-releaseFirst
		case 3:
			deferredCalled <- struct{}{}
		}
	})

	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "blocker", componentID: 1, readyCount: 1, totalCount: 1})
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking status sync did not start")
	}
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "queued", componentID: 2, readyCount: 1, totalCount: 1})
	waiter.syncComponentSnapshot(componentSnapshot{appID: "app-1", componentName: "deferred", componentID: 3, readyCount: 1, totalCount: 1})

	deferredKey := componentStatusSyncKey{appID: "app-1", componentID: 3}
	require.Eventually(t, func() bool {
		waiter.statusSyncMu.Lock()
		defer waiter.statusSyncMu.Unlock()
		lane := waiter.statusSyncLanes[deferredKey]
		return lane != nil && lane.active && lane.latest != nil
	}, time.Second, 5*time.Millisecond, "deferred lane was not activated by retry")

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
		t.Fatal("waiter close deadlocked with retry submit blocked")
	}
	select {
	case <-deferredCalled:
		t.Fatal("deferred status callback executed after close started")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestResourceReadyWaiterCloseIsIdempotent(t *testing.T) {
	waiter := NewResourceReadyWaiter()
	called := make(chan struct{}, 1)
	waiter.SetStatusSyncFunc(func(update *ComponentStatusUpdate) {
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

func readUpdate(t *testing.T, updates <-chan *ComponentStatusUpdate) *ComponentStatusUpdate {
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

func cloneStatusUpdate(update *ComponentStatusUpdate) *ComponentStatusUpdate {
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
