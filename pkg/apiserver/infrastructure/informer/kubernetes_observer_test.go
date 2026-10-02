package informer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestKubernetesWorkloadObserverFindsReadyMatchingPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "team-a", Labels: map[string]string{
			config.LabelAppID: "app-1", config.LabelComponentName: "api",
		}, Annotations: map[string]string{"rollout": "v2"}},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "example/api:v2"}}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	observer := NewKubernetesWorkloadObserver(fake.NewSimpleClientset(pod))
	startKubernetesWorkloadObserver(t, observer)

	err := observer.WaitForComponentReadyWithOptions(context.Background(), "app-1", "api", 1, ComponentReadyWaitOptions{
		ExpectedImages: []string{"example/api:v2"}, ExpectedAnnotations: map[string]string{"rollout": "v2"},
	}, time.Second)

	require.NoError(t, err)
}

func TestKubernetesWorkloadObserverRetriesTransientListError(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "team-a", Labels: map[string]string{
			config.LabelAppID: "app-1", config.LabelComponentName: "api",
		}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	client := fake.NewSimpleClientset(pod)
	var listCalls atomic.Int32
	client.Fake.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if listCalls.Add(1) == 1 {
			return true, nil, errors.New("temporary API outage")
		}
		return false, nil, nil
	})
	observer := NewKubernetesWorkloadObserver(client)
	observer.pollInterval = time.Millisecond
	startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, observer.Start(startCtx))

	err := observer.WaitForComponentReady(context.Background(), "app-1", "api", 1, time.Second)

	require.NoError(t, err)
	require.GreaterOrEqual(t, listCalls.Load(), int32(2))
}

func TestKubernetesWorkloadObserverFailsWhenInitialSyncTimesOut(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("pod list forbidden")
	})
	observer := NewKubernetesWorkloadObserver(client)
	observer.syncTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := observer.Start(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "synchronize kubernetes workload observer pod cache")
}

func TestKubernetesWorkloadObserverReusesSharedPodCache(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "team-a", Labels: map[string]string{
			config.LabelAppID: "app-1", config.LabelComponentName: "api",
		}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	client := fake.NewSimpleClientset(pod)
	var listCalls atomic.Int32
	client.Fake.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		listCalls.Add(1)
		return false, nil, nil
	})
	observer := NewKubernetesWorkloadObserver(client)
	startKubernetesWorkloadObserver(t, observer)
	initialListCalls := listCalls.Load()

	require.NoError(t, observer.WaitForComponentReady(context.Background(), "app-1", "api", 1, time.Second))
	require.NoError(t, observer.WaitForComponentReady(context.Background(), "app-1", "api", 1, time.Second))
	require.Equal(t, initialListCalls, listCalls.Load())
}

func TestKubernetesWorkloadObserverHonorsCancellation(t *testing.T) {
	observer := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	startKubernetesWorkloadObserver(t, observer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := observer.WaitForComponentReady(ctx, "app-1", "api", 1, time.Second)

	require.Error(t, err)
	var waitErr *WaitError
	require.ErrorAs(t, err, &waitErr)
	require.Equal(t, config.StatusCancelled, waitErr.Status)
}

type stagedPodLister struct {
	calls     atomic.Int32
	abnormal  *corev1.Pod
	recovered *corev1.Pod
}

func (l *stagedPodLister) List(labels.Selector) ([]*corev1.Pod, error) {
	if l.calls.Add(1) == 1 {
		return []*corev1.Pod{l.abnormal}, nil
	}
	return []*corev1.Pod{l.recovered}, nil
}

func (l *stagedPodLister) Pods(string) corelisters.PodNamespaceLister {
	return nil
}

func TestKubernetesWorkloadObserverClearsRecoveredAbnormalSnapshot(t *testing.T) {
	labels := map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "api",
	}
	abnormal := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "team-a", Labels: labels},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "api",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "CrashLoopBackOff",
			}},
		}}},
	}
	recovered := abnormal.DeepCopy()
	recovered.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}
	lister := &stagedPodLister{abnormal: abnormal, recovered: recovered}
	observer := &KubernetesWorkloadObserver{
		client:       fake.NewSimpleClientset(),
		podLister:    lister,
		pollInterval: time.Millisecond,
	}
	observer.synced.Store(true)

	err := observer.WaitForComponentReady(context.Background(), "app-1", "api", 1, 20*time.Millisecond)

	require.Error(t, err)
	waitErr, ok := ExtractWaitError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, waitErr.Status)
	require.Empty(t, waitErr.AbnormalReason)
	require.GreaterOrEqual(t, lister.calls.Load(), int32(2))
}

func startKubernetesWorkloadObserver(t *testing.T, observer *KubernetesWorkloadObserver) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, observer.Start(ctx))
}

func TestWaitForComponentReady(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- waiter.WaitForComponentReady(ctx, "app-1", "api", 1, time.Second)
	}()

	time.Sleep(10 * time.Millisecond)
	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	err := <-result
	require.NoError(t, err)
}

func TestWaitForComponentReadyUsesSnapshot(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- waiter.WaitForComponentReady(ctx, "app-1", "api", 1, time.Second)
	}()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected ready from snapshot before timeout")
	}
}

func TestWaitForComponentReadyWithImagesIgnoresReadyPodWithDifferentImage(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	pod := setTestPodImages(newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true), "api:v1")
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waiter.WaitForComponentReadyWithOptions(ctx, "app-1", "api", 1, ComponentReadyWaitOptions{ExpectedImages: []string{"api:v2"}}, 80*time.Millisecond)
	require.Error(t, err)

	we, ok := ExtractWaitError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, we.Status)
}

func TestWaitForComponentReadyWithImagesUsesReadyPodWithExpectedImage(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	pod := setTestPodImages(newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true), "api:v2")
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, waiter.WaitForComponentReadyWithOptions(ctx, "app-1", "api", 1, ComponentReadyWaitOptions{ExpectedImages: []string{"api:v2"}}, time.Second))
}

func TestWaitForComponentReadyWithImagesTimeoutReturnsFailedForMatchingAbnormalPod(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	oldReady := setTestPodImages(newTestPod("default", "demo-old", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true), "api:v1")
	newAbnormal := setTestPodImages(newTestPod("default", "demo-new", "app-1", "api", 7, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off",
		},
	}, false), "api:v2")
	require.NoError(t, waiter.podInformer.GetIndexer().Add(oldReady))
	require.NoError(t, waiter.podInformer.GetIndexer().Add(newAbnormal))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waiter.WaitForComponentReadyWithOptions(ctx, "app-1", "api", 1, ComponentReadyWaitOptions{ExpectedImages: []string{"api:v2"}}, 80*time.Millisecond)
	require.Error(t, err)

	we, ok := ExtractWaitError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, we.Status)
	require.Contains(t, we.Error(), "CrashLoopBackOff")
	require.Contains(t, we.AbnormalReason, "CrashLoopBackOff")
}

func TestWaitForComponentReadyWithOptionsIgnoresReadyPodWithoutExpectedAnnotation(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	oldReady := setTestPodImages(newTestPod("default", "demo-old", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true), "api:v1")
	require.NoError(t, waiter.podInformer.GetIndexer().Add(oldReady))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waiter.WaitForComponentReadyWithOptions(ctx, "app-1", "api", 1, ComponentReadyWaitOptions{
		ExpectedImages: []string{"api:v1"},
		ExpectedAnnotations: map[string]string{
			config.AnnotationWorkloadRestartAt: "2026-07-02T00:00:00Z",
		},
	}, 80*time.Millisecond)
	require.Error(t, err)

	we, ok := ExtractWaitError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, we.Status)
}

func TestWaitForComponentReadyWithOptionsUsesReadyPodWithExpectedAnnotation(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	restartedAt := "2026-07-02T00:00:00Z"
	pod := setTestPodAnnotations(setTestPodImages(newTestPod("default", "demo-new", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true), "api:v1"), map[string]string{
		config.AnnotationWorkloadRestartAt: restartedAt,
	})
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, waiter.WaitForComponentReadyWithOptions(ctx, "app-1", "api", 1, ComponentReadyWaitOptions{
		ExpectedImages: []string{"api:v1"},
		ExpectedAnnotations: map[string]string{
			config.AnnotationWorkloadRestartAt: restartedAt,
		},
	}, time.Second))
}

func TestWaitForComponentReadyWithOptionsTimeoutReturnsFailedForMatchingAbnormalPod(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	restartedAt := "2026-07-02T00:00:00Z"
	oldReady := setTestPodImages(newTestPod("default", "demo-old", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true), "api:v1")
	newAbnormal := setTestPodAnnotations(setTestPodImages(newTestPod("default", "demo-new", "app-1", "api", 7, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off",
		},
	}, false), "api:v1"), map[string]string{
		config.AnnotationWorkloadRestartAt: restartedAt,
	})
	require.NoError(t, waiter.podInformer.GetIndexer().Add(oldReady))
	require.NoError(t, waiter.podInformer.GetIndexer().Add(newAbnormal))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waiter.WaitForComponentReadyWithOptions(ctx, "app-1", "api", 1, ComponentReadyWaitOptions{
		ExpectedImages: []string{"api:v1"},
		ExpectedAnnotations: map[string]string{
			config.AnnotationWorkloadRestartAt: restartedAt,
		},
	}, 80*time.Millisecond)
	require.Error(t, err)

	we, ok := ExtractWaitError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, we.Status)
	require.Contains(t, we.Error(), "CrashLoopBackOff")
	require.Contains(t, we.AbnormalReason, "CrashLoopBackOff")
}

func TestWaitForComponentReadySnapshotRespectsDesiredReplicas(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- waiter.WaitForComponentReady(ctx, "app-1", "api", 2, 200*time.Millisecond)
	}()

	assertNoResult(t, result, 50*time.Millisecond)

	pod2 := newTestPod("default", "demo-2", "app-1", "api", 7, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod2))

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected ready after adding second pod")
	}
}

func TestWaitForComponentReadyRejectsNonPositiveDesiredReplicas(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)

	for _, desiredReplicas := range []int32{0, -1} {
		t.Run(fmt.Sprintf("replicas_%d", desiredReplicas), func(t *testing.T) {
			err := waiter.WaitForComponentReady(context.Background(), "app-1", "api", desiredReplicas, time.Hour)
			require.ErrorContains(t, err, "requires positive replicas and timeout")
		})
	}
}

func TestWaitForComponentReadyTimeoutReturnsFailedWhenLastAbnormalSeen(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off restarting failed container",
		},
	}, false)
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waiter.WaitForComponentReady(ctx, "app-1", "api", 1, 120*time.Millisecond)
	require.Error(t, err)

	we, ok := ExtractWaitError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, we.Status)
	require.Contains(t, we.Error(), "CrashLoopBackOff")
	require.Contains(t, we.AbnormalReason, "CrashLoopBackOff")
}

func TestWaitForComponentReadyTimeoutReturnsTimeoutWhenOnlyPending(t *testing.T) {
	waiter := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	waiter.pollInterval = time.Millisecond
	startKubernetesWorkloadObserver(t, waiter)
	pod := newTestPod("default", "demo", "app-1", "api", 7, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "ContainerCreating",
			Message: "pod is waiting to be scheduled",
		},
	}, false)
	require.NoError(t, waiter.podInformer.GetIndexer().Add(pod))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waiter.WaitForComponentReady(ctx, "app-1", "api", 1, 120*time.Millisecond)
	require.Error(t, err)

	we, ok := ExtractWaitError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, we.Status)
	require.Empty(t, we.AbnormalReason)
}

func assertNoResult(t *testing.T, result <-chan error, wait time.Duration) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("unexpected early result: %v", err)
	case <-time.After(wait):
	}
}

func setTestPodImages(pod *corev1.Pod, images ...string) *corev1.Pod {
	if pod == nil {
		return nil
	}
	pod.Spec.Containers = make([]corev1.Container, 0, len(images))
	for i, image := range images {
		name := "app-" + strconv.Itoa(i)
		if len(images) == 1 {
			name = "app"
		}
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
			Name:  name,
			Image: image,
		})
	}
	return pod
}

func setTestPodAnnotations(pod *corev1.Pod, annotations map[string]string) *corev1.Pod {
	if pod == nil {
		return nil
	}
	pod.Annotations = annotations
	return pod
}
