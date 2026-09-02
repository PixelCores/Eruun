package informer

import (
	"context"
	"errors"
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
