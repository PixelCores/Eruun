package application

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestListComponentContainersReturnsPodsAndContainerStates(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
	}))

	now := time.Now()
	podOld := newComponentContainersPod(
		"pod-old",
		config.DefaultNamespace,
		"app-1",
		"api",
		now.Add(-2*time.Minute),
		corev1.PodRunning,
		[]corev1.Container{
			{Name: "api", Image: "nginx:1.24"},
		},
		nil,
	)
	podNew := newComponentContainersPod(
		"pod-new",
		config.DefaultNamespace,
		"app-1",
		"api",
		now,
		corev1.PodRunning,
		[]corev1.Container{
			{Name: "api", Image: "nginx:1.25"},
			{Name: "sidecar", Image: "busybox:1.36"},
			{Name: "worker", Image: "alpine:3.20"},
			{Name: "ghost", Image: "scratch"},
		},
		[]corev1.ContainerStatus{
			{
				Name:         "api",
				Ready:        true,
				RestartCount: 2,
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			},
			{
				Name:         "sidecar",
				Ready:        false,
				RestartCount: 4,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
			},
			{
				Name:         "worker",
				Ready:        false,
				RestartCount: 1,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"},
				},
			},
		},
	)
	podDeleting := newComponentContainersPod(
		"pod-deleting",
		config.DefaultNamespace,
		"app-1",
		"api",
		now.Add(time.Minute),
		corev1.PodRunning,
		[]corev1.Container{{Name: "api", Image: "nginx:latest"}},
		nil,
	)
	podDeleting.DeletionTimestamp = &metav1.Time{Time: now.Add(2 * time.Minute)}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(podOld, podNew, podDeleting)

	resp, err := svc.ListComponentContainers(context.Background(), "app-1", "api")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "app-1", resp.AppID)
	require.Equal(t, "api", resp.ComponentName)
	require.Equal(t, config.ServerJob, resp.ComponentType)
	require.Len(t, resp.Pods, 2)
	require.Equal(t, "pod-new", resp.Pods[0].PodName)
	require.Equal(t, "pod-old", resp.Pods[1].PodName)

	containersByName := make(map[string]struct {
		state        string
		reason       string
		ready        bool
		restartCount int32
	})
	for _, c := range resp.Pods[0].Containers {
		containersByName[c.Name] = struct {
			state        string
			reason       string
			ready        bool
			restartCount int32
		}{
			state:        c.State,
			reason:       c.Reason,
			ready:        c.Ready,
			restartCount: c.RestartCount,
		}
	}
	require.Len(t, containersByName, 4)
	require.Equal(t, "running", containersByName["api"].state)
	require.Equal(t, true, containersByName["api"].ready)
	require.Equal(t, int32(2), containersByName["api"].restartCount)
	require.Equal(t, "", containersByName["api"].reason)

	require.Equal(t, "waiting", containersByName["sidecar"].state)
	require.Equal(t, "CrashLoopBackOff", containersByName["sidecar"].reason)
	require.Equal(t, int32(4), containersByName["sidecar"].restartCount)

	require.Equal(t, "terminated", containersByName["worker"].state)
	require.Equal(t, "Completed", containersByName["worker"].reason)
	require.Equal(t, int32(1), containersByName["worker"].restartCount)

	require.Equal(t, "unknown", containersByName["ghost"].state)
	require.Equal(t, "", containersByName["ghost"].reason)
	require.Equal(t, false, containersByName["ghost"].ready)
	require.Equal(t, int32(0), containersByName["ghost"].restartCount)
}

func TestListComponentContainersReturnsEmptyWhenNoPods(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
	}))

	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset()

	resp, err := svc.ListComponentContainers(context.Background(), "app-1", "api")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, config.ServerJob, resp.ComponentType)
	require.Empty(t, resp.Pods)
}

func TestListComponentContainersReturnsEmptyForNonPodComponent(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "cm",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ConfJob,
	}))

	svc := newMockServiceWithStore(store)

	resp, err := svc.ListComponentContainers(context.Background(), "app-1", "cm")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, config.ConfJob, resp.ComponentType)
	require.Empty(t, resp.Pods)
}

func TestListComponentContainersReturnsComponentNotFound(t *testing.T) {
	svc := newMockServiceWithStore(newInMemoryAppStore())

	resp, err := svc.ListComponentContainers(context.Background(), "app-1", "missing")
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrComponentNotFound)
}

func newComponentContainersPod(
	name, namespace, appID, componentName string,
	createdAt time.Time,
	phase corev1.PodPhase,
	containers []corev1.Container,
	statuses []corev1.ContainerStatus,
) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				config.LabelAppID:         appID,
				config.LabelComponentName: componentName,
			},
			CreationTimestamp: metav1.NewTime(createdAt),
		},
		Spec: corev1.PodSpec{
			Containers: containers,
		},
		Status: corev1.PodStatus{
			Phase:             phase,
			ContainerStatuses: statuses,
		},
	}
}
