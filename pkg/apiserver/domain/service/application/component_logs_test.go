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

func TestStreamComponentLogsInvalidRequestedContainer(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{
		{Name: "sidecar"},
		{Name: "api"},
	})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)

	stream, err := svc.StreamComponentLogs(context.Background(), "app-1", "api", "missing")
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentLogContainerInvalid)
}

func TestStreamComponentLogsNoNamedContainerReturnsUnavailable(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{
		{Name: ""},
	})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)

	stream, err := svc.StreamComponentLogs(context.Background(), "app-1", "api", "")
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentLogUnavailable)
}

func newComponentLogPod(name, namespace, appID, componentName string, containers []corev1.Container) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				config.LabelAppID:         appID,
				config.LabelComponentName: componentName,
			},
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Spec: corev1.PodSpec{
			Containers: containers,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}
