package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestCreateApplicationsDoesNotCreateNamespace(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()

	req := apisv1.CreateApplicationsRequest{
		Name:      "ns-create-demo",
		Namespace: "tenant-a",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "cfg",
				ComponentType: config.ConfJob,
				Properties: apisv1.Properties{
					Conf: map[string]string{"app.yaml": "key: value\n"},
				},
			},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Empty(t, svc.KubeClient.(*fake.Clientset).Actions())
}

func TestDeleteApplicationPreservesOwnedNamespaceWhenEmpty(t *testing.T) {
	store := newInMemoryAppStore()
	app := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "tenant-a",
	}
	store.apps[app.ID] = app

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "tenant-a",
			Labels: map[string]string{"eruun.io/workspace-id": "workspace"},
		},
	}
	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset(namespace)

	require.NoError(t, svc.DeleteApplication(context.Background(), app))
	_, exists := store.apps[app.ID]
	require.False(t, exists)

	_, err := svc.KubeClient.CoreV1().Namespaces().Get(context.Background(), "tenant-a", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestDeleteApplicationKeepsNamespaceWhenAnotherAppUsesIt(t *testing.T) {
	store := newInMemoryAppStore()
	app1 := &model.Applications{
		ID:        "app-1",
		Name:      "demo-1",
		Namespace: "tenant-a",
	}
	app2 := &model.Applications{
		ID:        "app-2",
		Name:      "demo-2",
		Namespace: "tenant-a",
	}
	store.apps[app1.ID] = app1
	store.apps[app2.ID] = app2

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "tenant-a",
			Labels: map[string]string{"eruun.io/workspace-id": "workspace"},
		},
	}
	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset(namespace)

	require.NoError(t, svc.DeleteApplication(context.Background(), app1))
	_, exists := store.apps[app1.ID]
	require.False(t, exists)

	_, err := svc.KubeClient.CoreV1().Namespaces().Get(context.Background(), "tenant-a", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestDeleteApplicationKeepsNamespaceWhenUserResourcesRemain(t *testing.T) {
	store := newInMemoryAppStore()
	app := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "tenant-a",
	}
	store.apps[app.ID] = app

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "tenant-a",
			Labels: map[string]string{"eruun.io/workspace-id": "workspace"},
		},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "manual-config",
			Namespace: "tenant-a",
		},
	}
	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset(namespace, cm)

	require.NoError(t, svc.DeleteApplication(context.Background(), app))
	_, exists := store.apps[app.ID]
	require.False(t, exists)

	_, err := svc.KubeClient.CoreV1().Namespaces().Get(context.Background(), "tenant-a", metav1.GetOptions{})
	require.NoError(t, err)
}
