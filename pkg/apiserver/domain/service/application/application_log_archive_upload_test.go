package application

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube/kubetest"
)

func TestDownloadLogArchiveReturnsComponentArchiveStream(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.components["worker"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "worker",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
	}

	pod := newComponentLogPod("pod-worker", config.DefaultNamespace, "app-1", "worker", []corev1.Container{{Name: "worker"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	var got kubetest.ExecRequest
	svc.KubeConfig = tarExecServer(t, func(req kubetest.ExecRequest) { got = req })

	stream, err := svc.DownloadLogArchive(context.Background(), "app-1", apisv1.LogArchiveDownloadRequest{
		Components: []string{"worker"},
		Path:       "/data/logs/archive",
		Container:  "worker",
	})
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.Equal(t, "pod-worker", stream.PodName)
	require.Equal(t, "worker", stream.ContainerName)
	require.Equal(t, "archive.zip", stream.FileName)
	require.Equal(t, "application/zip", stream.ContentType)
	_, err = io.ReadAll(stream.Reader)
	require.NoError(t, err)
	require.NoError(t, stream.Reader.Close())
	require.Equal(t, config.DefaultNamespace, got.Namespace)
	require.Equal(t, "pod-worker", got.Pod)
	require.Equal(t, "worker", got.Container)
	require.Equal(t, []string{"tar", "-C", "/data/logs", "-cf", "-", "--", "archive"}, got.Command)
}

func TestDownloadLogArchiveRejectsNonPodComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
	store.components["config"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "config",
		Namespace:     "default",
		ComponentType: config.ConfJob,
	}
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset()
	svc.KubeConfig = rejectingExecServer(t)

	stream, err := svc.DownloadLogArchive(context.Background(), "app-1", apisv1.LogArchiveDownloadRequest{
		Components: []string{"config"},
		Path:       "/data/logs/archive",
	})
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestDownloadLogArchiveRejectsInvalidRequest(t *testing.T) {
	tests := []struct {
		name       string
		appID      string
		components []string
		path       string
		wantErr    error
	}{
		{name: "missing app id", components: []string{"api"}, path: "/data/logs", wantErr: bcode.ErrApplicationNotExist},
		{name: "missing app", appID: "missing", components: []string{"api"}, path: "/data/logs", wantErr: bcode.ErrApplicationNotExist},
		{name: "missing component", appID: "app-1", path: "/data/logs", wantErr: bcode.ErrApplicationConfig},
		{name: "multiple components", appID: "app-1", components: []string{"api", "worker"}, path: "/data/logs", wantErr: bcode.ErrApplicationConfig},
		{name: "blank component", appID: "app-1", components: []string{" "}, path: "/data/logs", wantErr: bcode.ErrApplicationConfig},
		{name: "unknown component", appID: "app-1", components: []string{"missing"}, path: "/data/logs", wantErr: bcode.ErrApplicationConfig},
		{name: "blank path", appID: "app-1", components: []string{"api"}, path: " ", wantErr: bcode.ErrComponentFilePathInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
			store.components["api"] = &model.ApplicationComponent{
				ID:            1,
				AppID:         "app-1",
				Name:          "api",
				Namespace:     "default",
				ComponentType: config.ServerJob,
				Replicas:      1,
			}
			svc := newMockServiceWithStore(store)
			svc.KubeClient = k8sfake.NewSimpleClientset()
			svc.KubeConfig = rejectingExecServer(t)

			stream, err := svc.DownloadLogArchive(context.Background(), tt.appID, apisv1.LogArchiveDownloadRequest{
				Components: tt.components,
				Path:       tt.path,
			})
			require.Nil(t, stream)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
