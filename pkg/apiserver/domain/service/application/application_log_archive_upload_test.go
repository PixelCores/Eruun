package application

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
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
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	orig := archiveComponentPodPathAsZip
	defer func() { archiveComponentPodPathAsZip = orig }()
	var gotNamespace, gotPod, gotContainer, gotPath string
	archiveComponentPodPathAsZip = func(_ context.Context, _ kubernetes.Interface, _ *rest.Config, namespace, podName, container, targetPath string) (*kube.PodPathArchiveStream, error) {
		gotNamespace = namespace
		gotPod = podName
		gotContainer = container
		gotPath = targetPath
		return &kube.PodPathArchiveStream{
			Reader:      io.NopCloser(strings.NewReader("zip")),
			FileName:    "worker.zip",
			ContentType: "application/zip",
		}, nil
	}

	stream, err := svc.DownloadLogArchive(context.Background(), "app-1", apisv1.LogArchiveDownloadRequest{
		JobType:    config.JobLogArchiveUpload,
		Components: []string{"worker"},
		Path:       "/data/logs/archive",
		Container:  "worker",
	})
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.Equal(t, "pod-worker", stream.PodName)
	require.Equal(t, "worker", stream.ContainerName)
	require.Equal(t, "worker.zip", stream.FileName)
	require.Equal(t, "application/zip", stream.ContentType)
	require.Equal(t, config.DefaultNamespace, gotNamespace)
	require.Equal(t, "pod-worker", gotPod)
	require.Equal(t, "worker", gotContainer)
	require.Equal(t, "/data/logs/archive", gotPath)
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
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	orig := archiveComponentPodPathAsZip
	defer func() { archiveComponentPodPathAsZip = orig }()
	called := false
	archiveComponentPodPathAsZip = func(context.Context, kubernetes.Interface, *rest.Config, string, string, string, string) (*kube.PodPathArchiveStream, error) {
		called = true
		return nil, nil
	}

	stream, err := svc.DownloadLogArchive(context.Background(), "app-1", apisv1.LogArchiveDownloadRequest{
		Components: []string{"config"},
		Path:       "/data/logs/archive",
	})
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.False(t, called)
}

func TestDownloadLogArchiveRejectsInvalidRequest(t *testing.T) {
	tests := []struct {
		name       string
		appID      string
		components []string
		path       string
		jobType    config.JobType
		wantErr    error
	}{
		{name: "missing app id", components: []string{"api"}, path: "/data/logs", wantErr: bcode.ErrApplicationNotExist},
		{name: "missing app", appID: "missing", components: []string{"api"}, path: "/data/logs", wantErr: bcode.ErrApplicationNotExist},
		{name: "wrong job type", appID: "app-1", jobType: config.JobDeploy, components: []string{"api"}, path: "/data/logs", wantErr: bcode.ErrApplicationConfig},
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
			svc.KubeConfig = &rest.Config{Host: "https://example.test"}

			stream, err := svc.DownloadLogArchive(context.Background(), tt.appID, apisv1.LogArchiveDownloadRequest{
				JobType:    tt.jobType,
				Components: tt.components,
				Path:       tt.path,
			})
			require.Nil(t, stream)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
