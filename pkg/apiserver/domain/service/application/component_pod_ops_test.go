package application

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

func TestListComponentPodsUsesSourceUIDWithoutManagedLabels(t *testing.T) {
	controller := true
	deploymentUID := types.UID("deployment-uid")
	replicaSetUID := types.UID("replicaset-uid")
	selector, err := model.NewJSONStructByStruct(map[string]string{"app": "api"})
	require.NoError(t, err)
	uid := string(deploymentUID)
	component := &model.ApplicationComponent{
		AppID:                    "app-1",
		Name:                     "api",
		Namespace:                config.DefaultNamespace,
		SourceWorkloadAPIVersion: "apps/v1",
		SourceWorkloadKind:       "Deployment",
		SourceWorkloadName:       "legacy-api",
		SourceWorkloadUID:        &uid,
		SourcePodSelector:        selector,
	}
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name:      "legacy-api-rs",
		Namespace: config.DefaultNamespace,
		UID:       replicaSetUID,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Name:       component.SourceWorkloadName,
			UID:        deploymentUID,
			Controller: &controller,
		}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "legacy-api-rs-abc",
		Namespace: config.DefaultNamespace,
		Labels:    map[string]string{"app": "api"},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       replicaSet.Name,
			UID:        replicaSetUID,
			Controller: &controller,
		}},
	}}
	unrelated := pod.DeepCopy()
	unrelated.Name = "unrelated"
	unrelated.OwnerReferences[0].UID = types.UID("other-replicaset-uid")

	svc := newMockServiceWithStore(newInMemoryAppStore())
	svc.KubeClient = k8sfake.NewSimpleClientset(replicaSet, pod, unrelated)
	pods, err := svc.listComponentPods(context.Background(), component.AppID, component)

	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
	require.Equal(t, pod.Name, pods.Items[0].Name)
	require.Empty(t, pods.Items[0].Labels[config.LabelAppID])
}

func TestExportComponentFilesZipSelectsLatestReadyPod(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	now := time.Now()
	podOld := newComponentLogPod("pod-old", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	podOld.CreationTimestamp = metav1.NewTime(now.Add(-time.Minute))
	podNew := newComponentLogPod("pod-new", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	podNew.CreationTimestamp = metav1.NewTime(now)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(podOld, podNew)
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
			FileName:    "api.zip",
			ContentType: "application/zip",
		}, nil
	}

	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/tmp/out", Container: "api"})
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.Equal(t, "pod-new", stream.PodName)
	require.Equal(t, "api", stream.ContainerName)
	require.Equal(t, "api.zip", stream.FileName)
	require.Equal(t, "application/zip", stream.ContentType)
	require.Equal(t, config.DefaultNamespace, gotNamespace)
	require.Equal(t, "pod-new", gotPod)
	require.Equal(t, "api", gotContainer)
	require.Equal(t, "/tmp/out", gotPath)
}

func TestExportComponentFilesZipPendingPodReturnsPendingError(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-pending",
			Namespace: config.DefaultNamespace,
			Labels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: "api",
			},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "api"}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	orig := archiveComponentPodPathAsZip
	defer func() { archiveComponentPodPathAsZip = orig }()
	called := false
	archiveComponentPodPathAsZip = func(context.Context, kubernetes.Interface, *rest.Config, string, string, string, string) (*kube.PodPathArchiveStream, error) {
		called = true
		return nil, nil
	}

	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/tmp/out"})
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentPendingScheduling)
	require.False(t, called)
}

func TestExportComponentFilesZipMapsInvalidArchivePathError(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/"})
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentFilePathInvalid)
}

func TestExportComponentFilesZipMapsArchivePathLookupError(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	orig := archiveComponentPodPathAsZip
	defer func() { archiveComponentPodPathAsZip = orig }()
	archiveComponentPodPathAsZip = func(context.Context, kubernetes.Interface, *rest.Config, string, string, string, string) (*kube.PodPathArchiveStream, error) {
		return nil, fmt.Errorf("archive pod path: exit error: tar: out: cannot stat: no such file or directory")
	}

	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/tmp/out"})
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentFilePathInvalid)
}

func TestExecComponentShellScriptReturnsExitCodeResult(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1", ManagementMode: config.ManagementModeNative}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	orig := execComponentPodShellScript
	defer func() { execComponentPodShellScript = orig }()
	var gotPod, gotContainer, gotScript string
	execComponentPodShellScript = func(_ context.Context, _ kubernetes.Interface, _ *rest.Config, _, podName, container, script string) (*kube.PodExecResult, error) {
		gotPod = podName
		gotContainer = container
		gotScript = script
		return &kube.PodExecResult{Stdout: "done", Stderr: "failed", ExitCode: 9}, nil
	}

	resp, err := svc.ExecComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done", Container: "api"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "pod-api", resp.PodName)
	require.Equal(t, "api", resp.ContainerName)
	require.Equal(t, 9, resp.ExitCode)
	require.False(t, resp.Succeeded)
	require.Equal(t, "echo done", gotScript)
	require.Equal(t, "pod-api", gotPod)
	require.Equal(t, "api", gotContainer)
}

func TestExecComponentShellScriptRejectsInvalidContainer(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1", ManagementMode: config.ManagementModeNative}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	orig := execComponentPodShellScript
	defer func() { execComponentPodShellScript = orig }()
	called := false
	execComponentPodShellScript = func(context.Context, kubernetes.Interface, *rest.Config, string, string, string, string) (*kube.PodExecResult, error) {
		called = true
		return nil, nil
	}

	resp, err := svc.ExecComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done", Container: "sidecar"})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrComponentContainerInvalid)
	require.False(t, called)
}

func TestStreamComponentShellScriptReturnsEventStream(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1", ManagementMode: config.ManagementModeNative}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	orig := streamComponentPodShellScript
	defer func() { streamComponentPodShellScript = orig }()
	var gotPod, gotContainer, gotScript string
	streamComponentPodShellScript = func(_ context.Context, _ kubernetes.Interface, _ *rest.Config, _, podName, container, script string) (<-chan kube.PodShellStreamEvent, error) {
		gotPod = podName
		gotContainer = container
		gotScript = script
		events := make(chan kube.PodShellStreamEvent, 1)
		events <- kube.PodShellStreamEvent{Type: kube.PodShellStreamEventExit, ExitCode: 0, Succeeded: true}
		close(events)
		return events, nil
	}

	stream, err := svc.StreamComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done", Container: "api"})
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.Equal(t, "pod-api", stream.PodName)
	require.Equal(t, "api", stream.ContainerName)
	require.Equal(t, "echo done", gotScript)
	require.Equal(t, "pod-api", gotPod)
	require.Equal(t, "api", gotContainer)

	received, ok := <-stream.Events
	require.True(t, ok)
	require.Equal(t, kube.PodShellStreamEventExit, received.Type)
	require.Equal(t, 0, received.ExitCode)
	require.True(t, received.Succeeded)
	_, ok = <-stream.Events
	require.False(t, ok)
}

func TestExecComponentShellScriptRejectsObserveApplication(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1", ManagementMode: config.ManagementModeObserve}))

	svc := newMockServiceWithStore(store)
	svc.KubeConfig = &rest.Config{Host: "https://example.test"}

	resp, err := svc.ExecComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "touch /tmp/mutated"})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
}
