package application

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube/kubetest"
)

// rejectingExecServer fails the test when an operation reaches pods/exec.
func rejectingExecServer(t *testing.T) *rest.Config {
	return kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		t.Errorf("unexpected exec %v", req.Command)
		return fmt.Errorf("unexpected exec")
	})
}

// tarExecServer answers the tar probe and streams a one-file tar archive,
// reporting each archive request to record.
func tarExecServer(t *testing.T, record func(kubetest.ExecRequest)) *rest.Config {
	return kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if len(req.Command) == 3 && req.Command[2] == "command -v tar >/dev/null 2>&1" {
			return nil
		}
		record(req)
		tw := tar.NewWriter(req.Stdout)
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: "out/file.txt", Mode: 0644, Size: 4, Typeflag: tar.TypeReg}))
		_, _ = tw.Write([]byte("data"))
		return tw.Close()
	})
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
	var got kubetest.ExecRequest
	svc.KubeConfig = tarExecServer(t, func(req kubetest.ExecRequest) { got = req })

	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/tmp/out", Container: "api"})
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.Equal(t, "pod-new", stream.PodName)
	require.Equal(t, "api", stream.ContainerName)
	require.Equal(t, "out.zip", stream.FileName)
	require.Equal(t, "application/zip", stream.ContentType)
	_, err = io.ReadAll(stream.Reader)
	require.NoError(t, err)
	require.NoError(t, stream.Reader.Close())
	require.Equal(t, config.DefaultNamespace, got.Namespace)
	require.Equal(t, "pod-new", got.Pod)
	require.Equal(t, "api", got.Container)
	require.Equal(t, []string{"tar", "-C", "/tmp", "-cf", "-", "--", "out"}, got.Command)
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
				spec.LabelAppID:         "app-1",
				spec.LabelComponentName: "api",
			},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "api"}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = rejectingExecServer(t)

	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/tmp/out"})
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentPendingScheduling)
}

func TestComponentPodOperationsRejectCompletedPods(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			store := newInMemoryAppStore()
			require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1"}))
			require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
				AppID: "app-1", Name: "api", Namespace: config.DefaultNamespace,
			}))
			pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
			pod.Status.Phase = phase
			svc := newMockServiceWithStore(store)
			svc.KubeClient = k8sfake.NewSimpleClientset(pod)
			svc.KubeConfig = rejectingExecServer(t)

			archive, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/tmp/out"})
			require.Nil(t, archive)
			require.ErrorIs(t, err, bcode.ErrComponentPodUnavailable)
			result, err := svc.ExecComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done"})
			require.Nil(t, result)
			require.ErrorIs(t, err, bcode.ErrComponentPodUnavailable)
			stream, err := svc.StreamComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done"})
			require.Nil(t, stream)
			require.ErrorIs(t, err, bcode.ErrComponentPodUnavailable)
		})
	}
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
	svc.KubeConfig = rejectingExecServer(t)

	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/"})
	require.Nil(t, stream)
	require.ErrorIs(t, err, bcode.ErrComponentFilePathInvalid)
}

func TestExportComponentFilesZipReportsArchivePathLookupErrorOnRead(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if len(req.Command) == 3 && req.Command[2] == "command -v tar >/dev/null 2>&1" {
			return nil
		}
		_, _ = io.WriteString(req.Stderr, "tar: out: cannot stat: no such file or directory")
		return kubetest.ExitCode(2)
	})

	// tar runs while the caller reads, so a missing path surfaces on the first
	// read, where the HTTP and gRPC handlers map it to ErrComponentFilePathInvalid.
	stream, err := svc.ExportComponentFilesZip(context.Background(), "app-1", "api", apisv1.ExportComponentFilesRequest{Path: "/tmp/out"})
	require.NoError(t, err)
	defer stream.Reader.Close()
	_, err = bufio.NewReader(stream.Reader).Peek(1)
	require.True(t, kube.IsArchivePathLookupError(err), "got %v", err)
}

func TestExecComponentShellScriptReturnsExitCodeResult(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1"}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	var got kubetest.ExecRequest
	svc.KubeConfig = kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		got = req
		_, _ = io.WriteString(req.Stdout, "done")
		_, _ = io.WriteString(req.Stderr, "failed")
		return kubetest.ExitCode(9)
	})

	resp, err := svc.ExecComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done", Container: "api"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "pod-api", resp.PodName)
	require.Equal(t, "api", resp.ContainerName)
	require.Equal(t, 9, resp.ExitCode)
	require.False(t, resp.Succeeded)
	require.Equal(t, []string{"/bin/sh", "-c", "echo done"}, got.Command)
	require.Equal(t, "pod-api", got.Pod)
	require.Equal(t, "api", got.Container)
}

func TestExecComponentShellScriptRejectsInvalidContainer(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1"}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	svc.KubeConfig = rejectingExecServer(t)

	resp, err := svc.ExecComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done", Container: "sidecar"})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrComponentContainerInvalid)
}

func TestStreamComponentShellScriptReturnsEventStream(t *testing.T) {
	store := newInMemoryAppStore()
	require.NoError(t, store.Add(context.Background(), &model.Applications{ID: "app-1"}))
	require.NoError(t, store.Add(context.Background(), &model.ApplicationComponent{
		AppID:     "app-1",
		Name:      "api",
		Namespace: config.DefaultNamespace,
	}))

	pod := newComponentLogPod("pod-api", config.DefaultNamespace, "app-1", "api", []corev1.Container{{Name: "api"}})
	svc := newMockServiceWithStore(store)
	svc.KubeClient = k8sfake.NewSimpleClientset(pod)
	var got kubetest.ExecRequest
	release := make(chan struct{})
	svc.KubeConfig = kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		got = req
		// Output produced after the method returns, and therefore after the
		// application lock is released, must still stream.
		select {
		case <-release:
		case <-req.Context.Done():
			return req.Context.Err()
		}
		_, err := io.WriteString(req.Stdout, "done\n")
		return err
	})

	stream, err := svc.StreamComponentShellScript(context.Background(), "app-1", "api", apisv1.ExecComponentShellScriptRequest{Script: "echo done", Container: "api"})
	close(release)
	require.NoError(t, err)
	require.NotNil(t, stream)
	require.Equal(t, "pod-api", stream.PodName)
	require.Equal(t, "api", stream.ContainerName)

	received, ok := <-stream.Events
	require.True(t, ok)
	require.Equal(t, kube.PodShellStreamEvent{Type: kube.PodShellStreamEventStdout, Chunk: "done\n"}, received)
	received, ok = <-stream.Events
	require.True(t, ok)
	require.Equal(t, kube.PodShellStreamEventExit, received.Type)
	require.Equal(t, 0, received.ExitCode)
	require.True(t, received.Succeeded)
	_, ok = <-stream.Events
	require.False(t, ok)
	require.Equal(t, []string{"/bin/sh", "-c", "echo done"}, got.Command)
	require.Equal(t, "pod-api", got.Pod)
	require.Equal(t, "api", got.Container)
}
