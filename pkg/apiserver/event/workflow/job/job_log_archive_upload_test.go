package job

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type mockArchiveUploader struct {
	input  ArchiveUploadInput
	body   string
	err    error
	called int
}

func (m *mockArchiveUploader) UploadArchive(ctx context.Context, input ArchiveUploadInput) (*ArchiveUploadResult, error) {
	m.called++
	m.input = input
	data, err := io.ReadAll(input.Reader)
	if err != nil {
		return nil, err
	}
	m.body = string(data)
	if m.err != nil {
		return nil, m.err
	}
	return &ArchiveUploadResult{
		URL:       "https://static.example.com/logs/api.zip",
		FileName:  input.FileName,
		SizeBytes: int64(len(data)),
	}, nil
}

func TestLogArchiveUploadJobCtlFailsWhenUploaderNotConfigured(t *testing.T) {
	ctl := NewLogArchiveUploadJobCtl(logArchiveUploadTask("api"), fake.NewSimpleClientset(), &noopStore{}, nil)
	ctl.setRuntime(&jobRuntime{kubeConfig: &rest.Config{}})

	err := ctl.run(context.Background())
	require.ErrorIs(t, err, ErrArchiveUploaderNotConfigured)
}

func TestLogArchiveUploadJobCtlUploadsZipAndWritesResultInfo(t *testing.T) {
	oldArchive := archivePodPathForUpload
	t.Cleanup(func() { archivePodPathForUpload = oldArchive })
	archivePodPathForUpload = func(_ context.Context, _ kubernetes.Interface, _ *rest.Config, namespace, podName, container, targetPath string) (*kube.PodPathArchiveStream, error) {
		require.Equal(t, "default", namespace)
		require.Equal(t, "api-pod", podName)
		require.Equal(t, "api", container)
		require.Equal(t, "/var/log/app", targetPath)
		return &kube.PodPathArchiveStream{
			Reader:      io.NopCloser(strings.NewReader("zip-bytes")),
			FileName:    "api-logs.zip",
			ContentType: "application/zip",
		}, nil
	}
	uploader := &mockArchiveUploader{}
	ctl := NewLogArchiveUploadJobCtl(logArchiveUploadTask("api"), fake.NewSimpleClientset(logArchiveUploadPod("api")), &noopStore{}, nil)
	ctl.setRuntime(&jobRuntime{kubeConfig: &rest.Config{}, archiveUploader: uploader})

	err := ctl.run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, uploader.called)
	require.Equal(t, "zip-bytes", uploader.body)
	require.Equal(t, "api-logs.zip", uploader.input.FileName)
	require.Equal(t, "application/zip", uploader.input.ContentType)
	require.Equal(t, "app-1", uploader.input.AppID)
	require.Equal(t, "workflow-1", uploader.input.WorkflowID)
	require.Equal(t, "task-1", uploader.input.TaskID)
	require.Equal(t, "api", uploader.input.ComponentName)
	require.Equal(t, "default", uploader.input.Namespace)
	require.Equal(t, "api-pod", uploader.input.PodName)
	require.Equal(t, "api", uploader.input.ContainerName)
	require.Equal(t, "/var/log/app", uploader.input.Path)

	var result LogArchiveUploadJobResult
	require.NoError(t, json.Unmarshal([]byte(ctl.job.Info), &result))
	require.Equal(t, "https://static.example.com/logs/api.zip", result.ArchiveURL)
	require.Equal(t, "api", result.ComponentName)
	require.Equal(t, "default", result.Namespace)
	require.Equal(t, "api-pod", result.PodName)
	require.Equal(t, "api", result.ContainerName)
	require.Equal(t, "/var/log/app", result.Path)
	require.Equal(t, "api-logs.zip", result.FileName)
	require.Equal(t, int64(len("zip-bytes")), result.SizeBytes)
}

func TestLogArchiveUploadJobCtlRejectsNonZipArchive(t *testing.T) {
	oldArchive := archivePodPathForUpload
	t.Cleanup(func() { archivePodPathForUpload = oldArchive })
	archivePodPathForUpload = func(context.Context, kubernetes.Interface, *rest.Config, string, string, string, string) (*kube.PodPathArchiveStream, error) {
		return &kube.PodPathArchiveStream{
			Reader:      io.NopCloser(strings.NewReader("multipart")),
			FileName:    "api-logs.multipart",
			ContentType: "multipart/mixed; boundary=abc",
		}, nil
	}
	uploader := &mockArchiveUploader{}
	ctl := NewLogArchiveUploadJobCtl(logArchiveUploadTask("api"), fake.NewSimpleClientset(logArchiveUploadPod("api")), &noopStore{}, nil)
	ctl.setRuntime(&jobRuntime{kubeConfig: &rest.Config{}, archiveUploader: uploader})

	err := ctl.run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires zip archive")
	require.Equal(t, 0, uploader.called)
}

func TestLogArchiveUploadJobCtlRejectsInvalidRequestedContainer(t *testing.T) {
	oldArchive := archivePodPathForUpload
	t.Cleanup(func() { archivePodPathForUpload = oldArchive })
	archiveCalled := false
	archivePodPathForUpload = func(context.Context, kubernetes.Interface, *rest.Config, string, string, string, string) (*kube.PodPathArchiveStream, error) {
		archiveCalled = true
		return nil, nil
	}
	task := logArchiveUploadTask("api")
	task.JobInfo.(*LogArchiveUploadJobInfo).Container = "missing"
	ctl := NewLogArchiveUploadJobCtl(task, fake.NewSimpleClientset(logArchiveUploadPod("api")), &noopStore{}, nil)
	ctl.setRuntime(&jobRuntime{kubeConfig: &rest.Config{}, archiveUploader: &mockArchiveUploader{}})

	err := ctl.run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), `container "missing" not found`)
	require.False(t, archiveCalled)
}

func logArchiveUploadTask(componentName string) *model.JobTask {
	return &model.JobTask{
		Name:       componentName,
		Namespace:  "default",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobLogArchiveUpload),
		Status:     config.StatusQueued,
		JobInfo: &LogArchiveUploadJobInfo{
			Component: &model.ApplicationComponent{
				ID:            1,
				AppID:         "app-1",
				Name:          componentName,
				Namespace:     "default",
				ComponentType: config.ServerJob,
			},
			Path: "/var/log/app",
		},
	}
}

func logArchiveUploadPod(componentName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      componentName + "-pod",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: naming.BoundedLabelValue(componentName),
			},
			CreationTimestamp: metav1.Now(),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: componentName},
			{Name: "sidecar"},
		}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
}
