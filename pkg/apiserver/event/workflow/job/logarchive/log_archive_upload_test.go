package logarchive

import (
	"archive/tar"
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube/kubetest"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type mockArchiveUploader struct {
	input  jobexec.ArchiveUploadInput
	body   string
	err    error
	called int
}

func (m *mockArchiveUploader) UploadArchive(ctx context.Context, input jobexec.ArchiveUploadInput) (*jobexec.ArchiveUploadResult, error) {
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
	return &jobexec.ArchiveUploadResult{
		URL:       "https://static.example.com/logs/api.zip",
		FileName:  input.FileName,
		SizeBytes: int64(len(data)),
	}, nil
}

func TestLogArchiveUploadJobCtlFailsWhenUploaderNotConfigured(t *testing.T) {
	ctl := NewLogArchiveUploadJobCtl(logArchiveUploadTask("api"), &jobexec.Runtime{Client: fake.NewSimpleClientset(), Store: &jobtest.NoopStore{}, KubeConfig: &rest.Config{}})

	err := ctl.run(context.Background())
	require.ErrorIs(t, err, ErrArchiveUploaderNotConfigured)
}

func TestLogArchiveUploadJobCtlUploadsZipAndWritesResultInfo(t *testing.T) {
	var got kubetest.ExecRequest
	kubeConfig := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if isTarProbe(req.Command) {
			return nil
		}
		got = req
		tw := tar.NewWriter(req.Stdout)
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: "app/api.log", Mode: 0644, Size: 4, Typeflag: tar.TypeReg}))
		_, _ = tw.Write([]byte("logs"))
		return tw.Close()
	})
	uploader := &mockArchiveUploader{}
	ctl := NewLogArchiveUploadJobCtl(logArchiveUploadTask("api"), &jobexec.Runtime{Client: fake.NewSimpleClientset(logArchiveUploadPod("api")), Store: &jobtest.NoopStore{}, KubeConfig: kubeConfig, ArchiveUploader: uploader})

	err := ctl.run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, uploader.called)
	require.Equal(t, "default", got.Namespace)
	require.Equal(t, "api-pod", got.Pod)
	require.Equal(t, "api", got.Container)
	require.Equal(t, []string{"tar", "-C", "/var/log", "-cf", "-", "--", "app"}, got.Command)
	archive, err := zip.NewReader(strings.NewReader(uploader.body), int64(len(uploader.body)))
	require.NoError(t, err)
	require.Len(t, archive.File, 1)
	require.Equal(t, "app/api.log", archive.File[0].Name)
	require.Equal(t, "app.zip", uploader.input.FileName)
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
	require.NoError(t, json.Unmarshal([]byte(ctl.Job.Info), &result))
	require.Equal(t, "https://static.example.com/logs/api.zip", result.ArchiveURL)
	require.Equal(t, "api", result.ComponentName)
	require.Equal(t, "default", result.Namespace)
	require.Equal(t, "api-pod", result.PodName)
	require.Equal(t, "api", result.ContainerName)
	require.Equal(t, "/var/log/app", result.Path)
	require.Equal(t, "app.zip", result.FileName)
	require.Equal(t, int64(len(uploader.body)), result.SizeBytes)
}

func TestLogArchiveUploadJobCtlRejectsNonZipArchive(t *testing.T) {
	// Without tar in the container the archive falls back to multipart.
	kubeConfig := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		if isTarProbe(req.Command) {
			return kubetest.ExitCode(1)
		}
		return nil
	})
	uploader := &mockArchiveUploader{}
	ctl := NewLogArchiveUploadJobCtl(logArchiveUploadTask("api"), &jobexec.Runtime{Client: fake.NewSimpleClientset(logArchiveUploadPod("api")), Store: &jobtest.NoopStore{}, KubeConfig: kubeConfig, ArchiveUploader: uploader})

	err := ctl.run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires zip archive")
	require.Equal(t, 0, uploader.called)
}

func TestLogArchiveUploadJobCtlRejectsInvalidRequestedContainer(t *testing.T) {
	kubeConfig := kubetest.NewExecServer(t, func(req kubetest.ExecRequest) error {
		t.Errorf("unexpected exec %v", req.Command)
		return fmt.Errorf("unexpected exec")
	})
	task := logArchiveUploadTask("api")
	task.JobInfo.(*LogArchiveUploadJobInfo).Container = "missing"
	ctl := NewLogArchiveUploadJobCtl(task, &jobexec.Runtime{Client: fake.NewSimpleClientset(logArchiveUploadPod("api")), Store: &jobtest.NoopStore{}, KubeConfig: kubeConfig, ArchiveUploader: &mockArchiveUploader{}})

	err := ctl.run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), `container "missing" not found`)
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
				spec.LabelAppID:         "app-1",
				spec.LabelComponentName: naming.BoundedLabelValue(componentName),
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

func isTarProbe(command []string) bool {
	return len(command) == 3 && command[2] == "command -v tar >/dev/null 2>&1"
}
