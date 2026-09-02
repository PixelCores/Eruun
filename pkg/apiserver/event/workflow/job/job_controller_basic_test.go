package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestServiceJobCtlBasicBranches(t *testing.T) {
	require.Nil(t, NewDeployServiceJobCtl(nil, nil, nil, nil, nil))

	ackCount := 0
	service := applyv1.Service("svc-a", "default")
	jobTask := &model.JobTask{
		Name:      "svc-a",
		Namespace: "default",
		JobType:   string(config.JobDeployService),
		JobInfo:   service,
	}
	ctl := NewDeployServiceJobCtl(jobTask, nil, &noopStore{}, func() { ackCount++ }, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusFailed, jobTask.Status)

	ctl.client = fake.NewSimpleClientset()
	jobTask.JobInfo = "invalid"
	err = ctl.run(context.Background())
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = ctl.wait(ctx)
	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)

	ctl.job.Timeout = 0
	require.Greater(t, ctl.timeout(), 0)
}

func TestGetServiceStatus(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()

	exists, err := getServiceStatus(ctx, client, "default", "svc-a")
	require.NoError(t, err)
	require.False(t, exists)

	_, err = client.CoreV1().Services("default").Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-a", Namespace: "default"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	exists, err = getServiceStatus(ctx, client, "default", "svc-a")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestPVCJobCtlBasicBranches(t *testing.T) {
	require.Nil(t, NewDeployPVCJobCtl(nil, nil, nil, nil, nil))

	ackCount := 0
	jobTask := &model.JobTask{
		Name:      "pvc-a",
		Namespace: "default",
		JobType:   string(config.JobDeployPVC),
		JobInfo: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-a", Namespace: "default"},
		},
	}
	ctl := NewDeployPVCJobCtl(jobTask, nil, &noopStore{}, func() { ackCount++ }, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusFailed, jobTask.Status)

	ctl.client = fake.NewSimpleClientset()
	jobTask.JobInfo = "invalid"
	err = ctl.run(context.Background())
	require.Error(t, err)

	jobTask.JobInfo = "invalid"
	_, err = ctl.getPVCStatus(context.Background())
	require.Error(t, err)

	ctl.job.Timeout = 0
	require.Greater(t, ctl.timeout(), int64(0))
}

func TestIngressJobCtlBasicBranches(t *testing.T) {
	require.Nil(t, NewDeployIngressJobCtl(nil, nil, nil, nil, nil))

	ackCount := 0
	jobTask := &model.JobTask{
		Name:      "ing-a",
		Namespace: "default",
		JobType:   string(config.JobDeployIngress),
		JobInfo: &networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{Name: "ing-a", Namespace: "default"},
		},
	}
	ctl := NewDeployIngressJobCtl(jobTask, nil, &noopStore{}, func() { ackCount++ }, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusFailed, jobTask.Status)

	ctl.client = fake.NewSimpleClientset()
	jobTask.JobInfo = "invalid"
	err = ctl.run(context.Background())
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = ctl.wait(ctx)
	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)

	ctl.job.Timeout = 0
	require.Greater(t, ctl.timeout(), int64(0))
}
