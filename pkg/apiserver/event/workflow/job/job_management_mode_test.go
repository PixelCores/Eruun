package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestRunJobRechecksObserveModeBeforeKubernetesWrite(t *testing.T) {
	store := &componentStatusStore{
		managementMode: config.ManagementModeObserve,
		components: []*model.ApplicationComponent{{
			AppID:         "app-1",
			Name:          "app-config",
			Namespace:     "prod",
			ComponentType: config.ConfJob,
		}},
	}
	task := &model.JobTask{
		Name:      "app-config",
		Namespace: "prod",
		AppID:     "app-1",
		JobType:   string(config.JobDeployConfigMap),
		Status:    config.StatusQueued,
		JobInfo: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "prod",
		}},
	}
	client := fake.NewSimpleClientset()
	ackCount := 0

	runJob(context.Background(), task, client, store, func() {
		ackCount++
	}, nil)

	require.Equal(t, config.StatusFailed, task.Status)
	require.Contains(t, task.Error, "observe mode")
	require.Equal(t, 1, ackCount)
	require.Empty(t, client.Actions(), "an already queued observe job must not access Kubernetes")
	require.Len(t, store.jobInfos, 1)
	require.Equal(t, string(config.StatusFailed), store.jobInfos[0].Status)
	require.Contains(t, store.jobInfos[0].Error, "observe mode")
	require.NotNil(t, store.updated)
	require.Equal(t, string(config.ComponentStatusFailed), store.updated.Status)
	require.Contains(t, store.updated.LastAbnormal, "observe mode")
}
