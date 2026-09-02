package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestBuildJobInfoRecordUsesRawComponentNameAnnotation(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				config.LabelComponentName: "api-v1",
			},
			Annotations: map[string]string{
				config.AnnotationComponentName: "api.v1",
			},
		},
	}

	record := buildJobInfoRecord(&model.JobTask{
		JobInfo: deployment,
	})

	require.Equal(t, "api.v1", record.ServiceName)
}

func TestBuildJobInfoRecordUsesRawComponentNameAnnotationFromApplyService(t *testing.T) {
	service := applyv1.Service("public-api", "default").
		WithLabels(map[string]string{
			config.LabelComponentName: "api-v1",
		}).
		WithAnnotations(map[string]string{
			config.AnnotationComponentName: "api.v1",
		})

	record := buildJobInfoRecord(&model.JobTask{
		Name:    "public-api",
		JobInfo: service,
	})

	require.Equal(t, "api.v1", record.ServiceName)
}

func TestBuildJobInfoRecordFallsBackToBoundedComponentLabel(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				config.LabelComponentName: "api-v1",
			},
		},
	}

	record := buildJobInfoRecord(&model.JobTask{
		JobInfo: deployment,
	})

	require.Equal(t, "api-v1", record.ServiceName)
}

func TestSaveOrUpdateJobInfoIsolatesExecutionsWithinOneGeneration(t *testing.T) {
	store := newCloudJobCheckpointStore()
	first := &model.JobTask{
		TaskID: "task-1", JobType: string(config.JobDeployService), Name: "api",
		RunGeneration: 7, ExecutionKey: "step-0", Status: config.StatusRunning,
	}
	second := *first
	second.ExecutionKey = "step-1"
	second.Status = config.StatusWaiting

	require.NoError(t, saveOrUpdateJobInfo(context.Background(), store, first))
	require.NoError(t, saveOrUpdateJobInfo(context.Background(), store, &second))
	first.Status = config.StatusCompleted
	require.NoError(t, saveOrUpdateJobInfo(context.Background(), store, first))

	statuses := make(map[string]string, len(store.records))
	for _, record := range store.records {
		require.NotNil(t, record.ExecutionKey)
		statuses[*record.ExecutionKey] = record.Status
	}
	require.Len(t, statuses, 2)
	require.Equal(t, string(config.StatusCompleted), statuses["step-0"])
	require.Equal(t, string(config.StatusWaiting), statuses["step-1"])
}
