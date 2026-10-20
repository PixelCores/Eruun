package jobinfo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
)

func TestBuildJobInfoRecordUsesRawComponentNameAnnotation(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				spec.LabelComponentName: "api-v1",
			},
			Annotations: map[string]string{
				config.AnnotationComponentName: "api.v1",
			},
		},
	}

	record := BuildJobInfoRecord(&model.JobTask{
		JobInfo: deployment,
	})

	require.Equal(t, "api.v1", record.ServiceName)
}

func TestBuildJobInfoRecordUsesRawComponentNameAnnotationFromService(t *testing.T) {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "public-api", Namespace: "default",
		Labels:      map[string]string{spec.LabelComponentName: "api-v1"},
		Annotations: map[string]string{config.AnnotationComponentName: "api.v1"},
	}}

	record := BuildJobInfoRecord(&model.JobTask{
		Name:    "public-api",
		JobInfo: service,
	})

	require.Equal(t, "api.v1", record.ServiceName)
}

func TestBuildJobInfoRecordFallsBackToBoundedComponentLabel(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				spec.LabelComponentName: "api-v1",
			},
		},
	}

	record := BuildJobInfoRecord(&model.JobTask{
		JobInfo: deployment,
	})

	require.Equal(t, "api-v1", record.ServiceName)
}

func TestSaveOrUpdateJobInfoIsolatesExecutionsWithinOneGeneration(t *testing.T) {
	store := jobtest.NewJobInfoRecordStore()
	first := &model.JobTask{
		TaskID: "task-1", JobType: string(config.JobDeployService), Name: "api",
		RunGeneration: 7, ExecutionKey: "step-0", Status: config.StatusRunning,
	}
	second := *first
	second.ExecutionKey = "step-1"
	second.Status = config.StatusWaiting
	ownedStore := jobtest.WithJobTestOwner(store, first, &second)

	require.NoError(t, SaveOrUpdateJobInfo(context.Background(), ownedStore, first))
	require.NoError(t, SaveOrUpdateJobInfo(context.Background(), ownedStore, &second))
	first.Status = config.StatusCompleted
	require.NoError(t, SaveOrUpdateJobInfo(context.Background(), ownedStore, first))

	statuses := make(map[string]string, len(store.Records()))
	for _, record := range store.Records() {
		require.NotNil(t, record.ExecutionKey)
		statuses[*record.ExecutionKey] = record.Status
	}
	require.Len(t, statuses, 2)
	require.Equal(t, string(config.StatusCompleted), statuses["step-0"])
	require.Equal(t, string(config.StatusWaiting), statuses["step-1"])
}

func TestServiceJobInfoMetadataHandlesTypedNil(t *testing.T) {
	var service *corev1.Service
	require.Nil(t, jobInfoLabels(service))
	require.Nil(t, jobInfoAnnotations(service))
	_, err := ServiceFromJobInfo(&model.JobTask{JobInfo: service})
	require.ErrorContains(t, err, "nil")
}
