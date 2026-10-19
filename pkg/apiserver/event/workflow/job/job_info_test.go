package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestBuildJobInfoRecordUsesRawComponentNameAnnotationFromService(t *testing.T) {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: "public-api", Namespace: "default",
		Labels:      map[string]string{config.LabelComponentName: "api-v1"},
		Annotations: map[string]string{config.AnnotationComponentName: "api.v1"},
	}}

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

func TestServiceJobInfoMetadataHandlesTypedNil(t *testing.T) {
	var service *corev1.Service
	require.Nil(t, jobInfoLabels(service))
	require.Nil(t, jobInfoAnnotations(service))
	_, err := serviceFromJobInfo(&model.JobTask{JobInfo: service})
	require.ErrorContains(t, err, "nil")
}
