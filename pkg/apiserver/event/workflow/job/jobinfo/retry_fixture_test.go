package jobinfo

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func retryTestTask(t *testing.T, policy *workflowconfig.JobRetryPolicy) *model.JobTask {
	t.Helper()
	raw, err := json.Marshal(policy)
	require.NoError(t, err)
	backoffLimit := int32(0)
	desired := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "retry-job", Namespace: "ns", Annotations: map[string]string{
			workflowconfig.AnnotationJobRetryPolicy: string(raw),
			workflowconfig.AnnotationJobAttempt:     "1",
		}},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{Name: "retry-job", Image: "busybox:1.37", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi"), corev1.ResourceCPU: resource.MustParse("100m")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi"), corev1.ResourceCPU: resource.MustParse("200m")},
				}}},
			}},
		},
	}
	task := &model.JobTask{Name: desired.Name, Namespace: "ns", JobInfo: desired, JobType: string(config.JobDeployInstant),
		TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 1, Attempt: 1, Timeout: 10}
	ApplyTaskIDAnnotation(task)
	ApplyExecutionIdentity(task)
	return task
}
