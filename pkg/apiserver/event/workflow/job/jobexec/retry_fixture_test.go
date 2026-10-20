package jobexec

import (
	"strconv"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func retryTestTask(t *testing.T, policy *workflowconfig.JobRetryPolicy) *model.JobTask {
	t.Helper()
	component := &model.ApplicationComponent{Name: "retry-job", Namespace: "ns", Image: "busybox:1.37"}
	desired := BuildJob(component, &model.Properties{JobRetryPolicy: policy}, JobBuildOptions{})
	desired.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi"), corev1.ResourceCPU: resource.MustParse("100m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi"), corev1.ResourceCPU: resource.MustParse("200m")},
	}
	task := &model.JobTask{Name: desired.Name, Namespace: "ns", JobInfo: desired, JobType: string(config.JobDeployInstant),
		TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 1, Attempt: 1, Timeout: 10}
	stampJobExecutionIdentity(task, desired)
	return task
}

func stampJobExecutionIdentity(jobTask *model.JobTask, jobObj *batchv1.Job) {
	if jobTask == nil || jobObj == nil {
		return
	}
	if jobObj.Annotations == nil {
		jobObj.Annotations = make(map[string]string)
	}
	if jobObj.Labels == nil {
		jobObj.Labels = make(map[string]string)
	}
	jobObj.Labels[config.LabelManagedBy] = config.ManagedByEruun
	if strings.TrimSpace(jobTask.TaskID) != "" {
		jobObj.Annotations[config.AnnotationJobTaskID] = strings.TrimSpace(jobTask.TaskID)
	}
	if strings.TrimSpace(jobTask.ExecutionKey) == "" {
		return
	}
	jobObj.Annotations[config.AnnotationJobExecutionKey] = strings.TrimSpace(jobTask.ExecutionKey)
	if jobTask.RunGeneration > 0 {
		jobObj.Annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(jobTask.RunGeneration, 10)
	}
	attempt := jobTask.Attempt
	if attempt == 0 {
		attempt = uint(jobTask.RetryCount + 1)
	}
	jobObj.Annotations[workflowconfig.AnnotationJobAttempt] = strconv.FormatUint(uint64(attempt), 10)
	jobinfo.StampWorkspaceJobPodIdentity(jobTask, jobObj)
}
