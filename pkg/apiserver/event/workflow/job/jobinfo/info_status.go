package jobinfo

import (
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func StampWorkspaceJobPodIdentity(task *model.JobTask, workload *batchv1.Job) {
	if !config.IsWorkspaceJobType(config.JobType(task.JobType)) {
		return
	}
	if workload.Spec.Template.Annotations == nil {
		workload.Spec.Template.Annotations = map[string]string{}
	}
	workload.Spec.Template.Annotations[config.AnnotationJobTaskID] = task.TaskID
	workload.Spec.Template.Annotations[config.AnnotationJobExecutionKey] = task.ExecutionKey
	workload.Spec.Template.Annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(task.RunGeneration, 10)
}

func shouldKeepExistingJobInfoStatus(current string, next config.Status) bool {
	currentStatus := config.Status(strings.TrimSpace(current))
	if !isSuccessfulTerminalJobStatus(currentStatus) {
		return false
	}
	return !isSuccessfulTerminalJobStatus(next)
}

func isSuccessfulTerminalJobStatus(status config.Status) bool {
	switch status {
	case config.StatusCompleted, config.StatusPassed, config.StatusSkipped:
		return true
	default:
		return false
	}
}
