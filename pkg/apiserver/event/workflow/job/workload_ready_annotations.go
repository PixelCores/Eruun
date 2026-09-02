package job

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func applyPodTemplateReadyWaitAnnotations(job *model.JobTask, template *corev1.PodTemplateSpec) map[string]string {
	annotations := podTemplateReadyWaitAnnotations(job)
	if len(annotations) == 0 || template == nil {
		return nil
	}
	if template.Annotations == nil {
		template.Annotations = make(map[string]string, len(annotations))
	}
	for key, value := range annotations {
		template.Annotations[key] = value
	}
	return annotations
}

func podTemplateReadyWaitAnnotationsFromTemplate(job *model.JobTask, template *corev1.PodTemplateSpec) map[string]string {
	annotations := podTemplateReadyWaitAnnotations(job)
	if len(annotations) == 0 || template == nil || len(template.Annotations) == 0 {
		return nil
	}
	for key, value := range annotations {
		if template.Annotations[key] != value {
			return nil
		}
	}
	return annotations
}

func podTemplateReadyWaitAnnotations(job *model.JobTask) map[string]string {
	if job == nil {
		return nil
	}
	taskID := strings.TrimSpace(job.TaskID)
	if taskID == "" {
		return nil
	}
	return map[string]string{
		config.AnnotationJobTaskID: taskID,
	}
}
