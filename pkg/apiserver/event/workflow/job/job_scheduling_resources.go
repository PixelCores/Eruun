package job

import (
	"encoding/json"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Persist only the submitted short-lived Pod's resource envelope. Workload
// payloads, environment variables and Runner credentials are not copied.
func jobSchedulingResourceSnapshot(task *model.JobTask) (string, error) {
	if task.JobType != string(config.JobCommand) && task.JobType != string(config.JobEval) {
		return "", nil
	}
	workload, ok := task.JobInfo.(*batchv1.Job)
	if !ok || workload == nil {
		return "", nil
	}
	parallelism := int64(1)
	if workload.Spec.Parallelism != nil {
		parallelism = int64(*workload.Spec.Parallelism)
	}
	resources := corev1.ResourceList{corev1.ResourcePods: *resource.NewQuantity(parallelism, resource.DecimalSI)}
	pod := &workload.Spec.Template.Spec
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		for _, limits := range []bool{false, true} {
			quantity, known := admissionPodResource(pod, name, limits)
			if !known {
				continue // Admission fails closed when the configured quota needs this field.
			}
			quantity.Mul(parallelism)
			prefix := "requests."
			if limits {
				prefix = "limits."
			}
			resources[corev1.ResourceName(prefix+string(name))] = quantity
		}
	}
	encoded, err := json.Marshal(resources)
	return string(encoded), err
}

// Account for restartable init containers overlapping subsequent init/main
// containers. Missing declarations remain unknown because LimitRange admission
// may later inject values; assuming zero would under-reserve that Pod.
func admissionPodResource(pod *corev1.PodSpec, name corev1.ResourceName, limits bool) (resource.Quantity, bool) {
	containerResource := func(container corev1.Container) (resource.Quantity, bool) {
		values := container.Resources.Requests
		if limits {
			values = container.Resources.Limits
		}
		q, ok := values[name]
		return q.DeepCopy(), ok && q.Sign() >= 0
	}
	total := resource.Quantity{}
	for _, container := range pod.Containers {
		q, known := containerResource(container)
		if !known {
			return resource.Quantity{}, false
		}
		total.Add(q)
	}
	restartable, initPeak := resource.Quantity{}, resource.Quantity{}
	for _, container := range pod.InitContainers {
		q, known := containerResource(container)
		if !known {
			return resource.Quantity{}, false
		}
		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			restartable.Add(q)
			total.Add(q)
			q = restartable.DeepCopy()
		} else {
			q.Add(restartable)
		}
		if q.Cmp(initPeak) > 0 {
			initPeak = q
		}
	}
	if initPeak.Cmp(total) > 0 {
		total = initPeak
	}
	if pod.Resources != nil {
		values := pod.Resources.Requests
		if limits {
			values = pod.Resources.Limits
		}
		if declared, ok := values[name]; ok && declared.Cmp(total) > 0 {
			total = declared.DeepCopy()
		}
	}
	if overhead, ok := pod.Overhead[name]; ok {
		total.Add(overhead)
	}
	return total, true
}
