package job

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

// Only a current termination on a Pod controlled by this exact Job UID is OOM
// evidence. Exit code 137, eviction, throttling, and a previous restart are not.
func oomJobContainers(ctx context.Context, client kubernetes.Interface, job *batchv1.Job) (map[string]bool, error) {
	containers := make(map[string]bool)
	if job.UID == "" {
		return nil, fmt.Errorf("inspect OOM: job UID is required")
	}
	pods, err := client.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{batchv1.JobNameLabel: job.Name}.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list OOM evidence for job %s/%s: %w", job.Namespace, job.Name, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !retryPodOwnedByJob(pod, job) || pod.Status.Phase != corev1.PodFailed {
			continue
		}
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
			for _, status := range statuses {
				if terminated := status.State.Terminated; terminated != nil && terminated.Reason == "OOMKilled" {
					containers[status.Name] = true
				}
			}
		}
	}
	return containers, nil
}

func retryPodOwnedByJob(pod *corev1.Pod, job *batchv1.Job) bool {
	if pod == nil || job == nil || job.UID == "" {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == "batch/v1" && owner.Kind == "Job" && owner.Name == job.Name && owner.UID == job.UID && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func growOOMJobResources(job *batchv1.Job, policy *workflowconfig.JobRetryPolicy, oomContainers map[string]bool) (*batchv1.Job, error) {
	next := job.DeepCopy()
	if err := validateRetryJobResources(next, policy); err != nil {
		return nil, err
	}
	matched := 0
	for _, containers := range [][]corev1.Container{next.Spec.Template.Spec.InitContainers, next.Spec.Template.Spec.Containers} {
		for i := range containers {
			container := &containers[i]
			if !oomContainers[container.Name] {
				continue
			}
			matched++
			for name, factor := range map[corev1.ResourceName]int64{
				corev1.ResourceMemory: policy.MemoryGrowthFactor,
				corev1.ResourceCPU:    policy.CPUGrowthFactor,
			} {
				if factor <= 1 {
					continue
				}
				cap := policy.MaxResources[name]
				for _, resources := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
					quantity := resources[name].DeepCopy()
					if !quantity.Mul(factor) || quantity.Cmp(cap) > 0 {
						return nil, fmt.Errorf("OOM retry stopped: growing container %s %s by %d exceeds maxResources %s", container.Name, name, factor, cap.String())
					}
					resources[name] = quantity
				}
			}
		}
	}
	if matched != len(oomContainers) {
		return nil, fmt.Errorf("OOM retry stopped: failed container is absent from the submitted Job template")
	}
	return next, nil
}
