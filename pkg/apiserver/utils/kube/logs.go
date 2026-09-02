package kube

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// ComponentLogPodState indicates which type of Pod was selected for logs.
type ComponentLogPodState int

const (
	ComponentLogPodUnavailable ComponentLogPodState = iota
	ComponentLogPodPending
	ComponentLogPodRunning
	ComponentLogPodCompleted
)

// SelectComponentLogPod chooses the most suitable Pod for component log streaming.
func SelectComponentLogPod(pods []corev1.Pod) (*corev1.Pod, ComponentLogPodState) {
	var runningReady *corev1.Pod
	var running *corev1.Pod
	var completed *corev1.Pod
	hasPending := false

	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		switch pod.Status.Phase {
		case corev1.PodRunning:
			if podReady(pod) {
				runningReady = pickNewestPod(runningReady, pod)
				continue
			}
			running = pickNewestPod(running, pod)
		case corev1.PodSucceeded, corev1.PodFailed:
			completed = pickNewestPod(completed, pod)
		case corev1.PodPending:
			hasPending = true
		}
	}

	if runningReady != nil {
		return runningReady, ComponentLogPodRunning
	}
	if running != nil {
		return running, ComponentLogPodRunning
	}
	if completed != nil {
		return completed, ComponentLogPodCompleted
	}
	if hasPending {
		return nil, ComponentLogPodPending
	}
	return nil, ComponentLogPodUnavailable
}

var logContainerNameNormalizer = strings.NewReplacer(
	" ", "",
	"\n", "",
	"\r", "",
	"\t", "",
)

// SelectContainerName picks a container for logs.
// It prefers a normalized name match first and falls back to the first named container.
func SelectContainerName(pod *corev1.Pod, preferredContainer string) string {
	if pod == nil {
		return ""
	}
	normalizedPreferred := normalizeLogContainerName(preferredContainer)
	if normalizedPreferred != "" {
		for _, container := range pod.Spec.Containers {
			if container.Name == "" {
				continue
			}
			if normalizeLogContainerName(container.Name) == normalizedPreferred {
				return container.Name
			}
		}
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != "" {
			return container.Name
		}
	}
	return ""
}

// HasContainerName reports whether a Pod contains the provided container name.
func HasContainerName(pod *corev1.Pod, containerName string) bool {
	if pod == nil {
		return false
	}
	normalizedTarget := normalizeLogContainerName(containerName)
	if normalizedTarget == "" {
		return false
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "" {
			continue
		}
		if normalizeLogContainerName(container.Name) == normalizedTarget {
			return true
		}
	}
	return false
}

func normalizeLogContainerName(value string) string {
	if value == "" {
		return ""
	}
	return logContainerNameNormalizer.Replace(strings.ToLower(value))
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func pickNewestPod(current, candidate *corev1.Pod) *corev1.Pod {
	if candidate == nil {
		return current
	}
	if current == nil {
		return candidate
	}
	if candidate.CreationTimestamp.After(current.CreationTimestamp.Time) {
		return candidate
	}
	return current
}
