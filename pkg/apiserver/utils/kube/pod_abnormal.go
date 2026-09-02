package kube

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

var abnormalWaitingReasons = map[string]struct{}{
	"CrashLoopBackOff":           {},
	"ImagePullBackOff":           {},
	"ErrImagePull":               {},
	"CreateContainerError":       {},
	"CreateContainerConfigError": {},
}

// ExtractPodAbnormalReason inspects pod container states and returns a stable
// abnormal reason string when the pod is considered unhealthy.
func ExtractPodAbnormalReason(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	if reason := abnormalReasonFromStatuses(pod.Status.InitContainerStatuses, true); reason != "" {
		return reason
	}
	return abnormalReasonFromStatuses(pod.Status.ContainerStatuses, false)
}

func abnormalReasonFromStatuses(statuses []corev1.ContainerStatus, initContainer bool) string {
	for _, status := range statuses {
		if status.State.Waiting != nil {
			reason := strings.TrimSpace(status.State.Waiting.Reason)
			if IsAbnormalWaitingReason(reason) {
				return formatPodAbnormalReason(status.Name, reason, status.State.Waiting.Message)
			}
		}
		if status.State.Terminated != nil {
			if reason := abnormalTerminatedReason(status.State.Terminated); reason != "" {
				return formatPodAbnormalTerminatedReason(status.Name, reason, status.State.Terminated)
			}
		}
		if initContainer && initContainerCompleted(status) {
			continue
		}
		if status.State.Running != nil && status.Ready {
			continue
		}
		if status.LastTerminationState.Terminated != nil {
			if reason := abnormalTerminatedReason(status.LastTerminationState.Terminated); reason != "" {
				return formatPodAbnormalTerminatedReason(status.Name, reason, status.LastTerminationState.Terminated)
			}
		}
	}
	return ""
}

func initContainerCompleted(status corev1.ContainerStatus) bool {
	terminated := status.State.Terminated
	if terminated == nil || terminated.ExitCode != 0 {
		return false
	}
	reason := strings.TrimSpace(terminated.Reason)
	return reason == "" || reason == "Completed"
}

func abnormalTerminatedReason(terminated *corev1.ContainerStateTerminated) string {
	if terminated == nil {
		return ""
	}
	reason := strings.TrimSpace(terminated.Reason)
	if reason == "OOMKilled" || reason == "Error" {
		return reason
	}
	if terminated.ExitCode != 0 {
		if reason != "" {
			return reason
		}
		return "NonZeroExit"
	}
	return ""
}

// IsAbnormalWaitingReason reports whether a waiting reason is considered
// unhealthy and should be treated as an abnormal pod state.
func IsAbnormalWaitingReason(reason string) bool {
	if reason == "" {
		return false
	}
	_, ok := abnormalWaitingReasons[reason]
	return ok
}

func formatPodAbnormalReason(containerName, reason, message string) string {
	msg := strings.TrimSpace(message)
	if msg == "" {
		return fmt.Sprintf("container=%s reason=%s", containerName, reason)
	}
	return fmt.Sprintf("container=%s reason=%s message=%s", containerName, reason, msg)
}

func formatPodAbnormalTerminatedReason(containerName, reason string, terminated *corev1.ContainerStateTerminated) string {
	if terminated == nil {
		return formatPodAbnormalReason(containerName, reason, "")
	}
	msg := strings.TrimSpace(terminated.Message)
	if msg == "" {
		return fmt.Sprintf("container=%s reason=%s exitCode=%d", containerName, reason, terminated.ExitCode)
	}
	return fmt.Sprintf("container=%s reason=%s exitCode=%d message=%s", containerName, reason, terminated.ExitCode, msg)
}
