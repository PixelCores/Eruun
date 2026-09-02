package kube

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
)

func TestListPodsByLabels(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "match",
				Namespace: "ns",
				Labels: map[string]string{
					"app": "demo",
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "other",
				Namespace: "ns",
				Labels: map[string]string{
					"app": "other",
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "different-namespace",
				Namespace: "other",
				Labels: map[string]string{
					"app": "demo",
				},
			},
		},
	)

	pods, err := ListPodsByLabels(context.Background(), client, "ns", labels.Set{"app": "demo"})
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
	require.Equal(t, "match", pods.Items[0].Name)
}

func TestExtractPodAbnormalReasonWaiting(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "api",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "CrashLoopBackOff",
							Message: "back-off restarting failed container",
						},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Contains(t, reason, "container=api")
	require.Contains(t, reason, "reason=CrashLoopBackOff")
}

func TestExtractPodAbnormalReasonOOMKilled(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "api",
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:  "OOMKilled",
							Message: "killed due to out of memory",
						},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Contains(t, reason, "reason=OOMKilled")
}

func TestExtractPodAbnormalReasonTerminatedError(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "api",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "Error",
							ExitCode: 2,
							Message:  "panic: startup failed",
						},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Contains(t, reason, "container=api")
	require.Contains(t, reason, "reason=Error")
	require.Contains(t, reason, "exitCode=2")
	require.Contains(t, reason, "panic: startup failed")
}

func TestExtractPodAbnormalReasonLastTerminatedError(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "api",
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "Error",
							ExitCode: 1,
						},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Contains(t, reason, "reason=Error")
	require.Contains(t, reason, "exitCode=1")
}

func TestExtractPodAbnormalReasonRecoveredRunningReadyIgnoresLastTerminatedError(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "api",
					Ready: true,
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{},
					},
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "Error",
							ExitCode: 1,
						},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Empty(t, reason)
}

func TestExtractPodAbnormalReasonRecoveredInitContainerIgnoresLastTerminatedError(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "init-db",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "Completed",
							ExitCode: 0,
						},
					},
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "Error",
							ExitCode: 1,
						},
					},
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "api",
					Ready: true,
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Empty(t, reason)
}

func TestExtractPodAbnormalReasonNonZeroExitCode(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "api",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 127,
						},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Contains(t, reason, "reason=NonZeroExit")
	require.Contains(t, reason, "exitCode=127")
}

func TestExtractPodAbnormalReasonHealthyPod(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "api",
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{},
					},
				},
			},
		},
	}

	reason := ExtractPodAbnormalReason(pod)
	require.Empty(t, reason)
}
