package kube

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSelectComponentLogPodPrefersReadyRunning(t *testing.T) {
	now := time.Now()
	pods := []corev1.Pod{
		newLogPod("pending", corev1.PodPending, false, now.Add(-2*time.Minute)),
		newLogPod("running-not-ready", corev1.PodRunning, false, now.Add(-time.Minute)),
		newLogPod("running-ready", corev1.PodRunning, true, now),
	}
	got, state := SelectComponentLogPod(pods)
	require.Equal(t, ComponentLogPodRunning, state)
	require.NotNil(t, got)
	require.Equal(t, "running-ready", got.Name)
}

func TestSelectComponentLogPodPrefersLatestRunning(t *testing.T) {
	now := time.Now()
	pods := []corev1.Pod{
		newLogPod("running-old", corev1.PodRunning, true, now.Add(-2*time.Minute)),
		newLogPod("running-new", corev1.PodRunning, true, now),
	}
	got, state := SelectComponentLogPod(pods)
	require.Equal(t, ComponentLogPodRunning, state)
	require.NotNil(t, got)
	require.Equal(t, "running-new", got.Name)
}

func TestSelectComponentLogPodPrefersRunningOverCompleted(t *testing.T) {
	now := time.Now()
	pods := []corev1.Pod{
		newLogPod("completed", corev1.PodSucceeded, true, now.Add(-time.Minute)),
		newLogPod("running", corev1.PodRunning, true, now),
	}
	got, state := SelectComponentLogPod(pods)
	require.Equal(t, ComponentLogPodRunning, state)
	require.NotNil(t, got)
	require.Equal(t, "running", got.Name)
}

func TestSelectComponentLogPodReturnsCompletedWhenNoRunning(t *testing.T) {
	now := time.Now()
	pods := []corev1.Pod{
		newLogPod("completed-old", corev1.PodFailed, true, now.Add(-2*time.Minute)),
		newLogPod("completed-new", corev1.PodSucceeded, true, now),
	}
	got, state := SelectComponentLogPod(pods)
	require.Equal(t, ComponentLogPodCompleted, state)
	require.NotNil(t, got)
	require.Equal(t, "completed-new", got.Name)
}

func TestSelectComponentLogPodReturnsPendingWhenOnlyPending(t *testing.T) {
	now := time.Now()
	pods := []corev1.Pod{
		newLogPod("pending", corev1.PodPending, false, now),
	}
	got, state := SelectComponentLogPod(pods)
	require.Equal(t, ComponentLogPodPending, state)
	require.Nil(t, got)
}

func TestSelectContainerName(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main"},
				{Name: "sidecar"},
			},
		},
	}
	require.Equal(t, "main", SelectContainerName(pod, ""))
}

func TestSelectContainerNamePrefersRequestedContainer(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "sidecar"},
				{Name: "api"},
			},
		},
	}
	require.Equal(t, "api", SelectContainerName(pod, "api"))
}

func TestSelectContainerNameRequestedContainerNormalizedMatch(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main"},
				{Name: "api"},
			},
		},
	}
	require.Equal(t, "api", SelectContainerName(pod, " API\t"))
}

func TestSelectContainerNameRequestedContainerFallbackToFirst(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main"},
				{Name: "sidecar"},
			},
		},
	}
	require.Equal(t, "main", SelectContainerName(pod, "not-exists"))
}

func TestSelectContainerNameSkipsEmptyNames(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: ""},
				{Name: "main"},
			},
		},
	}
	require.Equal(t, "main", SelectContainerName(pod, ""))
}

func TestSelectContainerNameNilPod(t *testing.T) {
	require.Equal(t, "", SelectContainerName(nil, "main"))
}

func TestHasContainerName(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main"},
				{Name: "sidecar"},
			},
		},
	}
	require.True(t, HasContainerName(pod, "main"))
	require.True(t, HasContainerName(pod, " MAIN "))
	require.False(t, HasContainerName(pod, "missing"))
	require.False(t, HasContainerName(nil, "main"))
}

func newLogPod(name string, phase corev1.PodPhase, ready bool, created time.Time) corev1.Pod {
	conditions := []corev1.PodCondition{}
	if ready {
		conditions = append(conditions, corev1.PodCondition{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		})
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
		},
		Status: corev1.PodStatus{
			Phase:      phase,
			Conditions: conditions,
		},
	}
}
