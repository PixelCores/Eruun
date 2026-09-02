package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
)

func TestDeployJobCtlWaitAddsPodFailureDiagnostics(t *testing.T) {
	restoreWorkloadPodLogReader(t, func(_ context.Context, _ kubernetes.Interface, _, podName, container string, previous bool) (string, error) {
		if previous {
			return fmt.Sprintf("panic: startup failed\nat %s/%s", podName, container), nil
		}
		return fmt.Sprintf("boot log from %s/%s", podName, container), nil
	})

	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	pod := newWaitTestPod("app-1", "api", "CrashLoopBackOff")
	waiter.OnPodAdd(pod)

	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:      "api",
				Namespace: "default",
				AppID:     "app-1",
				Timeout:   1,
			},
			client:         fake.NewSimpleClientset(pod, diagnosticEventForPod(pod)),
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, statusErr.Status)

	msg := err.Error()
	require.Contains(t, msg, "summary:")
	require.Contains(t, msg, "CrashLoopBackOff")
	require.Contains(t, msg, "workload:")
	require.Contains(t, msg, "kind: Deployment")
	require.Contains(t, msg, "pods:")
	require.Contains(t, msg, "events:")
	require.Contains(t, msg, "BackOff")
	require.Contains(t, msg, "previous logs:")
	require.Contains(t, msg, "panic: startup failed")
	require.Contains(t, msg, "current logs:")
	require.Contains(t, msg, "boot log")
}

func TestDeployStatefulSetJobCtlWaitAddsPodFailureDiagnostics(t *testing.T) {
	restoreWorkloadPodLogReader(t, func(_ context.Context, _ kubernetes.Interface, _, _, _ string, previous bool) (string, error) {
		if previous {
			return "previous mysql stack", nil
		}
		return "current mysql log", nil
	})

	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	pod := newWaitTestPod("app-1", "mysql", "CrashLoopBackOff")
	waiter.OnPodAdd(pod)

	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:      "mysql",
				Namespace: "default",
				AppID:     "app-1",
				Timeout:   1,
			},
			client:         fake.NewSimpleClientset(pod),
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, statusErr.Status)
	require.Contains(t, err.Error(), "kind: StatefulSet")
	require.Contains(t, err.Error(), "previous mysql stack")
	require.Contains(t, err.Error(), "current mysql log")
}

func TestWorkloadFailureDiagnosticsHandlesMissingPods(t *testing.T) {
	diagnostics := collectWorkloadFailureDiagnostics(
		context.Background(),
		fake.NewSimpleClientset(),
		&model.JobTask{AppID: "app-1"},
		"Deployment",
		"default",
		"api",
		"api",
		errors.New("component app-1/api timeout after 1s"),
	)

	require.Contains(t, diagnostics, "summary:")
	require.Contains(t, diagnostics, "timeout after 1s")
	require.Contains(t, diagnostics, "no pods found")
}

func TestWorkloadWaitErrorPreservesCancellationCause(t *testing.T) {
	cause := fmt.Errorf("component app-1/api cancelled: %w", context.Canceled)

	err := workloadWaitError(
		context.Background(),
		fake.NewSimpleClientset(),
		&model.JobTask{AppID: "app-1"},
		"Deployment",
		"default",
		"api",
		"api",
		cause,
	)

	require.ErrorIs(t, err, context.Canceled)
	require.NotContains(t, err.Error(), "summary:")
}

func TestWorkloadWaitErrorFallsBackWhenDiagnosticsTimeout(t *testing.T) {
	previousTimeout := workloadDiagnosticsTimeout
	workloadDiagnosticsTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		workloadDiagnosticsTimeout = previousTimeout
	})

	logReadStarted := make(chan struct{})
	restoreWorkloadPodLogReader(t, func(ctx context.Context, _ kubernetes.Interface, _, _, _ string, _ bool) (string, error) {
		select {
		case <-logReadStarted:
		default:
			close(logReadStarted)
		}
		<-ctx.Done()
		return "", ctx.Err()
	})

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-0",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: "api",
				config.LabelComponentID:   "7",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app"}},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				},
			},
		},
	}
	cause := errors.New("original wait error")

	start := time.Now()
	err := workloadWaitError(
		context.Background(),
		fake.NewSimpleClientset(pod),
		&model.JobTask{AppID: "app-1"},
		"Deployment",
		"default",
		"api",
		"api",
		cause,
	)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, cause)
	require.EqualError(t, err, "original wait error")
	require.Less(t, elapsed, time.Second)
	require.NotContains(t, err.Error(), "summary:")
	require.NotContains(t, err.Error(), "previous logs:")
	require.NotContains(t, err.Error(), context.DeadlineExceeded.Error())
	select {
	case <-logReadStarted:
	default:
		t.Fatal("expected diagnostics to reach pod log collection")
	}
}

func TestWorkloadFailureDiagnosticsKeepsOriginalErrorWhenLogsFail(t *testing.T) {
	restoreWorkloadPodLogReader(t, func(_ context.Context, _ kubernetes.Interface, _, _, _ string, previous bool) (string, error) {
		if previous {
			return "", errors.New("previous log unavailable")
		}
		return "", errors.New("current log unavailable")
	})

	pod := newWaitTestPod("app-1", "api", "CrashLoopBackOff")
	diagnostics := collectWorkloadFailureDiagnostics(
		context.Background(),
		fake.NewSimpleClientset(pod),
		&model.JobTask{AppID: "app-1"},
		"Deployment",
		"default",
		"api",
		"api",
		errors.New("original wait error"),
	)

	require.Contains(t, diagnostics, "original wait error")
	require.Contains(t, diagnostics, "read previous logs failed")
	require.Contains(t, diagnostics, "previous log unavailable")
	require.Contains(t, diagnostics, "read current logs failed")
	require.Contains(t, diagnostics, "current log unavailable")
}

func TestWorkloadFailureDiagnosticsTruncatesLongOutput(t *testing.T) {
	restoreWorkloadPodLogReader(t, func(_ context.Context, _ kubernetes.Interface, _, _, _ string, _ bool) (string, error) {
		return strings.Repeat("x", workloadDiagnosticsLimitBytes*2), nil
	})

	pod := newWaitTestPod("app-1", "api", "CrashLoopBackOff")
	diagnostics := collectWorkloadFailureDiagnostics(
		context.Background(),
		fake.NewSimpleClientset(pod),
		&model.JobTask{AppID: "app-1"},
		"Deployment",
		"default",
		"api",
		"api",
		errors.New("original wait error"),
	)

	require.LessOrEqual(t, len(diagnostics), workloadDiagnosticsLimitBytes)
	require.Contains(t, diagnostics, "summary:")
	require.Contains(t, diagnostics, diagnosticsTruncatedMarker)
}

func TestWorkloadFailureDiagnosticsPrefersAbnormalPodsBeforeLimit(t *testing.T) {
	restoreWorkloadPodLogReader(t, func(_ context.Context, _ kubernetes.Interface, _, podName, _ string, previous bool) (string, error) {
		if podName == "failing-old" {
			if previous {
				return "panic from failing-old", nil
			}
			return "current log from failing-old", nil
		}
		return "log from " + podName, nil
	})

	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	failingOld := newDiagnosticSelectionPod("failing-old", now, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off",
		},
	}, false)
	client := fake.NewSimpleClientset(
		newDiagnosticSelectionPod("healthy-new-1", now.Add(5*time.Minute), corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}, true),
		newDiagnosticSelectionPod("healthy-new-2", now.Add(4*time.Minute), corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}, true),
		newDiagnosticSelectionPod("pending-new", now.Add(3*time.Minute), corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
		}, false),
		newDiagnosticSelectionPod("healthy-new-3", now.Add(2*time.Minute), corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}, true),
		failingOld,
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "failing-old-backoff",
				Namespace: "default",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: "default",
				Name:      "failing-old",
			},
			Type:          corev1.EventTypeWarning,
			Reason:        "BackOff",
			Message:       "failing pod event",
			LastTimestamp: metav1.NewTime(now.Add(6 * time.Minute)),
		},
	)

	diagnostics := collectWorkloadFailureDiagnostics(
		context.Background(),
		client,
		&model.JobTask{AppID: "app-1"},
		"Deployment",
		"default",
		"api",
		"api",
		errors.New("original wait error"),
	)

	require.Contains(t, diagnostics, "failing-old")
	require.Contains(t, diagnostics, "CrashLoopBackOff")
	require.Contains(t, diagnostics, "failing pod event")
	require.Contains(t, diagnostics, "panic from failing-old")
}

func TestWorkloadFailureDiagnosticsPrefersCurrentNotReadyPodsBeforeRecoveredRestarts(t *testing.T) {
	restoreWorkloadPodLogReader(t, func(_ context.Context, _ kubernetes.Interface, _, podName, _ string, previous bool) (string, error) {
		if podName == "pending-old" {
			if previous {
				return "previous log from pending-old", nil
			}
			return "current log from pending-old", nil
		}
		return "stale restart log from " + podName, nil
	})

	now := time.Date(2026, 6, 26, 13, 0, 0, 0, time.UTC)
	pendingOld := newDiagnosticSelectionPod("pending-old", now, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "ContainerCreating",
			Message: "pulling image",
		},
	}, false)
	pendingOld.Spec.Containers = []corev1.Container{{Name: "app"}}
	client := fake.NewSimpleClientset(
		markDiagnosticRestart(newDiagnosticSelectionPod("recovered-new-1", now.Add(5*time.Minute), corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}, true)),
		markDiagnosticRestart(newDiagnosticSelectionPod("recovered-new-2", now.Add(4*time.Minute), corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}, true)),
		markDiagnosticRestart(newDiagnosticSelectionPod("recovered-new-3", now.Add(3*time.Minute), corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{},
		}, true)),
		pendingOld,
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pending-old-creating",
				Namespace: "default",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: "default",
				Name:      "pending-old",
			},
			Type:          corev1.EventTypeWarning,
			Reason:        "FailedScheduling",
			Message:       "pending pod event",
			LastTimestamp: metav1.NewTime(now.Add(6 * time.Minute)),
		},
	)

	diagnostics := collectWorkloadFailureDiagnostics(
		context.Background(),
		client,
		&model.JobTask{AppID: "app-1"},
		"Deployment",
		"default",
		"api",
		"api",
		errors.New("original wait error"),
	)

	require.Contains(t, diagnostics, "pending-old")
	require.Contains(t, diagnostics, "ContainerCreating")
	require.Contains(t, diagnostics, "pending pod event")
	require.Contains(t, diagnostics, "current log from pending-old")
	require.NotContains(t, diagnostics, "recovered-new-3")
}

func TestDiagnosticPodRankPrefersCurrentNotReadyPodOverRecoveredReadyRestart(t *testing.T) {
	now := time.Date(2026, 6, 26, 14, 0, 0, 0, time.UTC)
	pending := newDiagnosticSelectionPod("pending", now, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
	}, false)
	recovered := markDiagnosticRestart(newDiagnosticSelectionPod("recovered", now.Add(time.Minute), corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true))

	require.Less(t, diagnosticPodRank(pending), diagnosticPodRank(recovered))
}

func diagnosticEventForPod(pod *corev1.Pod) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name + "-backoff",
			Namespace: pod.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: pod.Namespace,
			Name:      pod.Name,
		},
		Type:          corev1.EventTypeWarning,
		Reason:        "BackOff",
		Message:       "back-off restarting failed container",
		LastTimestamp: metav1.Now(),
	}
}

func markDiagnosticRestart(pod *corev1.Pod) *corev1.Pod {
	if pod == nil || len(pod.Status.ContainerStatuses) == 0 {
		return pod
	}
	pod.Status.ContainerStatuses[0].RestartCount = 1
	pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			Reason:   "Error",
			ExitCode: 1,
		},
	}
	return pod
}

func newDiagnosticSelectionPod(name string, created time.Time, state corev1.ContainerState, ready bool) *corev1.Pod {
	conditions := []corev1.PodCondition{}
	if ready {
		conditions = append(conditions, corev1.PodCondition{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: "api",
				config.LabelComponentID:   "7",
			},
		},
		Status: corev1.PodStatus{
			Conditions: conditions,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "app",
					Ready: ready,
					State: state,
				},
			},
		},
	}
}

func restoreWorkloadPodLogReader(t *testing.T, reader workloadPodLogReader) {
	t.Helper()
	workloadPodLogReaderMu.Lock()
	previous := readWorkloadPodLogs
	readWorkloadPodLogs = reader
	workloadPodLogReaderMu.Unlock()
	t.Cleanup(func() {
		workloadPodLogReaderMu.Lock()
		readWorkloadPodLogs = previous
		workloadPodLogReaderMu.Unlock()
	})
}
