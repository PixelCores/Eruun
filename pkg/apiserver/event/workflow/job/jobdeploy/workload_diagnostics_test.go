package jobdeploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
)

func TestDeployJobCtlWaitAddsPodFailureDiagnostics(t *testing.T) {
	logs := podLogs(func(_ context.Context, podName, container string, previous bool) (string, error) {
		if previous {
			return fmt.Sprintf("panic: startup failed\nat %s/%s", podName, container), nil
		}
		return fmt.Sprintf("boot log from %s/%s", podName, container), nil
	})

	pod := newWaitTestPod("app-1", "api", "CrashLoopBackOff")

	waiter := newWorkloadTestObserver(t, pod)

	ctl := &DeployJobCtl{
		DeployNamespacedResourceJobBase: jobexec.DeployNamespacedResourceJobBase{
			Job: &model.JobTask{
				Name:      "api",
				Namespace: "default",
				AppID:     "app-1",
				Timeout:   1,
			},
			Client:         newDiagnosticsClient(t, logs, pod, diagnosticEventForPod(pod)),
			ResourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := jobinfo.ExtractStatusError(err)
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
	logs := podLogs(func(_ context.Context, _, _ string, previous bool) (string, error) {
		if previous {
			return "previous mysql stack", nil
		}
		return "current mysql log", nil
	})

	pod := newWaitTestPod("app-1", "mysql", "CrashLoopBackOff")

	waiter := newWorkloadTestObserver(t, pod)

	ctl := &DeployStatefulSetJobCtl{
		DeployNamespacedResourceJobBase: jobexec.DeployNamespacedResourceJobBase{
			Job: &model.JobTask{
				Name:      "mysql",
				Namespace: "default",
				AppID:     "app-1",
				Timeout:   1,
			},
			Client:         newDiagnosticsClient(t, logs, pod),
			ResourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := jobinfo.ExtractStatusError(err)
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
	// The caller's deadline bounds diagnostics below workloadDiagnosticsTimeout.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	logReadStarted := make(chan struct{})
	logs := podLogs(func(ctx context.Context, _, _ string, _ bool) (string, error) {
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
				spec.LabelAppID:         "app-1",
				spec.LabelComponentName: "api",
				spec.LabelComponentID:   "7",
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
		ctx,
		newDiagnosticsClient(t, logs, pod),
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
	logs := podLogs(func(_ context.Context, _, _ string, previous bool) (string, error) {
		if previous {
			return "", errors.New("previous log unavailable")
		}
		return "", errors.New("current log unavailable")
	})

	pod := newWaitTestPod("app-1", "api", "CrashLoopBackOff")
	diagnostics := collectWorkloadFailureDiagnostics(
		context.Background(),
		newDiagnosticsClient(t, logs, pod),
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
	logs := podLogs(func(_ context.Context, _, _ string, _ bool) (string, error) {
		return strings.Repeat("x", workloadDiagnosticsLimitBytes*2), nil
	})

	pod := newWaitTestPod("app-1", "api", "CrashLoopBackOff")
	diagnostics := collectWorkloadFailureDiagnostics(
		context.Background(),
		newDiagnosticsClient(t, logs, pod),
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
	logs := podLogs(func(_ context.Context, podName, _ string, previous bool) (string, error) {
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
	client := newDiagnosticsClient(t, logs,
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
	logs := podLogs(func(_ context.Context, podName, _ string, previous bool) (string, error) {
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
	client := newDiagnosticsClient(t, logs,
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
				spec.LabelAppID:         "app-1",
				spec.LabelComponentName: "api",
				spec.LabelComponentID:   "7",
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

func newWorkloadTestObserver(t *testing.T, pods ...*corev1.Pod) *informer.KubernetesWorkloadObserver {
	t.Helper()
	client := fake.NewSimpleClientset()
	for _, pod := range pods {
		_, err := client.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	return jobtest.StartWorkloadTestObserver(t, client)
}

type podLogs func(ctx context.Context, pod, container string, previous bool) (string, error)

// newDiagnosticsClient serves the pod list, event list and pod log reads that
// failure diagnostics make, behind a real typed client.
func newDiagnosticsClient(t *testing.T, logs podLogs, objects ...runtime.Object) kubernetes.Interface {
	t.Helper()
	var pods []corev1.Pod
	var events []corev1.Event
	for _, object := range objects {
		switch typed := object.(type) {
		case *corev1.Pod:
			pods = append(pods, *typed)
		case *corev1.Event:
			events = append(events, *typed)
		default:
			t.Fatalf("unsupported diagnostics object %T", object)
		}
	}
	writeJSON := func(w http.ResponseWriter, status int, value any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		require.NoError(t, json.NewEncoder(w).Encode(value))
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// api/v1/namespaces/<namespace>/<resource>[/<name>/log]
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 5 || parts[2] != "namespaces" {
			http.NotFound(w, r)
			return
		}
		namespace, query := parts[3], r.URL.Query()
		switch {
		case len(parts) == 5 && parts[4] == "pods":
			selector, err := labels.Parse(query.Get("labelSelector"))
			require.NoError(t, err)
			list := &corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}}
			for _, pod := range pods {
				if pod.Namespace == namespace && selector.Matches(labels.Set(pod.Labels)) {
					list.Items = append(list.Items, pod)
				}
			}
			writeJSON(w, http.StatusOK, list)
		case len(parts) == 5 && parts[4] == "events":
			selector, err := fields.ParseSelector(query.Get("fieldSelector"))
			require.NoError(t, err)
			list := &corev1.EventList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "EventList"}}
			for _, event := range events {
				if selector.Matches(fields.Set{"involvedObject.name": event.InvolvedObject.Name, "involvedObject.namespace": event.InvolvedObject.Namespace}) {
					list.Items = append(list.Items, event)
				}
			}
			writeJSON(w, http.StatusOK, list)
		case len(parts) == 7 && parts[4] == "pods" && parts[6] == "log":
			text, err := logs(r.Context(), parts[5], query.Get("container"), query.Get("previous") == "true")
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, &metav1.Status{
					TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
					Status:   metav1.StatusFailure, Code: http.StatusInternalServerError, Message: err.Error(),
				})
				return
			}
			_, _ = io.WriteString(w, text)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	return client
}
