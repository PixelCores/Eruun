package informer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestSandboxObserverLimitsResourcesAndIndexesOwnedPods(t *testing.T) {
	sandbox := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "agents.kruise.io/v1alpha1", "kind": "Sandbox",
		"metadata": map[string]interface{}{"name": "trial", "namespace": "own", "uid": "sandbox-uid", "resourceVersion": "9", "generation": int64(2),
			"labels":        map[string]interface{}{config.LabelManagedBy: config.ManagedByEruun, "eruun.io/task-id": "task"},
			"managedFields": []interface{}{map[string]interface{}{"manager": "sandbox-controller"}}},
		"spec":   map[string]interface{}{"template": map[string]interface{}{}},
		"status": map[string]interface{}{"observedGeneration": int64(2), "phase": "Running"},
	}}
	foreign := sandbox.DeepCopy()
	foreign.SetName("foreign")
	foreign.SetLabels(nil)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{observedSandboxResource: "SandboxList"})
	// Tracker.Add guesses "sandboxs" from an unregistered custom Kind; seed
	// the exact CRD resource rather than relying on that pluralization guess.
	require.NoError(t, dynamicClient.Tracker().Create(observedSandboxResource, sandbox, sandbox.GetNamespace()))
	require.NoError(t, dynamicClient.Tracker().Create(observedSandboxResource, foreign, foreign.GetNamespace()))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "trial-pod", Namespace: "own", UID: "pod-uid",
		Labels:        map[string]string{"eruun.io/task-id": "task", "eruun.io/sandbox-id": "id"},
		ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	otherNamespace := pod.DeepCopy()
	otherNamespace.Namespace = "other"
	otherTask := pod.DeepCopy()
	otherTask.Name = "other-task"
	otherTask.Labels["eruun.io/sandbox-id"] = "other-id"
	unmanaged := pod.DeepCopy()
	unmanaged.Name = "unmanaged"
	delete(unmanaged.Labels, "eruun.io/task-id")
	pods := fake.NewSimpleClientset(pod, otherNamespace, otherTask, unmanaged)
	observer, err := NewKubernetesSandboxObserver(dynamicClient, pods)
	require.NoError(t, err)
	_, err = observer.Sandbox("own", "trial")
	require.ErrorIs(t, err, ErrSandboxObservationUnavailable)
	stop := runSandboxTestObserver(t, observer)
	require.Eventually(t, func() bool { return observer.synced.Load() }, 3*time.Second, time.Millisecond)
	got, err := observer.Sandbox("own", "trial")
	require.NoError(t, err)
	require.Empty(t, got.GetManagedFields())
	require.Equal(t, sandbox.Object["status"], got.Object["status"])
	require.Equal(t, sandbox.GetUID(), got.GetUID())
	got.SetUID("mutated")
	again, err := observer.Sandbox("own", "trial")
	require.NoError(t, err)
	require.Equal(t, sandbox.GetUID(), again.GetUID(), "consumers cannot mutate shared snapshots")
	_, err = observer.Sandbox("own", "foreign")
	require.True(t, apierrors.IsNotFound(err))
	matches, err := observer.SandboxPods("own", "id")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	require.Equal(t, pod.UID, matches[0].UID)
	require.Empty(t, matches[0].ManagedFields)
	matches[0].Labels["eruun.io/sandbox-id"] = "mutated"
	againPods, err := observer.SandboxPods("own", "id")
	require.NoError(t, err)
	require.Len(t, againPods, 1)
	require.Equal(t, "id", againPods[0].Labels["eruun.io/sandbox-id"])
	for _, action := range dynamicClient.Actions() {
		require.Equal(t, observedSandboxResource, action.GetResource())
		require.Contains(t, []string{"list", "watch"}, action.GetVerb())
		if action.GetVerb() == "list" {
			require.Equal(t, "app.kubernetes.io/managed-by=eruun", action.(ktesting.ListAction).GetListRestrictions().Labels.String())
		}
	}
	for _, action := range pods.Actions() {
		require.Equal(t, "pods", action.GetResource().Resource)
		if action.GetVerb() == "list" {
			require.Equal(t, "eruun.io/task-id", action.(ktesting.ListAction).GetListRestrictions().Labels.String())
		}
	}
	stop()
	_, err = observer.Sandbox("own", "trial")
	require.ErrorIs(t, err, ErrSandboxObservationUnavailable)
}

func TestSandboxObserverMissingCRDRemainsUnavailableUntilInitialSync(t *testing.T) {
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{observedSandboxResource: "SandboxList"})
	var missing atomic.Bool
	missing.Store(true)
	var lists atomic.Int32
	dynamicClient.PrependReactor("list", "sandboxes", func(ktesting.Action) (bool, runtime.Object, error) {
		lists.Add(1)
		if missing.Load() {
			return true, nil, apierrors.NewNotFound(observedSandboxResource.GroupResource(), "")
		}
		return false, nil, nil
	})
	observer, err := NewKubernetesSandboxObserver(dynamicClient, fake.NewSimpleClientset())
	require.NoError(t, err)
	runSandboxTestObserver(t, observer)
	require.Eventually(t, func() bool { return lists.Load() > 0 }, time.Second, time.Millisecond)
	for range 100 {
		_, err := observer.Sandbox("own", "trial")
		require.ErrorIs(t, err, ErrSandboxObservationUnavailable)
		_, err = observer.SandboxPods("own", "id")
		require.ErrorIs(t, err, ErrSandboxObservationUnavailable)
	}
	missing.Store(false)
	require.Eventually(t, func() bool { return observer.synced.Load() }, 5*time.Second, 10*time.Millisecond)
	_, err = observer.Sandbox("own", "not-yet-created")
	require.True(t, apierrors.IsNotFound(err))
	require.False(t, errors.Is(err, ErrSandboxObservationUnavailable))
	for _, action := range dynamicClient.Actions() {
		require.NotEqual(t, "get", action.GetVerb(), "cache reads must not silently fall back to API polling")
	}
}

func runSandboxTestObserver(t *testing.T, observer *KubernetesSandboxObserver) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); observer.Run(ctx) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return stop
}
