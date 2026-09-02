package adoption

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func controllerRef(kind, name string, uid types.UID) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{APIVersion: "apps/v1", Kind: kind, Name: name, UID: uid, Controller: &controller}
}

func testBinding(namespace, kind string, uid types.UID) SourceBinding {
	return SourceBinding{
		Namespace:          namespace,
		AppID:              "app-1",
		ComponentID:        42,
		ComponentName:      "backend",
		WorkloadAPIVersion: "apps/v1",
		WorkloadKind:       kind,
		WorkloadName:       "backend",
		WorkloadUID:        uid,
	}
}

func newTestCoordinator(client *fake.Clientset, binding SourceBinding) *PodCoordinator {
	c := NewPodCoordinator(client, func(context.Context) ([]SourceBinding, error) { return nil, nil })
	c.bindingsByNS[binding.Namespace] = map[string]SourceBinding{binding.controllerKey(): binding}
	c.labelClaimsByNS[binding.Namespace] = map[string]struct{}{
		managedLabelClaimKey(expectedLabels(binding)): {},
	}
	return c
}

func TestPodCoordinatorNoopWithoutAdoptedBindings(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged", Namespace: "prod"}})
	coordinator := NewPodCoordinator(client, func(context.Context) ([]SourceBinding, error) {
		return nil, nil
	})

	coordinator.reload(context.Background())

	require.Empty(t, coordinator.bindingsByNS)
	require.Empty(t, coordinator.namespaceCancels)
	require.Empty(t, client.Actions())
}

func TestPodCoordinatorLabelsDeploymentPodThroughReplicaSetOwner(t *testing.T) {
	const namespace = "adopted"
	deploymentUID := types.UID("deployment-uid")
	replicaSetUID := types.UID("rs-uid")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "backend-abc",
		UID:             types.UID("backend-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("ReplicaSet", "backend-rs", replicaSetUID)},
	}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "backend-rs",
		UID:             replicaSetUID,
		OwnerReferences: []metav1.OwnerReference{controllerRef("Deployment", "backend", deploymentUID)},
	}}
	client := fake.NewClientset(pod, rs)
	c := newTestCoordinator(client, testBinding(namespace, "Deployment", deploymentUID))

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/backend-abc"))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "app-1", got.Labels[config.LabelAppID])
	require.Equal(t, "backend", got.Labels[config.LabelComponentName])
	require.Equal(t, "42", got.Labels[config.LabelComponentID])
}

func TestPodCoordinatorLabelsStatefulSetPodWithoutWritingTemplate(t *testing.T) {
	const namespace = "adopted"
	statefulSetUID := types.UID("statefulset-uid")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("mysql-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", statefulSetUID)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", statefulSetUID))

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/mysql-0"))
	for _, action := range client.Actions() {
		require.NotEqual(t, "statefulsets", action.GetResource().Resource)
		require.NotEqual(t, "deployments", action.GetResource().Resource)
	}
}

func TestPodCoordinatorRefusesConflictingManagedLabels(t *testing.T) {
	const namespace = "adopted"
	uid := types.UID("statefulset-uid")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "redis-0",
		UID:             types.UID("redis-pod-uid"),
		ResourceVersion: "1",
		Labels:          map[string]string{config.LabelAppID: "other-app"},
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "redis", uid)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", uid))

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/redis-0"))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "other-app", got.Labels[config.LabelAppID])
	require.Empty(t, got.Labels[config.LabelComponentName])
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			t.Fatalf("conflicting managed labels must not be patched")
		}
	}
}

func TestPodCoordinatorReplacesLabelsFromDetachedBinding(t *testing.T) {
	const namespace = "adopted"
	uid := types.UID("statefulset-uid")
	binding := testBinding(namespace, "StatefulSet", uid)
	binding.AppID = "new-app"
	binding.ComponentID = 84
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "redis-0",
		UID:             types.UID("redis-pod-uid"),
		ResourceVersion: "1",
		Labels: map[string]string{
			config.LabelAppID:         "detached-app",
			config.LabelComponentName: "redis",
			config.LabelComponentID:   "12",
		},
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "redis", uid)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, binding)

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/redis-0"))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "new-app", got.Labels[config.LabelAppID])
	require.Equal(t, "backend", got.Labels[config.LabelComponentName])
	require.Equal(t, "84", got.Labels[config.LabelComponentID])
}

func TestPodCoordinatorPreservesLabelsFromAnotherLiveBinding(t *testing.T) {
	const namespace = "adopted"
	newUID := types.UID("new-statefulset-uid")
	newBinding := testBinding(namespace, "StatefulSet", newUID)
	newBinding.AppID = "new-app"
	newBinding.ComponentID = 84
	oldBinding := testBinding(namespace, "StatefulSet", types.UID("old-statefulset-uid"))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "redis-0",
		UID:             types.UID("redis-pod-uid"),
		ResourceVersion: "1",
		Labels:          expectedLabels(oldBinding),
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "redis", newUID)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, newBinding)
	c.bindingsByNS[namespace][oldBinding.controllerKey()] = oldBinding
	c.labelClaimsByNS[namespace][managedLabelClaimKey(expectedLabels(oldBinding))] = struct{}{}

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/redis-0"))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, oldBinding.AppID, got.Labels[config.LabelAppID])
	require.Equal(t, "42", got.Labels[config.LabelComponentID])
}

func TestPodCoordinatorPreservesLabelsClaimedByNativeApplication(t *testing.T) {
	const namespace = "adopted"
	newUID := types.UID("new-statefulset-uid")
	newBinding := testBinding(namespace, "StatefulSet", newUID)
	newBinding.AppID = "new-app"
	newBinding.ComponentID = 84
	nativeClaim := testBinding(namespace, "", "")
	nativeClaim.AppID = "native-app"
	nativeClaim.ComponentID = 12
	nativeClaim.ComponentName = "redis"
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "redis-0",
		UID:             types.UID("redis-pod-uid"),
		ResourceVersion: "1",
		Labels:          expectedLabels(nativeClaim),
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "redis", newUID)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, newBinding)
	c.labelClaimsByNS[namespace][managedLabelClaimKey(expectedLabels(nativeClaim))] = struct{}{}

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/"+pod.Name))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, nativeClaim.AppID, got.Labels[config.LabelAppID])
	require.Equal(t, "12", got.Labels[config.LabelComponentID])
	for _, action := range client.Actions() {
		require.NotEqual(t, "patch", action.GetVerb())
	}
}

func TestPodCoordinatorRequiresMatchingControllerUID(t *testing.T) {
	const namespace = "adopted"
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("replacement-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", types.UID("replacement-uid"))},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", types.UID("adopted-uid")))

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/mysql-0"))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, got.Labels)
}

func TestPodCoordinatorNoopWhenExpectedLabelsAlreadyPresent(t *testing.T) {
	const namespace = "adopted"
	uid := types.UID("statefulset-uid")
	binding := testBinding(namespace, "StatefulSet", uid)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("mysql-pod-uid"),
		ResourceVersion: "1",
		Labels:          expectedLabels(binding),
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", uid)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, binding)
	client.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		t.Fatalf("already labelled pod must not be patched")
		return true, nil, nil
	})

	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/mysql-0"))
}

func TestPodCoordinatorRefusesSameNameReplacementDuringPatch(t *testing.T) {
	const namespace = "adopted"
	controllerUID := types.UID("statefulset-uid")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("old-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", controllerUID)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", controllerUID))
	client.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patch := action.(k8stesting.PatchAction).GetPatch()
		require.Contains(t, string(patch), `"path":"/metadata/uid","value":"old-pod-uid"`)
		require.Contains(t, string(patch), `"path":"/metadata/resourceVersion","value":"1"`)

		replacement := pod.DeepCopy()
		replacement.UID = types.UID("replacement-pod-uid")
		replacement.ResourceVersion = "2"
		replacement.OwnerReferences = []metav1.OwnerReference{
			controllerRef("StatefulSet", "mysql", types.UID("replacement-controller-uid")),
		}
		require.NoError(t, client.Tracker().Update(
			corev1.SchemeGroupVersion.WithResource("pods"),
			replacement,
			namespace,
		))
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "pods"},
			pod.Name,
			fmt.Errorf("pod was replaced"),
		)
	})

	require.Error(t, c.reconcilePod(context.Background(), namespace+"/"+pod.Name))
	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/"+pod.Name))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, types.UID("replacement-pod-uid"), got.UID)
	require.Empty(t, got.Labels)
}

func TestPodCoordinatorReloadFailureClearsStaleBindingsAndStopsWatches(t *testing.T) {
	const namespace = "adopted"
	controllerUID := types.UID("statefulset-uid")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("mysql-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", controllerUID)},
	}}
	client := fake.NewClientset(pod)
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", controllerUID))
	t.Cleanup(c.shutdown)
	c.load = func(context.Context) ([]SourceBinding, error) {
		return nil, fmt.Errorf("database unavailable")
	}
	cancelCalls := 0
	c.namespaceCancels[namespace] = func() {
		cancelCalls++
	}

	c.reload(context.Background())

	require.False(t, c.hasNamespace(namespace))
	require.Empty(t, c.bindingsByNS)
	require.Empty(t, c.namespaceCancels)
	require.Equal(t, 1, cancelCalls)
	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/"+pod.Name))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, got.Labels)
	for _, action := range client.Actions() {
		require.NotEqual(t, "patch", action.GetVerb(), "stale source binding must not authorize a Pod patch")
	}
}

func TestPodCoordinatorReloadKeepsSourceUIDAmbiguousAfterThirdDuplicate(t *testing.T) {
	const namespace = "adopted"
	controllerUID := types.UID("statefulset-uid")
	first := testBinding(namespace, "StatefulSet", controllerUID)
	second := first
	second.AppID = "app-2"
	second.ComponentID = 43
	second.ComponentName = "mysql-copy"
	third := first
	third.AppID = "app-3"
	third.ComponentID = 44
	third.ComponentName = "mysql-copy-2"

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("mysql-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", controllerUID)},
	}}
	client := fake.NewClientset(pod)
	c := NewPodCoordinator(client, func(context.Context) ([]SourceBinding, error) {
		return []SourceBinding{first, second, third}, nil
	})
	t.Cleanup(c.shutdown)

	c.reload(context.Background())

	require.False(t, c.hasNamespace(namespace))
	require.Empty(t, c.bindingsByNS)
	require.NoError(t, c.reconcilePod(context.Background(), namespace+"/"+pod.Name))
	got, err := client.CoreV1().Pods(namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, got.Labels)
	for _, action := range client.Actions() {
		require.NotEqual(t, "patch", action.GetVerb(), "ambiguous source UID must not authorize a Pod patch")
	}
}

func TestPodCoordinatorRunWaitsForActiveWorkerOnLeaderCancellation(t *testing.T) {
	const namespace = "adopted"
	controllerUID := types.UID("statefulset-uid")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("mysql-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", controllerUID)},
	}}
	client := fake.NewClientset(pod)
	getStarted := make(chan struct{})
	releaseGet := make(chan struct{})
	var startedOnce sync.Once
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		startedOnce.Do(func() { close(getStarted) })
		<-releaseGet
		return true, pod.DeepCopy(), nil
	})
	binding := testBinding(namespace, "StatefulSet", controllerUID)
	c := NewPodCoordinator(
		client,
		func(context.Context) ([]SourceBinding, error) { return []SourceBinding{binding}, nil },
		WithBindingReloadInterval(time.Hour),
	)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(runDone)
	}()

	select {
	case <-getStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin pod reconciliation")
	}
	cancel()
	select {
	case <-runDone:
		t.Fatal("coordinator returned before the active worker exited")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseGet)
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not return after the active worker exited")
	}
}

func TestPodCoordinatorRetryRemainsCountedAgainstQueueCapacity(t *testing.T) {
	const namespace = "adopted"
	controllerUID := types.UID("statefulset-uid")
	first := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("mysql-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", controllerUID)},
	}}
	second := first.DeepCopy()
	second.Name = "mysql-1"
	second.UID = types.UID("mysql-pod-uid-2")
	client := fake.NewClientset(first, second)
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("temporary API failure")
	})
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", controllerUID))
	c.maxQueueItems = 1
	t.Cleanup(c.shutdown)

	c.enqueuePod(first)
	require.True(t, c.processNext(context.Background()))
	require.Contains(t, c.pending, namespace+"/"+first.Name)

	c.enqueuePod(second)
	require.NotContains(t, c.pending, namespace+"/"+second.Name)
	require.Len(t, c.pending, 1)
}

func TestPodCoordinatorPendingUpdateMarksWorkQueueDirty(t *testing.T) {
	const namespace = "adopted"
	controllerUID := types.UID("statefulset-uid")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            "mysql-0",
		UID:             types.UID("mysql-pod-uid"),
		ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", controllerUID)},
	}}
	client := fake.NewClientset(pod)
	getStarted := make(chan struct{})
	releaseGet := make(chan struct{})
	var blockFirstGet sync.Once
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		blockFirstGet.Do(func() {
			close(getStarted)
			<-releaseGet
		})
		return false, nil, nil
	})
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", controllerUID))
	t.Cleanup(c.shutdown)

	key := namespace + "/" + pod.Name
	c.enqueuePod(pod)
	processed := make(chan bool, 1)
	go func() {
		processed <- c.processNext(context.Background())
	}()
	select {
	case <-getStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin pod reconciliation")
	}
	c.enqueuePod(pod)
	close(releaseGet)
	require.True(t, <-processed)
	require.Equal(t, 1, c.queue.Len(), "an update received during processing must be requeued")
	require.Contains(t, c.pending, key, "requeued work must remain counted against capacity")

	require.True(t, c.processNext(context.Background()))
	require.NotContains(t, c.pending, key)
}

func TestPodCoordinatorNamespaceScanResumesAfterQueueCapacityReturns(t *testing.T) {
	const namespace = "adopted"
	controllerUID := types.UID("statefulset-uid")
	objects := make([]runtime.Object, 0, 5)
	for index := 0; index < 5; index++ {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            fmt.Sprintf("mysql-%d", index),
			UID:             types.UID(fmt.Sprintf("mysql-pod-uid-%d", index)),
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{controllerRef("StatefulSet", "mysql", controllerUID)},
		}})
	}
	client := fake.NewClientset(objects...)
	c := newTestCoordinator(client, testBinding(namespace, "StatefulSet", controllerUID))
	c.maxQueueItems = 1
	t.Cleanup(c.shutdown)

	seen := make(map[string]struct{})
	for scan := 0; scan < len(objects); scan++ {
		c.enqueueNamespacePods(context.Background(), namespace)
		require.Len(t, c.pending, 1)
		item, shutdown := c.queue.Get()
		require.False(t, shutdown)
		key := item.(string)
		seen[key] = struct{}{}
		c.queue.Done(item)
		c.queue.Forget(item)
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
	}

	require.Len(t, seen, len(objects), "bounded scans must eventually enqueue every listed Pod")
}
