package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

type sandboxTestObserver struct {
	client       dynamic.Interface
	kube         kubernetes.Interface
	unavailable  bool
	podsOverride []*corev1.Pod
}

func (o *sandboxTestObserver) Sandbox(namespace, name string) (*unstructured.Unstructured, error) {
	if o.unavailable {
		return nil, informer.ErrSandboxObservationUnavailable
	}
	return o.client.Resource(SandboxGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
}

func (o *sandboxTestObserver) SandboxPods(namespace, id string) ([]*corev1.Pod, error) {
	if o.podsOverride != nil {
		return o.podsOverride, nil
	}
	list, err := o.kube.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{LabelSelector: sandboxIDLabel + "=" + id})
	if err != nil {
		return nil, err
	}
	var pods []*corev1.Pod
	for index := range list.Items {
		pods = append(pods, &list.Items[index])
	}
	return pods, nil
}

func TestSandboxFreshMissingPinnedPodCannotBecomeReadyFromCache(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	pod := makeSandboxReady(t, f, client, "trial")
	_, err = f.service.RunnerSandboxGet(ctx, f.identity, "trial")
	require.NoError(t, err)
	f.service.SandboxObserver.(*sandboxTestObserver).podsOverride = []*corev1.Pod{pod}
	require.NoError(t, f.service.Kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
	response, err := f.service.RunnerSandboxGet(ctx, f.identity, "trial")
	require.NoError(t, err)
	require.Equal(t, sandboxFailed, response.State)
	require.Equal(t, "pod_missing", response.Reason)
	require.Equal(t, string(pod.UID), response.PodUID)
	require.True(t, sandboxRow(t, f, "trial").SlotReserved)
}

func sandboxFixture(t *testing.T, claim bool) (*runnerFixture, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	f := newRunnerFixture(t)
	return attachSandboxFixture(t, f, claim)
}

func attachSandboxFixture(t *testing.T, f *runnerFixture, claim bool) (*runnerFixture, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{SandboxGVR: "SandboxList"})
	client.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		object.SetUID(types.UID(uuid.NewString()))
		object.SetGeneration(1)
		err := client.Tracker().Create(SandboxGVR, object, object.GetNamespace())
		return true, object, err
	})
	f.service.SandboxClient = client
	f.service.SandboxObserver = &sandboxTestObserver{client: client, kube: f.service.Kube}
	if claim {
		claimRunner(t, f)
	}
	return f, client
}

func sandboxRequest(id string) SandboxRequest {
	return SandboxRequest{TrialID: id, Image: "example.com/task:1.0.0", StorageMiB: 2048}
}

func sandboxRow(t *testing.T, f *runnerFixture, trialID string) *model.JobSandbox {
	t.Helper()
	row := &model.JobSandbox{ID: sandboxID(f.parent.WorkspaceID, *f.record.ExecutionKey, trialID)}
	require.NoError(t, f.raw.Get(context.Background(), row))
	return row
}

func makeSandboxReady(t *testing.T, f *runnerFixture, client *dynamicfake.FakeDynamicClient, trialID string) *corev1.Pod {
	t.Helper()
	ctx := context.Background()
	row := sandboxRow(t, f, trialID)
	object, err := client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	template, _, err := unstructured.NestedMap(object.Object, "spec", "template")
	require.NoError(t, err)
	var typed corev1.PodTemplateSpec
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(template, &typed))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sandbox-pod-" + uuid.NewString(), Namespace: row.Namespace, UID: types.UID(uuid.NewString()), Labels: typed.Labels,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "agents.kruise.io/v1alpha1", Kind: "Sandbox", Name: row.SandboxName, UID: types.UID(row.SandboxUID), Controller: ptr.To(true)}}}, Spec: typed.Spec,
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	_, err = f.service.Kube.CoreV1().Pods(row.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)
	object.Object["status"] = map[string]interface{}{"observedGeneration": object.GetGeneration(), "conditions": []interface{}{map[string]interface{}{"type": "Ready", "status": "True"}}, "podInfo": map[string]interface{}{"podUID": string(pod.UID)}}
	require.NoError(t, client.Tracker().Update(SandboxGVR, object, row.Namespace))
	return pod
}

func TestSandboxIdempotencyReadyAndUIDSafeRelease(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	first, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial-1"))
	require.NoError(t, err)
	require.Equal(t, sandboxPending, first.State)
	require.True(t, first.Admitted)
	second, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial-1"))
	require.NoError(t, err)
	require.Equal(t, first.SandboxUID, second.SandboxUID)
	creates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			creates++
		}
	}
	require.Equal(t, 1, creates)
	pod := makeSandboxReady(t, f, client, "trial-1")
	ready, err := f.service.RunnerSandboxGet(ctx, f.identity, "trial-1")
	require.NoError(t, err)
	require.Equal(t, sandboxReady, ready.State)
	require.Equal(t, pod.Name, ready.PodName)
	require.Equal(t, string(pod.UID), ready.PodUID)
	require.Equal(t, "main", ready.ContainerName)
	client.PrependReactor("delete", "sandboxes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		require.Equal(t, types.UID(first.SandboxUID), *action.(k8stesting.DeleteAction).GetDeleteOptions().Preconditions.UID)
		return false, nil, nil
	})
	released, err := f.service.RunnerSandboxRelease(ctx, f.identity, "trial-1", SandboxReleaseRequest{SandboxUID: ready.SandboxUID, PodUID: ready.PodUID, CollectionComplete: ptr.To(true)})
	require.NoError(t, err)
	require.Equal(t, sandboxReleased, released.State)
	require.Equal(t, ready.SandboxUID, released.SandboxUID)
	require.False(t, sandboxRow(t, f, "trial-1").SlotReserved)
	_, err = client.Resource(SandboxGVR).Namespace(ready.Namespace).Get(ctx, ready.SandboxName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	replay, err := f.service.RunnerSandboxRelease(ctx, f.identity, "trial-1", SandboxReleaseRequest{CollectionComplete: ptr.To(true)})
	require.NoError(t, err)
	require.Equal(t, sandboxReleased, replay.State)
}

func TestSandboxRejectsConflictingSpecOrUnclaimedRunner(t *testing.T) {
	f, _ := sandboxFixture(t, false)
	ctx := context.Background()
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	claimRunner(t, f)
	_, err = f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	other := sandboxRequest("trial")
	other.Image = "example.com/task:2.0.0"
	_, err = f.service.RunnerSandboxCreate(ctx, f.identity, other)
	require.ErrorIs(t, err, ErrRunnerConflict)
	foreign := account.WithScope(ctx, account.Scope{WorkspaceID: "foreign", Namespace: "other", Role: "member"})
	_, err = f.service.RunnerSandboxGet(foreign, f.identity, "trial")
	require.Error(t, err)
	_, err = f.service.RunnerSandboxRelease(ctx, f.identity, "trial", SandboxReleaseRequest{SandboxUID: "forged", CollectionComplete: ptr.To(true)})
	require.ErrorIs(t, err, ErrRunnerConflict)
}

func TestSandboxCreateResponseLossReusesOwnedIntent(t *testing.T) {
	f, client := sandboxFixture(t, true)
	client.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
		object.SetUID("created-before-timeout")
		object.SetGeneration(1)
		require.NoError(t, client.Tracker().Create(SandboxGVR, object, object.GetNamespace()))
		return true, nil, k8serrors.NewTimeoutError("response lost", 1)
	})
	result, err := f.service.RunnerSandboxCreate(context.Background(), f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	require.Equal(t, "created-before-timeout", result.SandboxUID)
	require.Equal(t, 1, sandboxRow(t, f, "trial").CreateAttempts)
}

func TestSandboxRateBudgetDoesNotCreateOrConsumeAttempt(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	require.NoError(t, f.raw.Add(ctx, &model.ResourceCreationBudget{ID: "jobs-and-sandboxes", AvailableAt: time.Now().UTC().Add(time.Hour)}))
	result, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	require.False(t, result.Admitted)
	require.Equal(t, "creation_rate_limited", result.Reason)
	require.Zero(t, sandboxRow(t, f, "trial").CreateAttempts)
	for _, action := range client.Actions() {
		require.NotEqual(t, "create", action.GetVerb())
	}
}

func TestSandboxRetainedCapacityAndExpiry(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("first"))
	require.NoError(t, err)
	blocked, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("second"))
	require.NoError(t, err)
	require.False(t, blocked.Admitted)
	require.Equal(t, "execution_concurrency", blocked.Reason)
	retained, err := f.service.RunnerSandboxRelease(ctx, f.identity, "first", SandboxReleaseRequest{CollectionComplete: ptr.To(false)})
	require.NoError(t, err)
	require.Equal(t, sandboxRetained, retained.State)
	require.NotNil(t, retained.RetainUntil)
	require.WithinDuration(t, time.Now().UTC().Add(24*time.Hour), *retained.RetainUntil, 3*time.Second)
	object, err := client.Resource(SandboxGVR).Namespace(retained.Namespace).Get(ctx, retained.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	shutdown, _, err := unstructured.NestedString(object.Object, "spec", "shutdownTime")
	require.NoError(t, err)
	require.Equal(t, retained.RetainUntil.UTC().Format(time.RFC3339), shutdown)
	blocked, err = f.service.RunnerSandboxGet(ctx, f.identity, "second")
	require.NoError(t, err)
	require.Equal(t, "retained_capacity", blocked.Reason)
	row := sandboxRow(t, f, "first")
	past := time.Now().UTC().Add(-time.Minute)
	row.RetainUntil, row.ReconcileAt = &past, past
	require.NoError(t, f.raw.Put(ctx, row))
	require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
	require.Equal(t, sandboxReleased, sandboxRow(t, f, "first").State)
	next, err := f.service.RunnerSandboxGet(ctx, f.identity, "second")
	require.NoError(t, err)
	require.True(t, next.Admitted)
}

func TestSandboxRetainShortensLongTaskShutdown(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	first, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	object, err := client.Resource(SandboxGVR).Namespace(first.Namespace).Get(ctx, first.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(object.Object, time.Now().UTC().Add(15*24*time.Hour).Format(time.RFC3339), "spec", "shutdownTime"))
	require.NoError(t, client.Tracker().Update(SandboxGVR, object, first.Namespace))
	retained, err := f.service.RunnerSandboxRelease(ctx, f.identity, "trial", SandboxReleaseRequest{CollectionComplete: ptr.To(false)})
	require.NoError(t, err)
	object, err = client.Resource(SandboxGVR).Namespace(first.Namespace).Get(ctx, first.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	shutdown, _, _ := unstructured.NestedString(object.Object, "spec", "shutdownTime")
	require.Equal(t, retained.RetainUntil.UTC().Format(time.RFC3339), shutdown)
}

func TestSandboxDetailProjectionIsExecutionScoped(t *testing.T) {
	f, _ := sandboxFixture(t, true)
	ctx := context.Background()
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	other := *sandboxRow(t, f, "trial")
	other.ID, other.ExecutionKey, other.TrialID = "other-execution", "different-execution", "hidden-trial"
	require.NoError(t, f.raw.Add(ctx, &other))
	other.ID, other.ExecutionKey, other.WorkspaceID = "other-space", *f.record.ExecutionKey, "other-space"
	require.NoError(t, f.raw.Add(ctx, &other))
	scoped := account.WithScope(ctx, account.Scope{WorkspaceID: f.parent.WorkspaceID, Namespace: f.pod.Namespace, Role: "member"})
	detail, err := f.service.Get(scoped, f.parent.TaskID)
	require.NoError(t, err)
	require.Len(t, detail.Sandboxes, 1)
	require.False(t, detail.SandboxesTruncated)
	require.Equal(t, "trial", detail.Sandboxes[0].TrialID)
	raw, err := json.Marshal(detail.Sandboxes)
	require.NoError(t, err)
	for _, forbidden := range []string{"hidden-trial", "requestDigest", "runnerUID", "image", f.identity.Token} {
		require.NotContains(t, string(raw), forbidden)
	}
}

func TestSandboxDetailProjectionBoundsHistoryAndPrioritizesLiveReservations(t *testing.T) {
	f, _ := sandboxFixture(t, true)
	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: f.parent.WorkspaceID, Namespace: f.pod.Namespace, Role: "member"})
	created := time.Now().UTC().Add(-time.Hour)
	for index := 0; index < 100; index++ {
		row := &model.JobSandbox{ID: fmt.Sprintf("history-%03d", index), TrialID: fmt.Sprintf("trial-%03d", index), WorkspaceID: f.parent.WorkspaceID, TaskID: f.parent.TaskID, ExecutionKey: *f.record.ExecutionKey, State: sandboxReleased}
		require.NoError(t, f.raw.Add(ctx, row))
		updated, err := f.raw.CompareAndSwap(ctx, row, "id", row.ID, map[string]interface{}{"create_time": created})
		require.NoError(t, err)
		require.True(t, updated)
	}
	detail, err := f.service.Get(ctx, f.parent.TaskID)
	require.NoError(t, err)
	require.Len(t, detail.Sandboxes, 100)
	require.False(t, detail.SandboxesTruncated, "exactly 100 is complete")
	// A newer historical record sorts before equal-time history, and an older
	// retained live reservation must remain visible even after truncation.
	for _, row := range []*model.JobSandbox{
		{ID: "new-history", TrialID: "new-history", WorkspaceID: f.parent.WorkspaceID, TaskID: f.parent.TaskID, ExecutionKey: *f.record.ExecutionKey, State: sandboxReleased},
		{ID: "old-live", TrialID: "old-live", WorkspaceID: f.parent.WorkspaceID, TaskID: f.parent.TaskID, ExecutionKey: *f.record.ExecutionKey, State: sandboxRetained, SlotReserved: true},
		{ID: "foreign-space", TrialID: "foreign-space", WorkspaceID: "other", TaskID: f.parent.TaskID, ExecutionKey: *f.record.ExecutionKey, SlotReserved: true},
		{ID: "foreign-execution", TrialID: "foreign-execution", WorkspaceID: f.parent.WorkspaceID, TaskID: f.parent.TaskID, ExecutionKey: "other", SlotReserved: true},
	} {
		require.NoError(t, f.raw.Add(context.Background(), row))
	}
	updated, err := f.raw.CompareAndSwap(ctx, &model.JobSandbox{ID: "old-live"}, "id", "old-live", map[string]interface{}{"create_time": created.Add(-time.Hour)})
	require.NoError(t, err)
	require.True(t, updated)
	detail, err = f.service.Get(ctx, f.parent.TaskID)
	require.NoError(t, err)
	require.Len(t, detail.Sandboxes, 100)
	require.True(t, detail.SandboxesTruncated)
	require.Equal(t, "old-live", detail.Sandboxes[0].TrialID)
	require.Equal(t, "new-history", detail.Sandboxes[1].TrialID)
	require.Equal(t, "trial-099", detail.Sandboxes[2].TrialID)
	require.Equal(t, "trial-002", detail.Sandboxes[99].TrialID)
	raw, err := json.Marshal(detail)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"sandboxesTruncated":true`)
	require.NotContains(t, string(raw), "foreign-space")
	require.NotContains(t, string(raw), "foreign-execution")
	count, err := f.raw.Count(ctx, &model.JobSandbox{WorkspaceID: f.parent.WorkspaceID, TaskID: f.parent.TaskID, ExecutionKey: *f.record.ExecutionKey}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(102), count, "projection never discards durable history")
}

func TestSandboxRevalidatesClaimAfterFreshKubernetesRead(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	client.PrependReactor("get", "sandboxes", func(k8stesting.Action) (bool, runtime.Object, error) {
		// Simulates ownership changing after authorizeRunner and initial intent.
		record := &model.JobInfo{ID: f.record.ID}
		require.NoError(t, f.raw.Get(ctx, record))
		var checkpoint map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(record.InternalInfo), &checkpoint))
		delete(checkpoint, "runner")
		encoded, err := json.Marshal(checkpoint)
		require.NoError(t, err)
		record.InternalInfo = string(encoded)
		require.NoError(t, f.raw.Put(ctx, record))
		return false, nil, nil
	})
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.ErrorIs(t, err, bcode.ErrUnauthorized)
	for _, action := range client.Actions() {
		require.NotEqual(t, "create", action.GetVerb())
	}
}

func TestSandboxNeverAdoptsReplacementOrRecreatesKnownUID(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{true: "replacement", false: "missing"}[replace], func(t *testing.T) {
			f, client := sandboxFixture(t, true)
			ctx := context.Background()
			first, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
			require.NoError(t, err)
			object, err := client.Resource(SandboxGVR).Namespace(first.Namespace).Get(ctx, first.SandboxName, metav1.GetOptions{})
			require.NoError(t, err)
			if replace {
				object.SetUID("replacement")
				require.NoError(t, client.Tracker().Update(SandboxGVR, object, first.Namespace))
			} else {
				require.NoError(t, client.Tracker().Delete(SandboxGVR, first.Namespace, first.SandboxName))
			}
			client.ClearActions()
			failed, err := f.service.RunnerSandboxGet(ctx, f.identity, "trial")
			require.NoError(t, err)
			require.Equal(t, sandboxFailed, failed.State)
			require.Equal(t, first.SandboxUID, failed.SandboxUID)
			for _, action := range client.Actions() {
				require.NotContains(t, []string{"create", "delete", "patch"}, action.GetVerb())
			}
		})
	}
}

func TestSandboxPodReplacementRetainsOwnedSandboxAndCapacity(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	pod := makeSandboxReady(t, f, client, "trial")
	_, err = f.service.RunnerSandboxGet(ctx, f.identity, "trial")
	require.NoError(t, err)
	pod.UID = "replacement-pod"
	_, err = f.service.Kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	row := sandboxRow(t, f, "trial")
	object, err := client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, unstructured.SetNestedField(object.Object, string(pod.UID), "status", "podInfo", "podUID"))
	require.NoError(t, client.Tracker().Update(SandboxGVR, object, row.Namespace))
	failed, err := f.service.RunnerSandboxGet(ctx, f.identity, "trial")
	require.NoError(t, err)
	require.Equal(t, "pod_identity_changed", failed.Reason)
	require.True(t, sandboxRow(t, f, "trial").SlotReserved)
}

func TestSandboxDeleteAcknowledgementDoesNotReleaseCapacity(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	client.PrependReactor("delete", "sandboxes", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, nil })
	result, err := f.service.RunnerSandboxRelease(ctx, f.identity, "trial", SandboxReleaseRequest{CollectionComplete: ptr.To(true)})
	require.NoError(t, err)
	require.Equal(t, sandboxPending, result.State)
	require.Equal(t, "release_pending", result.Reason)
	require.True(t, sandboxRow(t, f, "trial").SlotReserved)
}

func TestSandboxCancellationAndLostRunnerRetainThenReap(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{true: "runner-lost", false: "cancelled"}[lost], func(t *testing.T) {
			f, _ := sandboxFixture(t, true)
			ctx := context.Background()
			_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
			require.NoError(t, err)
			if lost {
				require.NoError(t, f.service.Kube.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
			} else {
				f.parent.Status = config.StatusCancelled
				require.NoError(t, f.raw.Put(ctx, f.parent))
			}
			row := sandboxRow(t, f, "trial")
			row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
			require.NoError(t, f.raw.Put(ctx, row))
			require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
			row = sandboxRow(t, f, "trial")
			require.True(t, row.ReleaseRequested)
			require.Equal(t, sandboxRetained, row.State)
			require.True(t, row.SlotReserved)
			past := time.Now().UTC().Add(-time.Minute)
			row.RetainUntil, row.ReconcileAt = &past, past
			require.NoError(t, f.raw.Put(ctx, row))
			require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
			require.Equal(t, sandboxReleased, sandboxRow(t, f, "trial").State)
		})
	}
}

func TestSandboxMaintenanceRetainsThenReapsAfterOwnersAreDeleted(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	created, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	require.NotEmpty(t, created.SandboxUID)

	require.NoError(t, f.raw.Delete(ctx, f.parent))
	require.NoError(t, f.raw.Delete(ctx, f.record))
	row := sandboxRow(t, f, "trial")
	row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
	require.NoError(t, f.raw.Put(ctx, row))

	require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
	row = sandboxRow(t, f, "trial")
	require.Equal(t, sandboxRetained, row.State)
	require.Equal(t, "execution_finished", row.Reason)
	require.True(t, row.ReleaseRequested)
	require.True(t, row.SlotReserved)
	_, err = client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)

	past := time.Now().UTC().Add(-time.Minute)
	row.RetainUntil, row.ReconcileAt = &past, past
	require.NoError(t, f.raw.Put(ctx, row))
	require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
	row = sandboxRow(t, f, "trial")
	require.Equal(t, sandboxReleased, row.State)
	require.False(t, row.SlotReserved)
	_, err = client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
}

func TestOrphanedSandboxMaintenanceDoesNotDeleteReplacementUID(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	created, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	object, err := client.Resource(SandboxGVR).Namespace(created.Namespace).Get(ctx, created.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	object.SetUID("replacement-sandbox")
	require.NoError(t, client.Tracker().Update(SandboxGVR, object, object.GetNamespace()))

	require.NoError(t, f.raw.Delete(ctx, f.parent))
	require.NoError(t, f.raw.Delete(ctx, f.record))
	row := sandboxRow(t, f, "trial")
	row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
	require.NoError(t, f.raw.Put(ctx, row))
	require.NoError(t, f.service.reconcileSandboxes(ctx, 100))

	row = sandboxRow(t, f, "trial")
	require.Equal(t, sandboxFailed, row.State)
	require.Equal(t, "sandbox_identity_changed", row.Reason)
	require.False(t, row.SlotReserved)
	replacement, err := client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, types.UID("replacement-sandbox"), replacement.GetUID())
}

func TestSandboxMaintenanceValidatesRemainingOwnerWhenOneIsMissing(t *testing.T) {
	for _, scenario := range []string{"task-missing", "job-missing"} {
		t.Run(scenario, func(t *testing.T) {
			f, _ := sandboxFixture(t, true)
			ctx := context.Background()
			_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
			require.NoError(t, err)
			require.NoError(t, f.raw.Add(ctx, &model.Applications{ID: "app", WorkspaceID: f.parent.WorkspaceID, Namespace: "space-ns"}))
			f.parent.AppID, f.record.AppID = "app", "app"
			require.NoError(t, f.raw.Put(ctx, f.parent))
			require.NoError(t, f.raw.Put(ctx, f.record))

			switch scenario {
			case "task-missing":
				require.NoError(t, f.raw.Delete(ctx, f.parent))
				remaining := *f.record
				remaining.ExecutionKey = ptr.To("different-execution")
				require.NoError(t, f.raw.Put(ctx, &remaining))
			case "job-missing":
				require.NoError(t, f.raw.Delete(ctx, f.record))
				remaining := *f.parent
				remaining.WorkspaceID = "different-workspace"
				require.NoError(t, f.raw.Put(ctx, &remaining))
			}
			row := sandboxRow(t, f, "trial")
			row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
			require.NoError(t, f.raw.Put(ctx, row))

			require.ErrorIs(t, f.service.reconcileSandboxes(ctx, 100), ErrRunnerConflict)
			row = sandboxRow(t, f, "trial")
			require.True(t, row.SlotReserved)
			require.False(t, row.ReleaseRequested)
		})
	}
}

func TestSandboxUncertainCreateStaysReservedAfterCancellation(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	client.PrependReactor("create", "sandboxes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("lost create response")
	})
	_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.Error(t, err)
	row := sandboxRow(t, f, "trial")
	require.Equal(t, 1, row.CreateAttempts)
	require.Empty(t, row.SandboxUID)
	f.parent.Status = config.StatusCancelled
	require.NoError(t, f.raw.Put(ctx, f.parent))
	row.LeaseUntil = ptr.To(time.Now().UTC().Add(-time.Minute))
	row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
	require.NoError(t, f.raw.Put(ctx, row))
	require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
	row = sandboxRow(t, f, "trial")
	require.Equal(t, "creation_outcome_unknown", row.Reason)
	require.True(t, row.SlotReserved)
	auth, err := f.service.authorizeRunner(ctx, f.identity)
	require.NoError(t, err)
	late, err := buildSandbox(row, auth, time.Now())
	require.NoError(t, err)
	late.SetUID("late-server-create")
	require.NoError(t, client.Tracker().Create(SandboxGVR, late, row.Namespace))
	row.ReconcileAt = time.Now().UTC().Add(-time.Minute)
	require.NoError(t, f.raw.Put(ctx, row))
	require.NoError(t, f.service.reconcileSandboxes(ctx, 100))
	require.Equal(t, "late-server-create", sandboxRow(t, f, "trial").SandboxUID)
	require.Equal(t, sandboxRetained, sandboxRow(t, f, "trial").State)
}

func TestSandboxTemplateAndResponseExcludeCapabilities(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	response, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	object, err := client.Resource(SandboxGVR).Namespace(response.Namespace).Get(ctx, response.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, object.GetOwnerReferences())
	raw, err := json.Marshal(object.Object)
	require.NoError(t, err)
	require.NotContains(t, string(raw), f.identity.Token)
	require.NotContains(t, string(raw), "ERUUN_JOB_CONFIG")
	require.Contains(t, string(raw), `"persistentContents":["filesystem"]`)
	require.Contains(t, string(raw), `"name":"agent-runtime"`)
	require.Contains(t, string(raw), `"automountServiceAccountToken":false`)
	require.Contains(t, string(raw), `"allowPrivilegeEscalation":false`)
	raw, err = json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "requestDigest")
	require.NotContains(t, string(raw), f.identity.Token)
}

func TestSandboxRejectsInvalidInputAndUnavailableObservation(t *testing.T) {
	for _, value := range []SandboxRequest{{TrialID: "../trial", Image: "image:v1", StorageMiB: 1}, {TrialID: "你好", Image: "image:v1", StorageMiB: 1}, {TrialID: strings.Repeat("a", 129), Image: "image:v1", StorageMiB: 1}, {TrialID: "trial", Image: "image:latest", StorageMiB: 1}, {TrialID: "trial", Image: "image:v1", StorageMiB: 0}, {TrialID: "trial", Image: "image:v1", StorageMiB: 1048577}} {
		require.ErrorIs(t, value.validate(), bcode.ErrJobInput)
	}
	f, _ := sandboxFixture(t, true)
	f.service.SandboxObserver.(*sandboxTestObserver).unavailable = true
	_, err := f.service.RunnerSandboxCreate(context.Background(), f.identity, sandboxRequest("trial"))
	require.ErrorIs(t, err, bcode.ErrServiceUnavailable)
}

func TestSandboxConcurrentDuplicateAndDifferentTrialsStayBounded(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	var group sync.WaitGroup
	errs := make(chan error, 6)
	for _, id := range []string{"one", "one", "two", "two", "three", "three"} {
		group.Go(func() { _, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest(id)); errs <- err })
	}
	group.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	count, err := f.raw.Count(ctx, &model.JobSandbox{SlotReserved: true}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	creates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			creates++
		}
	}
	require.Equal(t, 1, creates)
}

func TestSandboxStartingCapacityDoesNotConsumeRateAndClearsOnReady(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		require.NoError(t, f.raw.Add(ctx, &model.JobSandbox{ID: uuid.NewString(), WorkspaceID: "foreign", StartReserved: true}))
	}
	blocked, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	require.Equal(t, "starting_capacity", blocked.Reason)
	require.Zero(t, sandboxRow(t, f, "trial").CreateAttempts)
	count, err := f.raw.Count(ctx, &model.ResourceCreationBudget{}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, f.raw.DeleteByFilter(ctx, &model.JobSandbox{WorkspaceID: "foreign"}, nil))
	_, err = f.service.RunnerSandboxGet(ctx, f.identity, "trial")
	require.NoError(t, err)
	require.True(t, sandboxRow(t, f, "trial").StartReserved)
	makeSandboxReady(t, f, client, "trial")
	_, err = f.service.RunnerSandboxGet(ctx, f.identity, "trial")
	require.NoError(t, err)
	require.False(t, sandboxRow(t, f, "trial").StartReserved)
}

func TestSandboxCancelledIntentNeverCreates(t *testing.T) {
	f, client := sandboxFixture(t, true)
	ctx := context.Background()
	f.parent.Status = config.StatusCancelled
	require.NoError(t, f.raw.Put(ctx, f.parent))
	result, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
	require.NoError(t, err)
	require.Equal(t, sandboxReleased, result.State)
	require.False(t, result.Admitted)
	for _, action := range client.Actions() {
		require.NotEqual(t, "create", action.GetVerb())
	}
}

func TestSandboxReadinessRequiresCurrentGenerationAndUniqueReadyPod(t *testing.T) {
	for _, scenario := range []string{"stale-generation", "not-ready-pod", "duplicate-pods", "wrong-owner"} {
		t.Run(scenario, func(t *testing.T) {
			f, client := sandboxFixture(t, true)
			ctx := context.Background()
			_, err := f.service.RunnerSandboxCreate(ctx, f.identity, sandboxRequest("trial"))
			require.NoError(t, err)
			pod := makeSandboxReady(t, f, client, "trial")
			row := sandboxRow(t, f, "trial")
			switch scenario {
			case "stale-generation":
				object, err := client.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
				require.NoError(t, err)
				object.SetGeneration(2)
				require.NoError(t, client.Tracker().Update(SandboxGVR, object, row.Namespace))
			case "not-ready-pod":
				pod.Status.Conditions = nil
				_, err = f.service.Kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "wrong-owner":
				pod.OwnerReferences[0].UID = "different-sandbox"
				_, err = f.service.Kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "duplicate-pods":
				pod.Name, pod.UID = "duplicate", "another-uid"
				_, err = f.service.Kube.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			result, err := f.service.RunnerSandboxGet(ctx, f.identity, "trial")
			require.NoError(t, err)
			require.NotEqual(t, sandboxReady, result.State)
			require.Empty(t, result.PodUID)
			if scenario == "duplicate-pods" {
				require.Equal(t, sandboxFailed, result.State)
				require.True(t, sandboxRow(t, f, "trial").SlotReserved)
			}
		})
	}
}
