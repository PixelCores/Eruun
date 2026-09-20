package informer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestJobObserverUsesManagedSelectorAndPreservesSnapshotFields(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "runner", Namespace: "own", UID: "job-uid", ResourceVersion: "8",
		Labels:        map[string]string{config.LabelManagedBy: config.ManagedByEruun},
		Annotations:   map[string]string{config.AnnotationJobTaskID: "task"},
		ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kube-controller-manager"}},
	}, Status: batchv1.JobStatus{Active: 1}}
	foreign := job.DeepCopy()
	foreign.Name = "foreign"
	foreign.Labels = nil
	client := fake.NewSimpleClientset(job, foreign)
	observer := NewKubernetesWorkloadObserver(client)
	startKubernetesWorkloadObserver(t, observer)
	got, err := observer.jobLister.Jobs("own").Get("runner")
	require.NoError(t, err)
	require.Empty(t, got.ManagedFields)
	require.Equal(t, job.UID, got.UID)
	require.Equal(t, job.ResourceVersion, got.ResourceVersion)
	require.Equal(t, job.Annotations, got.Annotations)
	require.Equal(t, job.Status, got.Status)
	_, err = observer.jobLister.Jobs("own").Get("foreign")
	require.Error(t, err)
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "jobs" {
			continue
		}
		switch action.GetVerb() {
		case "list":
			require.Equal(t, "app.kubernetes.io/managed-by=eruun", action.(ktesting.ListAction).GetListRestrictions().Labels.String())
		case "watch":
			require.Equal(t, "app.kubernetes.io/managed-by=eruun", action.(ktesting.WatchAction).GetWatchRestrictions().Labels.String())
		default:
			t.Fatalf("unexpected Job API operation: %s", action.GetVerb())
		}
	}
}

func TestJobObserverWakeupsAreKeyedCoalescedAndHandleTombstones(t *testing.T) {
	one, two := make(chan struct{}, 1), make(chan struct{}, 1)
	observer := &KubernetesWorkloadObserver{jobWaiters: map[string]map[chan struct{}]struct{}{
		"own/one": {one: {}}, "own/two": {two: {}},
	}}
	for range 100 {
		observer.notifyJobWaiters(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "own", Name: "one"}})
		observer.notifyJobWaiters(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "one"}})
	}
	require.Len(t, one, 1)
	require.Empty(t, two)
	require.Len(t, observer.jobWaiters, 2, "events for unwatched objects must not allocate waiters")
	observer.notifyJobWaiters(cache.DeletedFinalStateUnknown{Key: "own/two"})
	require.Len(t, two, 1)
}

func TestJobObserverCacheAbsenceAndCancellationDoNotImplyTerminal(t *testing.T) {
	observer := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	startKubernetesWorkloadObserver(t, observer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	err := observer.WaitForJob(ctx, "own", "missing", func(snapshot *batchv1.Job) (bool, error) {
		checks++
		require.Nil(t, snapshot)
		cancel()
		return false, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, checks)
	observer.jobWaitersMu.Lock()
	require.Empty(t, observer.jobWaiters)
	observer.jobWaitersMu.Unlock()
}

func TestJobObserverSelectorExitRemainsUnknown(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "runner", Namespace: "own", UID: "job-uid",
		Labels: map[string]string{config.LabelManagedBy: config.ManagedByEruun},
	}}
	observer := NewKubernetesWorkloadObserver(fake.NewSimpleClientset(job))
	startKubernetesWorkloadObserver(t, observer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	err := observer.WaitForJob(ctx, job.Namespace, job.Name, func(snapshot *batchv1.Job) (bool, error) {
		checks++
		if checks == 1 {
			require.Equal(t, job.UID, snapshot.UID)
			// A filtered DELETED event removes the object from this cache even
			// when it is still running under different labels in Kubernetes.
			require.NoError(t, observer.jobInformer.GetStore().Delete(job))
			observer.notifyJobWaiters(cache.DeletedFinalStateUnknown{Key: "own/runner", Obj: job})
		} else {
			require.Nil(t, snapshot)
			cancel()
		}
		return false, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, checks)
}

func TestJobObserverRejectsUnsyncedAndStoppedCache(t *testing.T) {
	observer := NewKubernetesWorkloadObserver(fake.NewSimpleClientset())
	check := func(*batchv1.Job) (bool, error) {
		t.Fatal("unavailable cache must not produce a candidate")
		return true, nil
	}
	require.ErrorContains(t, observer.WaitForJob(context.Background(), "own", "runner", check), "not synchronized")
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, observer.Start(ctx))
	cancel()
	require.ErrorContains(t, observer.WaitForJob(context.Background(), "own", "runner", check), "stopped")
}

func TestJobObserverRequiresInitialJobSnapshot(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("Job list forbidden")
	})
	observer := NewKubernetesWorkloadObserver(client)
	observer.syncTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.ErrorIs(t, observer.Start(ctx), context.DeadlineExceeded)
	require.False(t, observer.synced.Load())
}
