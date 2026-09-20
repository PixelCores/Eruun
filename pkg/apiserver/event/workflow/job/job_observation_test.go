package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func newObservedInstantJobCtl(t *testing.T, task *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *InstantJobCtl {
	t.Helper()
	return NewInstantJobCtl(task, client, store, ack, startJobTestObserver(t, client))
}

func startJobTestObserver(t *testing.T, client kubernetes.Interface) *informer.KubernetesWorkloadObserver {
	t.Helper()
	observer := informer.NewKubernetesWorkloadObserver(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, observer.Start(ctx))
	return observer
}

func TestRetrySharedJobObserverDoesNotPollRunningJob(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	live := task.JobInfo.(*batchv1.Job).DeepCopy()
	live.UID, live.ResourceVersion = "owned", "10"
	client := fake.NewSimpleClientset(live)
	ctl := newObservedInstantJobCtl(t, task, client, &retryCheckpointStore{}, func() {})
	cp := &instantJobRetryCheckpoint{Job: live, Attempt: 1, CurrentUID: live.UID}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	terminal, err := ctl.waitRetryAttempt(ctx, cp)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, terminal)
	require.Zero(t, countClientActions(client, "get", "jobs"))

	live.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	live.ResourceVersion = "11"
	_, err = client.BatchV1().Jobs(live.Namespace).UpdateStatus(context.Background(), live, metav1.UpdateOptions{})
	require.NoError(t, err)
	terminalCtx, cancelTerminal := context.WithTimeout(context.Background(), time.Second)
	defer cancelTerminal()
	terminal, err = ctl.waitRetryAttempt(terminalCtx, cp)
	require.NoError(t, err)
	require.Equal(t, live.UID, terminal.UID)
	require.Equal(t, 1, countClientActions(client, "get", "jobs"), "only the terminal candidate needs authoritative confirmation")
}

type fixedJobSnapshotObserver struct {
	informer.ComponentReadyObserver
	snapshot *batchv1.Job
}

func (o fixedJobSnapshotObserver) WaitForJob(ctx context.Context, _, _ string, check func(*batchv1.Job) (bool, error)) error {
	// Replay a duplicate notification before waiting, as an informer may do.
	for range 2 {
		if done, err := check(o.snapshot); done || err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestRetryConfirmsCachedTerminalIdentityAndFreshStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*batchv1.Job)
		getErr   error
		wantStop bool
	}{
		{name: "stale terminal during reconnect", change: func(job *batchv1.Job) { job.Status = batchv1.JobStatus{Active: 1} }},
		{name: "same name replacement", change: func(job *batchv1.Job) { job.UID = "replacement" }, wantStop: true},
		{name: "different task", change: func(job *batchv1.Job) { job.Annotations[config.AnnotationJobTaskID] = "other" }, wantStop: true},
		{name: "different attempt", change: func(job *batchv1.Job) { job.Annotations[workflowconfig.AnnotationJobAttempt] = "2" }, wantStop: true},
		{name: "API unavailable", getErr: errors.New("API unavailable"), wantStop: true},
		{name: "deleted object", getErr: apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "runner"), wantStop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			cached := task.JobInfo.(*batchv1.Job).DeepCopy()
			cached.UID, cached.ResourceVersion = "owned", "10"
			cached.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			fresh := cached.DeepCopy()
			if tc.change != nil {
				tc.change(fresh)
			}
			client := fake.NewSimpleClientset()
			client.PrependReactor("get", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, fresh, tc.getErr
			})
			ctl := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {})
			ctl.resourceWaiter = fixedJobSnapshotObserver{snapshot: cached}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			terminal, err := ctl.waitRetryAttempt(ctx, &instantJobRetryCheckpoint{Job: cached, Attempt: 1, CurrentUID: cached.UID})
			require.Nil(t, terminal)
			if tc.wantStop {
				require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.Equal(t, 1, countClientActions(client, "get", "jobs"))
		})
	}
}

func TestRetrySelectorExitAndMissingObserverDoNotProduceTerminal(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	live := task.JobInfo.(*batchv1.Job).DeepCopy()
	live.UID = "owned"
	cp := &instantJobRetryCheckpoint{Job: live, Attempt: 1, CurrentUID: live.UID}
	client := fake.NewSimpleClientset(live)
	ctl := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {})
	_, err := ctl.waitRetryAttempt(context.Background(), cp)
	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Zero(t, countClientActions(client, "get", "jobs"))
	ctl.resourceWaiter = fixedJobSnapshotObserver{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	terminal, err := ctl.waitRetryAttempt(ctx, cp)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, terminal)
	require.Equal(t, 2, countClientActions(client, "get", "jobs"), "absence must be confirmed for each notification")
}

func TestRetryConfirmsAbsentOrChangedRunningIdentity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		absent     bool
		change     func(*batchv1.Job)
		getErr     error
		freshOwned bool
	}{
		{name: "deleted", absent: true, getErr: apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "runner")},
		{name: "unavailable", absent: true, getErr: errors.New("API unavailable")},
		{name: "replacement running", change: func(job *batchv1.Job) { job.UID = "replacement" }},
		{name: "different running attempt", change: func(job *batchv1.Job) { job.Annotations[workflowconfig.AnnotationJobAttempt] = "2" }},
		{name: "different running execution", change: func(job *batchv1.Job) { job.Annotations[config.AnnotationJobExecutionKey] = "other" }},
		{name: "stale replacement snapshot", change: func(job *batchv1.Job) { job.UID = "replacement" }, freshOwned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			owned := task.JobInfo.(*batchv1.Job).DeepCopy()
			owned.UID, owned.ResourceVersion = "owned", "10"
			owned.Status.Active = 1
			snapshot := owned.DeepCopy()
			if tc.change != nil {
				tc.change(snapshot)
			}
			fresh := snapshot.DeepCopy()
			if tc.freshOwned {
				fresh = owned.DeepCopy()
			}
			if tc.absent {
				snapshot = nil
			}
			client := fake.NewSimpleClientset()
			client.PrependReactor("get", "jobs", func(ktesting.Action) (bool, runtime.Object, error) { return true, fresh, tc.getErr })
			ctl := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {})
			ctl.resourceWaiter = fixedJobSnapshotObserver{snapshot: snapshot}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			terminal, err := ctl.waitRetryAttempt(ctx, &instantJobRetryCheckpoint{Job: owned, Attempt: 1, CurrentUID: owned.UID})
			require.Nil(t, terminal)
			if tc.freshOwned {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			}
			require.Equal(t, 1, countClientActions(client, "get", "jobs"))
			require.Zero(t, countClientActions(client, "create", "jobs"))
		})
	}
}

func TestRetrySelectorExitRepairsObservationBeforeLaterDeletion(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	live := task.JobInfo.(*batchv1.Job).DeepCopy()
	live.UID, live.ResourceVersion, live.Labels = "owned", "10", nil
	client := fake.NewSimpleClientset(live)
	ctl := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {})
	cp := &instantJobRetryCheckpoint{Job: live.DeepCopy(), Attempt: 1, CurrentUID: live.UID}
	ctl.resourceWaiter = scriptedJobObserver{observe: func(check func(*batchv1.Job) (bool, error)) error {
		done, err := check(nil)
		require.NoError(t, err)
		require.False(t, done)
		object, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), live.Namespace, live.Name)
		require.NoError(t, err)
		repaired := object.(*batchv1.Job)
		require.Equal(t, config.ManagedByEruun, repaired.Labels[config.LabelManagedBy])
		require.Equal(t, live.UID, repaired.UID)
		done, err = check(repaired)
		require.NoError(t, err)
		require.False(t, done)
		require.Equal(t, 1, countClientActions(client, "get", "jobs"))
		require.NoError(t, client.BatchV1().Jobs(live.Namespace).Delete(context.Background(), live.Name, metav1.DeleteOptions{}))
		_, err = check(nil)
		return err
	}}
	terminal, err := ctl.waitRetryAttempt(context.Background(), cp)
	require.Nil(t, terminal)
	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.True(t, apierrors.IsNotFound(err))
	require.Equal(t, 2, countClientActions(client, "get", "jobs"))
	require.Equal(t, 1, countClientActions(client, "update", "jobs"))
	require.Zero(t, countClientActions(client, "create", "jobs"))
}

func TestRetryAddsObserverLabelOnlyToFencedOwnedJob(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		otherUID, otherOwner, conflict bool
	}{
		{name: "old unlabeled Job"},
		{name: "UID mismatch", otherUID: true},
		{name: "another manager", otherOwner: true},
		{name: "update conflict", conflict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			live := task.JobInfo.(*batchv1.Job).DeepCopy()
			live.UID, live.ResourceVersion = "owned", "10"
			live.Labels = nil
			ttl := int32(3600)
			live.Spec.TTLSecondsAfterFinished = &ttl
			cp := &instantJobRetryCheckpoint{Job: live.DeepCopy(), Attempt: 1, CurrentUID: live.UID}
			cp.Job.Labels = map[string]string{config.LabelManagedBy: config.ManagedByEruun}
			if tc.otherUID {
				live.UID = "foreign"
			}
			if tc.otherOwner {
				live.Labels = map[string]string{config.LabelManagedBy: "another-controller"}
			}
			client := fake.NewSimpleClientset(live)
			client.PrependReactor("update", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
				updated := action.(ktesting.UpdateAction).GetObject().(*batchv1.Job)
				require.Equal(t, cp.CurrentUID, updated.UID)
				require.Equal(t, "10", updated.ResourceVersion)
				if tc.conflict {
					return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "jobs"}, live.Name, errors.New("changed"))
				}
				return false, nil, nil
			})
			ctl := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {})
			err := ctl.retainLiveRetryAttempt(context.Background(), cp, live)
			if tc.otherUID || tc.otherOwner || tc.conflict {
				require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			} else {
				require.NoError(t, err)
				updated, getErr := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
				require.NoError(t, getErr)
				require.Equal(t, config.ManagedByEruun, updated.Labels[config.LabelManagedBy])
				require.Nil(t, live.Labels, "the authoritative snapshot must not be mutated")
			}
			if tc.otherUID || tc.otherOwner {
				require.Zero(t, countClientActions(client, "update", "jobs"))
			}
		})
	}
}

func TestRetryLabelBackfillStopsAfterLeaseLoss(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "old-token", "old-worker"
	live := task.JobInfo.(*batchv1.Job).DeepCopy()
	live.UID, live.ResourceVersion = "owned", "10"
	live.Labels = nil
	ttl := int32(3600)
	live.Spec.TTLSecondsAfterFinished = &ttl
	cp := &instantJobRetryCheckpoint{Job: live.DeepCopy(), Attempt: 1, CurrentUID: live.UID}
	cp.Job.Labels = map[string]string{config.LabelManagedBy: config.ManagedByEruun}
	client := fake.NewSimpleClientset(live)
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{
		TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 2, RunToken: "new-token", WorkerID: "new-worker",
	}}
	ctl := NewInstantJobCtl(task, client, store, func() {})
	require.ErrorIs(t, ctl.retainLiveRetryAttempt(context.Background(), cp, live), signal.ErrInfrastructureStop)
	require.Zero(t, countClientActions(client, "update", "jobs"))
}

type countingObservationStore struct {
	retryOwnedCheckpointStore
	reads int
}

func (s *countingObservationStore) Get(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.reads++
	}
	return s.retryOwnedCheckpointStore.Get(ctx, entity)
}

type scriptedJobObserver struct {
	informer.ComponentReadyObserver
	observe func(func(*batchv1.Job) (bool, error)) error
}

func (o scriptedJobObserver) WaitForJob(_ context.Context, _, _ string, check func(*batchv1.Job) (bool, error)) error {
	return o.observe(check)
}

func TestRetryEventsDoNotPollOwnershipAndTerminalStillFences(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token", "worker"
	active := task.JobInfo.(*batchv1.Job).DeepCopy()
	active.UID, active.ResourceVersion = "owned", "10"
	terminal := active.DeepCopy()
	terminal.ResourceVersion = "11"
	terminal.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	store := &countingObservationStore{retryOwnedCheckpointStore: retryOwnedCheckpointStore{owner: model.WorkflowQueue{
		TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID,
	}}}
	client := fake.NewSimpleClientset(terminal)
	ctl := NewInstantJobCtl(task, client, store, func() {})
	ctl.resourceWaiter = scriptedJobObserver{observe: func(check func(*batchv1.Job) (bool, error)) error {
		for range 10 {
			done, err := check(active)
			require.NoError(t, err)
			require.False(t, done)
		}
		require.Equal(t, 1, store.reads, "nonterminal notifications must not add database polling")
		require.Zero(t, countClientActions(client, "get", "jobs"))
		store.owner.RunGeneration = 2
		_, err := check(terminal)
		return err
	}}
	got, err := ctl.waitRetryAttempt(context.Background(), &instantJobRetryCheckpoint{Job: active, Attempt: 1, CurrentUID: active.UID})
	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Nil(t, got)
	require.Equal(t, 2, store.reads)
	require.Equal(t, 1, countClientActions(client, "get", "jobs"))
}
