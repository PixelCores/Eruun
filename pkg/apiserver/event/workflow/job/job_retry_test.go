package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinstant"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type retryCheckpointStore struct {
	jobtest.NoopStore
	record    *model.JobInfo
	afterSave func(*model.JobInfo) error
}

func (*retryCheckpointStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	return time.Now().UTC(), nil
}

func (s *retryCheckpointStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	if s.record == nil {
		return nil, nil
	}
	copy := *s.record
	return []datastore.Entity{&copy}, nil
}

func (s *retryCheckpointStore) Add(_ context.Context, entity datastore.Entity) error {
	record := *entity.(*model.JobInfo)
	if s.afterSave != nil {
		if err := s.afterSave(&record); err != nil {
			return err
		}
	}
	s.record = &record
	return nil
}

func (s *retryCheckpointStore) CompareAndSwapWithConditions(_ context.Context, _ datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if s.record == nil || s.record.Attempt != conditions["attempt"] || s.record.Status != conditions["status"] {
		return false, nil
	}
	record := *s.record
	record.Attempt = updates["attempt"].(uint)
	record.Status = updates["status"].(string)
	record.Info = updates["info"].(string)
	record.InternalInfo = updates["internal_info"].(string)
	if s.afterSave != nil {
		if err := s.afterSave(&record); err != nil {
			return false, err
		}
	}
	s.record = &record
	return true, nil
}

func retryTestTask(t *testing.T, policy *workflowconfig.JobRetryPolicy) *model.JobTask {
	t.Helper()
	component := &model.ApplicationComponent{Name: "retry-job", Namespace: "ns", Image: "busybox:1.37"}
	desired := jobexec.BuildJob(component, &model.Properties{JobRetryPolicy: policy}, jobexec.JobBuildOptions{})
	desired.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi"), corev1.ResourceCPU: resource.MustParse("100m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi"), corev1.ResourceCPU: resource.MustParse("200m")},
	}
	task := &model.JobTask{Name: desired.Name, Namespace: "ns", JobInfo: desired, JobType: string(config.JobDeployInstant),
		TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 1, Attempt: 1, Timeout: 10}
	jobresult.StampJobExecutionIdentity(task, desired)
	return task
}

type retryOwnedCheckpointStore struct {
	retryCheckpointStore
	owner           model.WorkflowQueue
	ownershipChecks int
}

func (s *retryOwnedCheckpointStore) Get(_ context.Context, entity datastore.Entity) error {
	if owner, ok := entity.(*model.WorkflowQueue); ok {
		*owner = s.owner
		return nil
	}
	return datastore.ErrRecordNotExist
}

// The fixture models an explicit transaction boundary; SQL tests verify isolation.
func (s *retryOwnedCheckpointStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.WithTransaction(ctx, fn)
}

func (s *retryOwnedCheckpointStore) WithTransaction(_ context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *retryOwnedCheckpointStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.ownershipChecks++
		return conditions["run_generation"] == s.owner.RunGeneration && conditions["run_token"] == s.owner.RunToken &&
			conditions["worker_id"] == s.owner.WorkerID && conditions["status"] == s.owner.Status, nil
	}
	return s.retryCheckpointStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func TestRetryCancellationWithCommittedParentPersistsTerminal(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
	task.OwnerStatus = config.StatusCancelled
	desired := task.JobInfo.(*batchv1.Job)
	desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
		CurrentUID: "owned", Deadline: time.Now().Add(time.Hour).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo = string(raw)
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusCancelled,
		RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
	client := fake.NewSimpleClientset()
	ctl := newObservedInstantJobCtl(t, task, client, store, func() {})
	task.Status = config.StatusCancelled
	require.NoError(t, persistTerminalJobState(context.Background(), ctl, task, store, nil))
	require.Equal(t, string(config.StatusCancelled), store.record.Status)
}

func TestRetryCancellationDuringCreatedUIDCheckpoint(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
		RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
	store.afterSave = func(record *model.JobInfo) error {
		var cp jobinfo.InstantJobRetryCheckpoint
		require.NoError(t, json.Unmarshal([]byte(record.InternalInfo), &cp))
		if cp.CurrentUID != "" && record.Status == string(config.StatusRunning) {
			store.owner.Status = config.StatusCancelled
			return repository.ErrWorkflowOwnershipLost
		}
		return nil
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		live := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		live.UID = "created-before-cancel"
		require.NoError(t, client.Tracker().Add(live))
		return true, live, nil
	})
	ctl := newObservedInstantJobCtl(t, task, client, store, func() {})
	err := ctl.Run(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Equal(t, config.StatusCancelled, task.OwnerStatus)
	ctl.Clean(context.Background())
	require.Equal(t, 1, jobtest.CountClientActions(client, "create", "jobs"))
	require.Equal(t, 1, jobtest.CountClientActions(client, "delete", "jobs"))
	task.Status = config.StatusCancelled
	require.NoError(t, persistTerminalJobState(context.Background(), ctl, task, store, nil))
	require.Equal(t, string(config.StatusCancelled), store.record.Status)
}

func TestRetryCancellationWithoutLiveJobPersistsTerminal(t *testing.T) {
	for _, attempt := range []uint{1, 2} {
		for _, tc := range []struct {
			name               string
			changeOwner        func(*model.WorkflowQueue)
			infrastructureStop bool
			wantOwnerStatus    config.Status
			wantStoredStatus   config.Status
			wantSaveErr        error
		}{
			{name: "cancelled owner", wantOwnerStatus: config.StatusCancelled, wantStoredStatus: config.StatusCancelled},
			{name: "running owner", changeOwner: func(owner *model.WorkflowQueue) { owner.Status = config.StatusRunning },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusCancelled},
			{name: "new generation", changeOwner: func(owner *model.WorkflowQueue) { owner.RunGeneration++ },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning, wantSaveErr: repository.ErrWorkflowOwnershipLost},
			{name: "new token", changeOwner: func(owner *model.WorkflowQueue) { owner.RunToken = "replacement" },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning, wantSaveErr: repository.ErrWorkflowOwnershipLost},
			{name: "new worker", changeOwner: func(owner *model.WorkflowQueue) { owner.WorkerID = "replacement" },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning, wantSaveErr: repository.ErrWorkflowOwnershipLost},
			{name: "infrastructure stop", infrastructureStop: true,
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning},
		} {
			t.Run(fmt.Sprintf("attempt-%d/%s", attempt, tc.name), func(t *testing.T) {
				task := retryTestTask(t, jobtest.RetryTestPolicy())
				task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
				task.OwnerStatus, task.Status, task.Attempt = config.StatusRunning, config.StatusRunning, attempt
				desired := task.JobInfo.(*batchv1.Job)
				desired.Annotations[workflowconfig.AnnotationJobAttempt] = strconv.FormatUint(uint64(attempt), 10)
				cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired,
					Attempt: attempt, Deadline: time.Now().Add(time.Hour).UnixNano()}
				if attempt > 1 {
					cp.PreviousUID, cp.RetryAt = "deleted-attempt-1", time.Now().Add(-time.Second).UnixNano()
				}
				raw, err := json.Marshal(cp)
				require.NoError(t, err)
				task.InternalInfo = string(raw)
				record := jobinfo.BuildJobInfoRecord(task)
				store := &retryOwnedCheckpointStore{retryCheckpointStore: retryCheckpointStore{record: &record},
					owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusCancelled,
						RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
				if tc.changeOwner != nil {
					tc.changeOwner(&store.owner)
				}
				client := fake.NewSimpleClientset()
				ctl := newObservedInstantJobCtl(t, task, client, store, func() {})
				ctx, cancel := context.WithCancelCause(context.Background())
				if tc.infrastructureStop {
					cancel(signal.ErrInfrastructureStop)
				} else {
					cancel(context.Canceled)
				}
				// The signal arrives between attempts: the checkpoint exists,
				// but no live Job can trigger cleanup's ownership check.
				require.ErrorIs(t, ctl.Run(ctx), context.Canceled)
				if !tc.infrastructureStop {
					ctl.Clean(ctx)
					// runJob sets the terminal cancellation status after cleanup.
					task.Status = config.StatusCancelled
				}
				err = persistTerminalJobState(ctx, ctl, task, store, nil)
				if tc.wantSaveErr != nil {
					require.ErrorIs(t, err, tc.wantSaveErr)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.wantOwnerStatus, task.OwnerStatus)
				require.Equal(t, string(tc.wantStoredStatus), store.record.Status)
				require.Equal(t, attempt, store.record.Attempt)
				require.Zero(t, jobtest.CountClientActions(client, "create", "jobs"))
				require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
			})
		}
	}
}

func TestRetrySuccessKeepsEvidenceUntilResultAndLogsAreSaved(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
	client := fake.NewSimpleClientset()
	var created []*batchv1.Job
	jobtest.InstallRetryJobReactor(t, client, 0, "", &created)
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
		RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
	ctl := newObservedInstantJobCtl(t, task, client, store, func() {})
	require.NoError(t, ctl.Run(context.Background()))
	var cp jobinfo.InstantJobRetryCheckpoint
	require.NoError(t, json.Unmarshal([]byte(store.record.InternalInfo), &cp))
	require.Equal(t, created[0].UID, cp.CurrentUID)
	require.Equal(t, string(config.StatusRunning), store.record.Status)
	require.NotNil(t, created[0].Spec.TTLSecondsAfterFinished)
	require.GreaterOrEqual(t, int64(*created[0].Spec.TTLSecondsAfterFinished), task.Timeout+int64(config.DefaultJobTTLSeconds))
	pod := jobtest.RetryTestPod(created[0], "Completed")
	pod.Status.Phase = corev1.PodSucceeded
	pod.Spec.Containers = created[0].Spec.Template.Spec.Containers
	require.NoError(t, client.Tracker().Add(pod))
	jobexec.FinalizeCompletedJobIfNeeded(context.Background(), client, task)
	require.NotEmpty(t, task.Info)
	require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
	store.afterSave = func(*model.JobInfo) error { return errors.New("database unavailable during terminal save") }
	require.ErrorContains(t, persistTerminalJobState(context.Background(), ctl, task, store, nil), "database unavailable")
	require.Equal(t, string(config.StatusRunning), store.record.Status)
	require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
	store.afterSave = nil

	// A new lease resumes the same execution and completed attempt without a
	// create, resource increase, fresh deadline, or fresh retry budget.
	recovered := retryTestTask(t, jobtest.RetryTestPolicy())
	recovered.ExecutionKey, recovered.RunGeneration = *store.record.ExecutionKey, store.record.RunGeneration
	recovered.OwnerRunGeneration, recovered.RunToken, recovered.WorkerID = 2, "token-2", "worker-2"
	store.owner.RunGeneration, store.owner.RunToken, store.owner.WorkerID = 2, recovered.RunToken, recovered.WorkerID
	recovered.InternalInfo, recovered.Attempt = store.record.InternalInfo, store.record.Attempt
	require.NoError(t, jobinstant.RestoreInstantJobRetryCheckpoint(recovered))
	recoveredCtl := newObservedInstantJobCtl(t, recovered, client, store, func() {})
	require.NoError(t, recoveredCtl.Run(context.Background()))
	jobexec.FinalizeCompletedJobIfNeeded(context.Background(), client, recovered)
	require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
	require.Equal(t, task.Info, recovered.Info)
	require.NoError(t, persistTerminalJobState(context.Background(), recoveredCtl, recovered, store, nil))
	require.Equal(t, string(config.StatusCompleted), store.record.Status)
	require.Equal(t, recovered.Info, store.record.Info)
	require.Equal(t, 1, jobtest.CountClientActions(client, "delete", "jobs"))
	require.Len(t, created, 1)
	var restored jobinfo.InstantJobRetryCheckpoint
	require.NoError(t, json.Unmarshal([]byte(store.record.InternalInfo), &restored))
	require.Equal(t, cp.Deadline, restored.Deadline)
	require.Equal(t, cp.Attempt, restored.Attempt)
	require.Equal(t, cp.CurrentUID, restored.CurrentUID)
}

func TestRetryFailureKeepsEvidenceUntilResultIsSaved(t *testing.T) {
	for _, interruption := range []string{"none", "terminal save failure", "process exit before save"} {
		t.Run(interruption, func(t *testing.T) {
			policy := &workflowconfig.JobRetryPolicy{OnOOM: "stop"}
			task := retryTestTask(t, policy)
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			jobtest.InstallRetryJobReactor(t, client, 1, "OOMKilled", &created)
			store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
				RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
			ctl := newObservedInstantJobCtl(t, task, client, store, func() {})
			require.ErrorContains(t, ctl.Run(context.Background()), "job failed after attempt 1")
			require.Equal(t, config.StatusFailed, task.Status)
			originalCheckpoint := store.record.InternalInfo
			// runJob invokes Clean before its deferred terminal persistence.
			ctl.Clean(context.Background())
			require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
			require.Equal(t, string(config.StatusRunning), store.record.Status)

			if interruption == "terminal save failure" {
				store.afterSave = func(*model.JobInfo) error { return errors.New("database unavailable during terminal save") }
				require.ErrorContains(t, persistTerminalJobState(context.Background(), ctl, task, store, nil), "database unavailable")
				require.Equal(t, string(config.StatusRunning), store.record.Status)
				require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
				store.afterSave = nil
			}
			if interruption != "none" {
				recovered := retryTestTask(t, policy)
				recovered.ExecutionKey, recovered.RunGeneration = *store.record.ExecutionKey, store.record.RunGeneration
				recovered.OwnerRunGeneration, recovered.RunToken, recovered.WorkerID = 2, "token-2", "worker-2"
				store.owner.RunGeneration, store.owner.RunToken, store.owner.WorkerID = 2, recovered.RunToken, recovered.WorkerID
				recovered.InternalInfo, recovered.Attempt = store.record.InternalInfo, store.record.Attempt
				require.NoError(t, jobinstant.RestoreInstantJobRetryCheckpoint(recovered))
				ctl = newObservedInstantJobCtl(t, recovered, client, store, func() {})
				err := ctl.Run(context.Background())
				require.ErrorContains(t, err, "job failed after attempt 1")
				require.NotErrorIs(t, err, signal.ErrInfrastructureStop)
				require.Equal(t, task.Error, recovered.Error)
				ctl.Clean(context.Background())
				task = recovered
			}
			require.Equal(t, originalCheckpoint, task.InternalInfo, "recovery must keep its attempt, UID, resources and deadline")
			require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
			require.NoError(t, persistTerminalJobState(context.Background(), ctl, task, store, nil))
			require.Equal(t, string(config.StatusFailed), store.record.Status)
			require.Equal(t, 1, jobtest.CountClientActions(client, "delete", "jobs"))
			require.Len(t, created, 1, "terminal recovery must not execute the workload again")
		})
	}
}

func TestRetryFailedTerminalSaveRejectsDifferentLease(t *testing.T) {
	for _, changed := range []string{"generation", "token", "worker"} {
		t.Run(changed, func(t *testing.T) {
			task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			jobtest.InstallRetryJobReactor(t, client, 1, "OOMKilled", &created)
			store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
				RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
			ctl := newObservedInstantJobCtl(t, task, client, store, func() {})
			require.ErrorContains(t, ctl.Run(context.Background()), "job failed after attempt 1")
			switch changed {
			case "generation":
				store.owner.RunGeneration++
			case "token":
				store.owner.RunToken = "token-2"
			case "worker":
				store.owner.WorkerID = "worker-2"
			}
			ctl.Clean(context.Background())
			require.ErrorIs(t, persistTerminalJobState(context.Background(), ctl, task, store, nil), repository.ErrWorkflowOwnershipLost)
			require.Equal(t, string(config.StatusRunning), store.record.Status)
			require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
		})
	}
}
