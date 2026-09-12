package job

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type cancelledJobRecoveryStore struct {
	noopStore
	space  model.Workspace
	record *model.JobInfo
}

func (s *cancelledJobRecoveryStore) Get(_ context.Context, entity datastore.Entity) error {
	space, ok := entity.(*model.Workspace)
	if !ok || space.ID != s.space.ID {
		return datastore.ErrRecordNotExist
	}
	*space = s.space
	return nil
}

func (s *cancelledJobRecoveryStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return []datastore.Entity{s.record}, nil
}

func (s *cancelledJobRecoveryStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	record, ok := entity.(*model.JobInfo)
	if !ok || record.ID != s.record.ID || conditions["status"] != s.record.Status ||
		conditions["execution_key"] != *s.record.ExecutionKey || conditions["run_generation"] != s.record.RunGeneration ||
		conditions["attempt"] != s.record.Attempt || conditions["scheduling_reason"] != s.record.SchedulingReason {
		return false, nil
	}
	s.record.SchedulingReason = updates["scheduling_reason"].(string)
	if state, ok := updates["scheduling_state"].(string); ok {
		s.record.SchedulingState = state
	}
	return true, nil
}

func TestCleanupRecoveredCancelledJobsDeletesOnlyExactExecution(t *testing.T) {
	key := "execution-1"
	record := &model.JobInfo{
		ID: 1, TaskID: "task-1", WorkspaceID: "space-1", Type: string(config.JobDeployInstant),
		Status: string(config.StatusCancelled), ServiceName: "job-1", ExecutionKey: &key,
		RunGeneration: 2, Attempt: 1, SchedulingReason: cancelledJobCleanupPending,
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "job-1", Namespace: "space-ns", UID: types.UID("uid-1"),
		Annotations: map[string]string{
			config.AnnotationJobTaskID:          record.TaskID,
			config.AnnotationJobExecutionKey:    key,
			config.AnnotationJobRunGeneration:   strconv.FormatUint(record.RunGeneration, 10),
			workflowconfig.AnnotationJobAttempt: strconv.FormatUint(uint64(record.Attempt), 10),
		},
	}}
	client := fake.NewSimpleClientset(job)
	store := &cancelledJobRecoveryStore{space: model.Workspace{ID: "space-1", Namespace: "space-ns"}, record: record}

	cleaned, err := CleanupRecoveredCancelledJobs(context.Background(), client, store)

	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.Equal(t, cancelledJobCleanupComplete, record.SchedulingReason)
	_, err = client.BatchV1().Jobs(job.Namespace).Get(context.Background(), job.Name, metav1.GetOptions{})
	require.Error(t, err)
}

func TestCleanupRecoveredCancelledJobsDeletesExactRetryCheckpoint(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	task.WorkspaceID = "space-1"
	desired := task.JobInfo.(*batchv1.Job)
	live := desired.DeepCopy()
	live.UID = types.UID("retry-job-current")
	checkpoint := &instantJobRetryCheckpoint{
		Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: task.Attempt,
		CurrentUID: live.UID, Deadline: time.Now().Add(time.Hour).UnixNano(),
	}
	raw, err := json.Marshal(checkpoint)
	require.NoError(t, err)
	task.InternalInfo = string(raw)
	record := buildJobInfoRecord(task)
	record.ID = 1
	record.Status = string(config.StatusCancelled)
	record.SchedulingReason = cancelledJobCleanupPending
	client := fake.NewSimpleClientset(live)
	store := &cancelledJobRecoveryStore{space: model.Workspace{ID: task.WorkspaceID, Namespace: task.Namespace}, record: &record}

	cleaned, err := CleanupRecoveredCancelledJobs(context.Background(), client, store)

	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.Equal(t, cancelledJobCleanupComplete, record.SchedulingReason)
	_, err = client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.Error(t, err)
}

func TestCleanupRecoveredCancelledJobsPreservesReplacementExecution(t *testing.T) {
	key := "execution-1"
	record := &model.JobInfo{
		ID: 1, TaskID: "task-1", WorkspaceID: "space-1", Type: string(config.JobDeployInstant),
		Status: string(config.StatusCancelled), ServiceName: "job-1", ExecutionKey: &key,
		RunGeneration: 2, Attempt: 1, SchedulingReason: cancelledJobCleanupPending,
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "job-1", Namespace: "space-ns", UID: types.UID("replacement"),
		Annotations: map[string]string{
			config.AnnotationJobTaskID:          record.TaskID,
			config.AnnotationJobExecutionKey:    "replacement-execution",
			config.AnnotationJobRunGeneration:   strconv.FormatUint(record.RunGeneration, 10),
			workflowconfig.AnnotationJobAttempt: "2",
		},
	}}
	client := fake.NewSimpleClientset(job)
	store := &cancelledJobRecoveryStore{space: model.Workspace{ID: "space-1", Namespace: "space-ns"}, record: record}

	cleaned, err := CleanupRecoveredCancelledJobs(context.Background(), client, store)

	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.Equal(t, cancelledJobCleanupPreserved, record.SchedulingReason)
	_, err = client.BatchV1().Jobs(job.Namespace).Get(context.Background(), job.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupRecoveredCancelledJobsRetriesDeletionFailure(t *testing.T) {
	key := "execution-1"
	record := &model.JobInfo{
		ID: 1, TaskID: "task-1", WorkspaceID: "space-1", Type: string(config.JobDeployInstant),
		Status: string(config.StatusCancelled), ServiceName: "job-1", ExecutionKey: &key,
		RunGeneration: 2, Attempt: 1, SchedulingReason: cancelledJobCleanupPending,
		SchedulingState: workflowconfig.JobSchedulingAdmitted,
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "job-1", Namespace: "space-ns", UID: types.UID("uid-1"),
		Annotations: map[string]string{
			config.AnnotationJobTaskID: record.TaskID, config.AnnotationJobExecutionKey: key,
			config.AnnotationJobRunGeneration:   strconv.FormatUint(record.RunGeneration, 10),
			workflowconfig.AnnotationJobAttempt: strconv.FormatUint(uint64(record.Attempt), 10),
		},
	}}
	client := fake.NewSimpleClientset(job)
	failedOnce := false
	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failedOnce {
			return false, nil, nil
		}
		failedOnce = true
		return true, nil, errors.New("api unavailable")
	})
	store := &cancelledJobRecoveryStore{space: model.Workspace{ID: "space-1", Namespace: "space-ns"}, record: record}

	cleaned, err := CleanupRecoveredCancelledJobs(context.Background(), client, store)
	require.ErrorContains(t, err, "api unavailable")
	require.Zero(t, cleaned)
	require.Equal(t, cancelledJobCleanupPending, record.SchedulingReason)
	require.Equal(t, workflowconfig.JobSchedulingAdmitted, record.SchedulingState)

	cleaned, err = CleanupRecoveredCancelledJobs(context.Background(), client, store)
	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.Equal(t, cancelledJobCleanupComplete, record.SchedulingReason)
	require.Equal(t, workflowconfig.JobSchedulingReleased, record.SchedulingState)
}

func TestCleanupRecoveredCancelledJobsDeletesExactCronJob(t *testing.T) {
	key := "execution-1"
	record := &model.JobInfo{
		ID: 1, TaskID: "task-1", WorkspaceID: "space-1", Type: string(config.JobDeployScheduled),
		Status: string(config.StatusCancelled), ServiceName: "cron-1", ExecutionKey: &key,
		RunGeneration: 2, Attempt: 1, SchedulingReason: cancelledJobCleanupPending,
	}
	cron := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
		Name: "cron-1", Namespace: "space-ns", UID: types.UID("uid-1"),
		Annotations: map[string]string{
			config.AnnotationJobTaskID: record.TaskID, config.AnnotationJobExecutionKey: key,
			config.AnnotationJobRunGeneration:   strconv.FormatUint(record.RunGeneration, 10),
			workflowconfig.AnnotationJobAttempt: strconv.FormatUint(uint64(record.Attempt), 10),
		},
	}}
	client := fake.NewSimpleClientset(cron)
	store := &cancelledJobRecoveryStore{space: model.Workspace{ID: "space-1", Namespace: "space-ns"}, record: record}

	cleaned, err := CleanupRecoveredCancelledJobs(context.Background(), client, store)

	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.Equal(t, cancelledJobCleanupComplete, record.SchedulingReason)
	_, err = client.BatchV1().CronJobs(cron.Namespace).Get(context.Background(), cron.Name, metav1.GetOptions{})
	require.Error(t, err)
}

func TestCleanupRecoveredCancelledScheduledJobIgnoresReplacementJobAndDeletesExactCronJob(t *testing.T) {
	key := "execution-1"
	record := &model.JobInfo{
		ID: 1, TaskID: "task-1", WorkspaceID: "space-1", Type: string(config.JobDeployScheduled),
		Status: string(config.StatusCancelled), ServiceName: "scheduled-1", ExecutionKey: &key,
		RunGeneration: 2, Attempt: 1, SchedulingReason: cancelledJobCleanupPending,
	}
	replacement := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "scheduled-1", Namespace: "space-ns", UID: types.UID("replacement-job"),
		Annotations: map[string]string{
			config.AnnotationJobTaskID:          record.TaskID,
			config.AnnotationJobExecutionKey:    "old-execution",
			config.AnnotationJobRunGeneration:   "1",
			workflowconfig.AnnotationJobAttempt: "1",
		},
	}}
	cron := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
		Name: "scheduled-1", Namespace: "space-ns", UID: types.UID("current-cron"),
		Annotations: map[string]string{
			config.AnnotationJobTaskID: record.TaskID, config.AnnotationJobExecutionKey: key,
			config.AnnotationJobRunGeneration:   strconv.FormatUint(record.RunGeneration, 10),
			workflowconfig.AnnotationJobAttempt: strconv.FormatUint(uint64(record.Attempt), 10),
		},
	}}
	client := fake.NewSimpleClientset(replacement, cron)
	store := &cancelledJobRecoveryStore{space: model.Workspace{ID: "space-1", Namespace: "space-ns"}, record: record}

	cleaned, err := CleanupRecoveredCancelledJobs(context.Background(), client, store)

	require.NoError(t, err)
	require.Equal(t, 1, cleaned)
	require.Equal(t, cancelledJobCleanupComplete, record.SchedulingReason)
	_, err = client.BatchV1().Jobs(replacement.Namespace).Get(context.Background(), replacement.Name, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.BatchV1().CronJobs(cron.Namespace).Get(context.Background(), cron.Name, metav1.GetOptions{})
	require.Error(t, err)
}
