package jobresult

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

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type resultJobInfoStore struct {
	jobtest.NoopStore
	listEntities            []datastore.Entity
	listErr                 error
	putErr                  error
	lastQuery               datastore.Entity
	lastOpts                *datastore.ListOptions
	putEntity               datastore.Entity
	filterExecutionIdentity bool
}

func (s *resultJobInfoStore) List(_ context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	s.lastQuery = query
	s.lastOpts = opts
	if s.listErr != nil {
		return nil, s.listErr
	}
	if !s.filterExecutionIdentity || opts == nil {
		return s.listEntities, nil
	}
	filtered := make([]datastore.Entity, 0, len(s.listEntities))
	for _, entity := range s.listEntities {
		jobInfo, ok := entity.(*model.JobInfo)
		if !ok || jobInfo == nil || jobInfoMatchesExecutionFilters(jobInfo, opts.FilterOptions.In) {
			filtered = append(filtered, entity)
		}
	}
	return filtered, nil
}

func jobInfoMatchesExecutionFilters(jobInfo *model.JobInfo, filters []datastore.InQueryOption) bool {
	for _, filter := range filters {
		switch filter.Key {
		case "execution_key":
			if jobInfo.ExecutionKey == nil || len(filter.Values) != 1 || *jobInfo.ExecutionKey != filter.Values[0] {
				return false
			}
		case "run_generation":
			if len(filter.Values) != 1 || strconv.FormatUint(jobInfo.RunGeneration, 10) != filter.Values[0] {
				return false
			}
		}
	}
	return true
}

func (s *resultJobInfoStore) Put(_ context.Context, entity datastore.Entity) error {
	s.putEntity = entity
	return s.putErr
}

func (s *resultJobInfoStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok {
		return false, datastore.ErrEntityInvalid
	}
	matched, err := resultOutboxJobInfoMatchesConditions(jobInfo, conditions)
	if !matched || err != nil {
		return matched, err
	}
	updated := *jobInfo
	if value, ok := updates["status"].(string); ok {
		updated.Status = value
	}
	if value, ok := updates["error"].(string); ok {
		updated.Error = value
	}
	if value, ok := updates["info"].(string); ok {
		updated.Info = value
	}
	if value, ok := updates["start_time"].(int64); ok {
		updated.StartTime = value
	}
	if value, ok := updates["end_time"].(int64); ok {
		updated.EndTime = value
	}
	if err := s.Put(ctx, &updated); err != nil {
		return false, err
	}
	*jobInfo = updated
	return true, nil
}

func TestEnqueueDelayJobBranches(t *testing.T) {
	ctx := context.Background()

	_, err := jobinfo.EnqueueDelayJob(ctx, nil, nil)
	require.Error(t, err)

	_, err = jobinfo.EnqueueDelayJob(ctx, nil, &jobinfo.DelayJobPayload{})
	require.Error(t, err)

	_, err = jobinfo.EnqueueDelayJob(ctx, nil, &jobinfo.DelayJobPayload{
		TaskID:        "task-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 1,
		Job:           &batchv1.Job{},
	})
	require.ErrorContains(t, err, "job name")

	_, err = jobinfo.EnqueueDelayJob(ctx, nil, &jobinfo.DelayJobPayload{
		TaskID:        "task-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 1,
		Job:           &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}},
	})
	require.ErrorIs(t, err, jobinfo.ErrDelayQueueUnavailable)

	queue := &jobtest.EnqueueCaptureQueue{EnqueueID: "delay-1"}
	id, err := jobinfo.EnqueueDelayJob(ctx, queue, &jobinfo.DelayJobPayload{
		ExecuteAt:     10,
		TaskID:        "task-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 3,
		RunToken:      "run-3",
		Job:           &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}},
	})
	require.NoError(t, err)
	require.Equal(t, "delay-1", id)
	require.Len(t, queue.Enqueued, 1)

	notification := jobinfo.NotificationForDelayJob(&jobinfo.DelayJobPayload{})
	notification.Version = 0
	require.NoError(t, json.Unmarshal(queue.Enqueued[0], &notification))
	require.Equal(t, jobinfo.DelayJobNotificationVersion, notification.Version)
	require.Equal(t, "task-1", notification.TaskID)
	require.Equal(t, uint64(3), notification.RunGeneration)
	require.Equal(t, "run-3", notification.RunToken)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(queue.Enqueued[0], &fields))
	require.NotContains(t, fields, "job")
	require.NotContains(t, fields, "namespace")
	require.NotContains(t, fields, "timeoutSeconds")
}

func TestPersistDelayJobCheckpointStoresRecoverablePayload(t *testing.T) {
	store := &jobInfoSaveStore{}
	jobTask := &model.JobTask{
		Name:          "demo",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployInstant),
		ExecutionKey:  "execution-1",
		RunGeneration: 3,
	}
	payload := &jobinfo.DelayJobPayload{
		ExecuteAt:     4102444800,
		TaskID:        jobTask.TaskID,
		ExecutionKey:  jobTask.ExecutionKey,
		RunGeneration: jobTask.RunGeneration,
		JobType:       jobTask.JobType,
		Job:           &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}},
	}

	require.NoError(t, jobinfo.PersistDelayJobCheckpoint(context.Background(), jobtest.WithJobTestOwner(store, jobTask), jobTask, payload))
	require.Equal(t, config.StatusDistributed, jobTask.Status)
	require.Equal(t, config.JobDelayStatePending, jobTask.DelayState)
	require.Equal(t, payload.ExecuteAt, jobTask.DelayExecuteAt)
	require.NotEmpty(t, jobTask.DelayPayload)

	record := store.added
	require.NotNil(t, record)
	require.Equal(t, config.JobDelayStatePending, record.DelayState)
	require.Equal(t, payload.ExecuteAt, record.DelayExecuteAt)
	var storedPayload jobinfo.DelayJobPayload
	require.NoError(t, json.Unmarshal([]byte(record.DelayPayload), &storedPayload))
	require.Equal(t, payload.ExecutionKey, storedPayload.ExecutionKey)
}

func TestEnqueueResultJob(t *testing.T) {
	ctx := context.Background()

	_, err := enqueueResultJob(ctx, nil, nil)
	require.Error(t, err)
	_, err = enqueueResultJob(ctx, nil, &JobResultPayload{})
	require.Error(t, err)

	valid := &JobResultPayload{
		OutboxID: "outbox-1", Name: "job-1", Namespace: "default", TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 1,
		JobType: string(config.JobDeployInstant), ServiceName: "svc-a", TimeoutSeconds: 60,
	}
	_, err = enqueueResultJob(ctx, nil, valid)
	require.ErrorIs(t, err, ErrResultQueueUnavailable)

	queue := &jobtest.EnqueueCaptureQueue{EnqueueID: "result-1"}
	id, err := enqueueResultJob(ctx, queue, valid)
	require.NoError(t, err)
	require.Equal(t, "result-1", id)
	require.Len(t, queue.Enqueued, 1)

	require.JSONEq(t, `{"outboxId":"outbox-1","name":"job-1","namespace":"default","taskId":"task-1","executionKey":"execution-1","runGeneration":1,"jobType":"instant_job","serviceName":"svc-a","timeoutSeconds":60}`, string(queue.Enqueued[0]))

	queue.EnqueueErr = errors.New("enqueue failed")
	_, err = enqueueResultJob(ctx, queue, valid)
	require.ErrorIs(t, err, queue.EnqueueErr)
}

func TestEnqueueResultJobReturnsQueueErrorWhenUnavailable(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := enqueueResultJob(canceledCtx, nil, &JobResultPayload{
		OutboxID: "outbox-1", Name: "job-1", Namespace: "default", TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 1,
	})
	require.ErrorIs(t, err, ErrResultQueueUnavailable)
}

func TestDelayResultPayloadAndDecode(t *testing.T) {
	_, err := decodeResultPayload([]byte(`{"taskId":`))
	require.Error(t, err)

	decoded, err := decodeResultPayload([]byte(`{"outboxId":"outbox-1","taskId":"task-1","executionKey":"execution-1","runGeneration":1,"namespace":"default","name":"job-1"}`))
	require.NoError(t, err)
	require.Equal(t, "task-1", decoded.TaskID)

	require.Nil(t, NewJobResultPayloadFromDelay(nil, nil))
	require.Nil(t, NewJobResultPayloadFromDelay(&jobinfo.DelayJobPayload{}, &batchv1.Job{}))

	delayPayload := &jobinfo.DelayJobPayload{
		TaskID:         "task-2",
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "ns-1",
		ExecutionKey:   "execution-2",
		RunGeneration:  8,
		TimeoutSeconds: 90,
	}
	delayJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-2",
			Namespace: "ns-1",
			Labels: map[string]string{
				spec.LabelComponentName: "svc-b",
			},
		},
	}
	resultPayload := NewJobResultPayloadFromDelay(delayPayload, delayJob)
	require.NotNil(t, resultPayload)
	require.Equal(t, "svc-b", resultPayload.ServiceName)
	require.Equal(t, "execution-2", resultPayload.ExecutionKey)
	require.Equal(t, uint64(8), resultPayload.RunGeneration)
	require.Equal(t, int64(90), resultPayload.TimeoutSeconds)

}

func TestJobResultExecutionIdentityMatchesStampedKubernetesJob(t *testing.T) {
	jobTask := &model.JobTask{TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 7}
	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "default"}}
	StampJobExecutionIdentity(jobTask, jobObj)

	require.Equal(t, "task-1", jobObj.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, "execution-1", jobObj.Annotations[config.AnnotationJobExecutionKey])
	require.Equal(t, "7", jobObj.Annotations[config.AnnotationJobRunGeneration])
	require.True(t, JobResultMatchesExecutionIdentity(&JobResultPayload{TaskID: "task-1", Name: "job-1", Namespace: "default", ExecutionKey: "execution-1", RunGeneration: 7}, jobObj))
	require.False(t, JobResultMatchesExecutionIdentity(&JobResultPayload{TaskID: "task-other", Name: "job-1", Namespace: "default", ExecutionKey: "execution-1", RunGeneration: 7}, jobObj))
	require.False(t, JobResultMatchesExecutionIdentity(&JobResultPayload{TaskID: "task-1", Name: "job-1", Namespace: "default", ExecutionKey: "execution-old", RunGeneration: 6}, jobObj))
	require.False(t, JobResultMatchesExecutionIdentity(&JobResultPayload{}, jobObj), "incomplete identities are rejected")
}

func TestUpdateJobInfoStatusDoesNotTargetNewerExecution(t *testing.T) {
	newExecutionKey := "execution-new"
	newest := &model.JobInfo{
		TaskID:        "task-shared",
		Type:          string(config.JobDeployScheduled),
		ServiceName:   "svc-a",
		Status:        string(config.StatusRunning),
		ExecutionKey:  &newExecutionKey,
		RunGeneration: 2,
	}
	store := &resultJobInfoStore{
		listEntities:            []datastore.Entity{newest},
		filterExecutionIdentity: true,
	}
	payload := &JobResultPayload{
		TaskID:        newest.TaskID,
		JobType:       newest.Type,
		ServiceName:   newest.ServiceName,
		ExecutionKey:  "execution-old",
		RunGeneration: 1,
		Namespace:     "default",
		Name:          "job-1",
	}

	require.NoError(t, UpdateJobInfoStatus(context.Background(), store, payload, config.StatusFailed, "old failure", 0, 0, ""))
	require.Nil(t, store.putEntity)
	require.Equal(t, string(config.StatusRunning), newest.Status)
	require.Len(t, store.lastOpts.FilterOptions.In, 4)
}

func TestJobTimesFromStatus(t *testing.T) {
	startTime, endTime := jobTimesFromStatus(nil)
	require.Zero(t, startTime)
	require.Zero(t, endTime)

	start := metav1.NewTime(time.Unix(10, 0))
	end := metav1.NewTime(time.Unix(20, 0))

	startTime, endTime = jobTimesFromStatus(&batchv1.Job{
		Status: batchv1.JobStatus{
			StartTime:      &start,
			CompletionTime: &end,
		},
	})
	require.Equal(t, int64(10), startTime)
	require.Equal(t, int64(20), endTime)
}

func TestUpdateJobInfoStatusBranches(t *testing.T) {
	ctx := context.Background()
	payload := &JobResultPayload{
		TaskID:        "task-1",
		JobType:       string(config.JobDeploy),
		ServiceName:   "svc-a",
		Namespace:     "default",
		Name:          "job-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 1,
	}

	require.ErrorIs(t, UpdateJobInfoStatus(ctx, nil, payload, config.StatusFailed, "msg", 0, 0, ""), ErrResultDispatchNoRetry)
	require.ErrorIs(t, UpdateJobInfoStatus(ctx, &resultJobInfoStore{}, nil, config.StatusFailed, "msg", 0, 0, ""), ErrResultDispatchNoRetry)
	require.ErrorIs(t, UpdateJobInfoStatus(ctx, &resultJobInfoStore{}, &JobResultPayload{}, config.StatusFailed, "msg", 0, 0, ""), ErrResultDispatchNoRetry)

	listErrStore := &resultJobInfoStore{listErr: errors.New("list failed")}
	require.Error(t, UpdateJobInfoStatus(ctx, listErrStore, payload, config.StatusFailed, "msg", 0, 0, ""))

	emptyStore := &resultJobInfoStore{}
	require.NoError(t, UpdateJobInfoStatus(ctx, emptyStore, payload, config.StatusFailed, "msg", 0, 0, ""))

	badTypeStore := &resultJobInfoStore{listEntities: []datastore.Entity{&model.Workflow{ID: "wf-1"}}}
	require.Error(t, UpdateJobInfoStatus(ctx, badTypeStore, payload, config.StatusFailed, "msg", 0, 0, ""))

	executionKey := payload.ExecutionKey
	jobInfo := &model.JobInfo{ID: 1, TaskID: "task-1", ExecutionKey: &executionKey, RunGeneration: payload.RunGeneration}
	okStore := &resultJobInfoStore{
		listEntities: []datastore.Entity{jobInfo},
	}
	require.NoError(t, UpdateJobInfoStatus(ctx, okStore, payload, config.StatusCompleted, " should clear ", 100, 200, "logs"))

	updated, ok := okStore.putEntity.(*model.JobInfo)
	require.True(t, ok)
	require.Equal(t, string(config.StatusCompleted), updated.Status)
	require.Equal(t, "", updated.Error)
	require.Equal(t, int64(100), updated.StartTime)
	require.Equal(t, int64(200), updated.EndTime)
	require.Equal(t, "logs", updated.Info)
	require.NotNil(t, okStore.lastOpts)
	require.Len(t, okStore.lastOpts.FilterOptions.In, 4)
}

func TestUpdateJobInfoStatusDoesNotDowngradeCompletedStatus(t *testing.T) {
	ctx := context.Background()
	payload := &JobResultPayload{
		TaskID:        "task-keep-completed",
		JobType:       string(config.JobDeployScheduled),
		ServiceName:   "svc-a",
		Namespace:     "default",
		Name:          "job-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 1,
	}
	jobInfo := &model.JobInfo{
		TaskID:      payload.TaskID,
		ServiceName: payload.ServiceName,
		Status:      string(config.StatusCompleted),
		Info:        "existing logs",
	}
	store := &resultJobInfoStore{
		listEntities: []datastore.Entity{jobInfo},
	}

	require.NoError(t, UpdateJobInfoStatus(ctx, store, payload, config.StatusTimeout, "timeout waiting for duplicate result", 0, 0, ""))
	require.Nil(t, store.putEntity)
	require.Equal(t, string(config.StatusCompleted), jobInfo.Status)
	require.Equal(t, "existing logs", jobInfo.Info)
	require.Equal(t, "", jobInfo.Error)
}
