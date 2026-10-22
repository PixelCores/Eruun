package jobresult

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// contextCheckingResultOutboxStore rejects persistence through a context that
// was created before a long processing step: such a context has already used up
// part of resultOutboxPersistTimeout.
type contextCheckingResultOutboxStore struct {
	*resultOutboxTestStore
}

const persistenceContextStaleTolerance = 100 * time.Millisecond

func ensureFreshPersistenceContext(ctx context.Context) error {
	if err := jobtest.EnsureActiveTestContext(ctx); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < resultOutboxPersistTimeout-persistenceContextStaleTolerance {
		return fmt.Errorf("stale persistence context: %s left", time.Until(deadline))
	}
	return nil
}

func (s *contextCheckingResultOutboxStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.WithReadCommittedTransaction(ctx, fn)
}

func (s *contextCheckingResultOutboxStore) Add(ctx context.Context, entity datastore.Entity) error {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Add(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.BatchAdd(ctx, entities)
}

func (s *contextCheckingResultOutboxStore) Put(ctx context.Context, entity datastore.Entity) error {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Put(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) Delete(ctx context.Context, entity datastore.Entity) error {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Delete(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) Get(ctx context.Context, entity datastore.Entity) error {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Get(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return nil, err
	}
	return s.resultOutboxTestStore.List(ctx, query, opts)
}

func (s *contextCheckingResultOutboxStore) CompareAndSwap(ctx context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return false, err
	}
	return s.resultOutboxTestStore.CompareAndSwap(ctx, entity, conditionField, conditionValue, updates)
}

func (s *contextCheckingResultOutboxStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if err := ensureFreshPersistenceContext(ctx); err != nil {
		return false, err
	}
	return s.resultOutboxTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func TestResultOutboxDispatcherClaimsPendingBeforeEnqueue(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.EnqueueCaptureQueue{EnqueueID: "result-1"}
	dispatcher := NewResultOutboxDispatcher(queue, store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-claim",
		ExecutionKey:   "execution-delay-claim",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-claim",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := BuildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	store.rejectNextTransition(config.JobResultOutboxStateResultPending, config.JobResultOutboxStateResultDispatching)

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))
	require.Empty(t, queue.Enqueued)

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, "", refreshed.MessageID)
}

func TestResultOutboxDispatcherPersistsQueuedStateAfterSuccessfulEnqueue(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.EnqueueCaptureQueue{EnqueueID: "result-queued-1"}
	dispatcher := NewResultOutboxDispatcher(queue, store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-queued",
		ExecutionKey:   "execution-delay-queued",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-queued",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := BuildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultQueued, refreshed.State)
	require.Equal(t, "result-queued-1", refreshed.MessageID)
	require.Equal(t, "", refreshed.LastError)
}

func TestResultOutboxDispatcherTransitionsOnlyTargetOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.EnqueueCaptureQueue{EnqueueID: "result-7"}
	dispatcher := NewResultOutboxDispatcher(queue, store)

	targetPayload := &JobResultPayload{
		TaskID:         "task-delay-target",
		ExecutionKey:   "execution-delay-target",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-target",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	siblingPayload := &JobResultPayload{
		TaskID:         "task-delay-sibling",
		ExecutionKey:   "execution-delay-sibling",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-sibling",
		ServiceName:    "svc-b",
		TimeoutSeconds: 60,
	}
	targetOutbox := BuildJobResultOutbox(targetPayload, config.JobResultOutboxStateResultPending)
	siblingOutbox := BuildJobResultOutbox(siblingPayload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), targetOutbox))
	require.NoError(t, store.Add(context.Background(), siblingOutbox))

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), targetOutbox))

	refreshedTarget, err := getJobResultOutboxByID(context.Background(), store, targetOutbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultQueued, refreshedTarget.State)
	require.Equal(t, "result-7", refreshedTarget.MessageID)

	refreshedSibling, err := getJobResultOutboxByID(context.Background(), store, siblingOutbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshedSibling.State)
}

func TestResultOutboxDispatcherReturnsToPendingWhenEnqueueFails(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.EnqueueCaptureQueue{EnqueueErr: errors.New("queue down")}
	dispatcher := NewResultOutboxDispatcher(queue, store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-enqueue-fail",
		ExecutionKey:   "execution-delay-enqueue-fail",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-enqueue-fail",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := BuildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	require.NoError(t, store.Add(context.Background(), &model.JobInfo{
		ID:          3,
		TaskID:      payload.TaskID,
		Type:        payload.JobType,
		ServiceName: payload.ServiceName,
		Status:      string(config.StatusWaiting),
	}))

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, 1, refreshed.Attempts)
	require.Contains(t, refreshed.LastError, "result enqueue failed")
	require.Contains(t, refreshed.LastError, "queue down")
	require.Equal(t, "", refreshed.MessageID)

	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Equal(t, string(config.StatusWaiting), jobInfo.Status)
}

func TestResultOutboxDispatcherLeavesDispatchingWhenPendingRequeueClaimLost(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.EnqueueCaptureQueue{EnqueueErr: errors.New("queue down")}
	dispatcher := NewResultOutboxDispatcher(queue, store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-requeue-cas-lost",
		ExecutionKey:   "execution-delay-requeue-cas-lost",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-requeue-cas-lost",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := BuildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	store.rejectNextTransition(config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultPending)

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultDispatching, refreshed.State)
	require.Equal(t, 0, refreshed.Attempts)
}

func TestResultOutboxDispatcherRecoversExpiredProcessingAcrossBatches(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&jobtest.EnqueueCaptureQueue{}, store)
	dispatcher.batchSize = 2

	for i := 0; i < 3; i++ {
		payload := &JobResultPayload{
			TaskID:         fmt.Sprintf("task-recover-%d", i),
			ExecutionKey:   fmt.Sprintf("execution-recover-%d", i),
			RunGeneration:  1,
			JobType:        string(config.JobDeployScheduled),
			Namespace:      "default",
			Name:           fmt.Sprintf("delay-job-recover-%d", i),
			ServiceName:    "svc-a",
			TimeoutSeconds: 60,
		}
		outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultProcessingQueue)
		expired := time.Now().Add(-time.Minute)
		outbox.LeaseExpiresAt = &expired
		require.NoError(t, store.Add(context.Background(), outbox))
	}

	for i := 0; i < 2; i++ {
		require.NoError(t, dispatcher.recoverResultOutboxes(context.Background(), []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingQueue}))
	}

	pending, err := listJobResultOutboxesByStates(context.Background(), store, []config.JobResultOutboxState{config.JobResultOutboxStateResultPending}, 10)
	require.NoError(t, err)
	require.Len(t, pending, 3)

	processing, err := listJobResultOutboxesByStates(context.Background(), store, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingQueue}, 10)
	require.NoError(t, err)
	require.Empty(t, processing)
}

func TestResultOutboxDispatcherRecoversStaleDispatchingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&jobtest.EnqueueCaptureQueue{}, store)

	payload := &JobResultPayload{
		TaskID:         "task-dispatching-stale",
		ExecutionKey:   "execution-dispatching-stale",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching-stale",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultDispatching)
	outbox.CreateTime = time.Now().Add(-2 * time.Minute)
	outbox.UpdateTime = time.Now().Add(-time.Minute)
	expired := time.Now().Add(-time.Minute)
	outbox.LeaseExpiresAt = &expired
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.recoverResultOutboxes(context.Background(), []config.JobResultOutboxState{config.JobResultOutboxStateResultDispatching}))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, 1, refreshed.Attempts)
	require.Contains(t, refreshed.LastError, "exceeded recovery grace")
}

func TestResultOutboxDispatcherKeepsFreshDispatchingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&jobtest.EnqueueCaptureQueue{}, store)

	payload := &JobResultPayload{
		TaskID:         "task-dispatching-fresh",
		ExecutionKey:   "execution-dispatching-fresh",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching-fresh",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultDispatching)
	deadline := time.Now().Add(time.Minute)
	outbox.LeaseExpiresAt = &deadline
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.recoverResultOutboxes(context.Background(), []config.JobResultOutboxState{config.JobResultOutboxStateResultDispatching}))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultDispatching, refreshed.State)
	require.Equal(t, 0, refreshed.Attempts)
	require.Equal(t, "", refreshed.LastError)
}

func TestResultOutboxDispatcherRecoversStaleQueueProcessingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&jobtest.EnqueueCaptureQueue{}, store)
	dispatcher.PollInterval = 5 * time.Millisecond

	payload := &JobResultPayload{
		TaskID:         "task-queue-processing-stale",
		ExecutionKey:   "execution-queue-processing-stale",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-queue-processing-stale",
		ServiceName:    "svc-a",
		TimeoutSeconds: 1,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultProcessingQueue)
	outbox.CreateTime = time.Now().Add(-2 * time.Minute)
	outbox.UpdateTime = time.Now().Add(-2 * time.Minute)
	expired := time.Now().Add(-time.Minute)
	outbox.LeaseExpiresAt = &expired
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.recoverResultOutboxes(context.Background(), []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingQueue}))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, 1, refreshed.Attempts)
	require.Contains(t, refreshed.LastError, "exceeded recovery grace")
}

func TestResultOutboxDispatcherKeepsFreshQueueProcessingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&jobtest.EnqueueCaptureQueue{}, store)
	dispatcher.PollInterval = 5 * time.Millisecond

	payload := &JobResultPayload{
		TaskID:         "task-queue-processing-fresh",
		ExecutionKey:   "execution-queue-processing-fresh",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-queue-processing-fresh",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultProcessingQueue)
	deadline := time.Now().Add(time.Minute)
	outbox.LeaseExpiresAt = &deadline
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.recoverResultOutboxes(context.Background(), []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingQueue}))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultProcessingQueue, refreshed.State)
	require.Equal(t, 0, refreshed.Attempts)
	require.Equal(t, "", refreshed.LastError)
}

func TestJobResultPayloadFromOutboxRequiresMandatoryFields(t *testing.T) {
	valid := &model.JobResultOutbox{
		ID:            "outbox-valid",
		TaskID:        "task-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 7,
		Namespace:     "default",
		Name:          "job-1",
	}
	payload := jobResultPayloadFromOutbox(valid)
	require.NotNil(t, payload)
	require.Equal(t, "execution-1", payload.ExecutionKey)
	require.Equal(t, uint64(7), payload.RunGeneration)

	missingTask := &model.JobResultOutbox{
		ID:        "outbox-missing-task",
		Namespace: "default",
		Name:      "job-1",
	}
	require.Nil(t, jobResultPayloadFromOutbox(missingTask))

	missingNamespace := &model.JobResultOutbox{
		ID:     "outbox-missing-namespace",
		TaskID: "task-1",
		Name:   "job-1",
	}
	require.Nil(t, jobResultPayloadFromOutbox(missingNamespace))

	missingName := &model.JobResultOutbox{
		ID:        "outbox-missing-name",
		TaskID:    "task-1",
		Namespace: "default",
	}
	require.Nil(t, jobResultPayloadFromOutbox(missingName))
}

func TestJobResultOutboxIDIsScopedByExecutionIdentity(t *testing.T) {
	first := &JobResultPayload{
		TaskID:        "task-1",
		Namespace:     "default",
		Name:          "job-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 1,
	}
	second := *first
	second.ExecutionKey = "execution-2"
	nextGeneration := *first
	nextGeneration.RunGeneration = 2
	incomplete := *first
	incomplete.ExecutionKey = ""

	require.NotEqual(t, jobResultOutboxID(first), jobResultOutboxID(&second))
	require.NotEqual(t, jobResultOutboxID(first), jobResultOutboxID(&nextGeneration))
	require.Empty(t, jobResultOutboxID(&incomplete))
}
