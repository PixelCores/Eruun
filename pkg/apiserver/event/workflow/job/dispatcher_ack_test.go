package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type dispatcherAckCall struct {
	group string
	ids   []string
}

type dispatcherAckQueue struct {
	ensureGroupErr error
	ackErr         error
	ackCalls       []dispatcherAckCall
}

type dispatchingClaimRaceStore struct {
	*resultOutboxTestStore
	raceOutboxID  string
	raceMessageID string
	triggered     bool
}

func testResultExecutionKey(taskID string) string {
	return "execution-" + taskID
}

func stampTestResultJob(jobObj *batchv1.Job, taskID string) {
	stampJobExecutionIdentity(&model.JobTask{
		TaskID: taskID, ExecutionKey: testResultExecutionKey(taskID), RunGeneration: 1,
	}, jobObj)
}

func testResultJobInfo(id int, payload *JobResultPayload) *model.JobInfo {
	executionKey := payload.ExecutionKey
	return &model.JobInfo{
		ID:            id,
		TaskID:        payload.TaskID,
		Type:          payload.JobType,
		ServiceName:   payload.ServiceName,
		ExecutionKey:  &executionKey,
		RunGeneration: payload.RunGeneration,
	}
}

func (q *dispatcherAckQueue) EnsureGroup(context.Context, string) error { return q.ensureGroupErr }
func (q *dispatcherAckQueue) Enqueue(context.Context, []byte) (string, error) {
	return "", nil
}
func (q *dispatcherAckQueue) ReadGroup(context.Context, string, string, int, time.Duration) ([]msg.Message, error) {
	return nil, nil
}
func (q *dispatcherAckQueue) Ack(_ context.Context, group string, ids ...string) error {
	q.ackCalls = append(q.ackCalls, dispatcherAckCall{
		group: group,
		ids:   append([]string(nil), ids...),
	})
	return q.ackErr
}
func (q *dispatcherAckQueue) AutoClaim(context.Context, string, string, time.Duration, int) ([]msg.Message, error) {
	return nil, nil
}
func (q *dispatcherAckQueue) Close(context.Context) error                         { return nil }
func (q *dispatcherAckQueue) Stats(context.Context, string) (int64, int64, error) { return 0, 0, nil }

func (s *dispatchingClaimRaceStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	outbox, ok := entity.(*model.JobResultOutbox)
	if ok && outbox != nil &&
		outbox.ID == strings.TrimSpace(s.raceOutboxID) &&
		strings.TrimSpace(fmt.Sprint(conditions["state"])) == string(config.JobResultOutboxStateResultDispatching) &&
		updates["state"] == config.JobResultOutboxStateResultProcessingQueue &&
		!s.triggered {
		s.triggered = true
		s.mu.Lock()
		if current, exists := s.outboxes[outbox.ID]; exists {
			current.State = config.JobResultOutboxStateResultQueued
			current.MessageID = strings.TrimSpace(s.raceMessageID)
			current.LastError = ""
			current.UpdateTime = time.Now()
		}
		s.mu.Unlock()
		return false, nil
	}
	return s.resultOutboxTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func TestDelayDispatcherHandleMessageAcksInvalidPayload(t *testing.T) {
	queue := &dispatcherAckQueue{}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "delay-1",
		Payload: []byte(`{"job":`),
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, "delay-workers", queue.ackCalls[0].group)
	require.Equal(t, []string{"delay-1"}, queue.ackCalls[0].ids)
}

func TestDelayDispatcherHandleMessageAcksEmptyPayload(t *testing.T) {
	queue := &dispatcherAckQueue{}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "delay-empty",
		Payload: nil,
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, "delay-workers", queue.ackCalls[0].group)
	require.Equal(t, []string{"delay-empty"}, queue.ackCalls[0].ids)
}

func TestDelayDispatcherHandleMessageAcksMissingJob(t *testing.T) {
	queue := &dispatcherAckQueue{}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}
	raw, err := json.Marshal(&DelayJobPayload{})
	require.NoError(t, err)

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "delay-2",
		Payload: raw,
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"delay-2"}, queue.ackCalls[0].ids)
}

func TestDelayDispatcherFinishRequeuesWhenAckFails(t *testing.T) {
	queue := &dispatcherAckQueue{ackErr: errors.New("ack failed")}
	dispatcher := &DelayDispatcher{
		queue:      queue,
		group:      "delay-workers",
		backoffMin: time.Second,
		backoffMax: 8 * time.Second,
		pending: map[string]struct{}{
			"delay-3": {},
		},
		wake: make(chan struct{}, 1),
	}
	item := &delayItem{
		msgID:     "delay-3",
		executeAt: time.Now().Unix(),
	}

	dispatcher.finish(context.Background(), item)

	require.Len(t, queue.ackCalls, 1)
	require.Len(t, dispatcher.items, 1)
	require.Equal(t, "delay-3", dispatcher.items[0].msgID)
	_, stillPending := dispatcher.pending["delay-3"]
	require.True(t, stillPending)
}

func TestResultDispatcherHandleMessageAcksInvalidPayload(t *testing.T) {
	queue := &dispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		group: "result-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-1",
		Payload: []byte(`{"taskId":`),
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, "result-workers", queue.ackCalls[0].group)
	require.Equal(t, []string{"result-1"}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageAcksEmptyPayload(t *testing.T) {
	queue := &dispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		group: "result-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-empty",
		Payload: nil,
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, "result-workers", queue.ackCalls[0].group)
	require.Equal(t, []string{"result-empty"}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageAcksMissingRequiredFields(t *testing.T) {
	queue := &dispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		group: "result-workers",
	}
	raw, err := json.Marshal(&JobResultPayload{Name: "job-only"})
	require.NoError(t, err)

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-2",
		Payload: raw,
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"result-2"}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageAcksNoRetryProcessError(t *testing.T) {
	queue := &dispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		group: "result-workers",
	}
	raw, err := json.Marshal(&JobResultPayload{
		TaskID: "task-1",
		Name:   "job-1",
	})
	require.NoError(t, err)

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-3",
		Payload: raw,
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"result-3"}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageAckFailureDoesNotPanic(t *testing.T) {
	queue := &dispatcherAckQueue{ackErr: errors.New("ack failed")}
	dispatcher := &ResultDispatcher{
		queue: queue,
		group: "result-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-4",
		Payload: []byte(`{"taskId":`),
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"result-4"}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageAcksStaleOutboxDuplicate(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &dispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue:  queue,
		group:  "result-workers",
		client: fake.NewSimpleClientset(),
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-dup",
		ExecutionKey:   "execution-outbox-dup",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dup",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "result-1"
	require.NoError(t, store.Add(context.Background(), outbox))

	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-2",
		Payload: raw,
	})

	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"result-2"}, queue.ackCalls[0].ids)

	refreshed, getErr := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, getErr)
	require.Equal(t, config.JobResultOutboxStateResultQueued, refreshed.State)
	require.Equal(t, "result-1", refreshed.MessageID)
}

func TestResultDispatcherHandleMessageResumesProcessingQueueMessage(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &dispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-redelivery",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelComponentName: "svc-a",
			},
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &end,
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	stampTestResultJob(jobObj, "task-outbox-redelivery")
	dispatcher := &ResultDispatcher{
		queue:  queue,
		group:  "result-workers",
		client: fake.NewSimpleClientset(jobObj),
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-redelivery",
		ExecutionKey:   testResultExecutionKey("task-outbox-redelivery"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-redelivery",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultProcessingQueue)
	outbox.MessageID = "result-5"
	require.NoError(t, store.Add(context.Background(), outbox))
	require.NoError(t, store.Add(context.Background(), testResultJobInfo(10, payload)))

	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)

	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-5",
		Payload: raw,
	}))

	_, getErr := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.ErrorIs(t, getErr, datastore.ErrRecordNotExist)

	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Equal(t, string(config.StatusCompleted), jobInfo.Status)
	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"result-5"}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageProcessesDispatchingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &dispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-dispatching",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelComponentName: "svc-a",
			},
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &end,
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	stampTestResultJob(jobObj, "task-outbox-dispatching")
	dispatcher := &ResultDispatcher{
		queue:  queue,
		group:  "result-workers",
		client: fake.NewSimpleClientset(jobObj),
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-dispatching",
		ExecutionKey:   testResultExecutionKey("task-outbox-dispatching"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultDispatching)
	require.NoError(t, store.Add(context.Background(), outbox))
	require.NoError(t, store.Add(context.Background(), testResultJobInfo(11, payload)))

	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)

	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-dispatching",
		Payload: raw,
	}))

	_, getErr := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.ErrorIs(t, getErr, datastore.ErrRecordNotExist)
	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Equal(t, string(config.StatusCompleted), jobInfo.Status)
	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"result-dispatching"}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageDispatchingClaimLostToQueuedContinuesProcessing(t *testing.T) {
	baseStore := newResultOutboxTestStore()
	store := &dispatchingClaimRaceStore{resultOutboxTestStore: baseStore}
	queue := &dispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-dispatching-race",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelComponentName: "svc-a",
			},
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &end,
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	stampTestResultJob(jobObj, "task-outbox-dispatching-race")
	dispatcher := &ResultDispatcher{
		queue:  queue,
		group:  "result-workers",
		client: fake.NewSimpleClientset(jobObj),
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-dispatching-race",
		ExecutionKey:   testResultExecutionKey("task-outbox-dispatching-race"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching-race",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultDispatching)
	store.raceOutboxID = outbox.ID
	store.raceMessageID = "result-dispatching-race"
	require.NoError(t, store.Add(context.Background(), outbox))
	require.NoError(t, store.Add(context.Background(), testResultJobInfo(13, payload)))

	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)

	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      store.raceMessageID,
		Payload: raw,
	}))

	require.True(t, store.triggered, "test race path should be triggered")
	_, getErr := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.ErrorIs(t, getErr, datastore.ErrRecordNotExist)
	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Equal(t, string(config.StatusCompleted), jobInfo.Status)
	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{store.raceMessageID}, queue.ackCalls[0].ids)
}

func TestResultDispatcherHandleMessageRefreshesPersistenceContextAfterLongProcessing(t *testing.T) {
	oldTimeout := resultOutboxPersistTimeout
	resultOutboxPersistTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		resultOutboxPersistTimeout = oldTimeout
	})

	store := &contextCheckingResultOutboxStore{resultOutboxTestStore: newResultOutboxTestStore()}
	queue := &dispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-persist-refresh",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelComponentName: "svc-a",
			},
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &end,
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	stampTestResultJob(jobObj, "task-outbox-persist-refresh")
	client := fake.NewSimpleClientset(jobObj)
	client.Fake.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		time.Sleep(25 * time.Millisecond)
		return false, nil, nil
	})
	dispatcher := &ResultDispatcher{
		queue:  queue,
		group:  "result-workers",
		client: client,
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-persist-refresh",
		ExecutionKey:   testResultExecutionKey("task-outbox-persist-refresh"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-persist-refresh",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "result-persist-refresh"
	require.NoError(t, store.Add(context.Background(), outbox))
	require.NoError(t, store.Add(context.Background(), testResultJobInfo(13, payload)))

	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)

	require.True(t, dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-persist-refresh",
		Payload: raw,
	}))

	_, getErr := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.ErrorIs(t, getErr, datastore.ErrRecordNotExist)
	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Equal(t, string(config.StatusCompleted), jobInfo.Status)
	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"result-persist-refresh"}, queue.ackCalls[0].ids)
}
