package jobresult

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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type dispatchingClaimRaceStore struct {
	*resultOutboxTestStore
	raceOutboxID  string
	raceMessageID string
	triggered     bool
}

func stampTestResultJob(jobObj *batchv1.Job, taskID string) {
	if jobObj.UID == "" {
		jobObj.UID = types.UID("uid-" + taskID)
	}
	StampJobExecutionIdentity(&model.JobTask{
		TaskID: taskID, ExecutionKey: jobtest.TestResultExecutionKey(taskID), RunGeneration: 1,
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

func TestResultDispatcherHandleMessageAcksInvalidPayload(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		Group: "result-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-1",
		Payload: []byte(`{"taskId":`),
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, "result-workers", queue.AckCalls[0].Group)
	require.Equal(t, []string{"result-1"}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageAcksEmptyPayload(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		Group: "result-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-empty",
		Payload: nil,
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, "result-workers", queue.AckCalls[0].Group)
	require.Equal(t, []string{"result-empty"}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageAcksMissingRequiredFields(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		Group: "result-workers",
	}
	raw, err := json.Marshal(&JobResultPayload{Name: "job-only"})
	require.NoError(t, err)

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-2",
		Payload: raw,
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"result-2"}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageAcksNoRetryProcessError(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue: queue,
		Group: "result-workers",
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

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"result-3"}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageAckFailureDoesNotPanic(t *testing.T) {
	queue := &jobtest.DispatcherAckQueue{AckErr: errors.New("ack failed")}
	dispatcher := &ResultDispatcher{
		queue: queue,
		Group: "result-workers",
	}

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-4",
		Payload: []byte(`{"taskId":`),
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"result-4"}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageAcksStaleOutboxDuplicate(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.DispatcherAckQueue{}
	dispatcher := &ResultDispatcher{
		queue:  queue,
		Group:  "result-workers",
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
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "result-1"
	require.NoError(t, store.Add(context.Background(), outbox))

	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)

	dispatcher.handleMessage(context.Background(), msg.Message{
		ID:      "result-2",
		Payload: raw,
	})

	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"result-2"}, queue.AckCalls[0].Ids)

	refreshed, getErr := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, getErr)
	require.Equal(t, config.JobResultOutboxStateResultQueued, refreshed.State)
	require.Equal(t, "result-1", refreshed.MessageID)
}

func TestResultDispatcherHandleMessageLeavesActiveProcessingQueueMessage(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.DispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-redelivery",
			Namespace: "default",
			Labels: map[string]string{
				spec.LabelComponentName: "svc-a",
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
		Group:  "result-workers",
		client: fake.NewSimpleClientset(jobObj, jobtest.ResultTestPod(jobObj)),
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-redelivery",
		ExecutionKey:   jobtest.TestResultExecutionKey("task-outbox-redelivery"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-redelivery",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultProcessingQueue)
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
	require.NoError(t, getErr)

	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Empty(t, jobInfo.Status)
	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"result-5"}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageProcessesDispatchingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &jobtest.DispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-dispatching",
			Namespace: "default",
			Labels: map[string]string{
				spec.LabelComponentName: "svc-a",
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
		Group:  "result-workers",
		client: fake.NewSimpleClientset(jobObj, jobtest.ResultTestPod(jobObj)),
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-dispatching",
		ExecutionKey:   jobtest.TestResultExecutionKey("task-outbox-dispatching"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultDispatching)
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
	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"result-dispatching"}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageDispatchingClaimLostToQueuedContinuesProcessing(t *testing.T) {
	baseStore := newResultOutboxTestStore()
	store := &dispatchingClaimRaceStore{resultOutboxTestStore: baseStore}
	queue := &jobtest.DispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-dispatching-race",
			Namespace: "default",
			Labels: map[string]string{
				spec.LabelComponentName: "svc-a",
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
		Group:  "result-workers",
		client: fake.NewSimpleClientset(jobObj, jobtest.ResultTestPod(jobObj)),
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-dispatching-race",
		ExecutionKey:   jobtest.TestResultExecutionKey("task-outbox-dispatching-race"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching-race",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultDispatching)
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
	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{store.raceMessageID}, queue.AckCalls[0].Ids)
}

func TestResultDispatcherHandleMessageRefreshesPersistenceContextAfterLongProcessing(t *testing.T) {
	oldTimeout := resultOutboxPersistTimeout
	resultOutboxPersistTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		resultOutboxPersistTimeout = oldTimeout
	})

	store := &contextCheckingResultOutboxStore{resultOutboxTestStore: newResultOutboxTestStore()}
	queue := &jobtest.DispatcherAckQueue{}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now().Add(-time.Second))
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-persist-refresh",
			Namespace: "default",
			Labels: map[string]string{
				spec.LabelComponentName: "svc-a",
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
	client := fake.NewSimpleClientset(jobObj, jobtest.ResultTestPod(jobObj))
	client.Fake.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		time.Sleep(25 * time.Millisecond)
		return false, nil, nil
	})
	dispatcher := &ResultDispatcher{
		queue:  queue,
		Group:  "result-workers",
		client: client,
		store:  store,
	}

	payload := &JobResultPayload{
		TaskID:         "task-outbox-persist-refresh",
		ExecutionKey:   jobtest.TestResultExecutionKey("task-outbox-persist-refresh"),
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-persist-refresh",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
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
	require.Len(t, queue.AckCalls, 1)
	require.Equal(t, []string{"result-persist-refresh"}, queue.AckCalls[0].Ids)
}
