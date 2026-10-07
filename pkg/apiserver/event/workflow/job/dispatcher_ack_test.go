package job

import (
	"context"
	"encoding/json"
	"errors"
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

func testResultExecutionKey(taskID string) string {
	return "execution-" + taskID
}

func stampTestResultJob(jobObj *batchv1.Job, taskID string) {
	if jobObj.UID == "" {
		jobObj.UID = types.UID("uid-" + taskID)
	}
	stampJobExecutionIdentity(&model.JobTask{
		TaskID: taskID, ExecutionKey: testResultExecutionKey(taskID), RunGeneration: 1,
	}, jobObj)
}

func resultTestPod(job *batchv1.Job) *corev1.Pod {
	pod := succeededPodForJob(job, job.Name+"-pod", job.UID)
	pod.Spec.Containers = []corev1.Container{{Name: "main"}}
	return pod
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

func TestDelayDispatcherAcksUnsupportedNotificationVersions(t *testing.T) {
	for _, raw := range []string{
		`{"version":3,"taskId":"task-1","executionKey":"key-1","runGeneration":1}`,
		`{"version":2,"taskId":"task-1","executionKey":"key-1","runGeneration":1,"job":null}`,
		`{"version":2,"taskId":"task-1","executionKey":"key-1"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			queue := &dispatcherAckQueue{}
			dispatcher := &DelayDispatcher{queue: queue, group: "delay-workers"}
			dispatcher.handleMessage(context.Background(), msg.Message{ID: "unsupported", Payload: []byte(raw)})
			require.Len(t, queue.ackCalls, 1)
			require.Equal(t, []string{"unsupported"}, queue.ackCalls[0].ids)
		})
	}
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

func TestResultDispatcherRefreshesPersistenceContextAfterLongProcessing(t *testing.T) {
	oldTimeout := resultOutboxPersistTimeout
	resultOutboxPersistTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		resultOutboxPersistTimeout = oldTimeout
	})

	store := &contextCheckingResultOutboxStore{resultOutboxTestStore: newResultOutboxTestStore()}
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
	client := fake.NewSimpleClientset(jobObj, resultTestPod(jobObj))
	client.Fake.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		time.Sleep(25 * time.Millisecond)
		return false, nil, nil
	})
	dispatcher := &ResultDispatcher{
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
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	require.NoError(t, store.Add(context.Background(), testResultJobInfo(13, payload)))

	require.NoError(t, processPendingTestResult(t, context.Background(), dispatcher, outbox.ID))

	_, getErr := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.ErrorIs(t, getErr, datastore.ErrRecordNotExist)
	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Equal(t, string(config.StatusCompleted), jobInfo.Status)
}
