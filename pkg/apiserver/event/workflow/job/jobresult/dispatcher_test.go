package jobresult

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type resultConcurrencyQueue struct {
	jobtest.DispatcherAckQueue
	acked chan string
}

func (q *resultConcurrencyQueue) Ack(_ context.Context, _ string, ids ...string) error {
	for _, id := range ids {
		q.acked <- id
	}
	return nil
}

func TestResultDispatcherHelperBranches(t *testing.T) {
	dispatcher := &ResultDispatcher{
		backoffMin: time.Second,
		backoffMax: 4 * time.Second,
	}

	require.Equal(t, 2*time.Second, dispatcher.backoffDelay(0))
	require.Equal(t, 4*time.Second, dispatcher.backoffDelay(4*time.Second))

	var nilDispatcher *ResultDispatcher
	require.NoError(t, nilDispatcher.ackMessage(context.Background(), "id-1", "reason"))

	queue := &jobtest.DispatcherAckQueue{AckErr: errors.New("ack failed")}
	dispatcher.queue = queue
	dispatcher.Group = "result-workers"
	require.Error(t, dispatcher.ackMessage(context.Background(), "id-1", "reason"))
	require.EqualValues(t, 1, dispatcher.ackFailures.Load())
}

func TestResultDispatcherReleasesUndispatchedMessagesOnStop(t *testing.T) {
	dispatcher := NewResultDispatcher(&jobtest.DispatcherAckQueue{}, nil, nil, "group", "consumer")
	require.True(t, dispatcher.markMessageInFlight("result-1"))
	require.True(t, dispatcher.markMessageInFlight("result-2"))

	dispatcher.releaseInFlightMessages()

	require.Empty(t, dispatcher.inFlight)
}

func TestResultDispatcherProcessesMessagesConcurrently(t *testing.T) {
	queue := &resultConcurrencyQueue{acked: make(chan string, 2)}
	client := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "slow-job",
			Namespace: "default",
			Annotations: map[string]string{
				config.AnnotationJobTaskID:        "task-slow",
				config.AnnotationJobExecutionKey:  "execution-slow",
				config.AnnotationJobRunGeneration: "1",
			},
		},
	})
	store := newResultOutboxTestStore()
	payload := &JobResultPayload{TaskID: "task-slow", Namespace: "default", Name: "slow-job", ExecutionKey: "execution-slow", RunGeneration: 1, TimeoutSeconds: 60}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "slow"
	require.NoError(t, store.Add(context.Background(), outbox))
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	dispatcher := NewResultDispatcher(queue, client, store, "result-workers", "result-consumer")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var processingWG sync.WaitGroup
	slots := make(chan struct{}, 2)

	dispatcher.dispatchMessages(ctx, []msg.Message{{
		ID:      "slow",
		Payload: raw,
	}}, slots, &processingWG)
	require.Eventually(t, func() bool {
		getJobActions := 0
		for _, action := range client.Actions() {
			if action.GetVerb() == "get" && action.GetResource().Resource == "jobs" {
				getJobActions++
			}
		}
		return getJobActions >= 2
	}, time.Second, time.Millisecond, "slow result handler did not enter its Job wait")

	dispatcher.dispatchMessages(ctx, []msg.Message{{
		ID:      "quick",
		Payload: []byte(`{"taskId":`),
	}}, slots, &processingWG)

	select {
	case id := <-queue.acked:
		require.Equal(t, "quick", id, "a waiting Job result must not block another message")
	case <-time.After(250 * time.Millisecond):
		t.Fatal("quick result message was blocked behind a waiting Job")
	}

	cancel()
	done := make(chan struct{})
	go func() {
		processingWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("result handlers did not stop after context cancellation")
	}
}

func TestResultDispatcherSkipsDuplicateInFlightMessage(t *testing.T) {
	queue := &resultConcurrencyQueue{acked: make(chan string, 2)}
	client := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "slow-job",
			Namespace: "default",
			Annotations: map[string]string{
				config.AnnotationJobTaskID:        "task-slow",
				config.AnnotationJobExecutionKey:  "execution-slow",
				config.AnnotationJobRunGeneration: "1",
			},
		},
	})
	store := newResultOutboxTestStore()
	payload := &JobResultPayload{TaskID: "task-slow", Namespace: "default", Name: "slow-job", ExecutionKey: "execution-slow", RunGeneration: 1, TimeoutSeconds: 60}
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "same-message"
	require.NoError(t, store.Add(context.Background(), outbox))
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	dispatcher := NewResultDispatcher(queue, client, store, "result-workers", "result-consumer")
	ctx, cancel := context.WithCancel(context.Background())
	var processingWG sync.WaitGroup
	slots := make(chan struct{}, 2)
	message := msg.Message{
		ID:      "same-message",
		Payload: raw,
	}

	dispatcher.dispatchMessages(ctx, []msg.Message{message}, slots, &processingWG)
	require.Eventually(t, func() bool { return len(slots) == 1 }, time.Second, 10*time.Millisecond)
	dispatcher.dispatchMessages(ctx, []msg.Message{{
		ID:      message.ID,
		Payload: []byte(`{"taskId":`),
	}}, slots, &processingWG)

	require.Equal(t, 1, len(slots), "a duplicate message ID must not consume another processing slot")
	select {
	case id := <-queue.acked:
		t.Fatalf("duplicate in-flight message was processed and acked: %s", id)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	done := make(chan struct{})
	go func() {
		processingWG.Wait()
		close(done)
	}()
	jobtest.RequireClosed(t, done)

	retryCtx, retryCancel := context.WithCancel(context.Background())
	defer retryCancel()
	dispatcher.dispatchMessages(retryCtx, []msg.Message{{
		ID:      message.ID,
		Payload: []byte(`{"taskId":`),
	}}, slots, &processingWG)
	select {
	case id := <-queue.acked:
		require.Equal(t, message.ID, id, "the message ID must be released after handling finishes")
	case <-time.After(time.Second):
		t.Fatal("released message ID could not be processed again")
	}
	processingWG.Wait()
}

func TestProcessJobResultEarlyBranches(t *testing.T) {
	ctx := context.Background()

	require.ErrorIs(t, processJobResultWithOutbox(ctx, nil, nil, nil, nil), ErrResultDispatchNoRetry)
	require.ErrorIs(t, processJobResultWithOutbox(ctx, nil, nil, &JobResultPayload{}, nil), ErrResultDispatchNoRetry)
	require.ErrorIs(t, processJobResultWithOutbox(ctx, nil, nil, &JobResultPayload{Name: "job", TaskID: "task"}, nil), ErrResultDispatchNoRetry)
	require.ErrorIs(t, processJobResultWithOutbox(ctx, fake.NewSimpleClientset(), &jobtest.NoopStore{}, &JobResultPayload{Name: "job", TaskID: "task"}, nil), ErrResultDispatchNoRetry)
}
