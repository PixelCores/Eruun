package job

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type blockingDispatcherQueue struct {
	readStarted      chan struct{}
	autoClaimStarted chan struct{}
	readOnce         sync.Once
	autoClaimOnce    sync.Once
}

type resultConcurrencyQueue struct {
	dispatcherAckQueue
	acked chan string
}

func (q *resultConcurrencyQueue) Ack(_ context.Context, _ string, ids ...string) error {
	for _, id := range ids {
		q.acked <- id
	}
	return nil
}

func newBlockingDispatcherQueue() *blockingDispatcherQueue {
	return &blockingDispatcherQueue{
		readStarted:      make(chan struct{}),
		autoClaimStarted: make(chan struct{}),
	}
}

func (q *blockingDispatcherQueue) EnsureGroup(context.Context, string) error { return nil }
func (q *blockingDispatcherQueue) Enqueue(context.Context, []byte) (string, error) {
	return "", nil
}
func (q *blockingDispatcherQueue) ReadGroup(ctx context.Context, _ string, _ string, _ int, _ time.Duration) ([]msg.Message, error) {
	q.readOnce.Do(func() { close(q.readStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}
func (q *blockingDispatcherQueue) Ack(context.Context, string, ...string) error { return nil }
func (q *blockingDispatcherQueue) AutoClaim(ctx context.Context, _ string, _ string, _ time.Duration, _ int) ([]msg.Message, error) {
	q.autoClaimOnce.Do(func() { close(q.autoClaimStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}
func (q *blockingDispatcherQueue) Close(context.Context) error { return nil }
func (q *blockingDispatcherQueue) Stats(context.Context, string) (int64, int64, error) {
	return 0, 0, nil
}

func TestDelayDispatcherHelperBranches(t *testing.T) {
	dispatcher := &DelayDispatcher{
		backoffMin: time.Second,
		backoffMax: 8 * time.Second,
		pending:    make(map[string]struct{}),
		wake:       make(chan struct{}, 1),
	}

	require.Equal(t, 2*time.Second, dispatcher.backoffDelay(0))
	require.Equal(t, 8*time.Second, dispatcher.backoffDelay(8*time.Second))

	require.Equal(t, time.Second, dispatcher.retryDelay(0))
	require.Equal(t, time.Second, dispatcher.retryDelay(1))
	require.Equal(t, 2*time.Second, dispatcher.retryDelay(2))
	require.Equal(t, 8*time.Second, dispatcher.retryDelay(10))

	require.True(t, dispatcher.addPending(&delayItem{msgID: "b", executeAt: 20}))
	require.True(t, dispatcher.addPending(&delayItem{msgID: "a", executeAt: 10}))
	require.False(t, dispatcher.addPending(&delayItem{msgID: "a", executeAt: 30}))
	require.Len(t, dispatcher.items, 2)
	require.Equal(t, "a", dispatcher.items[0].msgID)

	item, wait := dispatcher.nextItem()
	require.NotNil(t, item)
	require.Equal(t, "a", item.msgID)
	require.GreaterOrEqual(t, wait, time.Duration(0))

}

func TestDelayDispatcherRequeueDoesNotNotify(t *testing.T) {
	dispatcher := &DelayDispatcher{wake: make(chan struct{}, 1)}
	dispatcher.requeue(&delayItem{msgID: "delay-retry", executeAt: time.Now().Unix()})

	require.Len(t, dispatcher.items, 1)
	select {
	case <-dispatcher.wake:
		t.Fatal("requeue must not wake the scheduling loop itself")
	default:
	}
}

func TestDelayDispatcherAckAndDecodePayload(t *testing.T) {
	var nilDispatcher *DelayDispatcher
	require.NoError(t, nilDispatcher.ackMessage(context.Background(), "id-1", "reason", true))

	queue := &dispatcherAckQueue{ackErr: errors.New("ack failed")}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}
	require.Error(t, dispatcher.ackMessage(context.Background(), "id-1", "reason", true))
	require.EqualValues(t, 1, dispatcher.ackFailures.Load())

	_, err := dispatcher.decodePayload([]byte(`{"job":`))
	require.Error(t, err)
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

	queue := &dispatcherAckQueue{ackErr: errors.New("ack failed")}
	dispatcher.queue = queue
	dispatcher.group = "result-workers"
	require.Error(t, dispatcher.ackMessage(context.Background(), "id-1", "reason"))
	require.EqualValues(t, 1, dispatcher.ackFailures.Load())
}

func TestDispatcherConstructorsAndStartGuards(t *testing.T) {
	delay := NewDelayDispatcher(nil, nil, nil, "", "")
	require.NotNil(t, delay)
	require.Equal(t, "", delay.group)
	require.Equal(t, "", delay.consumer)

	delay.Start(context.Background())

	ctxDelay, cancelDelay := context.WithCancel(context.Background())
	delay = NewDelayDispatcher(&dispatcherAckQueue{}, fake.NewSimpleClientset(), &noopStore{}, "", "")
	delay.readBlock = 5 * time.Millisecond
	delay.autoClaimInterval = 5 * time.Millisecond
	delay.Start(ctxDelay)
	time.Sleep(10 * time.Millisecond)
	cancelDelay()
	require.Equal(t, config.DelayQueueGroup, delay.group)
	require.Equal(t, "delay-dispatcher", delay.consumer)
	require.EqualValues(t, 0, delay.ensureFailures.Load())

	result := NewResultDispatcher(nil, nil, nil, "", "")
	require.NotNil(t, result)
	require.Equal(t, "", result.group)
	require.Equal(t, "", result.consumer)

	result.Start(context.Background())

	ctxResult, cancelResult := context.WithCancel(context.Background())
	result = NewResultDispatcher(&dispatcherAckQueue{}, fake.NewSimpleClientset(), &noopStore{}, "", "")
	result.readBlock = 5 * time.Millisecond
	result.autoClaimInterval = 5 * time.Millisecond
	result.Start(ctxResult)
	time.Sleep(10 * time.Millisecond)
	cancelResult()
	require.Equal(t, config.ResultQueueGroup, result.group)
	require.Equal(t, "result-dispatcher", result.consumer)
	require.EqualValues(t, 0, result.ensureFailures.Load())

	ctxOutbox, cancelOutbox := context.WithCancel(context.Background())
	outbox := NewResultOutboxDispatcher(&dispatcherAckQueue{}, fake.NewSimpleClientset(), &noopStore{})
	outbox.pollInterval = time.Hour
	startDone := make(chan struct{})
	go func() {
		outbox.Start(ctxOutbox)
		close(startDone)
	}()
	select {
	case <-startDone:
	case <-time.After(100 * time.Millisecond):
		cancelOutbox()
		t.Fatal("result outbox Start blocked")
	}
	cancelOutbox()
}

func TestDispatcherStartCountsEnsureGroupFailures(t *testing.T) {
	ctxDelay, cancelDelay := context.WithCancel(context.Background())
	defer cancelDelay()
	delay := NewDelayDispatcher(&dispatcherAckQueue{ensureGroupErr: errors.New("ensure delay failed")}, fake.NewSimpleClientset(), &noopStore{}, "", "")
	delay.readBlock = time.Millisecond
	delay.autoClaimInterval = time.Millisecond
	delay.Start(ctxDelay)
	cancelDelay()
	time.Sleep(5 * time.Millisecond)
	require.EqualValues(t, 1, delay.ensureFailures.Load())

	ctxResult, cancelResult := context.WithCancel(context.Background())
	defer cancelResult()
	result := NewResultDispatcher(&dispatcherAckQueue{ensureGroupErr: errors.New("ensure result failed")}, fake.NewSimpleClientset(), &noopStore{}, "", "")
	result.readBlock = time.Millisecond
	result.autoClaimInterval = time.Millisecond
	result.Start(ctxResult)
	cancelResult()
	time.Sleep(5 * time.Millisecond)
	require.EqualValues(t, 1, result.ensureFailures.Load())
}

func TestDispatchersRunBlocksUntilContextCancelled(t *testing.T) {
	t.Run("delay", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := newBlockingDispatcherQueue()
		dispatcher := NewDelayDispatcher(queue, fake.NewSimpleClientset(), &noopStore{}, "", "")
		dispatcher.autoClaimInterval = time.Millisecond
		done := make(chan struct{})
		go func() {
			dispatcher.Run(ctx)
			close(done)
		}()
		requireClosed(t, queue.readStarted)
		requireClosed(t, queue.autoClaimStarted)
		requireStillRunning(t, done)
		cancel()
		requireClosed(t, done)
	})

	t.Run("result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := newBlockingDispatcherQueue()
		dispatcher := NewResultDispatcher(queue, fake.NewSimpleClientset(), &noopStore{}, "", "")
		dispatcher.autoClaimInterval = time.Millisecond
		done := make(chan struct{})
		go func() {
			dispatcher.Run(ctx)
			close(done)
		}()
		requireClosed(t, queue.readStarted)
		requireClosed(t, queue.autoClaimStarted)
		requireStillRunning(t, done)
		cancel()
		requireClosed(t, done)
	})

	t.Run("result_outbox", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		dispatcher := NewResultOutboxDispatcher(&dispatcherAckQueue{}, fake.NewSimpleClientset(), &noopStore{})
		dispatcher.pollInterval = time.Hour
		done := make(chan struct{})
		go func() {
			dispatcher.Run(ctx)
			close(done)
		}()
		time.Sleep(20 * time.Millisecond)
		requireStillRunning(t, done)
		cancel()
		requireClosed(t, done)
	})
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
	dispatcher := NewResultDispatcher(queue, client, &noopStore{}, "result-workers", "result-consumer")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var processingWG sync.WaitGroup
	slots := make(chan struct{}, 2)

	dispatcher.dispatchMessages(ctx, []msg.Message{{
		ID: "slow",
		Payload: []byte(
			`{"taskId":"task-slow","namespace":"default","name":"slow-job","executionKey":"execution-slow","runGeneration":1,"timeoutSeconds":60}`,
		),
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
	dispatcher := NewResultDispatcher(queue, client, &noopStore{}, "result-workers", "result-consumer")
	ctx, cancel := context.WithCancel(context.Background())
	var processingWG sync.WaitGroup
	slots := make(chan struct{}, 2)
	message := msg.Message{
		ID:      "same-message",
		Payload: []byte(`{"taskId":"task-slow","namespace":"default","name":"slow-job","executionKey":"execution-slow","runGeneration":1,"timeoutSeconds":60}`),
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
	requireClosed(t, done)

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

func requireStillRunning(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("expected dispatcher to keep running")
	default:
	}
}

func requireClosed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestProcessJobResultEarlyBranches(t *testing.T) {
	ctx := context.Background()

	require.ErrorIs(t, processJobResult(ctx, nil, nil, nil), errResultDispatchNoRetry)
	require.ErrorIs(t, processJobResult(ctx, nil, nil, &JobResultPayload{}), errResultDispatchNoRetry)
	require.ErrorIs(t, processJobResult(ctx, nil, nil, &JobResultPayload{Name: "job", TaskID: "task"}), errResultDispatchNoRetry)
	require.ErrorIs(t, processJobResult(ctx, fake.NewSimpleClientset(), &noopStore{}, &JobResultPayload{Name: "job", TaskID: "task"}), errResultDispatchNoRetry)
}
