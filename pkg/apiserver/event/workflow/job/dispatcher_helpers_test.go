package job

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type blockingDispatcherQueue struct {
	readStarted      chan struct{}
	ensureGroupErr   error
	autoClaimStarted chan struct{}
	readOnce         sync.Once
	autoClaimOnce    sync.Once
	doneMu           sync.Mutex
	done             []string
}

type lifecycleDispatcherQueue struct {
	dispatcherAckQueue
	done []string
}

func (q *lifecycleDispatcherQueue) MarkMessageHandlingStart(string) {}
func (q *lifecycleDispatcherQueue) MarkMessageHandlingDone(id string, acked bool) {
	if !acked {
		q.done = append(q.done, id)
	}
}

func newBlockingDispatcherQueue() *blockingDispatcherQueue {
	return &blockingDispatcherQueue{
		readStarted:      make(chan struct{}),
		autoClaimStarted: make(chan struct{}),
	}
}

func (q *blockingDispatcherQueue) EnsureGroup(context.Context, string) error { return q.ensureGroupErr }
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
func (q *blockingDispatcherQueue) MarkMessageHandlingStart(string) {}
func (q *blockingDispatcherQueue) MarkMessageHandlingDone(id string, acked bool) {
	if acked {
		return
	}
	q.doneMu.Lock()
	q.done = append(q.done, id)
	q.doneMu.Unlock()
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

func TestDelayDispatcherReleasesPendingMessagesOnStop(t *testing.T) {
	queue := &lifecycleDispatcherQueue{}
	dispatcher := NewDelayDispatcher(queue, nil, nil, "group", "consumer")
	require.True(t, dispatcher.addPending(&delayItem{msgID: "delay-1", key: "execution-1"}))
	require.True(t, dispatcher.addPending(&delayItem{msgID: "delay-2", key: "execution-2"}))

	dispatcher.releasePendingMessages()

	require.ElementsMatch(t, []string{"delay-1", "delay-2"}, queue.done)
	require.Empty(t, dispatcher.items)
	require.Empty(t, dispatcher.pending)
}

func TestDelayDispatcherRunReleasesItemWaitingOnTimer(t *testing.T) {
	queue := newBlockingDispatcherQueue()
	dispatcher := NewDelayDispatcher(
		queue,
		&workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}},
		&noopStore{},
		"group",
		"consumer",
	)
	require.True(t, dispatcher.addPending(&delayItem{
		msgID:     "future-delay",
		key:       "future-execution",
		executeAt: time.Now().Add(time.Hour).Unix(),
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		dispatcher.Run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		dispatcher.mu.Lock()
		defer dispatcher.mu.Unlock()
		return len(dispatcher.items) == 0
	}, time.Second, time.Millisecond, "schedule loop should own the timer item")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delay dispatcher did not stop")
	}

	queue.doneMu.Lock()
	released := append([]string(nil), queue.done...)
	queue.doneMu.Unlock()
	require.Equal(t, []string{"future-delay"}, released)
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

type delayRecoveryQueryStore struct {
	noopStore
	query datastore.Entity
	opts  *datastore.ListOptions
}

func (s *delayRecoveryQueryStore) List(_ context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	s.query = query
	s.opts = opts
	return nil, nil
}

func TestDelayDispatcherRecoveryUsesBoundedDueQuery(t *testing.T) {
	store := &delayRecoveryQueryStore{}
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}}, store, "", "")

	require.NoError(t, dispatcher.recoverDueCheckpoints(context.Background()))
	require.IsType(t, &model.JobInfo{}, store.query)
	require.NotNil(t, store.opts)
	require.Equal(t, 1, store.opts.Page)
	require.Equal(t, delayRecoveryBatchSize, store.opts.PageSize)
	require.Equal(t, []datastore.InQueryOption{
		{Key: "status", Values: []string{string(config.StatusDistributed)}},
		{Key: "delay_state", Values: []string{string(config.JobDelayStatePending)}},
	}, store.opts.FilterOptions.In)
	require.Len(t, store.opts.FilterOptions.LessThan, 1)
	require.Equal(t, "delay_execute_at", store.opts.FilterOptions.LessThan[0].Key)
	require.Equal(t, []datastore.SortOption{{Key: "id", Order: datastore.SortOrderDescending}}, store.opts.SortBy)
}

func TestDispatcherConstructorsAndRunGuards(t *testing.T) {
	delay := NewDelayDispatcher(nil, nil, nil, "", "")
	require.NotNil(t, delay)
	delay.Run(context.Background())
	(*DelayDispatcher)(nil).Run(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := newBlockingDispatcherQueue()
	delay = NewDelayDispatcher(queue, &workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}}, &noopStore{}, "", "")
	done := make(chan struct{})
	go func() { delay.Run(ctx); close(done) }()
	requireClosed(t, queue.readStarted)
	requireStillRunning(t, done)
	cancel()
	requireClosed(t, done)
	require.Equal(t, config.DelayQueueGroup, delay.group)
	require.Equal(t, "delay-dispatcher", delay.consumer)
	require.EqualValues(t, 0, delay.ensureFailures.Load())
	result := NewResultDispatcher(nil, nil)
	require.Equal(t, defaultResultProcessingConcurrency, result.processingConcurrency)
	result.Run(context.Background())
}

func TestDelayDispatcherRunCountsEnsureGroupFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := newBlockingDispatcherQueue()
	queue.ensureGroupErr = errors.New("ensure delay failed")
	delay := NewDelayDispatcher(queue, &workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}}, &noopStore{}, "", "")
	done := make(chan struct{})
	go func() { delay.Run(ctx); close(done) }()
	requireClosed(t, queue.readStarted)
	cancel()
	requireClosed(t, done)
	require.EqualValues(t, 1, delay.ensureFailures.Load())
}

func TestDispatchersRunBlocksUntilContextCancelled(t *testing.T) {
	t.Run("delay", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := newBlockingDispatcherQueue()
		dispatcher := NewDelayDispatcher(queue, &workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}}, &noopStore{}, "", "")
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
		dispatcher := NewResultDispatcher(fake.NewSimpleClientset(), newResultOutboxTestStore())
		dispatcher.pollInterval = time.Hour
		done := make(chan struct{})
		go func() { dispatcher.Run(ctx); close(done) }()
		requireStillRunning(t, done)
		cancel()
		requireClosed(t, done)
	})

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

	require.ErrorIs(t, processJobResultWithOutbox(ctx, nil, nil, nil, nil), errResultDispatchNoRetry)
	require.ErrorIs(t, processJobResultWithOutbox(ctx, nil, nil, &JobResultPayload{}, nil), errResultDispatchNoRetry)
	require.ErrorIs(t, processJobResultWithOutbox(ctx, nil, nil, &JobResultPayload{Name: "job", TaskID: "task"}, nil), errResultDispatchNoRetry)
	require.ErrorIs(t, processJobResultWithOutbox(ctx, fake.NewSimpleClientset(), &noopStore{}, &JobResultPayload{Name: "job", TaskID: "task"}, nil), errResultDispatchNoRetry)
}
