package jobdelay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
)

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
	dispatcher := NewDelayDispatcher(&jobtest.DispatcherAckQueue{}, nil, nil, "group", "consumer")
	require.True(t, dispatcher.addPending(&delayItem{msgID: "delay-1", key: "execution-1"}))
	require.True(t, dispatcher.addPending(&delayItem{msgID: "delay-2", key: "execution-2"}))

	dispatcher.releasePendingMessages()

	require.Empty(t, dispatcher.items)
	require.Empty(t, dispatcher.pending)
}

func TestDelayDispatcherRunReleasesItemWaitingOnTimer(t *testing.T) {
	queue := jobtest.NewBlockingDispatcherQueue()
	dispatcher := NewDelayDispatcher(
		queue,
		&workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}},
		&jobtest.NoopStore{},
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

	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	require.Empty(t, dispatcher.items)
	require.Empty(t, dispatcher.pending)
}

func TestDelayDispatcherAckAndDecodePayload(t *testing.T) {
	var nilDispatcher *DelayDispatcher
	require.NoError(t, nilDispatcher.ackMessage(context.Background(), "id-1", "reason"))

	queue := &jobtest.DispatcherAckQueue{AckErr: errors.New("ack failed")}
	dispatcher := &DelayDispatcher{
		queue: queue,
		group: "delay-workers",
	}
	require.Error(t, dispatcher.ackMessage(context.Background(), "id-1", "reason"))
	require.EqualValues(t, 1, dispatcher.ackFailures.Load())

	_, err := dispatcher.decodePayload([]byte(`{"job":`))
	require.Error(t, err)
}

type delayRecoveryQueryStore struct {
	jobtest.NoopStore
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

func TestDispatcherConstructorsAndStartGuards(t *testing.T) {
	delay := NewDelayDispatcher(nil, nil, nil, "", "")
	require.NotNil(t, delay)
	require.Equal(t, "", delay.group)
	require.Equal(t, "", delay.consumer)

	delay.Start(context.Background())

	ctxDelay, cancelDelay := context.WithCancel(context.Background())
	delay = NewDelayDispatcher(&jobtest.DispatcherAckQueue{}, &workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}}, &jobtest.NoopStore{}, "", "")
	delay.readBlock = 5 * time.Millisecond
	delay.autoClaimInterval = 5 * time.Millisecond
	delay.Start(ctxDelay)
	time.Sleep(10 * time.Millisecond)
	cancelDelay()
	require.Equal(t, config.DelayQueueGroup, delay.group)
	require.Equal(t, "delay-dispatcher", delay.consumer)
	require.EqualValues(t, 0, delay.ensureFailures.Load())

	result := jobresult.NewResultDispatcher(nil, nil, nil, "", "")
	require.NotNil(t, result)
	require.Equal(t, "", result.Group)
	require.Equal(t, "", result.Consumer)

	result.Start(context.Background())

	ctxResult, cancelResult := context.WithCancel(context.Background())
	result = jobresult.NewResultDispatcher(&jobtest.DispatcherAckQueue{}, fake.NewSimpleClientset(), &jobtest.NoopStore{}, "", "")
	result.ReadBlock = 5 * time.Millisecond
	result.AutoClaimInterval = 5 * time.Millisecond
	result.Start(ctxResult)
	time.Sleep(10 * time.Millisecond)
	cancelResult()
	require.Equal(t, config.ResultQueueGroup, result.Group)
	require.Equal(t, "result-dispatcher", result.Consumer)
	require.EqualValues(t, 0, result.EnsureFailures.Load())

	ctxOutbox, cancelOutbox := context.WithCancel(context.Background())
	outbox := jobresult.NewResultOutboxDispatcher(&jobtest.DispatcherAckQueue{}, &jobtest.NoopStore{})
	outbox.PollInterval = time.Hour
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
	delay := NewDelayDispatcher(&jobtest.DispatcherAckQueue{EnsureGroupErr: errors.New("ensure delay failed")}, &workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}}, &jobtest.NoopStore{}, "", "")
	delay.readBlock = time.Millisecond
	delay.autoClaimInterval = time.Millisecond
	delay.Start(ctxDelay)
	cancelDelay()
	time.Sleep(5 * time.Millisecond)
	require.EqualValues(t, 1, delay.ensureFailures.Load())

	ctxResult, cancelResult := context.WithCancel(context.Background())
	defer cancelResult()
	result := jobresult.NewResultDispatcher(&jobtest.DispatcherAckQueue{EnsureGroupErr: errors.New("ensure result failed")}, fake.NewSimpleClientset(), &jobtest.NoopStore{}, "", "")
	result.ReadBlock = time.Millisecond
	result.AutoClaimInterval = time.Millisecond
	result.Start(ctxResult)
	cancelResult()
	time.Sleep(5 * time.Millisecond)
	require.EqualValues(t, 1, result.EnsureFailures.Load())
}

func TestDispatchersRunBlocksUntilContextCancelled(t *testing.T) {
	t.Run("delay", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := jobtest.NewBlockingDispatcherQueue()
		dispatcher := NewDelayDispatcher(queue, &workspace.Manager{Client: fake.NewSimpleClientset(), RESTConfig: &rest.Config{}}, &jobtest.NoopStore{}, "", "")
		dispatcher.autoClaimInterval = time.Millisecond
		done := make(chan struct{})
		go func() {
			dispatcher.Run(ctx)
			close(done)
		}()
		jobtest.RequireClosed(t, queue.ReadStarted)
		jobtest.RequireClosed(t, queue.AutoClaimStarted)
		jobtest.RequireStillRunning(t, done)
		cancel()
		jobtest.RequireClosed(t, done)
	})

	t.Run("result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := jobtest.NewBlockingDispatcherQueue()
		dispatcher := jobresult.NewResultDispatcher(queue, fake.NewSimpleClientset(), &jobtest.NoopStore{}, "", "")
		dispatcher.AutoClaimInterval = time.Millisecond
		done := make(chan struct{})
		go func() {
			dispatcher.Run(ctx)
			close(done)
		}()
		jobtest.RequireClosed(t, queue.ReadStarted)
		jobtest.RequireClosed(t, queue.AutoClaimStarted)
		jobtest.RequireStillRunning(t, done)
		cancel()
		jobtest.RequireClosed(t, done)
	})

	t.Run("result_outbox", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		dispatcher := jobresult.NewResultOutboxDispatcher(&jobtest.DispatcherAckQueue{}, &jobtest.NoopStore{})
		dispatcher.PollInterval = time.Hour
		done := make(chan struct{})
		go func() {
			dispatcher.Run(ctx)
			close(done)
		}()
		time.Sleep(20 * time.Millisecond)
		jobtest.RequireStillRunning(t, done)
		cancel()
		jobtest.RequireClosed(t, done)
	})
}
