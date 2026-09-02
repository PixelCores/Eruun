package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

type lifecycleBlockingQueue struct {
	readStarted chan struct{}
	readCalls   chan struct{}
	readOnce    sync.Once
	linger      time.Duration
}

func newLifecycleBlockingQueue(linger time.Duration) *lifecycleBlockingQueue {
	return &lifecycleBlockingQueue{
		readStarted: make(chan struct{}),
		readCalls:   make(chan struct{}, 16),
		linger:      linger,
	}
}

func (q *lifecycleBlockingQueue) EnsureGroup(context.Context, string) error { return nil }
func (q *lifecycleBlockingQueue) Enqueue(context.Context, []byte) (string, error) {
	return "", nil
}
func (q *lifecycleBlockingQueue) ReadGroup(ctx context.Context, _ string, _ string, _ int, _ time.Duration) ([]msg.Message, error) {
	q.readOnce.Do(func() { close(q.readStarted) })
	q.readCalls <- struct{}{}
	<-ctx.Done()
	time.Sleep(q.linger)
	return nil, ctx.Err()
}
func (q *lifecycleBlockingQueue) Ack(context.Context, string, ...string) error { return nil }
func (q *lifecycleBlockingQueue) AutoClaim(context.Context, string, string, time.Duration, int) ([]msg.Message, error) {
	return nil, nil
}
func (q *lifecycleBlockingQueue) Close(context.Context) error { return nil }
func (q *lifecycleBlockingQueue) Stats(context.Context, string) (int64, int64, error) {
	return 0, 0, nil
}

func TestWorkflowStartWorkerStartsNewGenerationWhilePreviousStops(t *testing.T) {
	cfg := config.NewConfig()
	queue := newLifecycleBlockingQueue(200 * time.Millisecond)
	w := &Workflow{
		Queue: queue,
		Cfg:   cfg,
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		w.StartWorker(firstCtx, firstCtx, nil, nil, nil)
		close(firstDone)
	}()

	requireClosed(t, queue.readStarted)
	requireClosed(t, queue.readCalls)

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	secondDone := make(chan struct{})
	go func() {
		w.StartWorker(secondCtx, secondCtx, nil, nil, nil)
		close(secondDone)
	}()

	cancelFirst()
	select {
	case <-queue.readCalls:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("new worker generation waited for the previous generation to stop")
	}

	cancelSecond()
	requireClosed(t, firstDone)
	requireClosed(t, secondDone)
}

func TestWorkflowWorkerRunsShareConcurrencyLimiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config.NewConfig()
	cfg.Workflow.MaxConcurrentWorkflows = 1
	w := &Workflow{Cfg: cfg}
	firstRun := newWorkflowWorkerRun(ctx, w.workerConcurrencyLimiter())
	secondRun := newWorkflowWorkerRun(ctx, w.workerConcurrencyLimiter())
	limiter := firstRun.limiter
	require.NotNil(t, limiter)
	require.NotSame(t, firstRun.taskGroup, secondRun.taskGroup)
	require.Same(t, limiter, secondRun.limiter)
	require.NoError(t, limiter.Acquire(ctx, 1))

	secondAcquired := make(chan struct{})
	go func() {
		if secondRun.limiter.Acquire(ctx, 1) == nil {
			close(secondAcquired)
		}
	}()
	select {
	case <-secondAcquired:
		t.Fatal("second workflow slot must wait for the first slot")
	case <-time.After(50 * time.Millisecond):
	}
	limiter.Release(1)
	requireClosed(t, secondAcquired)
	limiter.Release(1)
}

func TestWorkflowWorkerRunWaitsForTasks(t *testing.T) {
	executionCtx, cancelExecution := context.WithCancel(context.Background())
	run := newWorkflowWorkerRun(executionCtx, nil)
	taskStarted := make(chan struct{})
	taskDone := make(chan struct{})
	run.taskGroup.Go(func() error {
		close(taskStarted)
		defer close(taskDone)
		<-executionCtx.Done()
		return nil
	})
	requireClosed(t, taskStarted)

	workerDone := make(chan struct{})
	go func() {
		_ = run.wait()
		close(workerDone)
	}()

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, executionCtx.Err())
	select {
	case <-taskDone:
		t.Fatal("consumer stop cancelled an in-flight workflow task")
	default:
	}
	select {
	case <-workerDone:
		t.Fatal("worker run returned before in-flight workflow tasks drained")
	default:
	}

	cancelExecution()
	requireClosed(t, taskDone)
	requireClosed(t, workerDone)
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}
