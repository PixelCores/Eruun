package workflow

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

func TestNewTaskRunRedisLockerRequiresRedisClient(t *testing.T) {
	lockProvider, err := NewTaskRunRedisLocker(nil)
	require.Nil(t, lockProvider)
	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow task run locker requires redis client")
}

func TestNewTaskRunRedisLockerUsesRedis(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})

	lockProvider, err := NewTaskRunRedisLocker(redisClient)
	require.NoError(t, err)
	require.NotNil(t, lockProvider)

	mutex := lockProvider.NewMutex("task:test", locker.WithRetryCount(0))
	require.NoError(t, mutex.TryLock(context.Background()))
	require.NoError(t, mutex.Unlock(context.Background()))
}

func TestTryAcquireTaskRunLeaseRequiresRedisClient(t *testing.T) {
	w := &Workflow{Cache: cache.NewMemCache(false)}

	lease, acquired, err := w.tryAcquireTaskRunLease(context.Background(), context.Background(), "task-1")
	require.Nil(t, lease)
	require.False(t, acquired)
	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow task run locker requires redis client")
}

func TestTryAcquireTaskRunLeaseUsesExplicitLocker(t *testing.T) {
	w := &Workflow{taskRunLocker: locker.NewMemoryLocker(workflowTaskRunLockerPrefix)}

	lease, acquired, err := w.tryAcquireTaskRunLease(context.Background(), context.Background(), "task-1")
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotNil(t, lease)
	lease.release()
}

func TestTryAcquireTaskRunLeaseUsesInjectedLocker(t *testing.T) {
	w := &Workflow{TaskRunLocker: locker.NewMemoryLocker(workflowTaskRunLockerPrefix)}

	lease, acquired, err := w.tryAcquireTaskRunLease(context.Background(), context.Background(), "task-1")
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotNil(t, lease)
	lease.release()
}

func TestTryAcquireTaskRunLeaseUsesRenewalContext(t *testing.T) {
	w := &Workflow{taskRunLocker: locker.NewMemoryLocker(workflowTaskRunLockerPrefix)}
	type contextKey struct{}
	key := contextKey{}
	acquireCtx, cancelAcquire := context.WithCancel(context.WithValue(context.Background(), key, "acquire"))
	renewalCtx, cancelRenewal := context.WithCancel(context.WithValue(context.Background(), key, "renewal"))
	t.Cleanup(cancelAcquire)
	t.Cleanup(cancelRenewal)

	originalAutoExtend := autoExtendTaskRunLease
	capturedCtx := make(chan context.Context, 1)
	autoExtendTaskRunLease = func(ctx context.Context, _ locker.Mutex, _ time.Duration) func() {
		capturedCtx <- ctx
		return func() {}
	}
	t.Cleanup(func() {
		autoExtendTaskRunLease = originalAutoExtend
	})

	lease, acquired, err := w.tryAcquireTaskRunLease(acquireCtx, renewalCtx, "task-1")
	require.NoError(t, err)
	require.True(t, acquired)
	require.NotNil(t, lease)
	t.Cleanup(lease.release)

	var got context.Context
	select {
	case got = <-capturedCtx:
	case <-time.After(time.Second):
		t.Fatal("task run lease auto-extension was not started")
	}
	require.Equal(t, "renewal", got.Value(key))

	cancelAcquire()
	require.NoError(t, got.Err())
	cancelRenewal()
	require.ErrorIs(t, got.Err(), context.Canceled)
}

func TestStartWorkerFailsFastWithoutTaskRunLocker(t *testing.T) {
	w := &Workflow{
		Queue: &fakeAckQueue{},
		Cache: cache.NewMemCache(false),
	}
	errChan := make(chan error, 1)

	w.StartWorker(context.Background(), context.Background(), errChan, nil, nil)

	select {
	case err := <-errChan:
		require.Error(t, err)
		require.Contains(t, err.Error(), "ensure workflow task run locker")
		require.Contains(t, err.Error(), "requires redis client")
	default:
		t.Fatal("expected worker startup to report missing task run locker")
	}
}
