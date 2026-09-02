package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

const (
	workflowTaskRunLockerPrefix = "eruun-workflow-run"
	workflowTaskRunKeyPrefix    = "task"
	workflowTaskRunMinTTL       = 2 * time.Minute
	workflowTaskRunUnlockWait   = 5 * time.Second
)

var errTaskRunLeaseHeld = errors.New("workflow task run lease held by another runner")

var autoExtendTaskRunLease = locker.AutoExtend

// NewTaskRunRedisLocker builds the production locker used to guard workflow task execution.
func NewTaskRunRedisLocker(redisClient *redis.Client) (locker.Locker, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("workflow task run locker requires redis client")
	}
	lockProvider, err := locker.New(locker.Config{
		Type:        locker.TypeRedis,
		RedisClient: redisClient,
		Prefix:      workflowTaskRunLockerPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("init workflow task run redis locker: %w", err)
	}
	return lockProvider, nil
}

type taskRunLease struct {
	mutex          locker.Mutex
	stopAutoExtend func()
}

func (l *taskRunLease) release() {
	if l == nil {
		return
	}
	if l.stopAutoExtend != nil {
		l.stopAutoExtend()
	}
	if l.mutex == nil {
		return
	}
	unlockCtx, cancel := context.WithTimeout(context.Background(), workflowTaskRunUnlockWait)
	defer cancel()
	if err := l.mutex.Unlock(unlockCtx); err != nil &&
		!errors.Is(err, locker.ErrLockNotHeld) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		klog.Warningf("release workflow task run lease failed: key=%s err=%v", l.mutex.Key(), err)
	}
}

func (w *Workflow) ensureTaskRunLocker() (locker.Locker, error) {
	w.taskRunLockOnce.Do(func() {
		if w.taskRunLocker != nil {
			return
		}
		if w.TaskRunLocker != nil {
			w.taskRunLocker = w.TaskRunLocker
			return
		}
		if w.Cache == nil {
			w.taskRunLockerErr = fmt.Errorf("workflow task run locker requires cache with redis client")
			return
		}
		redisClient := w.Cache.GetRedisClient()
		lockProvider, err := NewTaskRunRedisLocker(redisClient)
		if err != nil {
			w.taskRunLockerErr = err
			return
		}
		w.taskRunLocker = lockProvider
	})
	return w.taskRunLocker, w.taskRunLockerErr
}

func (w *Workflow) taskRunLeaseTTL() time.Duration {
	ttl := w.workerAutoClaimMinIdle() * 2
	if ttl < workflowTaskRunMinTTL {
		return workflowTaskRunMinTTL
	}
	return ttl
}

func (w *Workflow) taskRunLeaseKey(taskID string) string {
	return fmt.Sprintf("%s:%s", workflowTaskRunKeyPrefix, taskID)
}

func (w *Workflow) tryAcquireTaskRunLease(ctx, renewalCtx context.Context, taskID string) (*taskRunLease, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if renewalCtx == nil {
		renewalCtx = ctx
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, false, fmt.Errorf("empty taskID")
	}
	lockProvider, err := w.ensureTaskRunLocker()
	if err != nil {
		return nil, false, err
	}
	if lockProvider == nil {
		return nil, false, fmt.Errorf("workflow task run locker unavailable")
	}
	ttl := w.taskRunLeaseTTL()
	mutex := lockProvider.NewMutex(w.taskRunLeaseKey(taskID), locker.WithTTL(ttl), locker.WithRetryCount(0))
	if err := mutex.TryLock(ctx); err != nil {
		if errors.Is(err, locker.ErrLockAcquireFailed) || errors.Is(err, locker.ErrLockAlreadyHeld) {
			return nil, false, nil
		}
		return nil, false, err
	}
	lease := &taskRunLease{
		mutex:          mutex,
		stopAutoExtend: autoExtendTaskRunLease(renewalCtx, mutex, ttl/3),
	}
	return lease, true, nil
}
