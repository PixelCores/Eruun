package schedulelock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	appScheduleLockKey    = "app-schedule"
	appScheduleLockTTL    = 2 * time.Minute
	appScheduleUnlockWait = 5 * time.Second
	appScheduleExtendWait = 5 * time.Second
)

func WithAppScheduleLock(ctx context.Context, lockProvider locker.Locker, appID string, operation string, autoExtend bool, fn func(context.Context) error) (resultErr error) {
	appID = strings.ToLower(strings.TrimSpace(appID))
	if appID == "" {
		return bcode.ErrApplicationNotExist
	}
	if lockProvider == nil {
		return bcode.ErrDistributedLockUnavailable
	}

	key := fmt.Sprintf("%s:%s", appScheduleLockKey, appID)
	mutex := lockProvider.NewMutex(key, locker.WithTTL(appScheduleLockTTL), locker.WithRetryCount(0))
	if err := mutex.TryLock(ctx); err != nil {
		klog.Warningf("acquire app schedule lock failed appID=%s op=%s key=%s: %v", appID, operation, key, err)
		return mapAppScheduleLockError(err)
	}

	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), appScheduleUnlockWait)
		defer cancel()
		if err := mutex.Unlock(unlockCtx); err != nil &&
			!errors.Is(err, locker.ErrLockNotHeld) &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			klog.Warningf("release app schedule lock failed appID=%s op=%s key=%s: %v", appID, operation, key, err)
		}
	}()

	if !autoExtend {
		return fn(ctx)
	}
	criticalCtx, cancelCritical := context.WithCancelCause(ctx)
	defer cancelCritical(nil)
	renewCtx, stopRenew := context.WithCancel(criticalCtx)
	renewDone := make(chan struct{})
	var renewalErr error
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(appScheduleLockTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				extendCtx, cancel := context.WithTimeout(renewCtx, appScheduleExtendWait)
				err := mutex.Extend(extendCtx)
				cancel()
				if err == nil {
					continue
				}
				if renewCtx.Err() == nil {
					renewalErr = fmt.Errorf("%w: renew app schedule lock for %s: %w", bcode.ErrDistributedLockUnavailable, appID, err)
					cancelCritical(renewalErr)
				}
				return
			}
		}
	}()
	defer func() {
		stopRenew()
		<-renewDone
		// Preserve lock loss even if the operation returned nil after observing
		// cancellation. Wait for renewal before releasing the Redis mutex.
		if renewalErr != nil {
			resultErr = errors.Join(resultErr, renewalErr)
		}
	}()
	return fn(criticalCtx)
}

func mapAppScheduleLockError(err error) error {
	switch {
	case errors.Is(err, locker.ErrLockAcquireFailed), errors.Is(err, locker.ErrLockAlreadyHeld):
		return bcode.ErrApplicationOperationLocked
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	default:
		return bcode.ErrDistributedLockUnavailable
	}
}
