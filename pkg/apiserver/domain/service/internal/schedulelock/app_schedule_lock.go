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
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

const (
	appScheduleLockerPrefix = "eruun-app-schedule"
	appScheduleLockKey      = "app-schedule"
	appScheduleLockTTL      = 2 * time.Minute
	appScheduleUnlockWait   = 5 * time.Second
)

func ResolveAppScheduleLocker(explicit locker.Locker, cacheStore cache.ICache) (locker.Locker, error) {
	if explicit != nil {
		return explicit, nil
	}
	if cacheStore == nil {
		return nil, bcode.ErrDistributedLockUnavailable
	}
	redisClient := cacheStore.GetRedisClient()
	if redisClient == nil {
		return nil, bcode.ErrDistributedLockUnavailable
	}
	lockProvider, err := locker.New(locker.Config{
		Type:        locker.TypeRedis,
		RedisClient: redisClient,
		Prefix:      appScheduleLockerPrefix,
	})
	if err != nil {
		klog.Warningf("init app schedule locker failed: %v", err)
		return nil, bcode.ErrDistributedLockUnavailable
	}
	return lockProvider, nil
}

func WithAppScheduleLock(ctx context.Context, lockProvider locker.Locker, appID string, operation string, autoExtend bool, fn func(context.Context) error) error {
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

	if autoExtend {
		stopExtend := locker.AutoExtend(ctx, mutex, appScheduleLockTTL/3)
		defer stopExtend()
	}

	return fn(ctx)
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
