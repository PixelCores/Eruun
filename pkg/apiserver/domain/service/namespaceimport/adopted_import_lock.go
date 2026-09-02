package namespaceimport

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

const (
	adoptedImportLockerPrefix       = "eruun-adopted-import"
	adoptedImportLockTTL            = 2 * time.Minute
	adoptedImportLockExtendInterval = adoptedImportLockTTL / 3
	adoptedImportLockExtendTimeout  = 5 * time.Second
	adoptedImportLockUnlockTimeout  = 5 * time.Second
)

func (s *namespaceImportServiceImpl) adoptedImportLocker() (locker.Locker, error) {
	if s.AdoptedImportLocker != nil {
		return s.AdoptedImportLocker, nil
	}
	if s.Cache == nil || s.Cache.GetRedisClient() == nil {
		return nil, bcode.ErrDistributedLockUnavailable
	}
	lockProvider, err := locker.New(locker.Config{
		Type:        locker.TypeRedis,
		RedisClient: s.Cache.GetRedisClient(),
		Prefix:      adoptedImportLockerPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: initialize adopted import locker: %v", bcode.ErrDistributedLockUnavailable, err)
	}
	return lockProvider, nil
}

func (s *namespaceImportServiceImpl) withAdoptedNamespaceApplyLock(
	ctx context.Context,
	namespace string,
	run func(context.Context) (*apisv1.ImportNamespaceApplicationsResponse, error),
) (*apisv1.ImportNamespaceApplicationsResponse, error) {
	lockProvider, err := s.adoptedImportLocker()
	if err != nil {
		return nil, err
	}
	key := "namespace:" + strings.ToLower(strings.TrimSpace(namespace))
	mutex := lockProvider.NewMutex(key, locker.WithTTL(adoptedImportLockTTL))
	if mutex == nil {
		return nil, fmt.Errorf("%w: adopted import locker returned no mutex", bcode.ErrDistributedLockUnavailable)
	}
	if err := mutex.Lock(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: acquire adopted import lock for namespace %s: %v", bcode.ErrDistributedLockUnavailable, namespace, err)
	}

	criticalCtx, cancelCritical := context.WithCancel(ctx)
	renewCtx, stopRenew := context.WithCancel(context.Background())
	renewResult := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(adoptedImportLockExtendInterval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				renewResult <- nil
				return
			case <-ticker.C:
				extendCtx, cancel := context.WithTimeout(renewCtx, adoptedImportLockExtendTimeout)
				extendErr := mutex.Extend(extendCtx)
				cancel()
				if extendErr == nil {
					continue
				}
				if renewCtx.Err() != nil {
					renewResult <- nil
					return
				}
				cancelCritical()
				renewResult <- extendErr
				return
			}
		}
	}()

	var (
		cleaned   bool
		renewErr  error
		unlockErr error
	)
	cleanup := func() {
		if cleaned {
			return
		}
		cleaned = true
		stopRenew()
		renewErr = <-renewResult
		cancelCritical()

		unlockCtx, cancelUnlock := context.WithTimeout(context.Background(), adoptedImportLockUnlockTimeout)
		unlockErr = mutex.Unlock(unlockCtx)
		cancelUnlock()
		if unlockErr != nil {
			klog.ErrorS(unlockErr, "release adopted namespace import lock failed", "key", mutex.Key())
		}
	}
	defer cleanup()

	response, runErr := run(criticalCtx)
	cleanup()
	if renewErr != nil {
		return nil, fmt.Errorf("%w: renew adopted import lock for namespace %s: %v", bcode.ErrDistributedLockUnavailable, namespace, renewErr)
	}
	if runErr != nil {
		return nil, runErr
	}
	if unlockErr != nil {
		return nil, fmt.Errorf("%w: release adopted import lock for namespace %s: %v", bcode.ErrDistributedLockUnavailable, namespace, unlockErr)
	}
	return response, nil
}
