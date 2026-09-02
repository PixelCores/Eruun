package job

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

const shareLockerPrefix = "eruun-share"

// shareInfoFromLabels extracts share metadata from workload labels.
func shareInfoFromLabels(labels map[string]string) (string, config.ShareStrategy) {
	if labels == nil {
		return "", ""
	}
	name := strings.TrimSpace(labels[config.LabelShareName])
	if name == "" {
		return "", ""
	}
	rawStrategy := strings.TrimSpace(labels[config.LabelShareStrategy])
	strategy, _ := config.NormalizeShareStrategy(rawStrategy)
	return name, strategy
}

func shareListOptions(name string) metav1.ListOptions {
	selector := labels.Set{config.LabelShareName: name}.String()
	return metav1.ListOptions{LabelSelector: selector}
}

func hasSharedResources(ctx context.Context, name string, listFn func(context.Context, metav1.ListOptions) (int, error)) (bool, error) {
	if name == "" {
		return false, nil
	}
	count, err := listFn(ctx, shareListOptions(name))
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func shareLockKey(kind config.ResourceKind, name string) string {
	if kind == "" || name == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s:%s", shareLockerPrefix, kind, name)
}

func acquireShareLock(ctx context.Context, lockProvider locker.Locker, key string) (func(), error) {
	if key == "" {
		return nil, nil
	}
	if lockProvider == nil {
		return nil, fmt.Errorf("shared resource locker unavailable for key %s", key)
	}
	mutex := lockProvider.NewMutex(key)
	if err := mutex.Lock(ctx); err != nil {
		return nil, err
	}
	return func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mutex.Unlock(unlockCtx); err != nil {
			klog.Warningf("failed to release share lock %s: %v", key, err)
		}
	}, nil
}

func newShareLocker(redisClient *redis.Client) locker.Locker {
	if redisClient == nil {
		return nil
	}
	redisLocker, err := locker.New(locker.Config{
		Type:        locker.TypeRedis,
		RedisClient: redisClient,
		Prefix:      shareLockerPrefix,
	})
	if err != nil {
		klog.ErrorS(err, "init shared resource redis locker failed")
		return nil
	}
	return redisLocker
}

// resolveSharedResource applies share strategy and returns (unlock, skipped, err).
func resolveSharedResource(ctx context.Context, name string, strategy config.ShareStrategy, kind config.ResourceKind, listFn func(context.Context, metav1.ListOptions) (int, error), lockProvider locker.Locker) (func(), bool, error) {
	if name == "" || strategy == "" {
		return nil, false, nil
	}
	if strategy == config.ShareStrategyIgnore {
		return nil, true, nil
	}
	if strategy != config.ShareStrategyDefault {
		return nil, false, nil
	}
	unlock, err := acquireShareLock(ctx, lockProvider, shareLockKey(kind, name))
	if err != nil {
		return nil, false, err
	}
	release := func() {
		if unlock != nil {
			unlock()
		}
	}
	exists, err := hasSharedResources(ctx, name, listFn)
	if err != nil {
		release()
		return nil, false, err
	}
	if exists {
		release()
		return nil, true, nil
	}
	return unlock, false, nil
}
