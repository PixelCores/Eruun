package job

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

const (
	adoptedRecreationCriticalTimeout = 2 * config.DelTimeOut
	adoptedRecreationLeaseTTL        = 4 * adoptedRecreationCriticalTimeout
	adoptedRecreationUnlockTimeout   = 5 * time.Second
	adoptedRecreationExtendTimeout   = 5 * time.Second
)

type adoptedRecreationGuard struct {
	mutex          locker.Mutex
	criticalCtx    context.Context
	criticalCancel context.CancelFunc
	renewCancel    context.CancelFunc
	renewDone      chan struct{}
	releaseOnce    sync.Once
}

func acquireAdoptedRecreationGuard(
	ctx context.Context,
	lockProvider locker.Locker,
	binding *adoptedResourceBinding,
) (*adoptedRecreationGuard, error) {
	if binding == nil || binding.application == nil || binding.snapshot == nil || binding.resource == nil {
		return nil, fmt.Errorf("adopted recreation lock identity is incomplete")
	}
	if lockProvider == nil {
		return nil, fmt.Errorf("adopted recreation locker is unavailable")
	}
	appID := strings.TrimSpace(binding.application.ID)
	source := binding.resource.Source
	kind := strings.ToLower(strings.TrimSpace(source.Kind))
	namespace := strings.TrimSpace(source.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(binding.snapshot.Namespace)
	}
	name := strings.TrimSpace(source.Name)
	if appID == "" || kind == "" || namespace == "" || name == "" {
		return nil, fmt.Errorf("adopted recreation lock identity is incomplete")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := fmt.Sprintf("adopted-recreation:%s:%s:%s:%s", appID, kind, namespace, name)
	mutex := lockProvider.NewMutex(
		key,
		locker.WithTTL(adoptedRecreationLeaseTTL),
	)
	if mutex == nil {
		return nil, fmt.Errorf("adopted recreation locker returned no mutex for %s", key)
	}
	if err := mutex.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire adopted recreation lock %s: %w", key, err)
	}

	criticalCtx, criticalCancel := context.WithTimeout(ctx, adoptedRecreationCriticalTimeout)
	renewCtx, renewCancel := context.WithCancel(context.Background())
	guard := &adoptedRecreationGuard{
		mutex:          mutex,
		criticalCtx:    criticalCtx,
		criticalCancel: criticalCancel,
		renewCancel:    renewCancel,
		renewDone:      make(chan struct{}),
	}
	go guard.renew(renewCtx)
	return guard, nil
}

func (g *adoptedRecreationGuard) Context() context.Context {
	if g == nil || g.criticalCtx == nil {
		return context.Background()
	}
	return g.criticalCtx
}

func (g *adoptedRecreationGuard) renew(ctx context.Context) {
	defer close(g.renewDone)
	interval := adoptedRecreationLeaseTTL / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			extendCtx, cancel := context.WithTimeout(ctx, adoptedRecreationExtendTimeout)
			err := g.mutex.Extend(extendCtx)
			cancel()
			if err == nil {
				continue
			}
			// Cancel any not-yet-completed prepare/Create immediately. If Create
			// has already succeeded, persistence deliberately detaches from this
			// context and remains covered by the unexpired initial lease.
			g.criticalCancel()
			if ctx.Err() == nil {
				klog.ErrorS(err, "extend adopted recreation lock failed", "key", g.mutex.Key())
			}
			return
		}
	}
}

func (g *adoptedRecreationGuard) release() {
	if g == nil {
		return
	}
	g.releaseOnce.Do(func() {
		g.renewCancel()
		<-g.renewDone
		g.criticalCancel()
		unlockCtx, cancel := context.WithTimeout(context.Background(), adoptedRecreationUnlockTimeout)
		defer cancel()
		if err := g.mutex.Unlock(unlockCtx); err != nil {
			klog.ErrorS(err, "release adopted recreation lock failed", "key", g.mutex.Key())
		}
	})
}
