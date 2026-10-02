// Forked from github.com/k8sgpt-ai/k8sgpt
// Some parts of this file have been modified to make it functional in Zadig

package cache

import (
	"context"
	"time"
)

// ICache defines the interface for cache operations.
// Implementations include MemCache (in-memory) and RedisICache (Redis-backed).
type ICache interface {
	Store(ctx context.Context, key string, data string) error
	Load(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) error
	IsCacheDisabled() bool
}

// defaultOpTimeout bounds cache operations, including post-write invalidation.
const defaultOpTimeout = 5 * time.Second

// InvalidateAfterWrite removes stale data even if the writer's context has ended.
// Context values are retained, but cleanup gets its own bounded lifetime.
func InvalidateAfterWrite(ctx context.Context, c ICache, key string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultOpTimeout)
	defer cancel()
	return c.Delete(ctx, key)
}
