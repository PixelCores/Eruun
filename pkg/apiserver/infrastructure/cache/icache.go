// Forked from github.com/k8sgpt-ai/k8sgpt
// Some parts of this file have been modified to make it functional in Zadig

package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// ICache defines the interface for cache operations.
// Implementations include MemCache (in-memory) and RedisICache (Redis-backed).
type ICache interface {
	Store(ctx context.Context, key string, data string) error
	Load(ctx context.Context, key string) (string, error)
	Consume(ctx context.Context, key string) (string, error)
	List(ctx context.Context) ([]string, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) bool
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

type CacheType string

var (
	CacheTypeRedis CacheType = "redis"
	CacheTypeMem   CacheType = "memory"
)

func New(noCache bool, cacheType CacheType) ICache {
	return NewWithClient(noCache, cacheType, nil)
}

// NewWithClient creates the selected cache implementation.
func NewWithClient(noCache bool, cacheType CacheType, cli *redis.Client) ICache {
	switch cacheType {
	case CacheTypeMem:
		return NewMemCache(noCache)
	case CacheTypeRedis:
		return NewRedisICacheWithClient(cli, noCache)

	default:
		return NewMemCache(noCache)
	}
}
