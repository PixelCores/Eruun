// Forked from github.com/k8sgpt-ai/k8sgpt
// Some parts of this file have been modified to make it functional in Zadig

package cache

import "github.com/redis/go-redis/v9"

// ICache defines the interface for cache operations.
// Implementations include MemCache (in-memory) and RedisICache (Redis-backed).
type ICache interface {
	Store(key string, data string) error
	Load(key string) (string, error)
	Consume(key string) (string, error)
	List() ([]string, error)
	Delete(key string) error
	Exists(key string) bool
	IsCacheDisabled() bool
	// GetRedisClient returns the underlying Redis client if available.
	// Returns nil for non-Redis implementations (e.g., MemCache).
	// This method enables dependency injection for components that need
	// direct Redis access (e.g., distributed locks, cancellation signals).
	GetRedisClient() *redis.Client
}

type CacheType string

var (
	CacheTypeRedis CacheType = "redis"
	CacheTypeMem   CacheType = "memory"
)

func New(noCache bool, cacheType CacheType) ICache {
	return NewWithClient(noCache, cacheType, nil)
}

// NewWithClient creates an ICache implementation and carries an optional Redis client
// for dependency injection.
func NewWithClient(noCache bool, cacheType CacheType, cli *redis.Client) ICache {
	switch cacheType {
	case CacheTypeMem:
		return NewMemCacheWithClient(noCache, cli)
	case CacheTypeRedis:
		return NewRedisICacheWithClient(cli, noCache)

	default:
		return NewMemCacheWithClient(noCache, cli)
	}
}
