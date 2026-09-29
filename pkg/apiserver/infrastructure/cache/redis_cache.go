package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisICache implements ICache using Redis as backend with a default TTL.
type RedisICache struct {
	cli       *redis.Client
	noCache   bool
	ttl       time.Duration
	keyPrefix string
}

const defaultTTL = 24 * time.Hour
const defaultKeyPrefix = "eruun:cache:"

// NewRedisICache creates a Redis-backed cache with custom TTL and prefix.
func NewRedisICache(cli *redis.Client, noCache bool, ttl time.Duration, prefix string) (*RedisICache, error) {
	if cli == nil {
		return nil, errors.New("redis cache client is not initialized")
	}
	if ttl <= 0 {
		ttl = defaultTTL
	}
	if prefix == "" {
		prefix = defaultKeyPrefix
	}
	return &RedisICache{cli: cli, noCache: noCache, ttl: ttl, keyPrefix: prefix}, nil
}

func (c *RedisICache) key(k string) string { return c.keyPrefix + k }

func (c *RedisICache) Store(ctx context.Context, key string, data string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	return cacheOperationError(ctx, c.cli.Set(ctx, c.key(key), data, c.ttl).Err())
}

func (c *RedisICache) Load(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	val, err := c.cli.Get(ctx, c.key(key)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, cacheOperationError(ctx, err)
}

func (c *RedisICache) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	return cacheOperationError(ctx, c.cli.Del(ctx, c.key(key)).Err())
}

func (c *RedisICache) IsCacheDisabled() bool { return c.noCache }

// go-redis may return a socket timeout when the context deadline expires.
// Preserve the caller's cancellation error for errors.Is without masking a
// successful write whose response arrived concurrently with cancellation.
func cacheOperationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return err
}
