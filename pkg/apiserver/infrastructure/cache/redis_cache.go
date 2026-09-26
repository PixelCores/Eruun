package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache is a low-level helper used by legacy callers that need direct access.
type RedisCache struct {
	redisClient *redis.Client
}

// NewRedisCache returns a handle to the provided client.
func NewRedisCache(cli *redis.Client) *RedisCache {
	return &RedisCache{redisClient: cli}
}

// RedisICache implements ICache using Redis as backend with a default TTL.
type RedisICache struct {
	cli       *redis.Client
	noCache   bool
	ttl       time.Duration
	keyPrefix string
}

const defaultTTL = 24 * time.Hour
const defaultKeyPrefix = "eruun:cache:"

// NewRedisICacheWithClient creates an ICache backed by the provided client.
// If cli is nil, falls back to in-memory cache to remain functional.
func NewRedisICacheWithClient(cli *redis.Client, noCache bool) ICache {
	if cli == nil {
		return NewMemCache(noCache)
	}
	return &RedisICache{cli: cli, noCache: noCache, ttl: defaultTTL, keyPrefix: defaultKeyPrefix}
}

// NewRedisICache creates an ICache with custom ttl and prefix.
func NewRedisICache(cli *redis.Client, noCache bool, ttl time.Duration, prefix string) ICache {
	if cli == nil {
		return NewMemCache(noCache)
	}
	if ttl <= 0 {
		ttl = defaultTTL
	}
	if prefix == "" {
		prefix = defaultKeyPrefix
	}
	return &RedisICache{cli: cli, noCache: noCache, ttl: ttl, keyPrefix: prefix}
}

// defaultOpTimeout is the default timeout for Redis cache operations.
const defaultOpTimeout = 5 * time.Second

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

func (c *RedisICache) Consume(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	val, err := c.cli.GetDel(ctx, c.key(key)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, cacheOperationError(ctx, err)
}

// List returns the cached values for keys under the prefix.
// For performance, this uses SCAN; if keys are many, this can be expensive.
func (c *RedisICache) List(ctx context.Context) ([]string, error) {
	var (
		cursor uint64
		out    []string
	)
	pattern := c.keyPrefix + "*"
	// Use a longer timeout for List as it may involve multiple SCAN iterations
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		keys, next, err := c.cli.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return out, cacheOperationError(ctx, err)
		}
		cursor = next
		if len(keys) > 0 {
			vals, err := c.cli.MGet(ctx, keys...).Result()
			if err != nil {
				return out, cacheOperationError(ctx, err)
			}
			for _, v := range vals {
				if v == nil {
					continue
				}
				if s, ok := v.(string); ok {
					out = append(out, s)
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

func (c *RedisICache) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	return cacheOperationError(ctx, c.cli.Del(ctx, c.key(key)).Err())
}

func (c *RedisICache) Exists(ctx context.Context, key string) bool {
	ctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	defer cancel()
	n, err := c.cli.Exists(ctx, c.key(key)).Result()
	return err == nil && n == 1
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
