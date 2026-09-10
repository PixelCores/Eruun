package cache

import (
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type item struct {
	value     string
	expiresAt time.Time
}

type MemCache struct {
	noCache     bool
	items       map[string]*item
	mu          sync.Mutex
	ttl         time.Duration
	nextCleanup time.Time
	redisClient *redis.Client
}

func (m *MemCache) Store(key string, data string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	// Reclaim stale entries on writes instead of retaining every cache instance
	// through an unbounded background goroutine. Limit full scans to once a second.
	if !now.Before(m.nextCleanup) {
		for k, v := range m.items {
			if v.expired(now) {
				delete(m.items, k)
			}
		}
		m.nextCleanup = now.Add(time.Second)
	}
	// Upsert and refresh expiry
	expiresAt := time.Time{}
	if m.ttl > 0 {
		expiresAt = now.Add(m.ttl)
	}
	m.items[key] = &item{
		value:     data,
		expiresAt: expiresAt,
	}
	return nil
}

func (m *MemCache) Load(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result string
	if v, ok := m.items[key]; ok {
		// Honor expiration
		if !v.expired(time.Now()) {
			result = v.value
		} else {
			delete(m.items, key)
		}
	}
	return result, nil
}

func (m *MemCache) Consume(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.items[key]
	if !ok {
		return "", nil
	}
	delete(m.items, key)
	if value.expired(time.Now()) {
		return "", nil
	}
	return value.value, nil
}

func (m *MemCache) List() ([]string, error) {
	var ret []string
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for k, v := range m.items {
		if v.expired(now) {
			delete(m.items, k)
			continue
		}
		ret = append(ret, v.value)
	}
	return ret, nil
}

func (m *MemCache) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
	return nil
}

func (m *MemCache) Exists(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.items[key]; ok {
		if !v.expired(time.Now()) {
			return true
		}
		delete(m.items, key)
	}
	return false
}

func (m *MemCache) IsCacheDisabled() bool {
	return m.noCache
}

// GetRedisClient returns the optional Redis client carried for DI.
// MemCache does not use Redis for cache operations.
func (m *MemCache) GetRedisClient() *redis.Client {
	return m.redisClient
}

// expired 确定是否过期（ttl<=0 表示不过期）
func (i *item) expired(now time.Time) bool {
	if i.expiresAt.IsZero() {
		return false
	}
	return !now.Before(i.expiresAt)
}

func NewMemCache(noCache bool) ICache {
	return NewMemCacheWithClient(noCache, nil)
}

// NewMemCacheWithClient returns a MemCache and carries an optional Redis client for DI.
func NewMemCacheWithClient(noCache bool, redisClient *redis.Client) ICache {
	return &MemCache{
		noCache:     noCache,
		items:       make(map[string]*item),
		ttl:         24 * time.Hour, // 默认设置过期时间为1天
		redisClient: redisClient,
	}
}
