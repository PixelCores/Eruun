package cache

import (
	"context"
	"sync"
	"time"
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
}

func (m *MemCache) Store(ctx context.Context, key string, data string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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

func (m *MemCache) Load(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
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

func (m *MemCache) Consume(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
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

func (m *MemCache) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

func (m *MemCache) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
	return nil
}

func (m *MemCache) Exists(ctx context.Context, key string) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
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

// expired 确定是否过期（ttl<=0 表示不过期）
func (i *item) expired(now time.Time) bool {
	if i.expiresAt.IsZero() {
		return false
	}
	return !now.Before(i.expiresAt)
}

func NewMemCache(noCache bool) ICache {
	return &MemCache{
		noCache: noCache,
		items:   make(map[string]*item),
		ttl:     24 * time.Hour, // 默认设置过期时间为1天
	}
}
