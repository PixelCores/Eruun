package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMemCache_BasicStoreLoad(t *testing.T) {
	c := NewMemCache(false)
	mc := c.(*MemCache)
	mc.ttl = time.Second

	if err := c.Store("k", "v"); err != nil {
		t.Fatalf("store error: %v", err)
	}
	if !c.Exists("k") {
		t.Fatalf("expected key to exist")
	}
	got, _ := c.Load("k")
	if got != "v" {
		t.Fatalf("expected v, got %q", got)
	}
}

func TestMemCache_Expiration(t *testing.T) {
	c := NewMemCache(false)
	mc := c.(*MemCache)
	mc.ttl = 50 * time.Millisecond

	if err := c.Store("k", "v"); err != nil {
		t.Fatalf("store error: %v", err)
	}
	if got, _ := c.Load("k"); got != "v" {
		t.Fatalf("expected v before expiry, got %q", got)
	}
	time.Sleep(80 * time.Millisecond)
	if got, _ := c.Load("k"); got != "" {
		t.Fatalf("expected empty after expiry, got %q", got)
	}
	if c.Exists("k") {
		t.Fatalf("expected key to be expired and removed")
	}
}

func TestMemCache_Delete(t *testing.T) {
	c := NewMemCache(false)
	require.NoError(t, c.Store("k", "v"))
	require.True(t, c.Exists("k"))
	require.NoError(t, c.Delete("k"))
	require.False(t, c.Exists("k"))
	got, err := c.Load("k")
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestMemCache_ConsumeIsAtomic(t *testing.T) {
	c := NewMemCache(false)
	require.NoError(t, c.Store("k", "v"))

	type result struct {
		value string
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := c.Consume("k")
			results <- result{value: value, err: err}
		}()
	}
	wg.Wait()
	close(results)

	values := make([]string, 0, 2)
	for result := range results {
		require.NoError(t, result.err)
		values = append(values, result.value)
	}
	require.ElementsMatch(t, []string{"v", ""}, values)
}

func newTestRedisClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	cli := redis.NewClient(&redis.Options{Addr: s.Addr()})
	return s, cli
}

func TestRedisICache_Basic(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := NewRedisICacheWithClient(cli, false)
	require.NoError(t, c.Store("k", "v"))

	require.True(t, c.Exists("k"))
	got, err := c.Load("k")
	require.NoError(t, err)
	require.Equal(t, "v", got)
}

func TestRedisICache_List(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := NewRedisICacheWithClient(cli, false)
	require.NoError(t, c.Store("k1", "v1"))
	require.NoError(t, c.Store("k2", "v2"))

	vals, err := c.List()
	require.NoError(t, err)
	require.Len(t, vals, 2)
	m := map[string]bool{"v1": false, "v2": false}
	for _, v := range vals {
		m[v] = true
	}
	require.True(t, m["v1"] && m["v2"])
}

func TestRedisICache_Delete(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := NewRedisICacheWithClient(cli, false)
	require.NoError(t, c.Store("k", "v"))
	require.True(t, c.Exists("k"))
	require.NoError(t, c.Delete("k"))
	require.False(t, c.Exists("k"))
	got, err := c.Load("k")
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestRedisICache_Consume(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := NewRedisICacheWithClient(cli, false)
	require.NoError(t, c.Store("k", "v"))
	value, err := c.Consume("k")
	require.NoError(t, err)
	require.Equal(t, "v", value)
	value, err = c.Consume("k")
	require.NoError(t, err)
	require.Empty(t, value)
}

func TestRedisICache_NoCacheFlag(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()
	c := NewRedisICacheWithClient(cli, true)
	require.True(t, c.IsCacheDisabled())
}

func TestRedisICache_CustomTTLAndPrefix(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()
	ttl := 2 * time.Second
	prefix := "t:"
	c := NewRedisICache(cli, false, ttl, prefix).(*RedisICache)
	require.NoError(t, c.Store("kk", "vv"))

	ctx := context.Background()
	keys, _, err := cli.Scan(ctx, 0, prefix+"*", 100).Result()
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, prefix+"kk", keys[0])

	dur, err := cli.TTL(ctx, keys[0]).Result()
	require.NoError(t, err)
	require.Greater(t, int64(dur), int64(0))
	require.LessOrEqual(t, dur, ttl)
}
