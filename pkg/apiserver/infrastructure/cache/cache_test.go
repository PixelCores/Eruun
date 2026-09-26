package cache

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMemCache_BasicStoreLoad(t *testing.T) {
	c := NewMemCache(false)
	mc := c.(*MemCache)
	mc.ttl = time.Second

	if err := c.Store(context.Background(), "k", "v"); err != nil {
		t.Fatalf("store error: %v", err)
	}
	if !c.Exists(context.Background(), "k") {
		t.Fatalf("expected key to exist")
	}
	got, _ := c.Load(context.Background(), "k")
	if got != "v" {
		t.Fatalf("expected v, got %q", got)
	}
}

func TestMemCache_Expiration(t *testing.T) {
	c := NewMemCache(false)
	mc := c.(*MemCache)
	mc.ttl = 50 * time.Millisecond

	if err := c.Store(context.Background(), "k", "v"); err != nil {
		t.Fatalf("store error: %v", err)
	}
	if got, _ := c.Load(context.Background(), "k"); got != "v" {
		t.Fatalf("expected v before expiry, got %q", got)
	}
	time.Sleep(80 * time.Millisecond)
	if got, _ := c.Load(context.Background(), "k"); got != "" {
		t.Fatalf("expected empty after expiry, got %q", got)
	}
	if c.Exists(context.Background(), "k") {
		t.Fatalf("expected key to be expired and removed")
	}
}

func TestMemCache_Delete(t *testing.T) {
	c := NewMemCache(false)
	require.NoError(t, c.Store(context.Background(), "k", "v"))
	require.True(t, c.Exists(context.Background(), "k"))
	require.NoError(t, c.Delete(context.Background(), "k"))
	require.False(t, c.Exists(context.Background(), "k"))
	got, err := c.Load(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestMemCache_ListExcludesExpiredEntries(t *testing.T) {
	c := NewMemCache(false).(*MemCache)
	c.mu.Lock()
	c.items["expired"] = &item{value: "stale", expiresAt: time.Now().Add(-time.Second)}
	c.items["live"] = &item{value: "current", expiresAt: time.Now().Add(time.Hour)}
	c.items["permanent"] = &item{value: "permanent"}
	c.mu.Unlock()

	values, err := c.List(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"current", "permanent"}, values)
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotContains(t, c.items, "expired")
}

func TestMemCache_StoreReclaimsExpiredEntries(t *testing.T) {
	c := NewMemCache(false).(*MemCache)
	c.mu.Lock()
	c.items["expired"] = &item{value: "stale", expiresAt: time.Now().Add(-time.Second)}
	c.items["permanent"] = &item{value: "permanent"}
	c.mu.Unlock()

	require.NoError(t, c.Store(context.Background(), "new", "current"))
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotContains(t, c.items, "expired")
	require.Contains(t, c.items, "permanent")
	require.Contains(t, c.items, "new")
}

func TestMemCache_ConsumeIsAtomic(t *testing.T) {
	c := NewMemCache(false)
	require.NoError(t, c.Store(context.Background(), "k", "v"))

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
			value, err := c.Consume(context.Background(), "k")
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
	require.NoError(t, c.Store(context.Background(), "k", "v"))

	require.True(t, c.Exists(context.Background(), "k"))
	got, err := c.Load(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, "v", got)
}

func TestRedisICache_List(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := NewRedisICacheWithClient(cli, false)
	require.NoError(t, c.Store(context.Background(), "k1", "v1"))
	require.NoError(t, c.Store(context.Background(), "k2", "v2"))

	vals, err := c.List(context.Background())
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
	require.NoError(t, c.Store(context.Background(), "k", "v"))
	require.True(t, c.Exists(context.Background(), "k"))
	require.NoError(t, c.Delete(context.Background(), "k"))
	require.False(t, c.Exists(context.Background(), "k"))
	got, err := c.Load(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestRedisICache_Consume(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := NewRedisICacheWithClient(cli, false)
	require.NoError(t, c.Store(context.Background(), "k", "v"))
	value, err := c.Consume(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, "v", value)
	value, err = c.Consume(context.Background(), "k")
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
	require.NoError(t, c.Store(context.Background(), "kk", "vv"))

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

func TestRedisICacheCancelledWhileWaitingForConnection(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(context.Context, ICache) error
	}{
		{"store", func(ctx context.Context, c ICache) error { return c.Store(ctx, "k", "v") }},
		{"load", func(ctx context.Context, c ICache) error { _, err := c.Load(ctx, "k"); return err }},
		{"consume", func(ctx context.Context, c ICache) error { _, err := c.Consume(ctx, "k"); return err }},
		{"list", func(ctx context.Context, c ICache) error { _, err := c.List(ctx); return err }},
		{"delete", func(ctx context.Context, c ICache) error { return c.Delete(ctx, "k") }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			srv := miniredis.RunT(t)
			cli := redis.NewClient(&redis.Options{Addr: srv.Addr(), PoolSize: 1, MaxActiveConns: 1, PoolTimeout: 2 * time.Second, MaxRetries: -1, ContextTimeoutEnabled: true})
			t.Cleanup(func() { require.NoError(t, cli.Close()) })
			held := cli.Conn()
			require.NoError(t, held.Ping(t.Context()).Err())
			defer held.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			timer := time.AfterFunc(30*time.Millisecond, cancel)
			defer timer.Stop()
			start := time.Now()
			err := operation.run(ctx, NewRedisICacheWithClient(cli, false))
			require.ErrorIs(t, err, context.Canceled)
			require.Less(t, time.Since(start), time.Second)
		})
	}
}

func TestRedisICacheReadRespectsCallerDeadline(t *testing.T) {
	srv := miniredis.RunT(t)
	cli := redis.NewClient(&redis.Options{Addr: srv.Addr(), ReadTimeout: 3 * time.Second, MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	require.NoError(t, cli.Ping(t.Context()).Err())
	requested := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	srv.Server().SetPreHook(func(_ *server.Peer, command string, _ ...string) bool {
		if strings.EqualFold(command, "get") {
			close(requested)
			<-release
		}
		return false
	})
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := NewRedisICacheWithClient(cli, false).Load(ctx, "k")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-requested:
	default:
		t.Fatal("read never reached Redis")
	}
}

func TestMemCacheCancelledOperationsPreserveEntries(t *testing.T) {
	c := NewMemCache(false)
	require.NoError(t, c.Store(t.Context(), "k", "original"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.Store(ctx, "k", "replacement"), context.Canceled)
	_, err := c.Load(ctx, "k")
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.Consume(ctx, "k")
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.List(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, c.Delete(ctx, "k"), context.Canceled)
	require.False(t, c.Exists(ctx, "k"))
	value, err := c.Load(t.Context(), "k")
	require.NoError(t, err)
	require.Equal(t, "original", value)
}
