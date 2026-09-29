package cache

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMemCache_BasicStoreLoad(t *testing.T) {
	c := NewMemCache(false)
	c.(*MemCache).ttl = time.Second

	if err := c.Store(context.Background(), "k", "v"); err != nil {
		t.Fatalf("store error: %v", err)
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
	mc.mu.Lock()
	defer mc.mu.Unlock()
	require.NotContains(t, mc.items, "k")
}

func TestMemCache_Delete(t *testing.T) {
	c := NewMemCache(false)
	require.NoError(t, c.Store(context.Background(), "k", "v"))
	require.NoError(t, c.Delete(context.Background(), "k"))
	got, err := c.Load(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, "", got)
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

func newTestRedisClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	cli := redis.NewClient(&redis.Options{Addr: s.Addr()})
	return s, cli
}

func newTestRedisCache(t *testing.T, cli *redis.Client, noCache bool) ICache {
	t.Helper()
	c, err := NewRedisICache(cli, noCache, 0, "")
	require.NoError(t, err)
	return c
}

func TestNewRedisICacheRequiresClient(t *testing.T) {
	c, err := NewRedisICache(nil, false, 0, "")
	require.Nil(t, c)
	require.ErrorContains(t, err, "redis cache client is not initialized")
}

func TestRedisICache_Basic(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := newTestRedisCache(t, cli, false)
	require.NoError(t, c.Store(context.Background(), "k", "v"))

	got, err := c.Load(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, "v", got)
}

func TestRedisICache_Delete(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()

	c := newTestRedisCache(t, cli, false)
	require.NoError(t, c.Store(context.Background(), "k", "v"))
	require.NoError(t, c.Delete(context.Background(), "k"))
	got, err := c.Load(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestRedisICache_NoCacheFlag(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()
	c := newTestRedisCache(t, cli, true)
	require.True(t, c.IsCacheDisabled())
}

func TestRedisICache_CustomTTLAndPrefix(t *testing.T) {
	s, cli := newTestRedisClient(t)
	defer s.Close()
	ttl := 2 * time.Second
	prefix := "t:"
	c, err := NewRedisICache(cli, false, ttl, prefix)
	require.NoError(t, err)
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
			err := operation.run(ctx, newTestRedisCache(t, cli, false))
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
	_, err := newTestRedisCache(t, cli, false).Load(ctx, "k")
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
	require.ErrorIs(t, c.Delete(ctx, "k"), context.Canceled)
	value, err := c.Load(t.Context(), "k")
	require.NoError(t, err)
	require.Equal(t, "original", value)
}

type invalidationCache struct {
	ICache
	delete func(context.Context, string) error
}

func (c *invalidationCache) Delete(ctx context.Context, key string) error {
	return c.delete(ctx, key)
}

func TestInvalidateAfterWritePreservesValuesAndDeletesStaleData(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		for _, state := range []string{"active", "cancelled", "expired"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				c := NewMemCache(false)
				if backend == "redis" {
					srv := miniredis.RunT(t)
					cli := redis.NewClient(&redis.Options{Addr: srv.Addr(), ContextTimeoutEnabled: true})
					t.Cleanup(func() { require.NoError(t, cli.Close()) })
					c = newTestRedisCache(t, cli, false)
				}
				require.NoError(t, c.Store(t.Context(), "changed", "stale"))
				require.NoError(t, c.Store(t.Context(), "unrelated", "current"))
				type scopeKey struct{}
				ctx := context.WithValue(t.Context(), scopeKey{}, "workspace-1")
				switch state {
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "expired":
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer cancel()
				}
				var cleanupCtx context.Context
				observed := &invalidationCache{delete: func(ctx context.Context, key string) error {
					cleanupCtx = ctx
					require.NoError(t, ctx.Err())
					require.Equal(t, "workspace-1", ctx.Value(scopeKey{}))
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.Positive(t, time.Until(deadline))
					require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
					return c.Delete(ctx, key)
				}}
				require.NoError(t, InvalidateAfterWrite(ctx, observed, "changed"))
				require.ErrorIs(t, cleanupCtx.Err(), context.Canceled)
				value, err := c.Load(t.Context(), "changed")
				require.NoError(t, err)
				require.Empty(t, value)
				value, err = c.Load(t.Context(), "unrelated")
				require.NoError(t, err)
				require.Equal(t, "current", value)
			})
		}
	}
}

func TestInvalidateAfterWriteBoundsBlockedDeletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		c := &invalidationCache{delete: func(ctx context.Context, _ string) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		start := time.Now()
		err := InvalidateAfterWrite(ctx, c, "changed")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 5*time.Second, time.Since(start))
	})
}
