package signal

import (
	"context"
	"errors"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCancelWatcherReceivesSignal(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Skipf("start miniredis: %v", err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	watcher, jobCtx, cancelFn, err := WatchWithClient(context.Background(), "task-cancel-test", client)
	if err != nil {
		t.Fatalf("watcher setup failed: %v", err)
	}
	defer cancelFn()

	done := make(chan struct{})
	go func() {
		<-jobCtx.Done()
		close(done)
	}()

	if err := CancelWithClient(context.Background(), "task-cancel-test", "manual stop", client); err != nil {
		t.Fatalf("send cancel signal: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected cancel signal to close context")
	}

	if reason := watcher.Reason(); reason != "manual stop" {
		t.Fatalf("unexpected cancel reason: %s", reason)
	}

	watcher.Stop(context.Background())
	if exists, _ := client.Exists(context.Background(), cancelKeyPrefix+"task-cancel-test").Result(); exists != 1 {
		t.Fatalf("expected cancel key to remain until ttl, got %d", exists)
	}
}

func TestCancelWatcherRequiresRedisClient(t *testing.T) {
	watcher, jobCtx, cancelFn, err := WatchWithClient(context.Background(), "task-local", nil)
	if err == nil {
		t.Fatalf("expected missing redis client to fail")
	}
	if !errors.Is(err, ErrCancelSignalBackendUnavailable) {
		t.Fatalf("expected backend unavailable error, got %v", err)
	}
	if watcher != nil {
		t.Fatalf("expected watcher to be nil")
	}
	if jobCtx == nil {
		t.Fatalf("expected original context to be returned")
	}
	if cancelFn != nil {
		t.Fatalf("expected cancel func to be nil")
	}
}

func TestCancelRequiresRedisClient(t *testing.T) {
	err := CancelWithClient(context.Background(), "task-local", "local cancel", nil)
	if err == nil {
		t.Fatalf("expected missing redis client to fail")
	}
	if !errors.Is(err, ErrCancelSignalBackendUnavailable) {
		t.Fatalf("expected backend unavailable error, got %v", err)
	}
}

func TestCancelWatcherMultipleWatchersSameTask(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Skipf("start miniredis: %v", err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	watcher1, jobCtx1, cancelFn1, err := WatchWithClient(context.Background(), "task-multi", client)
	if err != nil {
		t.Fatalf("watcher1 setup failed: %v", err)
	}
	defer cancelFn1()
	defer watcher1.Stop(context.Background())

	watcher2, jobCtx2, cancelFn2, err := WatchWithClient(context.Background(), "task-multi", client)
	if err != nil {
		t.Fatalf("watcher2 setup failed: %v", err)
	}
	defer cancelFn2()
	defer watcher2.Stop(context.Background())

	done1 := make(chan struct{})
	go func() {
		<-jobCtx1.Done()
		close(done1)
	}()
	done2 := make(chan struct{})
	go func() {
		<-jobCtx2.Done()
		close(done2)
	}()

	if err := CancelWithClient(context.Background(), "task-multi", "manual stop", client); err != nil {
		t.Fatalf("send cancel signal: %v", err)
	}

	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected cancel signal to close watcher1 context")
	}
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected cancel signal to close watcher2 context")
	}

	if reason := watcher1.Reason(); reason != "manual stop" {
		t.Fatalf("unexpected watcher1 cancel reason: %s", reason)
	}
	if reason := watcher2.Reason(); reason != "manual stop" {
		t.Fatalf("unexpected watcher2 cancel reason: %s", reason)
	}
}
