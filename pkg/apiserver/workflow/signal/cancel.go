package signal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// cancelKeyPrefix is the Redis prefix for workflow cancellation keys.
	cancelKeyPrefix = "eruun:workflow:cancel:"
	// defaultExpiry defines how long the cancellation key should live before expiring.
	defaultExpiry = 45 * time.Second
	// cancelCheckInterval defines how often to poll for cancel markers.
	cancelCheckInterval = 1 * time.Second
)

var (
	ErrCancelSignalBackendUnavailable = errors.New("workflow cancel signal backend unavailable")
	ErrInfrastructureStop             = errors.New("workflow execution stopped by infrastructure")
)

// IsInfrastructureStop reports whether ctx was cancelled because the current
// runtime must stop producing side effects and leave recovery to the lease
// reaper. User cancellation continues to use the regular cancellation signal.
func IsInfrastructureStop(ctx context.Context) bool {
	return ctx != nil && errors.Is(context.Cause(ctx), ErrInfrastructureStop)
}

// CancelWatcher coordinates redis-backed cancellation signalling for a workflow task.
type CancelWatcher struct {
	cli      *redis.Client
	key      string
	stopCh   chan struct{}
	once     sync.Once
	wg       sync.WaitGroup
	state    *cancelState
	taskID   string
	cancelFn context.CancelFunc
}

type cancelState struct {
	mu     sync.RWMutex
	reason string
}

func (c *cancelState) set(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reason = reason
}

func (c *cancelState) get() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reason
}

// Watch establishes a cancellation watcher for the given workflow task. When Redis
// is not configured, setup fails so callers do not silently lose cross-process cancellation.
// Note: Prefer WatchWithClient for dependency injection.
func Watch(ctx context.Context, taskID string) (*CancelWatcher, context.Context, context.CancelFunc, error) {
	return WatchWithClient(ctx, taskID, nil)
}

// WatchWithClient is like Watch but accepts an explicit Redis client for dependency injection.
// This variant enables unit testing with mock Redis clients.
func WatchWithClient(ctx context.Context, taskID string, cli *redis.Client) (*CancelWatcher, context.Context, context.CancelFunc, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, ctx, nil, fmt.Errorf("taskID is required")
	}
	if cli == nil {
		return nil, ctx, nil, fmt.Errorf("%w: redis client is nil", ErrCancelSignalBackendUnavailable)
	}

	key := cancelKeyPrefix + taskID
	watcher := &CancelWatcher{
		cli:    cli,
		key:    key,
		stopCh: make(chan struct{}),
		state:  &cancelState{},
		taskID: taskID,
	}

	existing, err := cli.Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, ctx, nil, fmt.Errorf("inspect cancel key: %w", err)
	}

	derivedCtx, cancelFn := context.WithCancel(ctx)
	derivedCtx = context.WithValue(derivedCtx, cancelStateKey{}, watcher.state)
	watcher.cancelFn = cancelFn
	if isCancelledToken(existing) {
		watcher.state.set(extractCancelReason(existing))
		cancelFn()
		return watcher, derivedCtx, cancelFn, nil
	}

	watcher.wg.Add(1)
	go watcher.maintain(derivedCtx, cancelFn)

	return watcher, derivedCtx, cancelFn, nil
}

// Cancel marks the workflow task as cancelled. Running watchers will detect the
// marker and cancel their contexts.
// Note: Prefer CancelWithClient for dependency injection.
func Cancel(ctx context.Context, taskID, reason string) error {
	return CancelWithClient(ctx, taskID, reason, nil)
}

// CancelWithClient is like Cancel but accepts an explicit Redis client for dependency injection.
func CancelWithClient(ctx context.Context, taskID, reason string, cli *redis.Client) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("taskID is required")
	}
	if cli == nil {
		return fmt.Errorf("%w: redis client is nil", ErrCancelSignalBackendUnavailable)
	}
	value := cancelMarker(reason)
	return cli.Set(ctx, cancelKeyPrefix+taskID, value, defaultExpiry).Err()
}

// Stop stops polling for cancellation markers. The marker itself is retained
// until Redis expires it so every watcher for the same task can observe it.
func (w *CancelWatcher) Stop(ctx context.Context) {
	if w == nil {
		return
	}
	w.once.Do(func() {
		if w.stopCh != nil {
			close(w.stopCh)
		}
		w.wg.Wait()
	})
}

// Reason returns the cancellation reason observed by the watcher, if any.
func (w *CancelWatcher) Reason() string {
	if w == nil || w.state == nil {
		return ""
	}
	return w.state.get()
}

func (w *CancelWatcher) maintain(ctx context.Context, cancelFn context.CancelFunc) {
	defer w.wg.Done()
	checkTicker := time.NewTicker(cancelCheckInterval)
	defer checkTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-checkTicker.C:
			w.step(ctx, cancelFn)
		}
	}
}

func (w *CancelWatcher) step(ctx context.Context, cancelFn context.CancelFunc) {
	if w.cli == nil {
		return
	}
	val, err := w.cli.Get(ctx, w.key).Result()
	if err == redis.Nil {
		return
	}
	if err != nil {
		return
	}
	if isCancelledToken(val) {
		w.state.set(extractCancelReason(val))
		cancelFn()
	}
}

func cancelMarker(reason string) string {
	trimmed := strings.TrimSpace(reason)
	if trimmed == "" {
		trimmed = "cancelled"
	}
	return "cancelled:" + trimmed
}

func isCancelledToken(val string) bool {
	return strings.HasPrefix(val, "cancelled:")
}

func extractCancelReason(val string) string {
	if !isCancelledToken(val) {
		return "cancelled"
	}
	parts := strings.SplitN(val, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return "cancelled"
	}
	return parts[1]
}

type cancelStateKey struct{}

// ReasonFromContext retrieves the cancellation reason set by the watcher.
func ReasonFromContext(ctx context.Context) string {
	raw := ctx.Value(cancelStateKey{})
	if raw == nil {
		return ""
	}
	if state, ok := raw.(*cancelState); ok {
		return state.get()
	}
	return ""
}
