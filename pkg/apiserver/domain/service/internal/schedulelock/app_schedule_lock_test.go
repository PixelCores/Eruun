package schedulelock

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestWithAppScheduleLockNormalizesApplicationID(t *testing.T) {
	lockProvider := locker.NewMemoryLocker("test-app-schedule")
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- WithAppScheduleLock(context.Background(), lockProvider, " App-1 ", "first", false, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	err := WithAppScheduleLock(context.Background(), lockProvider, "app-1", "second", false, func(context.Context) error {
		return nil
	})
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)

	close(release)
	require.NoError(t, <-done)
}

// Advance the production renewal interval without adding a runtime timing knob.
func TestWithAppScheduleLockCancelsOperationAfterRenewalFailure(t *testing.T) {
	for _, ignoreCancellation := range []bool{false, true} {
		t.Run(fmt.Sprint(ignoreCancellation), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				renewErr := errors.New("injected renewal failure")
				mutex := &renewalTestMutex{Mutex: locker.NewNoopLocker("").NewMutex("app"), renewErr: renewErr}
				provider := &renewalTestLocker{mutex: mutex}
				err := WithAppScheduleLock(context.Background(), provider, "app", "submit", true, func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						if ignoreCancellation {
							return nil
						}
						return ctx.Err()
					case <-time.After(appScheduleLockTTL):
						t.Error("operation remained active after lock renewal failed")
						return nil
					}
				})
				require.ErrorIs(t, err, bcode.ErrDistributedLockUnavailable)
				require.ErrorIs(t, err, renewErr)
				require.True(t, mutex.unlocked)
			})
		})
	}
}

type renewalTestLocker struct{ mutex locker.Mutex }

func (l *renewalTestLocker) NewMutex(string, ...locker.Option) locker.Mutex { return l.mutex }
func (l *renewalTestLocker) Close() error                                   { return nil }

type renewalTestMutex struct {
	locker.Mutex
	renewErr error
	renewals int
	unlocked bool
}

func (m *renewalTestMutex) Extend(context.Context) error { m.renewals++; return m.renewErr }
func (m *renewalTestMutex) Unlock(context.Context) error { m.unlocked = true; return nil }

func TestWithAppScheduleLockStopsRenewalBeforeUnlock(t *testing.T) {
	for _, outcome := range []string{"success", "panic", "parent cancelled after commit"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mutex := &renewalTestMutex{Mutex: locker.NewNoopLocker("").NewMutex("app")}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				run := func() {
					err := WithAppScheduleLock(ctx, &renewalTestLocker{mutex: mutex}, "app", "submit", true, func(context.Context) error {
						time.Sleep(appScheduleLockTTL)
						if outcome == "panic" {
							panic("operation panic")
						}
						if outcome == "parent cancelled after commit" {
							cancel()
						}
						return nil
					})
					require.NoError(t, err)
				}
				if outcome == "panic" {
					require.Panics(t, run)
				} else {
					run()
				}
				require.True(t, mutex.unlocked)
				require.GreaterOrEqual(t, mutex.renewals, 2)
				completedRenewals := mutex.renewals
				time.Sleep(appScheduleLockTTL)
				require.Equal(t, completedRenewals, mutex.renewals, "renewal must stop before returning or propagating a panic")
			})
		})
	}
}
