package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitForPolledResourceReturnsWhenReady(t *testing.T) {
	attempts := 0
	err := waitForPolledResource(context.Background(), pollWaitOptions{
		timeout:  100 * time.Millisecond,
		interval: time.Millisecond,
		poll: func(context.Context) (bool, error) {
			attempts++
			return attempts >= 2, nil
		},
	})

	require.NoError(t, err)
	require.GreaterOrEqual(t, attempts, 2)
}

func TestWaitForPolledResourceReturnsMappedTimeout(t *testing.T) {
	expected := errors.New("timeout")
	err := waitForPolledResource(context.Background(), pollWaitOptions{
		timeout:  5 * time.Millisecond,
		interval: time.Millisecond,
		poll: func(context.Context) (bool, error) {
			return false, nil
		},
		onTimeout: func() error {
			return expected
		},
	})

	require.ErrorIs(t, err, expected)
}

func TestWaitForPolledResourceTimeoutCancelsInFlightPoll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	expected := errors.New("resource timeout")
	polls := 0
	err := waitForPolledResource(ctx, pollWaitOptions{
		timeout:  20 * time.Millisecond,
		interval: time.Millisecond,
		poll: func(ctx context.Context) (bool, error) {
			polls++
			<-ctx.Done()
			return false, ctx.Err()
		},
		onTimeout: func() error { return expected },
	})

	require.ErrorIs(t, err, expected)
	require.Equal(t, 1, polls)
	require.NoError(t, ctx.Err(), "resource timeout must expire before the parent deadline")
}

func TestWaitForPolledResourceCancellationDuringPollUsesCancelHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	expected := errors.New("cancelled")
	err := waitForPolledResource(ctx, pollWaitOptions{
		timeout:  time.Second,
		interval: time.Millisecond,
		poll: func(ctx context.Context) (bool, error) {
			cancel()
			return false, ctx.Err()
		},
		onCancel: func(err error) error {
			require.ErrorIs(t, err, context.Canceled)
			return expected
		},
		onError: func(error) error {
			t.Fatal("cancellation must not be reported as a polling failure")
			return nil
		},
	})

	require.ErrorIs(t, err, expected)
}

func TestWaitForPolledResourceTimeoutWithoutMapperReturnsError(t *testing.T) {
	err := waitForPolledResource(context.Background(), pollWaitOptions{
		timeout:  time.Millisecond,
		interval: time.Second,
		poll:     func(context.Context) (bool, error) { return false, nil },
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWaitForPolledResourceReturnsMappedPollError(t *testing.T) {
	cause := errors.New("poll failed")
	expected := errors.New("mapped poll failure")
	err := waitForPolledResource(context.Background(), pollWaitOptions{
		timeout:  time.Second,
		interval: time.Millisecond,
		poll:     func(context.Context) (bool, error) { return false, cause },
		onError: func(err error) error {
			require.ErrorIs(t, err, cause)
			return expected
		},
	})

	require.ErrorIs(t, err, expected)
}
