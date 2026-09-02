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
