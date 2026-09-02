package schedulelock

import (
	"context"
	"testing"

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
