package approvaltimeout

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterAndCancel(t *testing.T) {
	var cancelledCount atomic.Int32
	id := Register("task-approval-timeout-1", func() {
		cancelledCount.Add(1)
	})
	require.NotZero(t, id)

	require.True(t, Cancel("task-approval-timeout-1"))
	require.Equal(t, int32(1), cancelledCount.Load())
	require.False(t, Cancel("task-approval-timeout-1"))
}

func TestRegisterReplacesExistingTimer(t *testing.T) {
	var firstCancelled atomic.Int32
	var secondCancelled atomic.Int32

	firstID := Register("task-approval-timeout-2", func() {
		firstCancelled.Add(1)
	})
	require.NotZero(t, firstID)

	secondID := Register("task-approval-timeout-2", func() {
		secondCancelled.Add(1)
	})
	require.NotZero(t, secondID)
	require.NotEqual(t, firstID, secondID)
	require.Equal(t, int32(1), firstCancelled.Load())

	Unregister("task-approval-timeout-2", firstID)
	require.True(t, Cancel("task-approval-timeout-2"))
	require.Equal(t, int32(1), secondCancelled.Load())
}

func TestUnregisterCurrentTimer(t *testing.T) {
	var cancelledCount atomic.Int32
	id := Register("task-approval-timeout-3", func() {
		cancelledCount.Add(1)
	})
	require.NotZero(t, id)

	Unregister("task-approval-timeout-3", id)
	require.False(t, Cancel("task-approval-timeout-3"))
	require.Equal(t, int32(0), cancelledCount.Load())
}
