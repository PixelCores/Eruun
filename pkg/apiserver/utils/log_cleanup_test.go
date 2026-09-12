package utils

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartLogCleanupStopsOnCancellation(t *testing.T) {
	logDir := t.TempDir()
	oldLog := filepath.Join(logDir, "eruun-server.host.user.log.INFO.20000101-000000.1")
	unrelated := filepath.Join(logDir, "keep.txt")
	require.NoError(t, os.WriteFile(oldLog, []byte("old"), 0600))
	require.NoError(t, os.WriteFile(unrelated, []byte("keep"), 0600))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		StartLogCleanup(ctx, logDir, time.Hour)
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(oldLog)
		return os.IsNotExist(err)
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("log cleanup did not stop after cancellation")
	}
	require.FileExists(t, unrelated)
}

func TestStartLogCleanupSkipsCanceledWork(t *testing.T) {
	logDir := t.TempDir()
	oldLog := filepath.Join(logDir, "eruun-server.host.user.log.INFO.20000101-000000.1")
	require.NoError(t, os.WriteFile(oldLog, nil, 0600))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	StartLogCleanup(ctx, logDir, time.Hour)
	require.FileExists(t, oldLog)
}
