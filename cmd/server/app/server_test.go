package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/cmd/server/app/options"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestEnsureLogDir(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		require.NoError(t, ensureLogDir(""))
	})

	t.Run("create directory", func(t *testing.T) {
		root := t.TempDir()
		logDir := filepath.Join(root, "logs")
		require.NoError(t, ensureLogDir(logDir))
		info, err := os.Stat(logDir)
		require.NoError(t, err)
		require.True(t, info.IsDir())
	})

	t.Run("invalid path returns error", func(t *testing.T) {
		root := t.TempDir()
		blocker := filepath.Join(root, "blocker")
		require.NoError(t, os.WriteFile(blocker, []byte("x"), 0644))

		err := ensureLogDir(filepath.Join(blocker, "logs"))
		require.Error(t, err)
	})
}

func TestNewAPIServerCommandUsesBinaryName(t *testing.T) {
	require.Equal(t, "eruun-server", NewAPIServerCommand().Use)
}

func TestRunMigrateOnlySkipsRuntimeStartup(t *testing.T) {
	original := migrateSchema
	t.Cleanup(func() { migrateSchema = original })
	called := false
	migrateSchema = func(_ context.Context, cfg config.Config) error {
		called = true
		require.Equal(t, config.DatastoreSchemaModeMigrateOnly, cfg.NormalizedDatastoreSchemaMode())
		return nil
	}
	serverOptions := options.NewServerRunOptions()
	serverOptions.GenericServerRunOptions.DatastoreSchemaMode = config.DatastoreSchemaModeMigrateOnly

	require.NoError(t, Run(serverOptions))
	require.True(t, called)
}

func TestRunMigrateOnlyReturnsMigrationFailure(t *testing.T) {
	original := migrateSchema
	t.Cleanup(func() { migrateSchema = original })
	expected := errors.New("migration failed")
	migrateSchema = func(context.Context, config.Config) error { return expected }
	serverOptions := options.NewServerRunOptions()
	serverOptions.GenericServerRunOptions.DatastoreSchemaMode = config.DatastoreSchemaModeMigrateOnly

	require.ErrorIs(t, Run(serverOptions), expected)
}

func TestWaitForServerRunCancelsAndWaitsAfterTerminationSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	term := make(chan os.Signal, 1)
	runtimeErrors := make(chan error)
	runDone := make(chan error, 1)
	result := make(chan error, 1)

	go func() {
		result <- waitForServerRun(term, runtimeErrors, runDone, cancel, time.Second)
	}()
	term <- syscall.SIGTERM

	require.Eventually(t, func() bool {
		return errors.Is(ctx.Err(), context.Canceled)
	}, time.Second, time.Millisecond)
	select {
	case <-result:
		t.Fatal("server command returned before APIServer.Run completed")
	default:
	}

	runDone <- nil
	require.NoError(t, <-result)
}

func TestWaitForServerRunPreservesRuntimeErrorWhileWaitingForShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	term := make(chan os.Signal)
	runtimeErrors := make(chan error, 1)
	runDone := make(chan error, 1)
	expected := errors.New("worker failed")
	runtimeErrors <- expected
	result := make(chan error, 1)

	go func() {
		result <- waitForServerRun(term, runtimeErrors, runDone, cancel, time.Second)
	}()
	require.Eventually(t, func() bool {
		return errors.Is(ctx.Err(), context.Canceled)
	}, time.Second, time.Millisecond)
	runDone <- nil

	require.ErrorIs(t, <-result, expected)
}

func TestWaitForServerRunBoundsShutdownWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	term := make(chan os.Signal, 1)
	runtimeErrors := make(chan error)
	runDone := make(chan error)
	term <- syscall.SIGTERM

	err := waitForServerRun(term, runtimeErrors, runDone, cancel, 20*time.Millisecond)

	require.ErrorIs(t, err, errServerShutdownTimedOut)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestServerShutdownWaitTimeoutCoversConcurrentDrainPhases(t *testing.T) {
	require.Equal(t, config.DefaultWorkerDrainTimeout+5*time.Second, serverShutdownWaitTimeout(nil))

	serverOptions := options.NewServerRunOptions()
	serverOptions.GenericServerRunOptions.Workflow.WorkerDrainTimeout = 10 * time.Second
	require.Equal(t, config.DefaultHTTPShutdownTimeout+5*time.Second, serverShutdownWaitTimeout(serverOptions))
}
