package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWatchRuntimeShutdownPreservesRuntimeContextDuringDrain(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	runtimeCtx, cancelRuntime := newRuntimeLifecycleContext(parentCtx)
	t.Cleanup(cancelRuntime)

	drainStarted := make(chan struct{})
	finishDrain := make(chan struct{})
	watchRuntimeShutdown(parentCtx, runtimeCtx, func() {
		close(drainStarted)
		<-finishDrain
		cancelRuntime()
	})
	cancelParent()

	select {
	case <-drainStarted:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not start after parent cancellation")
	}
	require.NoError(t, runtimeCtx.Err())
	require.ErrorIs(t, parentCtx.Err(), context.Canceled)

	close(finishDrain)
	select {
	case <-runtimeCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("runtime context was not cancelled after drain")
	}
}

func TestRunBootstrapStepStopsWhenParentIsCancelled(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	server := &restServer{}
	err := server.runBootstrapStep(parentCtx, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	require.ErrorIs(t, err, context.Canceled)
}
