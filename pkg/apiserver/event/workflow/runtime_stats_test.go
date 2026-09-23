package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestRuntimeStatsCountsBlockedAndCancelledSlotAcquisition(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Workflow.MaxConcurrentWorkflows = 1
	w := &Workflow{Cfg: cfg}
	limiter := w.workerConcurrencyLimiter()
	release, err := w.acquireWorkflowSlot(context.Background(), limiter)
	require.NoError(t, err)
	require.EqualValues(t, 1, w.RuntimeStats().Controllers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := w.acquireWorkflowSlot(ctx, limiter)
		done <- err
	}()
	require.Eventually(t, func() bool { return w.RuntimeStats().WaitingForSlot == 1 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	stats := w.RuntimeStats()
	require.Zero(t, stats.WaitingForSlot)
	require.EqualValues(t, 1, stats.Controllers)
	require.EqualValues(t, 1, stats.SlotAcquisitions)
	release()
	require.Zero(t, w.RuntimeStats().Controllers)
	// A cancelled waiter must neither release another controller's slot nor
	// retain capacity after the owner exits.
	release, err = w.acquireWorkflowSlot(context.Background(), limiter)
	require.NoError(t, err)
	release()
	require.EqualValues(t, 2, w.RuntimeStats().SlotAcquisitions)
}

func TestRuntimeStatsSharedAcrossConcurrentWorkerGenerations(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Workflow.MaxConcurrentWorkflows = 4
	w := &Workflow{Cfg: cfg}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			run := newWorkflowWorkerRun(ctx, w.workerConcurrencyLimiter())
			release, err := w.acquireWorkflowSlot(ctx, run.limiter)
			if err != nil {
				t.Error(err)
				return
			}
			if stats := w.RuntimeStats(); stats.Controllers > 4 || stats.Controllers < 1 {
				t.Errorf("invalid active controller count: %d", stats.Controllers)
			}
			release()
		})
	}
	wg.Wait()
	stats := w.RuntimeStats()
	require.Zero(t, stats.Controllers)
	require.Zero(t, stats.WaitingForSlot)
	require.EqualValues(t, 32, stats.SlotAcquisitions)
}
