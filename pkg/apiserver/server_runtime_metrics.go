package apiserver

import (
	"context"
	"runtime"
	"time"

	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow"
)

// Sample once per process, including controller generations still draining.
// The existing structured log stream carries measurements without exposing a
// new unauthenticated diagnostics endpoint or adding per-task metric labels.
func (s *restServer) runWorkerRuntimeMetrics(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		for _, worker := range s.eventWorkers {
			w, ok := worker.(*workflow.Workflow)
			if !ok {
				continue
			}
			stats := w.RuntimeStats()
			klog.InfoS("workflow runtime stats",
				"concurrencyLimit", stats.ConcurrencyLimit, "controllers", stats.Controllers,
				"waitingForSlot", stats.WaitingForSlot, "heartbeats", stats.Heartbeats,
				"slotAcquisitions", stats.SlotAcquisitions, "slotWaitSecondsTotal", stats.SlotWaitSeconds,
				"leaseRenewAttempts", stats.RenewAttempts, "leaseRenewErrors", stats.RenewErrors,
				"leaseRenewRejected", stats.RenewRejected, "leaseRenewSecondsTotal", stats.RenewSeconds,
				"goroutines", runtime.NumGoroutine(), "heapAllocBytes", memory.HeapAlloc,
				"heapInuseBytes", memory.HeapInuse, "gcCycles", memory.NumGC,
				"gcPauseSecondsTotal", float64(memory.PauseTotalNs)/float64(time.Second))
		}
	}
}
