package workflow

import (
	"context"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// These counters belong to the Workflow instance, so draining and replacement
// consumer generations share the same measurements and concurrency budget.
type workflowRuntimeCounters struct {
	controllers      atomic.Int64
	waiting          atomic.Int64
	heartbeats       atomic.Int64
	slotAcquisitions atomic.Uint64
	slotWaitNanos    atomic.Uint64
	renewAttempts    atomic.Uint64
	renewErrors      atomic.Uint64
	renewRejected    atomic.Uint64
	renewNanoseconds atomic.Uint64
}

// RuntimeStats is a low-cost process-local sample. Counters are cumulative;
// gauges are sampled independently and are not a transactional snapshot.
// Controllers include admission wait and finalization, not just running Jobs.
type RuntimeStats struct {
	ConcurrencyLimit int64
	Controllers      int64
	WaitingForSlot   int64
	Heartbeats       int64
	SlotAcquisitions uint64
	SlotWaitSeconds  float64
	RenewAttempts    uint64
	RenewErrors      uint64
	RenewRejected    uint64
	RenewSeconds     float64
}

func (w *Workflow) RuntimeStats() RuntimeStats {
	s := &w.runtimeStats
	return RuntimeStats{
		ConcurrencyLimit: w.maxWorkflowConcurrency(),
		Controllers:      s.controllers.Load(), WaitingForSlot: s.waiting.Load(),
		Heartbeats: s.heartbeats.Load(), SlotAcquisitions: s.slotAcquisitions.Load(),
		SlotWaitSeconds: float64(s.slotWaitNanos.Load()) / float64(time.Second),
		RenewAttempts:   s.renewAttempts.Load(), RenewErrors: s.renewErrors.Load(),
		RenewRejected: s.renewRejected.Load(),
		RenewSeconds:  float64(s.renewNanoseconds.Load()) / float64(time.Second),
	}
}

func (w *Workflow) acquireWorkflowSlot(ctx context.Context, limiter *semaphore.Weighted) (func(), error) {
	started := time.Now()
	if limiter != nil {
		w.runtimeStats.waiting.Add(1)
		err := limiter.Acquire(ctx, 1)
		w.runtimeStats.waiting.Add(-1)
		if err != nil {
			return nil, err
		}
	}
	w.runtimeStats.slotWaitNanos.Add(uint64(time.Since(started)))
	w.runtimeStats.slotAcquisitions.Add(1)
	w.runtimeStats.controllers.Add(1)
	return func() {
		w.runtimeStats.controllers.Add(-1)
		if limiter != nil {
			limiter.Release(1)
		}
	}, nil
}
