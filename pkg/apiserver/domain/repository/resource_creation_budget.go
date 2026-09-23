package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// ReserveResourceCreation consumes one global create-attempt permit. A
// positive wait means no permit was consumed: callers must wait and retry.
// It does not reserve running capacity, and it is not a Kubernetes API limiter.
func ReserveResourceCreation(ctx context.Context, store datastore.DataStore) (time.Duration, error) {
	transactional, ok := store.(datastore.ReadCommittedTransactional)
	if !ok {
		return 0, fmt.Errorf("resource creation budget requires read-committed transactions")
	}
	var wait time.Duration
	err := transactional.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		// Reuse the admission policy lock order. This also serializes first-row
		// initialization and policy reads across API and Worker replicas.
		locked, err := tx.CompareAndSwap(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}, "type", model.SystemSettingTypeWorkflowScheduler, map[string]interface{}{})
		if err != nil {
			return err
		}
		if !locked {
			return datastore.ErrRecordNotExist
		}
		policy, err := LoadJobSchedulerPolicy(ctx, tx)
		if err != nil {
			return err
		}
		now, err := currentWorkflowDatabaseTime(ctx, tx)
		if err != nil {
			return err
		}
		budget := &model.ResourceCreationBudget{ID: "jobs-and-sandboxes"}
		err = tx.Get(ctx, budget)
		create := errors.Is(err, datastore.ErrRecordNotExist)
		if err != nil && !create {
			return err
		}
		// Round upward to the database's microsecond precision so persistence
		// cannot shorten the interval and exceed the configured rate.
		interval := time.Duration(math.Ceil(1e6/policy.ResourceCreationQPS)) * time.Microsecond
		unknownInterval := !create && budget.IntervalMicros == 0
		if unknownInterval {
			budget.InitializeUnknownInterval(now, interval, policy.ResourceCreationBurst)
		}
		previousInterval := time.Duration(budget.IntervalMicros) * time.Microsecond
		next, delay := resourceCreationReservation(now, budget.AvailableAt, previousInterval, interval, policy.ResourceCreationBurst)
		wait = delay
		intervalMicros := int64(interval / time.Microsecond)
		changed := create || unknownInterval || budget.IntervalMicros != intervalMicros || !budget.AvailableAt.Equal(next)
		budget.AvailableAt = next
		budget.IntervalMicros = intervalMicros
		if !changed {
			return nil
		}
		if create {
			return tx.Add(ctx, budget)
		}
		return tx.Put(ctx, budget)
	})
	if err != nil {
		return 0, fmt.Errorf("reserve resource creation: %w", err)
	}
	return wait, nil
}

func resourceCreationReservation(now, available time.Time, previousInterval, interval time.Duration, burst int) (time.Time, time.Duration) {
	if available.Before(now) {
		available = now
	} else if previousInterval > 0 && previousInterval != interval && available.After(now) {
		// AvailableAt is virtual scheduling time expressed in units of the
		// previous interval. Preserve the outstanding token debt, including
		// fractional progress toward the next permit, when QPS changes online.
		remaining := available.Sub(now)
		whole, remainder := remaining/previousInterval, remaining%previousInterval
		// Convert the fractional token in microseconds and round upward so the
		// precision:6 column cannot shorten the rescaled debt on persistence.
		previousMicros := int64(previousInterval / time.Microsecond)
		intervalMicros := int64(interval / time.Microsecond)
		remainderMicros := int64((remainder + time.Microsecond - 1) / time.Microsecond)
		fractionMicros := (remainderMicros*intervalMicros + previousMicros - 1) / previousMicros
		fraction := time.Duration(fractionMicros) * time.Microsecond
		scaled := whole*interval + fraction
		available = now.Add(scaled)
	}
	earliest := available.Add(-time.Duration(burst-1) * interval)
	if earliest.After(now) {
		return available, earliest.Sub(now)
	}
	return available.Add(interval), 0
}
