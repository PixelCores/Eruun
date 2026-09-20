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
		next, delay := resourceCreationReservation(now, budget.AvailableAt, interval, policy.ResourceCreationBurst)
		wait = delay
		if wait > 0 {
			return nil
		}
		budget.AvailableAt = next
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

func resourceCreationReservation(now, available time.Time, interval time.Duration, burst int) (time.Time, time.Duration) {
	if available.Before(now) {
		available = now
	}
	earliest := available.Add(-time.Duration(burst-1) * interval)
	if earliest.After(now) {
		return available, earliest.Sub(now)
	}
	return available.Add(interval), 0
}
