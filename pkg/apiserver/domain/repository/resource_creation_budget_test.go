package repository

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestResourceCreationReservationBurstAndSteadyRate(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	available := time.Time{}
	for range 3 {
		next, wait := resourceCreationReservation(now, available, time.Second, time.Second, 3)
		require.Zero(t, wait)
		available = next
	}
	next, wait := resourceCreationReservation(now, available, time.Second, time.Second, 3)
	require.Equal(t, time.Second, wait)
	require.Equal(t, available, next, "a rejected attempt must not extend rate debt")
	next, wait = resourceCreationReservation(now.Add(time.Second), available, time.Second, time.Second, 3)
	require.Zero(t, wait)
	require.Equal(t, now.Add(4*time.Second), next)
	_, wait = resourceCreationReservation(now.Add(time.Second), next, time.Second, time.Second, 1)
	require.Equal(t, 3*time.Second, wait, "lowering burst must preserve existing debt")
}

func TestResourceCreationReservationQPSDecreasePreservesDebt(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldInterval, newInterval := time.Second, 10*time.Second
	available := now.Add(3 * oldInterval) // Three permits consumed from a burst of three.

	next, wait := resourceCreationReservation(now, available, oldInterval, newInterval, 3)
	require.Equal(t, 10*time.Second, wait, "lower QPS must not grant a fresh burst")
	require.Equal(t, now.Add(30*time.Second), next)

	next, wait = resourceCreationReservation(now.Add(10*time.Second), next, newInterval, newInterval, 3)
	require.Zero(t, wait)
	require.Equal(t, now.Add(40*time.Second), next)
}

func TestResourceCreationReservationRoundsConvertedDebtToDatabasePrecision(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next, wait := resourceCreationReservation(now, now.Add(time.Millisecond), 3*time.Millisecond, 10*time.Millisecond, 1)
	require.Equal(t, 3334*time.Microsecond, wait)
	require.Equal(t, now.Add(3334*time.Microsecond), next)
}

// Also exercised against MySQL: the same policy lock protects every API/Worker
// caller, and constructing another caller cannot refill persisted debt.
func testResourceCreationConcurrentBudget(t *testing.T, store datastore.DataStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := workflowconfig.DefaultJobSchedulerPolicy()
	policy.ResourceCreationQPS, policy.ResourceCreationBurst = 1, 3
	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded}))
	results := make(chan time.Duration, 16)
	errors := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			wait, err := ReserveResourceCreation(ctx, store)
			results <- wait
			errors <- err
		})
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	allowed := 0
	for wait := range results {
		if wait == 0 {
			allowed++
		}
	}
	require.Equal(t, 3, allowed)
	wait, err := ReserveResourceCreation(ctx, store)
	require.NoError(t, err)
	require.Positive(t, wait)
	// An online update must not reset tokens in an independent runtime row.
	require.NoError(t, store.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded}))
	wait, err = ReserveResourceCreation(ctx, store)
	require.NoError(t, err)
	require.Positive(t, wait)

	// Persist an exact full-burst debt, then lower QPS by 10x. AvailableAt is
	// expressed in the old interval and must be converted before admission.
	now, err := currentWorkflowDatabaseTime(ctx, store)
	require.NoError(t, err)
	budget := &model.ResourceCreationBudget{ID: "jobs-and-sandboxes"}
	require.NoError(t, store.Get(ctx, budget))
	budget.AvailableAt = now.Add(3 * time.Second)
	budget.IntervalMicros = int64(time.Second / time.Microsecond)
	require.NoError(t, store.Put(ctx, budget))
	policy.ResourceCreationQPS = 0.1
	encoded, err = json.Marshal(policy)
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded}))
	wait, err = ReserveResourceCreation(ctx, store)
	require.NoError(t, err)
	require.Greater(t, wait, 9*time.Second)
	require.NoError(t, store.Get(ctx, budget))
	require.Equal(t, int64((10*time.Second)/time.Microsecond), budget.IntervalMicros)
	require.True(t, budget.AvailableAt.After(now.Add(29*time.Second)), "converted debt must be persisted under the new interval")
}

func TestResourceCreationPersistedBudget(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	require.NoError(t, store.Client.AutoMigrate(&model.ResourceCreationBudget{}))
	// SQLite validates the contract with one connection; it does not establish
	// cross-connection locking semantics, which the MySQL test covers.
	db, err := store.Client.DB()
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	testResourceCreationConcurrentBudget(t, store)
}

func TestResourceCreationLegacyBudgetWithoutIntervalDoesNotRefillBurst(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	require.NoError(t, store.Client.AutoMigrate(&model.ResourceCreationBudget{}))
	db, err := store.Client.DB()
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	policy := workflowconfig.DefaultJobSchedulerPolicy()
	policy.ResourceCreationQPS, policy.ResourceCreationBurst = 0.1, 3
	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded}))
	now, err := currentWorkflowDatabaseTime(ctx, store)
	require.NoError(t, err)
	require.NoError(t, store.Add(ctx, &model.ResourceCreationBudget{
		ID: "jobs-and-sandboxes", AvailableAt: now.Add(3 * time.Second),
	}))

	wait, err := ReserveResourceCreation(ctx, store)
	require.NoError(t, err)
	require.Greater(t, wait, 9*time.Second, "unknown legacy interval must not be interpreted as a fresh burst")
	budget := &model.ResourceCreationBudget{ID: "jobs-and-sandboxes"}
	require.NoError(t, store.Get(ctx, budget))
	require.Equal(t, int64((10*time.Second)/time.Microsecond), budget.IntervalMicros)
	require.True(t, budget.AvailableAt.After(now.Add(29*time.Second)))
}
