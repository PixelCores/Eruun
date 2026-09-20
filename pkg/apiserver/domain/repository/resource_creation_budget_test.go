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
		next, wait := resourceCreationReservation(now, available, time.Second, 3)
		require.Zero(t, wait)
		available = next
	}
	next, wait := resourceCreationReservation(now, available, time.Second, 3)
	require.Equal(t, time.Second, wait)
	require.Equal(t, available, next, "a rejected attempt must not extend rate debt")
	next, wait = resourceCreationReservation(now.Add(time.Second), available, time.Second, 3)
	require.Zero(t, wait)
	require.Equal(t, now.Add(4*time.Second), next)
	_, wait = resourceCreationReservation(now.Add(time.Second), next, time.Second, 1)
	require.Equal(t, 3*time.Second, wait, "lowering burst must preserve existing debt")
}

// Also exercised against MySQL: the same policy lock protects every API/Worker
// caller, and constructing another caller cannot refill persisted debt.
func testResourceCreationConcurrentBudget(t *testing.T, store datastore.DataStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := workflowconfig.DefaultJobSchedulerPolicy()
	policy.ResourceCreationQPS, policy.ResourceCreationBurst = 0.1, 3
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
