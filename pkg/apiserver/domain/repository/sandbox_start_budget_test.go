package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/stretchr/testify/require"
)

func testSandboxStartBudget(t *testing.T, store datastore.DataStore) {
	t.Helper()
	ctx := context.Background()
	setSchedulerTestPolicy(t, store, `{"maxStartingSandboxes":2}`)
	for i := range 8 {
		require.NoError(t, store.Add(ctx, &model.JobSandbox{ID: fmt.Sprint(i), WorkspaceID: fmt.Sprintf("space-%d", i%2),
			State: "pending", SlotReserved: true, Deadline: time.Now().Add(time.Hour), ReconcileAt: time.Now()}))
	}
	var group sync.WaitGroup
	results := make(chan string, 8)
	errs := make(chan error, 8)
	for i := range 8 {
		group.Go(func() {
			ok, err := ReserveSandboxStart(ctx, store, fmt.Sprint(i))
			if ok {
				results <- fmt.Sprint(i)
			}
			errs <- err
		})
	}
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var allowed []string
	for id := range results {
		allowed = append(allowed, id)
	}
	require.Len(t, allowed, 2, "the limit spans both workspaces and every caller")
	ok, err := ReserveSandboxStart(ctx, store, allowed[0])
	require.NoError(t, err)
	require.True(t, ok, "an uncertain create keeps its existing permit")
	setSchedulerTestPolicy(t, store, `{"maxStartingSandboxes":1}`)
	for _, id := range allowed {
		changed, err := store.CompareAndSwap(ctx, &model.JobSandbox{ID: id}, "id", id, map[string]interface{}{
			"start_reserved": false, "release_requested": true, "state": "retained",
		})
		require.NoError(t, err)
		require.True(t, changed)
		ok, err := ReserveSandboxStart(ctx, store, id)
		require.NoError(t, err)
		require.False(t, ok, "stopped intents cannot reserve again")
	}
	admitted := 0
	for i := range 8 {
		ok, err := ReserveSandboxStart(ctx, store, fmt.Sprint(i))
		require.NoError(t, err)
		if ok {
			admitted++
		}
	}
	require.Equal(t, 1, admitted, "new callers observe the lowered limit after release")
}

func TestSandboxStartBudget(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	require.NoError(t, store.Client.AutoMigrate(&model.JobSandbox{}))
	db, err := store.Client.DB()
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	testSandboxStartBudget(t, store)
}
