package artifacts

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type dataOnlyStore struct{ datastore.DataStore }

type transactionWithoutCapabilities struct{ Backend }

func (s *transactionWithoutCapabilities) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.Backend.WithTransaction(ctx, func(tx datastore.DataStore) error { return fn(&dataOnlyStore{tx}) })
}

func TestBackendCapabilityValidation(t *testing.T) {
	_, raw := testStore(t)
	for _, input := range []datastore.DataStore{nil, &dataOnlyStore{raw}, struct {
		datastore.DataStore
		datastore.Transactional
		datastore.DatabaseClock
		datastore.ConditionalCompareAndSwap
	}{raw, raw, raw, raw}} {
		_, err := New(input, nil)
		require.ErrorContains(t, err, "requires transactions, row locks, database clock and conditional updates")
	}
	called := false
	err := WithTransaction(context.Background(), &transactionWithoutCapabilities{raw}, func(Backend) error { called = true; return nil })
	require.ErrorContains(t, err, "Jobs transaction")
	require.False(t, called, "reject erased transaction capabilities before running any business writes")
}

func TestBackendTransactionRetainsCapabilitiesAndRollsBack(t *testing.T) {
	_, raw := testStore(t)
	ctx := context.Background()
	rejected := errors.New("reject after write")
	err := WithTransaction(ctx, raw, func(tx Backend) error {
		row := &model.Workspace{ID: "space-a"}
		require.NoError(t, tx.GetForUpdate(ctx, row))
		now, err := tx.CurrentDatabaseTime(ctx)
		require.NoError(t, err)
		require.False(t, now.IsZero())
		updated, err := tx.CompareAndSwapWithConditions(ctx, row, map[string]interface{}{"namespace": "space-a"}, map[string]interface{}{"namespace": "changed"})
		require.NoError(t, err)
		require.True(t, updated)
		return rejected
	})
	require.ErrorIs(t, err, rejected)
	row := &model.Workspace{ID: "space-a"}
	require.NoError(t, raw.Get(ctx, row))
	require.Equal(t, "space-a", row.Namespace)
}
