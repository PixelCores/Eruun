package sql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type testBatchEntity struct {
	id string
}

func (e *testBatchEntity) SetCreateTime(time.Time) {}
func (e *testBatchEntity) SetUpdateTime(time.Time) {}
func (e *testBatchEntity) PrimaryKey() string      { return e.id }
func (e *testBatchEntity) TableName() string       { return "test_batch_entities" }
func (e *testBatchEntity) ShortTableName() string  { return "test_batch_entities" }
func (e *testBatchEntity) Index() map[string]interface{} {
	return map[string]interface{}{"id": e.id}
}

type fakeBatchTransactionalStore struct {
	records map[string]datastore.Entity
	txCalls int

	failOnAddAt int
	addErr      error
	txErr       error
}

func newFakeBatchTransactionalStore() *fakeBatchTransactionalStore {
	return &fakeBatchTransactionalStore{
		records: make(map[string]datastore.Entity),
	}
}

func (s *fakeBatchTransactionalStore) WithTransaction(_ context.Context, fn func(tx datastore.DataStore) error) error {
	s.txCalls++
	if s.txErr != nil {
		return s.txErr
	}
	txStore := &fakeBatchTxStore{
		records:     cloneBatchEntityMap(s.records),
		failOnAddAt: s.failOnAddAt,
		addErr:      s.addErr,
	}
	if err := fn(txStore); err != nil {
		return err
	}
	s.records = txStore.records
	return nil
}

type fakeBatchTxStore struct {
	records map[string]datastore.Entity

	failOnAddAt int
	addErr      error
	addCalls    int
}

func (s *fakeBatchTxStore) Add(_ context.Context, entity datastore.Entity) error {
	s.addCalls++
	if s.failOnAddAt > 0 && s.addCalls == s.failOnAddAt {
		return s.addErr
	}
	s.records[entity.PrimaryKey()] = entity
	return nil
}

func (s *fakeBatchTxStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (s *fakeBatchTxStore) Put(context.Context, datastore.Entity) error        { return nil }
func (s *fakeBatchTxStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (s *fakeBatchTxStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *fakeBatchTxStore) Get(context.Context, datastore.Entity) error { return nil }
func (s *fakeBatchTxStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}
func (s *fakeBatchTxStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (s *fakeBatchTxStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *fakeBatchTxStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *fakeBatchTxStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func cloneBatchEntityMap(in map[string]datastore.Entity) map[string]datastore.Entity {
	out := make(map[string]datastore.Entity, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func TestBatchAddWithTransaction(t *testing.T) {
	ctx := context.Background()
	entities := []datastore.Entity{
		&testBatchEntity{id: "a"},
		&testBatchEntity{id: "b"},
	}

	t.Run("commit all on success", func(t *testing.T) {
		txStore := newFakeBatchTransactionalStore()

		err := batchAddWithTransaction(ctx, txStore, entities)
		require.NoError(t, err)
		require.Equal(t, 1, txStore.txCalls)
		require.Len(t, txStore.records, 2)
	})

	t.Run("rollback all on mid-transaction failure", func(t *testing.T) {
		txStore := newFakeBatchTransactionalStore()
		txStore.failOnAddAt = 2
		txStore.addErr = errors.New("insert failed")

		err := batchAddWithTransaction(ctx, txStore, entities)
		require.Error(t, err)
		var dbErr *datastore.DBError
		require.True(t, errors.As(err, &dbErr))
		require.ErrorContains(t, err, "insert failed")
		require.Empty(t, txStore.records)
	})

	t.Run("pass through datastore db error", func(t *testing.T) {
		txStore := newFakeBatchTransactionalStore()
		txStore.failOnAddAt = 1
		txStore.addErr = datastore.ErrRecordExist

		err := batchAddWithTransaction(ctx, txStore, entities)
		require.Error(t, err)
		require.ErrorIs(t, err, datastore.ErrRecordExist)
		var dbErr *datastore.DBError
		require.True(t, errors.As(err, &dbErr))
		require.Empty(t, txStore.records)
	})

	t.Run("wrap transaction runner error into datastore db error", func(t *testing.T) {
		txStore := newFakeBatchTransactionalStore()
		txStore.txErr = errors.New("transaction unavailable")

		err := batchAddWithTransaction(ctx, txStore, entities)
		require.Error(t, err)
		var dbErr *datastore.DBError
		require.True(t, errors.As(err, &dbErr))
		require.ErrorContains(t, err, "transaction unavailable")
	})

	t.Run("skip transaction for empty input", func(t *testing.T) {
		txStore := newFakeBatchTransactionalStore()

		err := batchAddWithTransaction(ctx, txStore, nil)
		require.NoError(t, err)
		require.Equal(t, 0, txStore.txCalls)
	})
}
