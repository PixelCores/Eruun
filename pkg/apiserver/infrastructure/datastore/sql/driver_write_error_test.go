package sql

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

func TestNormalizeWriteError(t *testing.T) {
	t.Run("duplicate key maps to record exists", func(t *testing.T) {
		err := normalizeWriteError(gorm.ErrDuplicatedKey)

		require.ErrorIs(t, err, datastore.ErrRecordExist)
	})

	t.Run("record not found maps to record not exists", func(t *testing.T) {
		err := normalizeWriteError(gorm.ErrRecordNotFound)

		require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	})

	t.Run("regular error is wrapped as datastore db error", func(t *testing.T) {
		err := normalizeWriteError(errors.New("write failed"))

		var dbErr *datastore.DBError
		require.ErrorAs(t, err, &dbErr)
		require.ErrorContains(t, err, "write failed")
	})
}

func TestApplyCompareAndSwapConditionsUsesISNULL(t *testing.T) {
	db, err := gorm.Open(mysqlgorm.New(mysqlgorm.Config{
		DSN:                       "gorm:gorm@tcp(127.0.0.1:9910)/gorm?charset=utf8mb4&parseTime=True&loc=Local",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun:                 true,
		DisableAutomaticPing:   true,
		SkipDefaultTransaction: true,
		NamingStrategy:         sqlnamer.SQLNamer{},
	})
	require.NoError(t, err)

	result := applyCompareAndSwapConditions(db.Model(&model.JobInfo{}), map[string]interface{}{
		"execution_key": nil,
		"status":        "running",
	}).Updates(map[string]interface{}{"status": "completed"})

	require.NoError(t, result.Error)
	require.Contains(t, result.Statement.SQL.String(), "execution_key IS NULL")
	require.NotContains(t, result.Statement.SQL.String(), "execution_key = ?")
}

func TestApplyFilterOptionsSupportsLeaseReaperPredicates(t *testing.T) {
	now := time.Unix(1700000000, 0)
	expressions := _applyFilterOptions(nil, datastore.FilterOptions{
		In:       []datastore.InQueryOption{{Key: "status", Values: []string{"queued", "running"}}},
		NotEqual: []datastore.ComparisonQueryOption{{Key: "run_token", Value: ""}},
		LessThan: []datastore.ComparisonQueryOption{{Key: "lease_expires_at", Value: now}},
	})

	require.Len(t, expressions, 3)
	require.IsType(t, clause.IN{}, expressions[0])
	require.Equal(t, clause.Neq{Column: "run_token", Value: ""}, expressions[1])
	require.Equal(t, clause.Lt{Column: "lease_expires_at", Value: now}, expressions[2])
}

func TestSyncCompareAndSwapUpdateTime(t *testing.T) {
	t.Run("syncs update time when swapped", func(t *testing.T) {
		entity := &testWriteEntity{id: "entity-1"}
		updateTime := time.Unix(1700000000, 0)

		updated := syncCompareAndSwapUpdateTime(entity, true, updateTime)

		require.True(t, updated)
		require.Equal(t, updateTime, entity.updateTime)
	})

	t.Run("keeps update time when not swapped", func(t *testing.T) {
		originalTime := time.Unix(1600000000, 0)
		entity := &testWriteEntity{id: "entity-1", updateTime: originalTime}
		updateTime := time.Unix(1700000000, 0)

		updated := syncCompareAndSwapUpdateTime(entity, false, updateTime)

		require.False(t, updated)
		require.Equal(t, originalTime, entity.updateTime)
	})
}

type testWriteEntity struct {
	id         string
	createTime time.Time
	updateTime time.Time
}

func (e *testWriteEntity) SetCreateTime(t time.Time) {
	e.createTime = t
}

func (e *testWriteEntity) SetUpdateTime(t time.Time) {
	e.updateTime = t
}

func (e *testWriteEntity) PrimaryKey() string {
	return e.id
}

func (e *testWriteEntity) TableName() string {
	return "test_write_entities"
}

func (e *testWriteEntity) ShortTableName() string {
	return "test_write_entity"
}

func (e *testWriteEntity) Index() map[string]interface{} {
	return map[string]interface{}{"id": e.id}
}
