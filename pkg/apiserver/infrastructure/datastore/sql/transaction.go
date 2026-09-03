package sql

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// WithTransaction runs fn within a database transaction.
// The provided tx datastore must be used for all operations that should be atomic.
func (m *Driver) WithTransaction(ctx context.Context, fn func(tx datastore.DataStore) error) error {
	return m.Client.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&Driver{Client: *tx})
	})
}

func (m *Driver) GetForUpdate(ctx context.Context, entity datastore.Entity) error {
	locked := &Driver{Client: *m.Client.Clauses(clause.Locking{Strength: "UPDATE"})}
	return locked.Get(ctx, entity)
}

func (m *Driver) WithReadCommittedTransaction(ctx context.Context, fn func(tx datastore.DataStore) error) error {
	return m.Client.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&Driver{Client: *tx})
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}
