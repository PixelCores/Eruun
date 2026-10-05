package repository

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// WithApplicationSchedulingTransaction serializes the idle check and task insert
// on the existing application row. Redis coordinates requests, but a lost Redis
// lease must not permit two database submissions. Lock the app before any task,
// schedule or component writes; read-committed reads see the previous holder's
// committed task even when an access wrapper read before obtaining the row lock.
func WithApplicationSchedulingTransaction(ctx context.Context, store datastore.DataStore, appID string, fn func(datastore.DataStore) error) error {
	if store == nil || appID == "" {
		return datastore.ErrPrimaryEmpty
	}
	if fn == nil {
		return fmt.Errorf("application scheduling callback is nil")
	}
	transactional, ok := store.(datastore.ReadCommittedTransactional)
	if !ok {
		return fmt.Errorf("application scheduling requires read-committed transactions")
	}
	return transactional.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		locker, ok := tx.(datastore.RowLocker)
		if !ok {
			return fmt.Errorf("application scheduling requires row locking")
		}
		if err := locker.GetForUpdate(ctx, &model.Applications{ID: appID}); err != nil {
			return fmt.Errorf("lock application scheduling: %w", err)
		}
		return fn(tx)
	})
}
