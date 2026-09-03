package repository

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// Accounts uses the same datastore and transaction boundary as runtime resources.
type Accounts struct{ Store datastore.DataStore }

func (r Accounts) One(ctx context.Context, query datastore.Entity) error {
	rows, err := r.Store.List(ctx, query, &datastore.ListOptions{Page: 1, PageSize: 1})
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return datastore.ErrRecordNotExist
	}
	switch q := query.(type) {
	case *model.User:
		*q = *rows[0].(*model.User)
	case *model.Identity:
		*q = *rows[0].(*model.Identity)
	case *model.Session:
		*q = *rows[0].(*model.Session)
	case *model.Workspace:
		*q = *rows[0].(*model.Workspace)
	case *model.WorkspaceMember:
		*q = *rows[0].(*model.WorkspaceMember)
	case *model.WorkspaceInvitation:
		*q = *rows[0].(*model.WorkspaceInvitation)
	default:
		return fmt.Errorf("unsupported account query %T", query)
	}
	return nil
}

func (r Accounts) Transaction(ctx context.Context, fn func(Accounts) error) error {
	tx, ok := r.Store.(datastore.ReadCommittedTransactional)
	if !ok {
		return fmt.Errorf("accounts require read committed transactions")
	}
	return tx.WithReadCommittedTransaction(ctx, func(s datastore.DataStore) error { return fn(Accounts{Store: s}) })
}

// Lock obtains a row write lock until the enclosing transaction commits. Reads
// following this call observe concurrent password/member changes in MySQL.
func (r Accounts) Lock(ctx context.Context, entity datastore.Entity) error {
	locker, ok := r.Store.(datastore.RowLocker)
	if !ok {
		return fmt.Errorf("accounts require row locking")
	}
	return locker.GetForUpdate(ctx, entity)
}

func (r Accounts) Update(ctx context.Context, entity datastore.Entity, values map[string]interface{}) error {
	ok, err := r.Store.CompareAndSwap(ctx, entity, "id", entity.PrimaryKey(), values)
	if err != nil {
		return err
	}
	if !ok {
		// An idempotent update can affect zero rows in MySQL. The immutable ID
		// is the only predicate; a current read distinguishes it from deletion.
		return r.Store.Get(ctx, entity)
	}
	return nil
}
