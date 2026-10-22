package jobtest

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// noopStore is a minimal datastore implementation for tests that do not persist.
type NoopStore struct{}

func (*NoopStore) Add(context.Context, datastore.Entity) error        { return nil }
func (*NoopStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }
func (*NoopStore) Put(context.Context, datastore.Entity) error        { return nil }
func (*NoopStore) Delete(context.Context, datastore.Entity) error     { return nil }
func (*NoopStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (*NoopStore) Get(context.Context, datastore.Entity) error { return nil }
func (*NoopStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}
func (*NoopStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}
func (*NoopStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (*NoopStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return true, nil
}
