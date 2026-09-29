package contracts

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type dataStoreContextKey struct{}

// WithDataStore attaches a datastore to cloudjob runtime context.
func WithDataStore(ctx context.Context, store datastore.DataStore) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil {
		return ctx
	}
	return context.WithValue(ctx, dataStoreContextKey{}, store)
}

// DataStoreFromContext returns the datastore previously attached to context.
func DataStoreFromContext(ctx context.Context) datastore.DataStore {
	if ctx == nil {
		return nil
	}
	store, _ := ctx.Value(dataStoreContextKey{}).(datastore.DataStore)
	return store
}
