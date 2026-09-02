package contracts

import (
	"context"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type dataStoreContextKey struct{}
type runtimeProviderSnapshotContextKey struct {
	provider string
}

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

// WithRuntimeProviderSnapshot attaches provider-specific runtime snapshot for the current invocation only.
func WithRuntimeProviderSnapshot(ctx context.Context, provider string, config interface{}) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if config == nil {
		return ctx
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return ctx
	}
	return context.WithValue(ctx, runtimeProviderSnapshotContextKey{provider: provider}, config)
}

// RuntimeProviderSnapshotFromContext returns provider-specific runtime snapshot for the current invocation only.
func RuntimeProviderSnapshotFromContext(ctx context.Context, provider string) interface{} {
	if ctx == nil {
		return nil
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return nil
	}
	return ctx.Value(runtimeProviderSnapshotContextKey{provider: provider})
}
