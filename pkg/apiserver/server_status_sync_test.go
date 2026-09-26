package apiserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
)

type statusSyncDeadlineStore struct {
	datastore.DataStore
	ctx context.Context
}

func (s *statusSyncDeadlineStore) List(ctx context.Context, _ datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	s.ctx = ctx
	return nil, nil
}

func TestSyncComponentStatusCallbackBoundsPersistenceContext(t *testing.T) {
	store := &statusSyncDeadlineStore{}
	server := &restServer{dataStore: store}
	before := time.Now()
	server.syncComponentStatus(&informer.ComponentStatusUpdate{AppID: "app-1", ComponentID: 7})
	after := time.Now()

	require.NotNil(t, store.ctx)
	deadline, bounded := store.ctx.Deadline()
	require.True(t, bounded)
	require.False(t, deadline.Before(before.Add(5*time.Second)))
	require.False(t, deadline.After(after.Add(5*time.Second)))
	require.ErrorIs(t, store.ctx.Err(), context.Canceled)
}
