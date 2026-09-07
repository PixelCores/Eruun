package account

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type accessReadCommittedTestStore struct {
	datastore.DataStore
	called bool
}

func (s *accessReadCommittedTestStore) WithReadCommittedTransaction(_ context.Context, fn func(datastore.DataStore) error) error {
	s.called = true
	return fn(s)
}

func TestScopedStoreReadCommittedTransactionPreservesScope(t *testing.T) {
	raw := &accessReadCommittedTestStore{}
	store := NewStore(raw)
	ctx := WithScope(context.Background(), ForWorkspace(&model.Workspace{ID: "allowed", Namespace: "allowed-ns"}))
	err := store.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		scoped, ok := tx.(*Store)
		require.True(t, ok, "transaction must retain the access wrapper")
		return scoped.Check(ctx, &model.Applications{WorkspaceID: "different", Namespace: "other-ns"})
	})
	require.True(t, raw.called)
	require.ErrorIs(t, err, bcode.ErrForbidden)
	called := false
	require.ErrorContains(t, NewStore(nil).WithReadCommittedTransaction(ctx, func(datastore.DataStore) error { called = true; return nil }), "read-committed transactions")
	require.False(t, called)
}

func TestScopedJobAdmissionTransactionUsesCanonicalApplicationWorkspace(t *testing.T) {
	service, _, _ := testAccounts(t)
	raw := service.Repo.Store
	ctx := context.Background()
	for _, app := range []*model.Applications{
		{ID: "allowed-app", Name: "allowed", WorkspaceID: "allowed", Namespace: "allowed-ns"},
		{ID: "foreign-app", Name: "foreign", WorkspaceID: "foreign", Namespace: "foreign-ns"},
	} {
		require.NoError(t, raw.Add(ctx, app))
	}
	store := NewStore(raw)
	ctx = WithScope(ctx, ForWorkspace(&model.Workspace{ID: "allowed", Namespace: "allowed-ns"}))
	for _, tc := range []struct {
		name      string
		appID     string
		workspace string
		allowed   bool
	}{
		{name: "current job", appID: "allowed-app", workspace: "allowed", allowed: true},
		{name: "historical job without workspace", appID: "allowed-app", allowed: true},
		{name: "foreign job with spoofed workspace", appID: "foreign-app", workspace: "allowed"},
		{name: "foreign historical job", appID: "foreign-app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &model.JobInfo{AppID: tc.appID, WorkspaceID: tc.workspace, TaskID: tc.name, SchedulingState: "queued"}
			require.NoError(t, raw.Add(context.Background(), job))
			err := store.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
				loaded := &model.JobInfo{ID: job.ID}
				if err := tx.Get(ctx, loaded); err != nil {
					return err
				}
				updated, err := tx.CompareAndSwap(ctx, loaded, "scheduling_state", "queued", map[string]interface{}{"scheduling_state": "admitted"})
				if err == nil {
					require.True(t, updated)
				}
				return err
			})
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, bcode.ErrForbidden)
				// A direct CAS must enforce the same scope as a read then CAS.
				err = store.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
					updated, err := tx.CompareAndSwap(ctx, job, "scheduling_state", "queued", map[string]interface{}{"scheduling_state": "admitted"})
					require.False(t, updated)
					return err
				})
				require.ErrorIs(t, err, bcode.ErrForbidden)
			}
			stored := &model.JobInfo{ID: job.ID}
			require.NoError(t, raw.Get(context.Background(), stored))
			if tc.allowed {
				require.Equal(t, "admitted", stored.SchedulingState)
			} else {
				require.Equal(t, "queued", stored.SchedulingState)
			}
		})
	}
}
