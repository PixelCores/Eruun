package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/stretchr/testify/require"
)

func TestAccountsListUsers(t *testing.T) {
	user := &model.User{ID: "user"}
	store := &repositoryTestStore{listEntities: []datastore.Entity{user}}
	users, err := (Accounts{Store: store}).ListUsers(context.Background(), 2, 10)
	require.NoError(t, err)
	require.Equal(t, []*model.User{user}, users)
	require.IsType(t, &model.User{}, store.lastQuery)
	require.Equal(t, &datastore.ListOptions{Page: 2, PageSize: 10, SortBy: []datastore.SortOption{{Key: "id", Order: datastore.SortOrderAscending}}}, store.lastListOpts)

	for _, rows := range [][]datastore.Entity{nil, {}} {
		store.listEntities = rows
		users, err = (Accounts{Store: store}).ListUsers(context.Background(), 1, 20)
		require.NoError(t, err)
		require.Empty(t, users)
		require.Equal(t, rows == nil, users == nil)
	}
	store.listEntities = []datastore.Entity{&model.Workspace{}}
	_, err = (Accounts{Store: store}).ListUsers(context.Background(), 1, 20)
	require.ErrorContains(t, err, "user list contains *model.Workspace")
	store.listErr = errors.New("list failed")
	_, err = (Accounts{Store: store}).ListUsers(context.Background(), 1, 20)
	require.ErrorIs(t, err, store.listErr)
}
