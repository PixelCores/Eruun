package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestNodeSelectorProfileFindByIDEmpty(t *testing.T) {
	repo := &nodeSelectorProfileRepository{Store: &repositoryTestStore{}}
	_, err := repo.FindByID(context.Background(), " ")
	require.ErrorIs(t, err, datastore.ErrPrimaryEmpty)
}

func TestNodeSelectorProfileFindByNameNotFound(t *testing.T) {
	repo := &nodeSelectorProfileRepository{Store: &repositoryTestStore{}}
	_, err := repo.FindByName(context.Background(), "demo")
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}

func TestNodeSelectorProfileCreateUpsertByName(t *testing.T) {
	store := &repositoryTestStore{
		addErr: datastore.ErrRecordExist,
		listEntities: []datastore.Entity{
			&model.NodeSelectorProfile{ID: "old-id", Name: "demo"},
		},
	}
	repo := &nodeSelectorProfileRepository{Store: store}
	profile := &model.NodeSelectorProfile{Name: "demo"}

	err := repo.Create(context.Background(), profile)
	require.NoError(t, err)
	require.Equal(t, "old-id", profile.ID)
	require.NotNil(t, store.putEntity)
}

func TestNodeSelectorProfileCreateReturnsAddError(t *testing.T) {
	expected := errors.New("add failed")
	repo := &nodeSelectorProfileRepository{Store: &repositoryTestStore{addErr: expected}}

	err := repo.Create(context.Background(), &model.NodeSelectorProfile{Name: "demo"})
	require.ErrorIs(t, err, expected)
}

func TestRBACProfileFindByIDEmpty(t *testing.T) {
	repo := &rbacProfileRepository{Store: &repositoryTestStore{}}
	_, err := repo.FindByID(context.Background(), "")
	require.ErrorIs(t, err, datastore.ErrPrimaryEmpty)
}

func TestRBACProfileFindByNameNotFound(t *testing.T) {
	repo := &rbacProfileRepository{Store: &repositoryTestStore{}}
	_, err := repo.FindByName(context.Background(), "demo")
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}

func TestRBACProfileCreateUpsertByName(t *testing.T) {
	store := &repositoryTestStore{
		addErr: datastore.ErrRecordExist,
		listEntities: []datastore.Entity{
			&model.RBACProfile{ID: "old-id", Name: "demo"},
		},
	}
	repo := &rbacProfileRepository{Store: store}
	profile := &model.RBACProfile{Name: "demo"}

	err := repo.Create(context.Background(), profile)
	require.NoError(t, err)
	require.Equal(t, "old-id", profile.ID)
	require.NotNil(t, store.putEntity)
}

func TestRBACProfileListSkipsInvalidEntities(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.RBACProfile{ID: "rbac-1", Name: "demo"},
			&model.Applications{ID: "app-1"},
		},
	}
	repo := &rbacProfileRepository{Store: store}

	list, err := repo.List(context.Background(), datastore.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "rbac-1", list[0].ID)
}
