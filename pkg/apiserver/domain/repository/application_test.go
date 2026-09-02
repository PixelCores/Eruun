package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestApplicationRepositoryDelegatesToStore(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.Applications{ID: "app-1", Name: "demo"},
			&model.Workflow{ID: "wf-1"},
		},
	}
	repo := &applicationRepository{Store: store}

	_, err := repo.FindByID(context.Background(), "app-1")
	require.NoError(t, err)
	require.IsType(t, &model.Applications{}, store.getEntity)

	store.getErr = nil
	_, err = repo.FindByName(context.Background(), "demo")
	require.NoError(t, err)

	require.NoError(t, repo.Create(context.Background(), &model.Applications{ID: "app-2"}))
	require.IsType(t, &model.Applications{}, store.addEntity)

	require.NoError(t, repo.Update(context.Background(), &model.Applications{ID: "app-2"}))
	require.IsType(t, &model.Applications{}, store.putEntity)

	require.NoError(t, repo.Delete(context.Background(), &model.Applications{ID: "app-2"}))
	require.IsType(t, &model.Applications{}, store.deleteEntity)

	list, err := repo.List(context.Background(), datastore.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "app-1", list[0].ID)
}

func TestApplicationRepositoryFindByNameReturnsNotFound(t *testing.T) {
	store := &repositoryTestStore{}
	repo := &applicationRepository{Store: store}

	_, err := repo.FindByName(context.Background(), "missing")
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)

	query, ok := store.lastQuery.(*model.Applications)
	require.True(t, ok)
	require.Equal(t, "missing", query.Name)
}

func TestApplicationRepositoryFindByNameRejectsEmptyName(t *testing.T) {
	store := &repositoryTestStore{}
	repo := &applicationRepository{Store: store}

	_, err := repo.FindByName(context.Background(), "")
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	require.Nil(t, store.lastQuery)
}

func TestCreateApplicationsUpsertsOnRecordExist(t *testing.T) {
	store := &repositoryTestStore{
		addErr: datastore.ErrRecordExist,
	}
	err := CreateApplications(context.Background(), store, &model.Applications{ID: "app-1"})
	require.NoError(t, err)
	require.NotNil(t, store.putEntity)
}

func TestCreateApplicationsReturnsRecordExistWhenConflictIsNotPrimaryKey(t *testing.T) {
	store := &repositoryTestStore{
		addErr: datastore.ErrRecordExist,
		getErr: datastore.ErrRecordNotExist,
	}

	err := CreateApplications(context.Background(), store, &model.Applications{ID: "app-1"})
	require.ErrorIs(t, err, datastore.ErrRecordExist)
	require.Nil(t, store.putEntity)
}

func TestCreateApplicationsReturnsUnexpectedAddError(t *testing.T) {
	expected := errors.New("add failed")
	store := &repositoryTestStore{addErr: expected}

	err := CreateApplications(context.Background(), store, &model.Applications{ID: "app-1"})
	require.ErrorIs(t, err, expected)
}

func TestApplicationByIDReturnsStoreError(t *testing.T) {
	expected := errors.New("get failed")
	store := &repositoryTestStore{getErr: expected}

	_, err := ApplicationByID(context.Background(), store, "app-1")
	require.ErrorIs(t, err, expected)
}

func TestApplicationRepositoryFindByIDsBuildsTypedBatchIntent(t *testing.T) {
	store := &repositoryTestStore{listEntities: []datastore.Entity{
		&model.Applications{ID: "app-1"},
		&model.Applications{ID: "app-2"},
	}}
	repo := &applicationRepository{Store: store}

	applications, err := repo.FindByIDs(context.Background(), []string{"app-1", "app-2"})

	require.NoError(t, err)
	require.Len(t, applications, 2)
	require.IsType(t, &model.Applications{}, store.lastQuery)
	require.NotNil(t, store.lastListOpts)
	require.Equal(t, []datastore.InQueryOption{{Key: "id", Values: []string{"app-1", "app-2"}}}, store.lastListOpts.FilterOptions.In)
}

func TestApplicationRepositoryFindByIDsPreservesStoreErrorAndEntityTypeChecks(t *testing.T) {
	expected := errors.New("list failed")
	_, err := (&applicationRepository{Store: &repositoryTestStore{listErr: expected}}).FindByIDs(context.Background(), []string{"app-1"})
	require.ErrorIs(t, err, expected)

	_, err = (&applicationRepository{Store: &repositoryTestStore{
		listEntities: []datastore.Entity{&model.Workflow{ID: "wf-1"}},
	}}).FindByIDs(context.Background(), []string{"app-1"})
	require.EqualError(t, err, "unexpected application entity type: *model.Workflow")
}

func TestApplicationRepositoryFindByIDsReturnsNonNilEmptySliceWithoutQuery(t *testing.T) {
	store := &repositoryTestStore{}
	repo := &applicationRepository{Store: store}

	applications, err := repo.FindByIDs(context.Background(), nil)

	require.NoError(t, err)
	require.NotNil(t, applications)
	require.Empty(t, applications)
	require.Nil(t, store.lastQuery)
}
