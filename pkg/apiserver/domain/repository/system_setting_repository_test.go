package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestSystemSettingRepositoryConstructors(t *testing.T) {
	require.NotNil(t, NewSystemSettingRepository())
	store := &repositoryTestStore{}
	repo := NewSystemSettingRepositoryWithStore(store)
	require.NotNil(t, repo)
}

func TestSystemSettingRepositoryDelegates(t *testing.T) {
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.SystemSetting{Type: model.SystemSettingTypeNodeSelector},
			&model.Applications{ID: "app-1"},
		},
	}
	repo := &systemSettingRepository{Store: store}

	_, err := repo.FindByType(context.Background(), model.SystemSettingTypeNodeSelector)
	require.NoError(t, err)
	require.IsType(t, &model.SystemSetting{}, store.getEntity)

	require.NoError(t, repo.Create(context.Background(), &model.SystemSetting{Type: model.SystemSettingTypeNodeSelector}))
	require.IsType(t, &model.SystemSetting{}, store.addEntity)

	require.NoError(t, repo.Update(context.Background(), &model.SystemSetting{Type: model.SystemSettingTypeNodeSelector}))
	require.IsType(t, &model.SystemSetting{}, store.putEntity)

	require.NoError(t, repo.Delete(context.Background(), &model.SystemSetting{Type: model.SystemSettingTypeNodeSelector}))
	require.IsType(t, &model.SystemSetting{}, store.deleteEntity)

	list, err := repo.List(context.Background(), datastore.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list, 1)
}

func TestInitRepositoryBeanContainsCoreRepositories(t *testing.T) {
	beans := InitRepositoryBean()
	require.NotEmpty(t, beans)
}

func TestSystemSettingRepositoryFindByTypeError(t *testing.T) {
	expected := errors.New("get failed")
	repo := &systemSettingRepository{Store: &repositoryTestStore{getErr: expected}}

	_, err := repo.FindByType(context.Background(), model.SystemSettingTypeNodeSelector)
	require.ErrorIs(t, err, expected)
}
