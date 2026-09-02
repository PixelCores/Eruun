package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestNewProgrammingLanguageRepositoryWithStoreRequiresDependency(t *testing.T) {
	repo, err := NewProgrammingLanguageRepositoryWithStore(nil)
	require.EqualError(t, err, "create programming language repository: datastore is nil")
	require.Nil(t, repo)

	store := &repositoryTestStore{}
	repo, err = NewProgrammingLanguageRepositoryWithStore(store)
	require.NoError(t, err)
	require.NotNil(t, repo)
}

func TestProgrammingLanguageRepositoryDelegates(t *testing.T) {
	updateTime := time.Unix(1700000000, 0)
	store := &repositoryTestStore{
		listEntities: []datastore.Entity{
			&model.ProgrammingLanguage{ID: "lang-1", Code: "golang", Version: "1.24"},
			&model.Applications{ID: "app-1"},
		},
		casSwapped:    true,
		casUpdateTime: updateTime,
	}
	repo := &programmingLanguageRepository{Store: store}

	_, err := repo.FindByID(context.Background(), "lang-1")
	require.NoError(t, err)
	require.IsType(t, &model.ProgrammingLanguage{}, store.getEntity)

	found, err := repo.FindByCodeVersion(context.Background(), "golang", "1.24")
	require.NoError(t, err)
	require.Equal(t, "lang-1", found.ID)
	query, ok := store.lastQuery.(*model.ProgrammingLanguage)
	require.True(t, ok)
	require.Equal(t, "golang", query.Code)
	require.Equal(t, "1.24", query.Version)

	require.NoError(t, repo.Create(context.Background(), &model.ProgrammingLanguage{ID: "lang-2"}))
	require.IsType(t, &model.ProgrammingLanguage{}, store.addEntity)

	enabled := false
	language := &model.ProgrammingLanguage{
		ID:      "lang-2",
		Name:    "Go",
		Version: "1.25",
		Enabled: &enabled,
		CPUReq:  "",
		MemReq:  "",
	}
	require.NoError(t, repo.Update(context.Background(), language))
	require.Equal(t, "id", store.casField)
	require.Equal(t, "lang-2", store.casValue)
	require.Equal(t, map[string]interface{}{
		"name":    "Go",
		"version": "1.25",
		"enabled": false,
		"cpu_req": "",
		"mem_req": "",
	}, store.casUpdates)
	require.Equal(t, updateTime, language.UpdateTime)

	require.NoError(t, repo.Delete(context.Background(), &model.ProgrammingLanguage{ID: "lang-2"}))
	require.IsType(t, &model.ProgrammingLanguage{}, store.deleteEntity)

	list, err := repo.List(context.Background(), datastore.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list, 1)
}

func TestProgrammingLanguageRepositoryFindByCodeVersionReturnsNotFound(t *testing.T) {
	store := &repositoryTestStore{}
	repo := &programmingLanguageRepository{Store: store}

	_, err := repo.FindByCodeVersion(context.Background(), "golang", "1.24")
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}

func TestProgrammingLanguageRepositoryUpdateTreatsNoChangeAsSuccessWhenLanguageExists(t *testing.T) {
	store := &repositoryTestStore{
		casSwapped:         false,
		isExistByCondValue: true,
	}
	repo := &programmingLanguageRepository{Store: store}

	err := repo.Update(context.Background(), &model.ProgrammingLanguage{
		ID:      "lang-1",
		Name:    "Golang",
		Version: "1.24",
		CPUReq:  "",
		MemReq:  "",
	})

	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"id": "lang-1"}, store.isExistByCondCond)
	require.Equal(t, (&model.ProgrammingLanguage{}).TableName(), store.isExistByCondTable)
	require.IsType(t, &model.ProgrammingLanguage{}, store.isExistByCondDest)
}

func TestProgrammingLanguageRepositoryUpdateReturnsNotFoundWhenLanguageMissing(t *testing.T) {
	store := &repositoryTestStore{
		casSwapped:         false,
		isExistByCondValue: false,
	}
	repo := &programmingLanguageRepository{Store: store}

	err := repo.Update(context.Background(), &model.ProgrammingLanguage{ID: "missing"})

	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
	require.Equal(t, map[string]interface{}{"id": "missing"}, store.isExistByCondCond)
}
