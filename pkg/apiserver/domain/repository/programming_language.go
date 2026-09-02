package repository

import (
	"context"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// ProgrammingLanguageRepository defines data access for programming languages.
type ProgrammingLanguageRepository interface {
	FindByID(ctx context.Context, id string) (*model.ProgrammingLanguage, error)
	FindByCodeVersion(ctx context.Context, code, version string) (*model.ProgrammingLanguage, error)
	Create(ctx context.Context, language *model.ProgrammingLanguage) error
	Update(ctx context.Context, language *model.ProgrammingLanguage) error
	Delete(ctx context.Context, language *model.ProgrammingLanguage) error
	List(ctx context.Context, options datastore.ListOptions) ([]*model.ProgrammingLanguage, error)
	ListByQuery(ctx context.Context, query *model.ProgrammingLanguage, options datastore.ListOptions) ([]*model.ProgrammingLanguage, error)
}

type programmingLanguageRepository struct {
	Store datastore.DataStore `inject:"datastore"`
}

// NewProgrammingLanguageRepository creates a ProgrammingLanguageRepository.
func NewProgrammingLanguageRepository() ProgrammingLanguageRepository {
	return &programmingLanguageRepository{}
}

// NewProgrammingLanguageRepositoryWithStore creates a ready-to-use repository.
func NewProgrammingLanguageRepositoryWithStore(store datastore.DataStore) (ProgrammingLanguageRepository, error) {
	if store == nil {
		return nil, fmt.Errorf("create programming language repository: datastore is nil")
	}
	return &programmingLanguageRepository{Store: store}, nil
}

func (r *programmingLanguageRepository) FindByID(ctx context.Context, id string) (*model.ProgrammingLanguage, error) {
	language := &model.ProgrammingLanguage{ID: id}
	if err := r.Store.Get(ctx, language); err != nil {
		return nil, err
	}
	return language, nil
}

func (r *programmingLanguageRepository) FindByCodeVersion(ctx context.Context, code, version string) (*model.ProgrammingLanguage, error) {
	items, err := r.ListByQuery(ctx, &model.ProgrammingLanguage{Code: code, Version: version}, datastore.ListOptions{Page: 1, PageSize: 1})
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, datastore.ErrRecordNotExist
	}
	return items[0], nil
}

func (r *programmingLanguageRepository) Create(ctx context.Context, language *model.ProgrammingLanguage) error {
	return r.Store.Add(ctx, language)
}

func (r *programmingLanguageRepository) Update(ctx context.Context, language *model.ProgrammingLanguage) error {
	if language == nil {
		return datastore.ErrNilEntity
	}
	updates := map[string]interface{}{
		"name":    language.Name,
		"version": language.Version,
		"cpu_req": language.CPUReq,
		"mem_req": language.MemReq,
	}
	if language.Enabled != nil {
		updates["enabled"] = *language.Enabled
	}
	updated, err := r.Store.CompareAndSwap(ctx, language, "id", language.ID, updates)
	if err != nil {
		return err
	}
	if updated {
		return nil
	}
	exists, err := r.Store.IsExistByCondition(ctx, language.TableName(), map[string]interface{}{"id": language.ID}, &model.ProgrammingLanguage{})
	if err != nil {
		return err
	}
	if !exists {
		return datastore.ErrRecordNotExist
	}
	return nil
}

func (r *programmingLanguageRepository) Delete(ctx context.Context, language *model.ProgrammingLanguage) error {
	return r.Store.Delete(ctx, language)
}

func (r *programmingLanguageRepository) List(ctx context.Context, options datastore.ListOptions) ([]*model.ProgrammingLanguage, error) {
	return r.ListByQuery(ctx, &model.ProgrammingLanguage{}, options)
}

func (r *programmingLanguageRepository) ListByQuery(ctx context.Context, query *model.ProgrammingLanguage, options datastore.ListOptions) ([]*model.ProgrammingLanguage, error) {
	if query == nil {
		query = &model.ProgrammingLanguage{}
	}
	entities, err := r.Store.List(ctx, query, &options)
	if err != nil {
		return nil, err
	}
	out := make([]*model.ProgrammingLanguage, 0, len(entities))
	for _, entity := range entities {
		language, ok := entity.(*model.ProgrammingLanguage)
		if !ok || language == nil {
			continue
		}
		out = append(out, language)
	}
	return out, nil
}
