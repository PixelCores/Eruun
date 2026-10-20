package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type RBACProfileRepository interface {
	List(context.Context) ([]*model.RBACProfile, error)
	Get(context.Context, string) (*model.RBACProfile, error)
	Create(context.Context, *model.RBACProfile) error
	Update(context.Context, *model.RBACProfile) error
	Delete(context.Context, *model.RBACProfile) error
}

type rbacprofileRepository struct{ Store datastore.DataStore }

func NewRBACProfileRepositoryWithStore(store datastore.DataStore) (RBACProfileRepository, error) {
	if store == nil {
		return nil, fmt.Errorf("create rbacprofile repository: datastore is nil")
	}
	return &rbacprofileRepository{Store: store}, nil
}

func (r *rbacprofileRepository) List(ctx context.Context) ([]*model.RBACProfile, error) {
	entities, err := r.Store.List(ctx, &model.RBACProfile{}, &datastore.ListOptions{SortBy: []datastore.SortOption{{Key: "name", Order: datastore.SortOrderAscending}}})
	if err != nil {
		return nil, err
	}
	out := make([]*model.RBACProfile, 0, len(entities))
	for _, entity := range entities {
		item, ok := entity.(*model.RBACProfile)
		if !ok || item == nil {
			return nil, fmt.Errorf("list rbacprofile: unexpected entity %T", entity)
		}
		out = append(out, item)
	}
	return out, nil
}

func (r *rbacprofileRepository) Get(ctx context.Context, name string) (*model.RBACProfile, error) {
	item := &model.RBACProfile{Name: name}
	if err := r.Store.Get(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (r *rbacprofileRepository) Create(ctx context.Context, item *model.RBACProfile) error {
	return r.Store.Add(ctx, item)
}

func (r *rbacprofileRepository) Update(ctx context.Context, item *model.RBACProfile) error {
	if item == nil {
		return datastore.ErrNilEntity
	}
	rules, err := json.Marshal(item.Rules)
	if err != nil {
		return fmt.Errorf("encode rules: %w", err)
	}
	updated, err := r.Store.CompareAndSwap(ctx, item, "name", item.Name, map[string]interface{}{"description": item.Description, "rules": string(rules)})
	if err != nil {
		return err
	}
	if updated {
		return nil
	}
	exists, err := r.Store.IsExistByCondition(ctx, item.TableName(), map[string]interface{}{"name": item.Name}, &model.RBACProfile{})
	if err != nil {
		return err
	}
	if !exists {
		return datastore.ErrRecordNotExist
	}
	return nil
}

func (r *rbacprofileRepository) Delete(ctx context.Context, item *model.RBACProfile) error {
	return r.Store.Delete(ctx, item)
}
