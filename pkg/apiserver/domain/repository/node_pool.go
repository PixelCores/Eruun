package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type NodePoolRepository interface {
	List(context.Context) ([]*model.NodePool, error)
	Get(context.Context, string) (*model.NodePool, error)
	Create(context.Context, *model.NodePool) error
	Update(context.Context, *model.NodePool) error
	Delete(context.Context, *model.NodePool) error
}

type nodepoolRepository struct{ Store datastore.DataStore }

func NewNodePoolRepositoryWithStore(store datastore.DataStore) (NodePoolRepository, error) {
	if store == nil {
		return nil, fmt.Errorf("create nodepool repository: datastore is nil")
	}
	return &nodepoolRepository{Store: store}, nil
}

func (r *nodepoolRepository) List(ctx context.Context) ([]*model.NodePool, error) {
	entities, err := r.Store.List(ctx, &model.NodePool{}, &datastore.ListOptions{SortBy: []datastore.SortOption{{Key: "name", Order: datastore.SortOrderAscending}}})
	if err != nil {
		return nil, err
	}
	out := make([]*model.NodePool, 0, len(entities))
	for _, entity := range entities {
		item, ok := entity.(*model.NodePool)
		if !ok || item == nil {
			return nil, fmt.Errorf("list nodepool: unexpected entity %T", entity)
		}
		out = append(out, item)
	}
	return out, nil
}

func (r *nodepoolRepository) Get(ctx context.Context, name string) (*model.NodePool, error) {
	item := &model.NodePool{Name: name}
	if err := r.Store.Get(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (r *nodepoolRepository) Create(ctx context.Context, item *model.NodePool) error {
	return r.Store.Add(ctx, item)
}

func (r *nodepoolRepository) Update(ctx context.Context, item *model.NodePool) error {
	if item == nil {
		return datastore.ErrNilEntity
	}
	nodeSelector, err := json.Marshal(item.NodeSelector)
	if err != nil {
		return fmt.Errorf("encode node_selector: %w", err)
	}
	tolerations, err := json.Marshal(item.Tolerations)
	if err != nil {
		return fmt.Errorf("encode tolerations: %w", err)
	}
	updated, err := r.Store.CompareAndSwap(ctx, item, "name", item.Name, map[string]interface{}{"description": item.Description, "node_selector": string(nodeSelector), "tolerations": string(tolerations)})
	if err != nil {
		return err
	}
	if updated {
		return nil
	}
	exists, err := r.Store.IsExistByCondition(ctx, item.TableName(), map[string]interface{}{"name": item.Name}, &model.NodePool{})
	if err != nil {
		return err
	}
	if !exists {
		return datastore.ErrRecordNotExist
	}
	return nil
}

func (r *nodepoolRepository) Delete(ctx context.Context, item *model.NodePool) error {
	return r.Store.Delete(ctx, item)
}
