package nodepool

import (
	"context"
	"errors"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	corev1 "k8s.io/api/core/v1"
)

// CreateNodePoolCommand includes the immutable configuration name.
type CreateNodePoolCommand struct {
	Name string
	UpdateNodePoolCommand
}

// UpdateNodePoolCommand replaces all mutable configuration fields.
type UpdateNodePoolCommand struct {
	Description  string
	NodeSelector map[string]string
	Tolerations  []corev1.Toleration
}

type NodePoolService interface {
	List(context.Context) ([]*model.NodePool, error)
	Get(context.Context, string) (*model.NodePool, error)
	Create(context.Context, CreateNodePoolCommand) (*model.NodePool, error)
	Update(context.Context, string, UpdateNodePoolCommand) (*model.NodePool, error)
	Delete(context.Context, string) error
}

type nodepoolService struct{ Repo repository.NodePoolRepository }

func NewNodePoolServiceWithRepository(repo repository.NodePoolRepository) (NodePoolService, error) {
	if repo == nil {
		return nil, fmt.Errorf("create nodepool service: repository is nil")
	}
	return &nodepoolService{Repo: repo}, nil
}

func (s *nodepoolService) List(ctx context.Context) ([]*model.NodePool, error) {
	return s.Repo.List(ctx)
}

func (s *nodepoolService) Get(ctx context.Context, name string) (*model.NodePool, error) {
	if err := spec.ValidateNodePoolName(name); err != nil {
		return nil, bcode.WithSafeClientMessage(bcode.ErrNodePoolInvalid, err.Error())
	}
	item, err := s.Repo.Get(ctx, name)
	if errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, bcode.ErrNodePoolNotFound
	}
	return item, err
}

func (s *nodepoolService) Create(ctx context.Context, command CreateNodePoolCommand) (*model.NodePool, error) {
	name := command.Name
	if err := spec.ValidateNodePool(name, command.NodeSelector, command.Tolerations); err != nil {
		return nil, bcode.WithSafeClientMessage(bcode.ErrNodePoolInvalid, err.Error())
	}
	if _, err := s.Repo.Get(ctx, name); err == nil {
		return nil, bcode.ErrNodePoolExists
	} else if !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	item := &model.NodePool{Name: name, Description: command.Description, NodeSelector: command.NodeSelector, Tolerations: command.Tolerations}
	if err := s.Repo.Create(ctx, item); err != nil {
		if errors.Is(err, datastore.ErrRecordExist) {
			return nil, bcode.ErrNodePoolExists
		}
		return nil, err
	}
	return item, nil
}

func (s *nodepoolService) Update(ctx context.Context, name string, command UpdateNodePoolCommand) (*model.NodePool, error) {
	item, err := s.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := spec.ValidateNodePool(name, command.NodeSelector, command.Tolerations); err != nil {
		return nil, bcode.WithSafeClientMessage(bcode.ErrNodePoolInvalid, err.Error())
	}
	item.Description = command.Description
	item.NodeSelector = command.NodeSelector
	item.Tolerations = command.Tolerations
	if err := s.Repo.Update(ctx, item); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrNodePoolNotFound
		}
		return nil, err
	}
	return item, nil
}

func (s *nodepoolService) Delete(ctx context.Context, name string) error {
	if err := spec.ValidateNodePoolName(name); err != nil {
		return bcode.WithSafeClientMessage(bcode.ErrNodePoolInvalid, err.Error())
	}
	if err := s.Repo.Delete(ctx, &model.NodePool{Name: name}); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return bcode.ErrNodePoolNotFound
		}
		return err
	}
	return nil
}
