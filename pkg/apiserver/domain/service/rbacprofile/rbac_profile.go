package rbacprofile

import (
	"context"
	"errors"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// CreateRBACProfileCommand includes the immutable configuration name.
type CreateRBACProfileCommand struct {
	Name string
	UpdateRBACProfileCommand
}

// UpdateRBACProfileCommand replaces all mutable configuration fields.
type UpdateRBACProfileCommand struct {
	Description string
	Rules       []spec.RBACRuleSpec
}

type RBACProfileService interface {
	List(context.Context) ([]*model.RBACProfile, error)
	Get(context.Context, string) (*model.RBACProfile, error)
	Create(context.Context, CreateRBACProfileCommand) (*model.RBACProfile, error)
	Update(context.Context, string, UpdateRBACProfileCommand) (*model.RBACProfile, error)
	Delete(context.Context, string) error
}

type rbacprofileService struct {
	Repo repository.RBACProfileRepository
}

func NewRBACProfileServiceWithRepository(repo repository.RBACProfileRepository) (RBACProfileService, error) {
	if repo == nil {
		return nil, fmt.Errorf("create rbacprofile service: repository is nil")
	}
	return &rbacprofileService{Repo: repo}, nil
}

func (s *rbacprofileService) List(ctx context.Context) ([]*model.RBACProfile, error) {
	return s.Repo.List(ctx)
}

func (s *rbacprofileService) Get(ctx context.Context, name string) (*model.RBACProfile, error) {
	if err := spec.ValidateSchedulingProfileName(name); err != nil {
		return nil, bcode.WithSafeClientMessage(bcode.ErrRBACProfileInvalid, err.Error())
	}
	item, err := s.Repo.Get(ctx, name)
	if errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, bcode.ErrRBACProfileNotFound
	}
	return item, err
}

func (s *rbacprofileService) Create(ctx context.Context, command CreateRBACProfileCommand) (*model.RBACProfile, error) {
	name := command.Name
	if err := spec.ValidateRBACProfile(name, command.Rules); err != nil {
		return nil, bcode.WithSafeClientMessage(bcode.ErrRBACProfileInvalid, err.Error())
	}
	if _, err := s.Repo.Get(ctx, name); err == nil {
		return nil, bcode.ErrRBACProfileExists
	} else if !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}
	item := &model.RBACProfile{Name: name, Description: command.Description, Rules: command.Rules}
	if err := s.Repo.Create(ctx, item); err != nil {
		if errors.Is(err, datastore.ErrRecordExist) {
			return nil, bcode.ErrRBACProfileExists
		}
		return nil, err
	}
	return item, nil
}

func (s *rbacprofileService) Update(ctx context.Context, name string, command UpdateRBACProfileCommand) (*model.RBACProfile, error) {
	item, err := s.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := spec.ValidateRBACProfile(name, command.Rules); err != nil {
		return nil, bcode.WithSafeClientMessage(bcode.ErrRBACProfileInvalid, err.Error())
	}
	item.Description = command.Description
	item.Rules = command.Rules
	if err := s.Repo.Update(ctx, item); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrRBACProfileNotFound
		}
		return nil, err
	}
	return item, nil
}

func (s *rbacprofileService) Delete(ctx context.Context, name string) error {
	if err := spec.ValidateSchedulingProfileName(name); err != nil {
		return bcode.WithSafeClientMessage(bcode.ErrRBACProfileInvalid, err.Error())
	}
	if err := s.Repo.Delete(ctx, &model.RBACProfile{Name: name}); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return bcode.ErrRBACProfileNotFound
		}
		return err
	}
	return nil
}
