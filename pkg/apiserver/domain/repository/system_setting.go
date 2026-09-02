package repository

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// SystemSettingRepository defines the interface for system setting data operations.
type SystemSettingRepository interface {
	FindByType(ctx context.Context, settingType string) (*model.SystemSetting, error)
	Create(ctx context.Context, setting *model.SystemSetting) error
	Update(ctx context.Context, setting *model.SystemSetting) error
	Delete(ctx context.Context, setting *model.SystemSetting) error
	List(ctx context.Context, options datastore.ListOptions) ([]*model.SystemSetting, error)
}

// NewSystemSettingRepository creates a new SystemSettingRepository.
func NewSystemSettingRepository() SystemSettingRepository {
	return &systemSettingRepository{}
}

// NewSystemSettingRepositoryWithStore creates a SystemSettingRepository with an explicit datastore.
func NewSystemSettingRepositoryWithStore(store datastore.DataStore) SystemSettingRepository {
	return &systemSettingRepository{Store: store}
}

type systemSettingRepository struct {
	Store datastore.DataStore `inject:"datastore"`
}

func (r *systemSettingRepository) FindByType(ctx context.Context, settingType string) (*model.SystemSetting, error) {
	setting := &model.SystemSetting{Type: settingType}
	if err := r.Store.Get(ctx, setting); err != nil {
		return nil, err
	}
	return setting, nil
}

func (r *systemSettingRepository) Create(ctx context.Context, setting *model.SystemSetting) error {
	return r.Store.Add(ctx, setting)
}

func (r *systemSettingRepository) Update(ctx context.Context, setting *model.SystemSetting) error {
	return r.Store.Put(ctx, setting)
}

func (r *systemSettingRepository) Delete(ctx context.Context, setting *model.SystemSetting) error {
	return r.Store.Delete(ctx, setting)
}

func (r *systemSettingRepository) List(ctx context.Context, options datastore.ListOptions) ([]*model.SystemSetting, error) {
	var query model.SystemSetting
	entities, err := r.Store.List(ctx, &query, &options)
	if err != nil {
		return nil, err
	}
	out := make([]*model.SystemSetting, 0, len(entities))
	for _, entity := range entities {
		setting, ok := entity.(*model.SystemSetting)
		if !ok {
			continue
		}
		out = append(out, setting)
	}
	return out, nil
}
