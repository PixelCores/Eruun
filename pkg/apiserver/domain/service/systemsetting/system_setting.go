package systemsetting

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

// systemSettingNormalizers lists every public setting type. Each value is a
// JSON object normalized by its owner before it is stored.
var systemSettingNormalizers = map[string]func(json.RawMessage) (json.RawMessage, error){
	model.SystemSettingTypeWorkflowScheduler: workflowconfig.NormalizeJobSchedulerPolicyValue,
	model.SystemSettingTypeURLSecurityPolicy: normalizeURLSecurityPolicySettingValue,
	model.SystemSettingTypePodRestartMonitor: spec.NormalizePodRestartMonitorSettingValue,
}

// SystemSettingService manages system settings.
type SystemSettingService interface {
	Create(ctx context.Context, req apisv1.CreateSystemSettingRequest) (*apisv1.SystemSetting, error)
	Update(ctx context.Context, settingType string, req apisv1.UpdateSystemSettingRequest) (*apisv1.SystemSetting, error)
	Delete(ctx context.Context, settingType string) error
	Get(ctx context.Context, settingType string) (*apisv1.SystemSetting, error)
	List(ctx context.Context) ([]*apisv1.SystemSetting, error)
}

type systemSettingServiceImpl struct {
	SettingRepo repository.SystemSettingRepository `inject:""`
}

// NewSystemSettingService creates a new SystemSettingService.
func NewSystemSettingService() SystemSettingService {
	return &systemSettingServiceImpl{}
}

func (s *systemSettingServiceImpl) Create(ctx context.Context, req apisv1.CreateSystemSettingRequest) (*apisv1.SystemSetting, error) {
	settingType := strings.TrimSpace(req.Type)
	if err := validateSettingType(settingType); err != nil {
		return nil, err
	}
	normalizedValue, err := normalizeAndValidateSettingValue(settingType, req.Value)
	if err != nil {
		return nil, err
	}
	setting := &model.SystemSetting{
		Type:  settingType,
		Value: normalizedValue,
	}
	if err := s.SettingRepo.Create(ctx, setting); err != nil {
		if errors.Is(err, datastore.ErrRecordExist) {
			return nil, bcode.ErrSystemSettingExists
		}
		return nil, err
	}
	return toSystemSettingDTO(setting), nil
}

func (s *systemSettingServiceImpl) Update(ctx context.Context, settingType string, req apisv1.UpdateSystemSettingRequest) (*apisv1.SystemSetting, error) {
	settingType = strings.TrimSpace(settingType)
	if err := validateSettingType(settingType); err != nil {
		return nil, err
	}
	normalizedValue, err := normalizeAndValidateSettingValue(settingType, req.Value)
	if err != nil {
		return nil, err
	}

	setting, err := s.SettingRepo.FindByType(ctx, settingType)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrSystemSettingNotFound
		}
		return nil, err
	}
	setting.Value = normalizedValue
	if err := s.SettingRepo.Update(ctx, setting); err != nil {
		return nil, err
	}
	return toSystemSettingDTO(setting), nil
}

func (s *systemSettingServiceImpl) Delete(ctx context.Context, settingType string) error {
	settingType = strings.TrimSpace(settingType)
	if settingType == model.SystemSettingTypeWorkflowScheduler {
		return bcode.ErrSystemSettingTypeInvalid
	}
	if err := validateSettingType(settingType); err != nil {
		return err
	}
	setting := &model.SystemSetting{Type: settingType}
	if err := s.SettingRepo.Delete(ctx, setting); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return bcode.ErrSystemSettingNotFound
		}
		return err
	}
	return nil
}

func (s *systemSettingServiceImpl) Get(ctx context.Context, settingType string) (*apisv1.SystemSetting, error) {
	settingType = strings.TrimSpace(settingType)
	if err := validateSettingType(settingType); err != nil {
		return nil, err
	}
	setting, err := s.SettingRepo.FindByType(ctx, settingType)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrSystemSettingNotFound
		}
		return nil, err
	}
	return toSystemSettingDTO(setting), nil
}

func (s *systemSettingServiceImpl) List(ctx context.Context) ([]*apisv1.SystemSetting, error) {
	items, err := s.SettingRepo.List(ctx, datastore.ListOptions{Page: 1, PageSize: 1000})
	if err != nil {
		return nil, err
	}
	out := make([]*apisv1.SystemSetting, 0, len(items))
	for _, item := range items {
		// Internal markers and retired types share the persistence table but are
		// not part of the public system-setting API.
		if item == nil || validateSettingType(item.Type) != nil {
			continue
		}
		out = append(out, toSystemSettingDTO(item))
	}
	return out, nil
}

func validateSettingType(settingType string) error {
	if _, ok := systemSettingNormalizers[strings.TrimSpace(settingType)]; ok {
		return nil
	}
	return bcode.ErrSystemSettingTypeInvalid
}

func normalizeAndValidateSettingValue(settingType string, value json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(value)
	var decoded map[string]interface{}
	if err := json.Unmarshal(trimmed, &decoded); err != nil || decoded == nil {
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	normalized, err := systemSettingNormalizers[settingType](trimmed)
	if err != nil {
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	return normalized, nil
}

func normalizeURLSecurityPolicySettingValue(value json.RawMessage) (json.RawMessage, error) {
	var cfg spec.URLSecurityPolicySpec
	if err := json.Unmarshal(value, &cfg); err != nil {
		return nil, err
	}
	if err := spec.ValidateURLSecurityPolicy(cfg); err != nil {
		return nil, err
	}
	return json.RawMessage(value), nil
}

func toSystemSettingDTO(setting *model.SystemSetting) *apisv1.SystemSetting {
	if setting == nil {
		return nil
	}
	return &apisv1.SystemSetting{
		Type:       setting.Type,
		Value:      setting.Value,
		CreateTime: setting.CreateTime,
		UpdateTime: setting.UpdateTime,
	}
}
