package systemsetting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	wfcloudjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob"
	wfaliyun "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/aliyun"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

var builtinSystemSettingSupports = map[string]wfcloudjob.CloudProviderSettingSupport{
	model.SystemSettingTypeAliyunCloud: wfaliyun.NewProvider(),
}

type systemSettingCodec struct {
	accept    func(interface{}) bool
	normalize func(json.RawMessage) (json.RawMessage, error)
	sanitize  func(json.RawMessage) json.RawMessage
}

type jsonFieldMask struct {
	path        []string
	replacement interface{}
}

var builtinSystemSettingCodecs = map[string]systemSettingCodec{
	model.SystemSettingTypeNodeSelector: {
		normalize: normalizeRawSystemSettingValue,
	},
	model.SystemSettingTypeRBACPolicies: {
		accept:    isJSONArray,
		normalize: normalizeRawSystemSettingValue,
	},
	model.SystemSettingTypeURLSecurityPolicy: {
		accept:    isJSONObject,
		normalize: normalizeURLSecurityPolicySettingValue,
	},
	model.SystemSettingTypePodRestartMonitor: {
		accept:    isJSONObject,
		normalize: spec.NormalizePodRestartMonitorSettingValue,
	},
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
	if _, ok := getSystemSettingSupport(settingType); ok {
		if _, err := s.SettingRepo.FindByType(ctx, settingType); err == nil {
			return nil, bcode.ErrSystemSettingExists
		} else if err != datastore.ErrRecordNotExist {
			return nil, err
		}
		if err := validateProviderSettingConnectivity(ctx, settingType, normalizedValue); err != nil {
			return nil, err
		}
	}

	setting := &model.SystemSetting{
		Type:  settingType,
		Value: normalizedValue,
	}
	if err := s.SettingRepo.Create(ctx, setting); err != nil {
		if err == datastore.ErrRecordExist {
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
		if err == datastore.ErrRecordNotExist {
			return nil, bcode.ErrSystemSettingNotFound
		}
		return nil, err
	}
	if err := validateProviderSettingConnectivity(ctx, settingType, normalizedValue); err != nil {
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
	if err := validateSettingType(settingType); err != nil {
		return err
	}
	setting := &model.SystemSetting{Type: settingType}
	if err := s.SettingRepo.Delete(ctx, setting); err != nil {
		if err == datastore.ErrRecordNotExist {
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
		if err == datastore.ErrRecordNotExist {
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
		if item == nil {
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(item.Type), "migration.") {
			// Internal migration markers share the persistence table but are not
			// part of the public system-setting API.
			continue
		}
		out = append(out, toSystemSettingDTO(item))
	}
	return out, nil
}

func validateSettingType(settingType string) error {
	if _, ok := getSystemSettingCodec(settingType); ok {
		return nil
	}
	return bcode.ErrSystemSettingTypeInvalid
}

func normalizeAndValidateSettingValue(settingType string, value json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 {
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	var decoded interface{}
	if err := json.Unmarshal(trimmed, &decoded); err != nil {
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	switch decoded.(type) {
	case map[string]interface{}, []interface{}:
		// ok
	default:
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	codec, ok := getSystemSettingCodec(settingType)
	if !ok {
		return json.RawMessage(trimmed), nil
	}
	if codec.accept != nil && !codec.accept(decoded) {
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	normalized, err := codec.normalize(trimmed)
	if err != nil {
		return nil, bcode.ErrSystemSettingValueInvalid
	}
	return normalized, nil
}

func normalizeRawSystemSettingValue(value json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(value), nil
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

func isJSONObject(decoded interface{}) bool {
	_, ok := decoded.(map[string]interface{})
	return ok
}

func isJSONArray(decoded interface{}) bool {
	_, ok := decoded.([]interface{})
	return ok
}

func validateProviderSettingConnectivity(ctx context.Context, settingType string, value json.RawMessage) error {
	settingSupport, ok := getSystemSettingSupport(settingType)
	if !ok {
		return nil
	}
	if err := settingSupport.ValidateSystemSettingConnectivity(ctx, value); err != nil {
		return fmt.Errorf("%w: %v", bcode.ErrSystemSettingConnectivityCheckFailed, err)
	}
	return nil
}

func toSystemSettingDTO(setting *model.SystemSetting) *apisv1.SystemSetting {
	if setting == nil {
		return nil
	}
	value := sanitizeSystemSettingValue(setting.Type, setting.Value)
	return &apisv1.SystemSetting{
		Type:       setting.Type,
		Value:      value,
		CreateTime: setting.CreateTime,
		UpdateTime: setting.UpdateTime,
	}
}

func sanitizeSystemSettingValue(settingType string, value json.RawMessage) json.RawMessage {
	codec, ok := getSystemSettingCodec(settingType)
	if !ok || codec.sanitize == nil {
		return json.RawMessage(value)
	}
	return codec.sanitize(value)
}

func sanitizeJSONObjectValue(value json.RawMessage, masks ...jsonFieldMask) json.RawMessage {
	var obj map[string]interface{}
	if err := json.Unmarshal(value, &obj); err != nil {
		return json.RawMessage(`{}`)
	}

	for _, mask := range masks {
		_ = maskNestedFieldCaseInsensitive(obj, mask.path, mask.replacement)
	}

	sanitized, err := json.Marshal(obj)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(sanitized)
}

func maskNestedFieldCaseInsensitive(obj map[string]interface{}, path []string, replacement interface{}) bool {
	if len(path) == 0 || obj == nil {
		return false
	}

	if len(path) == 1 {
		masked := false
		target := strings.TrimSpace(path[0])
		for key := range obj {
			if strings.EqualFold(strings.TrimSpace(key), target) {
				obj[key] = replacement
				masked = true
			}
		}
		return masked
	}

	masked := false
	target := strings.TrimSpace(path[0])
	for key, value := range obj {
		if !strings.EqualFold(strings.TrimSpace(key), target) {
			continue
		}

		switch node := value.(type) {
		case map[string]interface{}:
			if maskNestedFieldCaseInsensitive(node, path[1:], replacement) {
				masked = true
			}
		case []interface{}:
			for _, item := range node {
				child, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				if maskNestedFieldCaseInsensitive(child, path[1:], replacement) {
					masked = true
				}
			}
		}
	}
	return masked
}

func getSystemSettingCodec(settingType string) (systemSettingCodec, bool) {
	normalized := strings.TrimSpace(settingType)
	if normalized == "" {
		return systemSettingCodec{}, false
	}
	if codec, ok := builtinSystemSettingCodecs[normalized]; ok {
		return codec, true
	}
	if settingSupport, ok := getSystemSettingSupport(normalized); ok {
		return systemSettingCodec{
			accept:    isJSONObject,
			normalize: settingSupport.NormalizeSystemSettingValue,
			sanitize:  settingSupport.SanitizeSystemSettingValue,
		}, true
	}
	return systemSettingCodec{}, false
}

func getSystemSettingSupport(settingType string) (wfcloudjob.CloudProviderSettingSupport, bool) {
	normalized := strings.TrimSpace(settingType)
	if normalized == "" {
		return nil, false
	}
	if support, ok := wfcloudjob.GetCloudProviderSettingSupport(normalized); ok {
		return support, true
	}
	support, ok := builtinSystemSettingSupports[normalized]
	return support, ok
}
