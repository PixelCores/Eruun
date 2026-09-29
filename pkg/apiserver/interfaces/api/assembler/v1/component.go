package v1

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func ConvertComponentModelsToDTO(components []*model.ApplicationComponent) ([]*apisv1.ApplicationComponent, error) {
	secrets, err := buildComponentSecretIndex(components)
	if err != nil {
		return nil, err
	}
	result := make([]*apisv1.ApplicationComponent, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		dto, err := convertComponentModelToDTOBase(component)
		if err != nil {
			return nil, err
		}
		if dto != nil {
			result = append(result, dto)
		}
	}
	for _, component := range result {
		enrichComponentResourceDetails(component, secrets)
	}
	return result, nil
}

func ConvertComponentModelToDTO(component *model.ApplicationComponent) (*apisv1.ApplicationComponent, error) {
	dto, err := convertComponentModelToDTOBase(component)
	if err != nil || dto == nil {
		return dto, err
	}
	enrichComponentResourceDetails(dto, nil)
	return dto, nil
}

func convertComponentModelToDTOBase(component *model.ApplicationComponent) (*apisv1.ApplicationComponent, error) {
	if component == nil {
		return nil, nil
	}

	dto := &apisv1.ApplicationComponent{
		ID:              component.ID,
		AppID:           component.AppID,
		ResourceAppName: component.ResourceAppName,
		Name:            component.Name,
		Namespace:       component.Namespace,
		Image:           component.Image,
		Replicas:        component.Replicas,
		ComponentType:   component.ComponentType,
		Status:          component.Status,
		LastAbnormal:    component.LastAbnormal,
		CreateTime:      component.CreateTime,
		UpdateTime:      component.UpdateTime,
	}
	if strings.TrimSpace(dto.Status) == "" {
		dto.Status = string(config.ComponentStatusNotDeploy)
	}

	if err := decodeJSONStruct(component.Properties, &dto.Properties); err != nil {
		return nil, fmt.Errorf("convert component %s properties: %w", component.Name, err)
	}
	if err := decodeJSONStruct(component.Traits, &dto.Traits); err != nil {
		return nil, fmt.Errorf("convert component %s traits: %w", component.Name, err)
	}
	if len(dto.Traits.Sidecar) > 0 {
		dto.Sidecars = append(dto.Sidecars, dto.Traits.Sidecar...)
	}
	dto.ExternalLinks = buildComponentExternalLinks(dto)
	return dto, nil
}

type componentSecretIndex map[string]componentSecretValues

type componentSecretValues struct {
	entries map[string]componentSecretValue
}

type componentSecretValue struct {
	value string
	ready bool
}

func enrichComponentResourceDetails(component *apisv1.ApplicationComponent, secrets componentSecretIndex) {
	if component == nil {
		return
	}
	component.Services = buildComponentServices(component)
	component.Ingresses = buildComponentIngresses(component)
	component.ResourceConfigs = buildComponentResourceConfigs(component)
	component.Credentials = buildComponentCredentials(component, secrets)
}

func buildComponentSecretIndex(components []*model.ApplicationComponent) (componentSecretIndex, error) {
	secrets := make(componentSecretIndex)
	for _, component := range components {
		if component == nil || component.ComponentType != config.SecretJob {
			continue
		}
		key := componentSecretKey(component.Namespace, component.Name)
		if key == "" {
			continue
		}
		if _, exists := secrets[key]; exists {
			continue
		}
		var properties model.Properties
		if err := decodeJSONStruct(component.Properties, &properties); err != nil {
			return nil, fmt.Errorf("decode secret component %s properties: %w", component.Name, err)
		}
		secrets[key] = componentSecretValues{
			entries: buildComponentSecretValueEntries(component, properties.Secret),
		}
	}
	return secrets, nil
}
