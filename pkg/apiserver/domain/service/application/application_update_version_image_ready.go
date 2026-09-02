package application

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func normalizeVersionUpdateImageReadyTimeoutSeconds(value int64) (int64, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: imageReadyTimeoutSeconds must be >= 0", bcode.ErrApplicationConfig)
	}
	if value == 0 {
		return int64(config.DefaultVersionUpdateImageReadyTimeout), nil
	}
	if value > int64(config.DeployTimeout) {
		return 0, fmt.Errorf("%w: imageReadyTimeoutSeconds must be <= %d", bcode.ErrApplicationConfig, config.DeployTimeout)
	}
	return value, nil
}

func versionUpdateImageReadyComponents(componentMap map[string]*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec) ([]string, error) {
	return versionUpdateReadyComponents(componentMap, specs, true)
}

func versionUpdateReadyUpdateComponents(componentMap map[string]*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec) ([]string, error) {
	return versionUpdateReadyComponents(componentMap, specs, false)
}

func versionUpdateReadyComponents(componentMap map[string]*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec, includeAdds bool) ([]string, error) {
	seen := make(map[string]struct{})
	targets := make([]string, 0)
	appendTarget := func(name string) {
		name = strings.TrimSpace(name)
		key := strings.ToLower(name)
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, name)
	}

	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return nil, err
		}
		name := strings.TrimSpace(spec.Name)
		key := strings.ToLower(name)
		switch action {
		case config.ComponentActionUpdate:
			comp := componentMap[key]
			if comp == nil || !versionUpdateImageReadyComponentType(comp.ComponentType) {
				continue
			}
			changed, err := componentUpdateSpecHasChanges(comp, spec)
			if err != nil {
				return nil, err
			}
			if !changed {
				continue
			}
			appendTarget(comp.Name)
		case config.ComponentActionAdd:
			if !includeAdds || strings.TrimSpace(spec.Image) == "" || !versionUpdateImageReadyComponentType(spec.ComponentType) {
				continue
			}
			appendTarget(name)
		}
	}
	return targets, nil
}

func versionUpdateImageReadyComponentType(componentType config.JobType) bool {
	switch componentType {
	case config.ServerJob, config.StoreJob:
		return true
	default:
		return false
	}
}
