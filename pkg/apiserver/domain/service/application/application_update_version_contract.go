package application

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func validateVersionUpdateSharedRemovals(specs []apisv1.ComponentUpdateSpec, componentMap map[string]*model.ApplicationComponent) error {
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return err
		}
		if action != config.ComponentActionRemove {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(spec.Name))
		if key == "" {
			continue
		}
		comp, exists := componentMap[key]
		if !exists || comp == nil {
			continue
		}
		strategy, shared := SharedLifecycleStrategyForComponent(comp)
		if !shared {
			continue
		}
		return fmt.Errorf("%w: shared component %s cannot be removed by version update (strategy=%s)", bcode.ErrApplicationConfig, comp.Name, strategy)
	}
	return nil
}

func parseVersionUpdateComponentAction(spec apisv1.ComponentUpdateSpec) (config.ComponentAction, error) {
	rawAction := strings.TrimSpace(spec.Action)
	if rawAction == "" {
		return config.ComponentActionUpdate, nil
	}
	switch strings.ToLower(rawAction) {
	case string(config.ComponentActionUpdate):
		return config.ComponentActionUpdate, nil
	case string(config.ComponentActionAdd):
		return config.ComponentActionAdd, nil
	case string(config.ComponentActionRemove):
		return config.ComponentActionRemove, nil
	case string(config.ComponentActionRestart):
		return config.ComponentActionRestart, nil
	default:
		componentName := strings.TrimSpace(spec.Name)
		if componentName == "" {
			componentName = "<empty>"
		}
		return "", fmt.Errorf("%w: component %s action %q", bcode.ErrInvalidComponentAction, componentName, spec.Action)
	}
}

func validateVersionUpdateActionContract(specs []apisv1.ComponentUpdateSpec, componentMap map[string]*model.ApplicationComponent) error {
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return err
		}
		key := strings.ToLower(strings.TrimSpace(spec.Name))
		if key == "" {
			continue
		}
		if (action == config.ComponentActionUpdate || action == config.ComponentActionAdd) && spec.Replicas != nil && *spec.Replicas <= 0 {
			return fmt.Errorf("%w: component %s replicas must be greater than 0; /version does not support scale-to-zero", bcode.ErrApplicationConfig, strings.TrimSpace(spec.Name))
		}
		_, exists := componentMap[key]
		switch action {
		case config.ComponentActionUpdate:
			if !exists {
				return fmt.Errorf("%w: component %s not found for update", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
		case config.ComponentActionAdd:
			if exists {
				return fmt.Errorf("%w: component %s already exists for add", bcode.ErrComponentAlreadyExists, strings.TrimSpace(spec.Name))
			}
		case config.ComponentActionRemove:
			if !exists {
				return fmt.Errorf("%w: component %s not found for remove", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
		case config.ComponentActionRestart:
			if !exists {
				return fmt.Errorf("%w: component %s not found for restart", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
		}
	}
	return nil
}

func validateVersionUpdateComponentActionConflicts(specs []apisv1.ComponentUpdateSpec) error {
	type actions struct {
		add     bool
		remove  bool
		restart bool
		update  bool
		name    string
	}
	seen := make(map[string]actions)
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return err
		}
		key := strings.ToLower(strings.TrimSpace(spec.Name))
		if key == "" {
			continue
		}
		current := seen[key]
		current.name = strings.TrimSpace(spec.Name)
		switch action {
		case config.ComponentActionAdd:
			current.add = true
		case config.ComponentActionRemove:
			if current.remove {
				return fmt.Errorf("%w: component %s cannot be removed more than once in one version update request", bcode.ErrDuplicateComponentName, current.name)
			}
			current.remove = true
		case config.ComponentActionRestart:
			if current.restart {
				return fmt.Errorf("%w: component %s cannot be restarted more than once in one version update request", bcode.ErrDuplicateComponentName, current.name)
			}
			current.restart = true
		case config.ComponentActionUpdate:
			if current.update {
				return fmt.Errorf("%w: component %s cannot be updated more than once in one version update request", bcode.ErrDuplicateComponentName, current.name)
			}
			current.update = true
		}
		if current.add && current.remove {
			return fmt.Errorf("%w: component %s cannot be removed and added in one version update request", bcode.ErrDuplicateComponentName, current.name)
		}
		if current.remove && current.update {
			return fmt.Errorf("%w: component %s cannot be removed and updated in one version update request", bcode.ErrDuplicateComponentName, current.name)
		}
		if current.restart && current.add {
			return fmt.Errorf("%w: component %s cannot be restarted and added in one version update request", bcode.ErrDuplicateComponentName, current.name)
		}
		if current.restart && current.remove {
			return fmt.Errorf("%w: component %s cannot be restarted and removed in one version update request", bcode.ErrDuplicateComponentName, current.name)
		}
		if current.restart && current.update {
			return fmt.Errorf("%w: component %s cannot be restarted and updated in one version update request", bcode.ErrDuplicateComponentName, current.name)
		}
		seen[key] = current
	}
	return nil
}

func buildVersionUpdateResolvedComponents(existing []*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec) ([]apisv1.CreateComponentRequest, error) {
	componentsByName := make(map[string]apisv1.CreateComponentRequest, len(existing)+len(specs))
	orderedNames := make([]string, 0, len(existing)+len(specs))
	orderedNameSet := make(map[string]struct{}, len(existing)+len(specs))
	for _, component := range existing {
		if component == nil {
			continue
		}
		request, err := convertComponentModelToCreateRequest(component)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(strings.TrimSpace(request.Name))
		if key == "" {
			continue
		}
		if _, exists := orderedNameSet[key]; !exists {
			orderedNames = append(orderedNames, key)
			orderedNameSet[key] = struct{}{}
		}
		componentsByName[key] = request
	}

	for _, spec := range specs {
		key := strings.ToLower(strings.TrimSpace(spec.Name))
		if key == "" {
			continue
		}
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return nil, err
		}
		switch action {
		case config.ComponentActionUpdate:
			current, exists := componentsByName[key]
			if !exists {
				return nil, fmt.Errorf("%w: component %s not found for update", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			updated, err := applyComponentUpdateSpecToResolvedComponent(current, spec)
			if err != nil {
				return nil, err
			}
			componentsByName[key] = updated
		case config.ComponentActionAdd:
			if _, exists := componentsByName[key]; exists {
				return nil, fmt.Errorf("%w: component %s already exists for add", bcode.ErrComponentAlreadyExists, strings.TrimSpace(spec.Name))
			}
			added, err := componentUpdateSpecToResolvedComponent(spec)
			if err != nil {
				return nil, err
			}
			componentsByName[key] = added
			if _, exists := orderedNameSet[key]; !exists {
				orderedNames = append(orderedNames, key)
				orderedNameSet[key] = struct{}{}
			}
		case config.ComponentActionRemove:
			if _, exists := componentsByName[key]; !exists {
				return nil, fmt.Errorf("%w: component %s not found for remove", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			delete(componentsByName, key)
		case config.ComponentActionRestart:
			if _, exists := componentsByName[key]; !exists {
				return nil, fmt.Errorf("%w: component %s not found for restart", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
		}
	}

	resolved := make([]apisv1.CreateComponentRequest, 0, len(componentsByName))
	for _, name := range orderedNames {
		component, exists := componentsByName[name]
		if !exists {
			continue
		}
		resolved = append(resolved, component)
	}
	return resolved, nil
}

func componentUpdateSpecToResolvedComponent(spec apisv1.ComponentUpdateSpec) (apisv1.CreateComponentRequest, error) {
	replicas := int32(1)
	if spec.Replicas != nil {
		replicas = *spec.Replicas
	}

	resolved := apisv1.CreateComponentRequest{
		Name:          spec.Name,
		ComponentType: spec.ComponentType,
		Image:         spec.Image,
		Replicas:      replicas,
	}
	if spec.Properties != nil {
		if reserved := reservedComponentLabelsIn(spec.Properties.Labels); len(reserved) > 0 {
			return apisv1.CreateComponentRequest{}, fmt.Errorf("%w: component %s properties.labels contains reserved keys: %s", bcode.ErrInvalidProperties, spec.Name, strings.Join(reserved, ","))
		}
		resolved.Properties = *spec.Properties
	}
	if len(spec.Env) > 0 {
		if resolved.Properties.Env == nil {
			resolved.Properties.Env = make(map[string]string, len(spec.Env))
		}
		for k, v := range spec.Env {
			resolved.Properties.Env[k] = v
		}
	}
	if spec.Traits != nil {
		if err := validateComponentTraitsForWrite(spec.ComponentType, *spec.Traits, fmt.Sprintf("component[%s].traits", strings.TrimSpace(spec.Name))); err != nil {
			return apisv1.CreateComponentRequest{}, err
		}
		resolved.Traits = *spec.Traits
	}
	return resolved, nil
}

func applyComponentUpdateSpecToResolvedComponent(current apisv1.CreateComponentRequest, spec apisv1.ComponentUpdateSpec) (apisv1.CreateComponentRequest, error) {
	if spec.Image != "" {
		current.Image = spec.Image
	}
	if spec.Replicas != nil {
		current.Replicas = *spec.Replicas
	}
	if spec.Properties != nil {
		if reserved := reservedComponentLabelsIn(spec.Properties.Labels); len(reserved) > 0 {
			return apisv1.CreateComponentRequest{}, fmt.Errorf("%w: component %s properties.labels contains reserved keys: %s", bcode.ErrInvalidProperties, spec.Name, strings.Join(reserved, ","))
		}
		current.Properties = *spec.Properties
	}
	if len(spec.Env) > 0 {
		if current.Properties.Env == nil {
			current.Properties.Env = make(map[string]string, len(spec.Env))
		}
		for k, v := range spec.Env {
			current.Properties.Env[k] = v
		}
	}
	if spec.Traits != nil {
		if err := validateComponentTraitsForWrite(current.ComponentType, *spec.Traits, fmt.Sprintf("component[%s].traits", strings.TrimSpace(spec.Name))); err != nil {
			return apisv1.CreateComponentRequest{}, err
		}
		current.Traits = *spec.Traits
	}
	return current, nil
}

func componentPropertiesEqual(raw *model.JSONStruct, desired apisv1.Properties) (bool, error) {
	var current apisv1.Properties
	if err := decodeJSONStruct(raw, &current); err != nil {
		return false, err
	}
	return reflect.DeepEqual(current, desired), nil
}

func componentTraitsEqual(raw *model.JSONStruct, desired apisv1.Traits) (bool, error) {
	var current apisv1.Traits
	if err := decodeJSONStruct(raw, &current); err != nil {
		return false, err
	}
	return reflect.DeepEqual(current, desired), nil
}

func hasVersionUpdateComponentChanges(componentMap map[string]*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec) (bool, error) {
	hasChanges := false
	for _, spec := range specs {
		action, err := parseVersionUpdateComponentAction(spec)
		if err != nil {
			return false, err
		}
		compName := strings.ToLower(strings.TrimSpace(spec.Name))
		if compName == "" {
			continue
		}

		switch action {
		case config.ComponentActionUpdate:
			comp, exists := componentMap[compName]
			if !exists {
				return false, fmt.Errorf("%w: component %s not found for update", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			changed, err := componentUpdateSpecHasChanges(comp, spec)
			if err != nil {
				return false, err
			}
			if changed {
				hasChanges = true
			}
		case config.ComponentActionAdd:
			if _, exists := componentMap[compName]; exists {
				return false, fmt.Errorf("%w: component %s already exists for add", bcode.ErrComponentAlreadyExists, strings.TrimSpace(spec.Name))
			}
			hasChanges = true
		case config.ComponentActionRemove:
			if _, exists := componentMap[compName]; !exists {
				return false, fmt.Errorf("%w: component %s not found for remove", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			hasChanges = true
		case config.ComponentActionRestart:
			if _, exists := componentMap[compName]; !exists {
				return false, fmt.Errorf("%w: component %s not found for restart", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
		}
	}
	return hasChanges, nil
}

func componentUpdateSpecHasChanges(comp *model.ApplicationComponent, spec apisv1.ComponentUpdateSpec) (bool, error) {
	if comp == nil {
		return false, nil
	}

	hasChanges := false
	if spec.Image != "" && spec.Image != comp.Image {
		hasChanges = true
	}
	if spec.Replicas != nil && *spec.Replicas != comp.Replicas {
		hasChanges = true
	}
	if spec.Properties != nil {
		if reserved := reservedComponentLabelsIn(spec.Properties.Labels); len(reserved) > 0 {
			return false, fmt.Errorf("%w: component %s properties.labels contains reserved keys: %s", bcode.ErrInvalidProperties, spec.Name, strings.Join(reserved, ","))
		}
		equal, err := componentPropertiesEqual(comp.Properties, *spec.Properties)
		if err != nil {
			return false, err
		}
		if !equal {
			hasChanges = true
		}
	}
	if len(spec.Env) > 0 {
		changed, err := componentEnvUpdatesHaveChanges(comp.Properties, spec.Env)
		if err != nil {
			return false, err
		}
		if changed {
			hasChanges = true
		}
	}
	if spec.Traits != nil {
		if err := validateComponentTraitsForWrite(comp.ComponentType, *spec.Traits, fmt.Sprintf("component[%s].traits", strings.TrimSpace(spec.Name))); err != nil {
			return false, err
		}
		equal, err := componentTraitsEqual(comp.Traits, *spec.Traits)
		if err != nil {
			return false, err
		}
		if !equal {
			hasChanges = true
		}
	}
	return hasChanges, nil
}

func componentEnvUpdatesHaveChanges(raw *model.JSONStruct, envUpdates map[string]string) (bool, error) {
	if len(envUpdates) == 0 {
		return false, nil
	}
	if raw == nil {
		return true, nil
	}

	var props apisv1.Properties
	if err := decodeJSONStruct(raw, &props); err != nil {
		return false, err
	}
	for key, value := range envUpdates {
		if props.Env == nil || props.Env[key] != value {
			return true, nil
		}
	}
	return false, nil
}
