package application

import (
	"fmt"

	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func normalizeVersionUpdateJobFailurePolicies(specs []apisv1.ComponentUpdateSpec, componentMap map[string]*model.ApplicationComponent) ([]apisv1.ComponentUpdateSpec, error) {
	normalized := append([]apisv1.ComponentUpdateSpec(nil), specs...)
	for i := range normalized {
		spec := &normalized[i]
		if spec.Properties == nil || (spec.Properties.FailurePolicy == nil && spec.Properties.JobRetryPolicy == nil) {
			continue
		}
		action, err := parseVersionUpdateComponentAction(*spec)
		if err != nil {
			return nil, err
		}
		if action != domainspec.ComponentActionAdd && action != domainspec.ComponentActionUpdate {
			return nil, fmt.Errorf("%w: components[%d].properties.failurePolicy/jobRetryPolicy is only supported for add or update actions", bcode.ErrInvalidProperties, i)
		}

		componentType := spec.ComponentType
		if action == domainspec.ComponentActionUpdate {
			component := componentMap[spec.Name]
			if component == nil {
				return nil, fmt.Errorf("%w: component %s not found for update", bcode.ErrComponentNotFound, spec.Name)
			}
			componentType = component.ComponentType
		}
		properties := *spec.Properties
		if err := firstValidationError(bcode.ErrInvalidProperties, validateJobPolicies(componentType, properties, fmt.Sprintf("components[%d].properties", i))); err != nil {
			return nil, err
		}
		normalizeJobFailurePolicy(&properties)
		spec.Properties = &properties
	}
	return normalized, nil
}
