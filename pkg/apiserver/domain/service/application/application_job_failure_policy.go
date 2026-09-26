package application

import (
	"fmt"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func normalizeJobFailurePolicyForWrite(componentType config.JobType, properties *apisv1.Properties, field string) error {
	if properties == nil {
		return nil
	}
	if properties.JobRetryPolicy != nil {
		retryField := strings.TrimSuffix(field, "failurePolicy") + "jobRetryPolicy"
		if componentType != config.InstantJob || properties.StartTime != 0 {
			return fmt.Errorf("%w: %s only supports immediate job components without startTime", bcode.ErrInvalidProperties, retryField)
		}
		if err := properties.JobRetryPolicy.Validate(); err != nil {
			return fmt.Errorf("%w: %s: %v", bcode.ErrInvalidProperties, retryField, err)
		}
	}
	if properties.FailurePolicy == nil {
		return nil
	}
	if componentType != config.InstantJob {
		return fmt.Errorf("%w: %s is only supported for type job", bcode.ErrInvalidProperties, field)
	}
	policy, ok := workflowconfig.NormalizeJobFailurePolicy(*properties.FailurePolicy)
	if !ok {
		return fmt.Errorf("%w: %s only supports cleanup_failed", bcode.ErrInvalidProperties, field)
	}
	if policy == "" {
		properties.FailurePolicy = nil
		return nil
	}
	properties.FailurePolicy = &policy
	return nil
}

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
			component := componentMap[strings.ToLower(strings.TrimSpace(spec.Name))]
			if component == nil {
				return nil, fmt.Errorf("%w: component %s not found for update", bcode.ErrComponentNotFound, strings.TrimSpace(spec.Name))
			}
			componentType = component.ComponentType
		}
		properties := *spec.Properties
		if err := normalizeJobFailurePolicyForWrite(componentType, &properties, fmt.Sprintf("components[%d].properties.failurePolicy", i)); err != nil {
			return nil, err
		}
		spec.Properties = &properties
	}
	return normalized, nil
}

func validateNoNestedJobFailurePoliciesForWrite(traits apisv1.Traits, fieldPrefix string) error {
	for i, initTrait := range traits.Init {
		field := fmt.Sprintf("%s.init[%d].properties.failurePolicy", fieldPrefix, i)
		if initTrait.Properties.JobRetryPolicy != nil {
			return fmt.Errorf("%w: %s.init[%d].properties.jobRetryPolicy is only supported for top-level job component properties", bcode.ErrInvalidProperties, fieldPrefix, i)
		}
		if initTrait.Properties.FailurePolicy != nil {
			return fmt.Errorf("%w: %s is only supported for top-level job component properties", bcode.ErrInvalidProperties, field)
		}
		if err := validateNoNestedJobFailurePoliciesForWrite(initTrait.Traits, fmt.Sprintf("%s.init[%d].traits", fieldPrefix, i)); err != nil {
			return err
		}
	}
	for i, sidecar := range traits.Sidecar {
		if err := validateNoNestedJobFailurePoliciesForWrite(sidecar.Traits, fmt.Sprintf("%s.sidecar[%d].traits", fieldPrefix, i)); err != nil {
			return err
		}
	}
	return nil
}

func validateTemplateRequestJobFailurePolicyOverrides(components []apisv1.CreateComponentRequest) error {
	for i, component := range components {
		if component.Template == nil || strings.TrimSpace(component.Template.ID) == "" {
			continue
		}
		if err := validateNoNestedJobFailurePoliciesForWrite(component.Traits, fmt.Sprintf("component[%d].traits", i)); err != nil {
			return err
		}
	}
	return nil
}
