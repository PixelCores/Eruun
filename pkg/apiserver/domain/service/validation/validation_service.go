package validation

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

// validateServiceTrait validates a service trait.
func (v *validationServiceImpl) validateServiceTrait(service spec.ServiceTraitSpec, field string) []apisv1.ValidationError {
	return validateServiceTraitSpec(service, field)
}

func validateServiceTraitSpec(service spec.ServiceTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	if serviceName := strings.TrimSpace(service.Name); serviceName != "" {
		errors = append(errors, validateKubeResourceName(serviceName, fmt.Sprintf("%s.name", field))...)
	}
	errors = append(errors, validateReservedLabelMap(service.Labels, fmt.Sprintf("%s.labels", field), "traits.service.labels")...)

	serviceType, known := config.NormalizeServiceAccessType(service.Type)
	if !known {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.type", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid service type: %s, must be one of: internal, node, public, external", service.Type),
		})
	}

	if service.Headless && serviceType != config.ServiceAccessInternal {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.headless", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: "headless service only supports internal type",
		})
	}

	if serviceType != config.ServiceAccessExternal && len(service.Selector) == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.selector", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "selector is required for non-external service",
		})
	}
	if serviceType == config.ServiceAccessExternal && strings.TrimSpace(service.ExternalName) == "" {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.externalName", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "externalName is required for external service",
		})
	}

	if len(service.Ports) == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.ports", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "service ports are required",
		})
		return errors
	}
	for i, port := range service.Ports {
		errors = append(errors, validateServicePortTrait(port, fmt.Sprintf("%s.ports[%d]", field, i))...)
	}
	return errors
}

func validateKubeResourceName(name, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	if len(name) > maxNameLength {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeNameTooLong,
			Message: fmt.Sprintf("%s must be at most %d characters", field, maxNameLength),
		})
		return errors
	}
	if !nameRegexp.MatchString(name) {
		errors = append(errors, apisv1.ValidationError{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidNameFormat,
			Message: fmt.Sprintf("%s must match DNS-1123 subdomain (lowercase alphanumeric, may contain hyphens, must start and end with alphanumeric)", field),
		})
	}
	return errors
}

func validateServicePortTrait(port spec.ServicePortTraitSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError

	if port.Port <= 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.port", field),
			Code:    apisv1.ErrCodeMissingRequiredField,
			Message: "service port is required and must be positive",
		})
	}

	if port.TargetPort < 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.targetPort", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: "targetPort must be greater than or equal to zero",
		})
	}

	protocol := strings.ToUpper(strings.TrimSpace(port.Protocol))
	if !validServiceProtocols[protocol] {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.protocol", field),
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("invalid service protocol: %s, must be one of: TCP, UDP, SCTP", port.Protocol),
		})
	}

	return errors
}
