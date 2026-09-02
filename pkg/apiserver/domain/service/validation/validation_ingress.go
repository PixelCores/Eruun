package validation

import (
	"fmt"
	"net"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

// validateIngressTrait validates an ingress trait
func (v *validationServiceImpl) validateIngressTrait(ingress spec.IngressTraitsSpec, field string) []apisv1.ValidationError {
	return validateIngressTraitSpec(ingress, field)
}

func validateIngressTraitSpec(ingress spec.IngressTraitsSpec, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	errors = append(errors, validateReservedLabelMap(ingress.Label, fmt.Sprintf("%s.label", field), "traits.ingress.label")...)

	if ingressName := strings.TrimSpace(ingress.Name); ingressName != "" {
		errors = append(errors, validateKubeResourceName(ingressName, fmt.Sprintf("%s.name", field))...)
	}

	for i, host := range ingress.Hosts {
		errors = append(errors, validateIngressHost(host, fmt.Sprintf("%s.hosts[%d]", field, i))...)
	}
	for i, tls := range ingress.TLS {
		for j, host := range tls.Hosts {
			errors = append(errors, validateIngressHost(host, fmt.Sprintf("%s.tls[%d].hosts[%d]", field, i, j))...)
		}
	}
	if len(ingress.Routes) == 0 {
		errors = append(errors, apisv1.ValidationError{
			Field:   fmt.Sprintf("%s.routes", field),
			Code:    apisv1.ErrCodeMissingIngressRoutes,
			Message: "ingress routes are required",
		})
		return errors
	}
	for i, route := range ingress.Routes {
		errors = append(errors, validateIngressHost(route.Host, fmt.Sprintf("%s.routes[%d].host", field, i))...)
	}

	return errors
}

func validateIngressHost(host, field string) []apisv1.ValidationError {
	trimmed := strings.TrimSpace(host)
	if trimmed != host {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("%s must not contain leading or trailing whitespace", field),
		}}
	}
	if trimmed == "" {
		return nil
	}
	host = trimmed
	if net.ParseIP(host) != nil {
		return []apisv1.ValidationError{{
			Field:   field,
			Code:    apisv1.ErrCodeInvalidTraitConfig,
			Message: fmt.Sprintf("%s must be a DNS name, not an IP address", field),
		}}
	}

	var causes []string
	if strings.HasPrefix(host, "*.") {
		causes = k8svalidation.IsWildcardDNS1123Subdomain(host)
	} else {
		causes = k8svalidation.IsDNS1123Subdomain(host)
	}
	if len(causes) == 0 {
		return nil
	}
	return []apisv1.ValidationError{{
		Field:   field,
		Code:    apisv1.ErrCodeInvalidTraitConfig,
		Message: fmt.Sprintf("%s must be a valid Kubernetes Ingress host: %s", field, strings.Join(causes, "; ")),
	}}
}

func validateIngressBackendServiceReferences(traits apisv1.Traits, fieldPrefix string) []apisv1.ValidationError {
	nonExternalServices := 0
	for _, svc := range traits.Service {
		serviceType, known := spec.NormalizeServiceAccessType(svc.Type)
		if known && serviceType != spec.ServiceAccessExternal {
			nonExternalServices++
		}
	}
	if nonExternalServices <= 1 {
		return nil
	}

	var errors []apisv1.ValidationError
	for ingressIdx, ingress := range traits.Ingress {
		for routeIdx, route := range ingress.Routes {
			if strings.TrimSpace(route.Backend.ServiceName) != "" {
				continue
			}
			errors = append(errors, apisv1.ValidationError{
				Field:   fmt.Sprintf("%s.ingress[%d].routes[%d].backend.serviceName", fieldPrefix, ingressIdx, routeIdx),
				Code:    apisv1.ErrCodeMissingServiceName,
				Message: "ingress backend serviceName is required when multiple non-external service traits are defined",
			})
		}
	}
	return errors
}
