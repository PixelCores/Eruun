package traitvalidation

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

const (
	maxGatewayHosts  = 8
	maxGatewayRoutes = 16
)

// gatewayDurationRegexp is the Gateway API Duration format.
var gatewayDurationRegexp = regexp.MustCompile(`^([0-9]{1,5}(h|m|s|ms)){1,4}$`)

// ValidateGateway checks a resolved component's gateway trait against the
// contract and the platform gateway configuration. It is the single rule set
// for Try and every write; nested traits are rejected by ValidateTraits.
func ValidateGateway(component apisv1.CreateComponentRequest, namespace string, cfg spec.GatewayConfig, field string) []apisv1.ValidationError {
	gateway := component.Traits.Gateway
	if gateway == nil {
		return nil
	}
	reject := func(field, code, message string) []apisv1.ValidationError {
		return []apisv1.ValidationError{{Field: field, Code: code, Message: message}}
	}
	if cfg.Provider == spec.GatewayProviderNone {
		return reject(field, apisv1.ErrCodeGatewayNotAllowed, "the platform has no gateway provider; component routes are disabled")
	}
	if component.ComponentType != config.ServerJob && component.ComponentType != config.StoreJob {
		return reject(field, apisv1.ErrCodeGatewayNotAllowed, "gateway is only supported by webservice and store components")
	}

	var errors []apisv1.ValidationError
	add := func(field, code, format string, args ...any) {
		errors = append(errors, apisv1.ValidationError{Field: field, Code: code, Message: fmt.Sprintf(format, args...)})
	}
	if len(gateway.Hosts) > maxGatewayHosts {
		add(field+".hosts", apisv1.ErrCodeInvalidGatewayHost, "at most %d hosts are allowed", maxGatewayHosts)
	}
	seenHosts := make(map[string]bool, len(gateway.Hosts))
	for i, host := range gateway.Hosts {
		hostField := fmt.Sprintf("%s.hosts[%d]", field, i)
		if seenHosts[host] {
			add(hostField, apisv1.ErrCodeInvalidGatewayHost, "host %q is listed twice", host)
		}
		seenHosts[host] = true
	}
	if _, err := spec.GatewayHosts(*gateway, component.Name, namespace, cfg); err != nil {
		add(field+".hosts", apisv1.ErrCodeInvalidGatewayHost, "%v", err)
	}

	if len(gateway.Routes) == 0 {
		add(field+".routes", apisv1.ErrCodeMissingGatewayRoutes, "gateway routes are required")
		return errors
	}
	if len(gateway.Routes) > maxGatewayRoutes {
		add(field+".routes", apisv1.ErrCodeInvalidGatewayRoute, "at most %d routes are allowed", maxGatewayRoutes)
	}
	seenRoutes := make(map[string]bool, len(gateway.Routes))
	for i, route := range gateway.Routes {
		routeField := fmt.Sprintf("%s.routes[%d]", field, i)
		errors = append(errors, validateGatewayRoute(route, routeField, cfg)...)
		key := route.EffectiveMatch() + " " + route.EffectivePath()
		if seenRoutes[key] {
			add(routeField, apisv1.ErrCodeInvalidGatewayRoute, "a %s route for %q is already declared", route.EffectiveMatch(), route.EffectivePath())
		}
		seenRoutes[key] = true
		if _, _, err := spec.ResolveGatewayBackend(route.Backend, component.Traits.Service, component.Properties.Ports); err != nil {
			add(routeField+".backend", apisv1.ErrCodeInvalidGatewayBackend, "%v", err)
		}
	}
	return errors
}

// ValidateNestedGateway rejects gateway traits of init and sidecar containers.
// The remaining gateway rules need the resolved component and the platform
// configuration; ValidateGateway applies them.
func ValidateNestedGateway(traits spec.Traits, field string) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	check := func(nested spec.Traits, field string) {
		if nested.Gateway != nil {
			errors = append(errors, apisv1.ValidationError{Field: field + ".gateway", Code: apisv1.ErrCodeGatewayNotAllowed, Message: "gateway is only supported in component-level traits"})
		}
		errors = append(errors, ValidateNestedGateway(nested, field)...)
	}
	for i, init := range traits.Init {
		check(init.Traits, fmt.Sprintf("%s.init[%d].traits", field, i))
	}
	for i, sidecar := range traits.Sidecar {
		check(sidecar.Traits, fmt.Sprintf("%s.sidecar[%d].traits", field, i))
	}
	return errors
}

func validateGatewayRoute(route spec.GatewayRouteSpec, field string, cfg spec.GatewayConfig) []apisv1.ValidationError {
	var errors []apisv1.ValidationError
	add := func(field, code, format string, args ...any) {
		errors = append(errors, apisv1.ValidationError{Field: field, Code: code, Message: fmt.Sprintf(format, args...)})
	}
	if path := route.Path; path != "" && !validGatewayPath(path) {
		add(field+".path", apisv1.ErrCodeInvalidGatewayRoute, "path must start with / and must not contain ?, #, .. or //")
	}
	switch route.Match {
	case "", spec.GatewayMatchPrefix, spec.GatewayMatchExact:
	default:
		add(field+".match", apisv1.ErrCodeInvalidGatewayRoute, "match must be %s or %s", spec.GatewayMatchPrefix, spec.GatewayMatchExact)
	}
	if route.Rewrite != "" {
		switch {
		case !validGatewayPath(route.Rewrite):
			add(field+".rewrite", apisv1.ErrCodeInvalidGatewayRoute, "rewrite must start with / and must not contain ?, #, .. or //")
		case route.EffectiveMatch() != spec.GatewayMatchPrefix:
			add(field+".rewrite", apisv1.ErrCodeInvalidGatewayRoute, "rewrite replaces a matched prefix and requires match %s", spec.GatewayMatchPrefix)
		case cfg.Provider == spec.GatewayProviderIngress:
			add(field+".rewrite", apisv1.ErrCodeGatewayUnsupported, "the ingress gateway does not support rewrite")
		}
	}
	if route.Timeout != "" {
		switch timeout, err := time.ParseDuration(route.Timeout); {
		case !gatewayDurationRegexp.MatchString(route.Timeout) || err != nil:
			add(field+".timeout", apisv1.ErrCodeInvalidGatewayRoute, "timeout must be a Gateway API duration such as 600s")
		case timeout != 0 && (timeout < time.Second || timeout > time.Hour):
			add(field+".timeout", apisv1.ErrCodeInvalidGatewayRoute, "timeout must be 0s or between 1s and 1h")
		case cfg.Provider == spec.GatewayProviderIngress:
			add(field+".timeout", apisv1.ErrCodeGatewayUnsupported, "the ingress gateway does not support timeout")
		}
	}
	return errors
}

func validGatewayPath(path string) bool {
	return strings.HasPrefix(path, "/") && !strings.ContainsAny(path, "?#") && !strings.Contains(path, "..") && !strings.Contains(path, "//")
}
