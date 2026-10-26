package conversion

import (
	"fmt"
	"slices"
	"strings"

	networkingv1 "k8s.io/api/networking/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// applyIngressTraits converts each Ingress into the gateway trait of the
// component with the same namespace and name. Inputs the gateway contract
// cannot express are rejected instead of being dropped.
func applyIngressTraits(ingresses []*networkingv1.Ingress, components []apis.CreateComponentRequest, gateway spec.GatewayConfig) ([]string, error) {
	if len(ingresses) == 0 || len(components) == 0 {
		return nil, nil
	}
	indexes := make(map[string][]int)
	for i := range components {
		name := strings.TrimSpace(components[i].Name)
		if name == "" {
			continue
		}
		key := normalizeResourceNamespace(components[i].Namespace) + "/" + name
		indexes[key] = append(indexes[key], i)
	}
	var warnings []string
	for _, ingress := range ingresses {
		if ingress == nil {
			continue
		}
		name := strings.TrimSpace(ingress.Name)
		if name == "" {
			continue
		}
		key := normalizeResourceNamespace(ingress.Namespace) + "/" + name
		targets := indexes[key]
		if len(targets) == 0 {
			warnings = append(warnings, fmt.Sprintf("ingress %s has no matching component; skipped", key))
			continue
		}
		for _, idx := range targets {
			component := &components[idx]
			if component.ComponentType != config.ServerJob && component.ComponentType != config.StoreJob {
				return warnings, unsupportedIngress(ingress, "only webservice and store components accept a gateway")
			}
			if component.Traits.Gateway != nil {
				return warnings, unsupportedIngress(ingress, "a component accepts one Ingress")
			}
			trait, err := buildGatewayTrait(ingress, component.Traits.Service, gateway)
			if err != nil {
				return warnings, err
			}
			component.Traits.Gateway = trait
		}
	}
	return warnings, nil
}

func buildGatewayTrait(ingress *networkingv1.Ingress, services []spec.ServiceTraitSpec, gateway spec.GatewayConfig) (*spec.GatewayTraitSpec, error) {
	if len(ingress.Annotations) > 0 {
		return nil, unsupportedIngress(ingress, "annotations are not supported")
	}
	// The platform gateway selects the controller; an Ingress that chose another
	// one cannot keep that choice.
	if class := ingress.Spec.IngressClassName; class != nil {
		if gateway.Provider != spec.GatewayProviderIngress {
			return nil, unsupportedIngress(ingress, fmt.Sprintf("ingressClassName %q cannot be kept because the platform has no ingress gateway", *class))
		}
		if *class != gateway.IngressClass {
			return nil, unsupportedIngress(ingress, fmt.Sprintf("ingressClassName %q differs from the platform IngressClass %q", *class, gateway.IngressClass))
		}
	}
	if len(ingress.Spec.TLS) > 0 {
		return nil, unsupportedIngress(ingress, "TLS terminates at the platform gateway")
	}
	if ingress.Spec.DefaultBackend != nil {
		return nil, unsupportedIngress(ingress, "a default backend is not supported")
	}
	trait := &spec.GatewayTraitSpec{}
	var first []spec.GatewayRouteSpec
	for i, rule := range ingress.Spec.Rules {
		if rule.Host == "" || rule.HTTP == nil {
			return nil, unsupportedIngress(ingress, "every rule needs a host and HTTP paths")
		}
		routes, err := gatewayRoutesFromPaths(ingress, rule.HTTP.Paths, services)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			first = routes
		} else if !sameGatewayRoutes(first, routes) {
			return nil, unsupportedIngress(ingress, "all hosts must share the same paths")
		}
		trait.Hosts = append(trait.Hosts, rule.Host)
	}
	if len(first) == 0 {
		return nil, unsupportedIngress(ingress, "it has no routes")
	}
	trait.Routes = first
	return trait, nil
}

func gatewayRoutesFromPaths(ingress *networkingv1.Ingress, paths []networkingv1.HTTPIngressPath, services []spec.ServiceTraitSpec) ([]spec.GatewayRouteSpec, error) {
	routes := make([]spec.GatewayRouteSpec, 0, len(paths))
	for _, path := range paths {
		backend := path.Backend.Service
		if backend == nil {
			return nil, unsupportedIngress(ingress, "only Service backends are supported")
		}
		if backend.Port.Number == 0 {
			return nil, unsupportedIngress(ingress, "named Service ports are not supported")
		}
		if !slices.ContainsFunc(services, func(s spec.ServiceTraitSpec) bool { return s.Name == backend.Name }) {
			return nil, unsupportedIngress(ingress, fmt.Sprintf("backend Service %q is not a Service of the same component", backend.Name))
		}
		route := spec.GatewayRouteSpec{Path: path.Path, Backend: &spec.GatewayBackendSpec{Service: backend.Name, Port: backend.Port.Number}}
		switch {
		case path.PathType == nil:
			return nil, unsupportedIngress(ingress, "every path needs a pathType")
		case *path.PathType == networkingv1.PathTypePrefix:
			route.Match = spec.GatewayMatchPrefix
		case *path.PathType == networkingv1.PathTypeExact:
			route.Match = spec.GatewayMatchExact
		default:
			return nil, unsupportedIngress(ingress, fmt.Sprintf("path type %s is not supported", *path.PathType))
		}
		routes = append(routes, route)
	}
	return routes, nil
}

// sameGatewayRoutes compares the routes of two Ingress rules as sets: path
// order within a rule does not change which request a route matches.
func sameGatewayRoutes(a, b []spec.GatewayRouteSpec) bool {
	key := func(route spec.GatewayRouteSpec) string {
		return fmt.Sprintf("%s %s %s %d", route.Match, route.Path, route.Backend.Service, route.Backend.Port)
	}
	keys := func(routes []spec.GatewayRouteSpec) []string {
		out := make([]string, 0, len(routes))
		for _, route := range routes {
			out = append(out, key(route))
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(keys(a), keys(b))
}

func unsupportedIngress(ingress *networkingv1.Ingress, reason string) error {
	return bcode.WithSafeClientMessage(bcode.ErrApplicationConfig, fmt.Sprintf("ingress %s cannot be converted to a gateway trait: %s", ingress.Name, reason))
}
