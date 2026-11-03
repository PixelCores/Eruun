package traits

import (
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

// gatewayBackend is a route's resolved Service and TCP port.
type gatewayBackend struct {
	service string
	port    int32
}

// BuildGatewayRoute renders the route object of a component's gateway trait for
// the configured provider: an Ingress, or a Gateway API HTTPRoute attached to
// the platform Gateway's HTTPS listener.
func BuildGatewayRoute(component *model.ApplicationComponent, gateway spec.GatewayTraitSpec, services []spec.ServiceTraitSpec, ports []spec.Ports, cfg spec.GatewayConfig) (*unstructured.Unstructured, error) {
	route, err := GatewayRouteRef(component, cfg)
	if err != nil {
		return nil, err
	}
	hosts, err := spec.GatewayHosts(gateway, component.Name, component.Namespace, cfg)
	if err != nil {
		return nil, err
	}
	backends := make([]gatewayBackend, 0, len(gateway.Routes))
	for i, item := range gateway.Routes {
		if cfg.Provider == spec.GatewayProviderIngress && (item.Rewrite != "" || item.Timeout != "") {
			return nil, fmt.Errorf("routes[%d]: the ingress gateway supports neither rewrite nor timeout", i)
		}
		service, port, err := spec.ResolveGatewayBackend(item.Backend, services, ports)
		if err != nil {
			return nil, fmt.Errorf("routes[%d]: %w", i, err)
		}
		if service == "" {
			service = naming.ServiceName(component.Name, component.ResourceNameKey())
		}
		backends = append(backends, gatewayBackend{service: service, port: port})
	}
	if cfg.Provider == spec.GatewayProviderIngress {
		route.Object["spec"], err = ingressRouteSpec(hosts, gateway.Routes, backends, cfg.IngressClass)
	} else {
		route.Object["spec"] = httpRouteSpec(hosts, gateway.Routes, backends, cfg.ParentRef)
	}
	return route, err
}

// GatewayRouteRef identifies the single route object a component owns under the
// configured provider, without a spec.
func GatewayRouteRef(component *model.ApplicationComponent, cfg spec.GatewayConfig) (*unstructured.Unstructured, error) {
	route := &unstructured.Unstructured{}
	switch cfg.Provider {
	case spec.GatewayProviderIngress:
		route.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind(string(spec.KubeKindIngress)))
	case spec.GatewayProviderGatewayAPI:
		route.SetGroupVersionKind(spec.GatewayAPIGroupVersion.WithKind(string(spec.KubeKindHTTPRoute)))
	default:
		return nil, fmt.Errorf("gateway provider %q has no route resource", cfg.Provider)
	}
	route.SetName(naming.GatewayRouteName(component.Name, component.ResourceNameKey()))
	route.SetNamespace(component.Namespace)
	return route, nil
}

// ingressRouteSpec gives every host one rule carrying the same paths.
func ingressRouteSpec(hosts []string, routes []spec.GatewayRouteSpec, backends []gatewayBackend, className string) (map[string]interface{}, error) {
	paths := make([]networkingv1.HTTPIngressPath, 0, len(routes))
	for i, item := range routes {
		pathType := networkingv1.PathTypePrefix
		if item.EffectiveMatch() == spec.GatewayMatchExact {
			pathType = networkingv1.PathTypeExact
		}
		paths = append(paths, networkingv1.HTTPIngressPath{
			Path:     item.EffectivePath(),
			PathType: &pathType,
			Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
				Name: backends[i].service,
				Port: networkingv1.ServiceBackendPort{Number: backends[i].port},
			}},
		})
	}
	rules := make([]networkingv1.IngressRule, 0, len(hosts))
	for _, host := range hosts {
		rules = append(rules, networkingv1.IngressRule{
			Host:             host,
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: append([]networkingv1.HTTPIngressPath(nil), paths...)}},
		})
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&networkingv1.IngressSpec{IngressClassName: &className, Rules: rules})
	if err != nil {
		return nil, fmt.Errorf("convert ingress route: %w", err)
	}
	return object, nil
}

// httpRouteSpec renders one HTTPRoute rule per trait route. The object is built
// as unstructured JSON so Eruun does not depend on the Gateway API module.
func httpRouteSpec(hosts []string, routes []spec.GatewayRouteSpec, backends []gatewayBackend, parent spec.GatewayParentRef) map[string]interface{} {
	hostnames := make([]interface{}, 0, len(hosts))
	for _, host := range hosts {
		hostnames = append(hostnames, host)
	}
	rules := make([]interface{}, 0, len(routes))
	for i, item := range routes {
		pathType := "PathPrefix"
		if item.EffectiveMatch() == spec.GatewayMatchExact {
			pathType = "Exact"
		}
		rule := map[string]interface{}{
			"matches":     []interface{}{map[string]interface{}{"path": map[string]interface{}{"type": pathType, "value": item.EffectivePath()}}},
			"backendRefs": []interface{}{map[string]interface{}{"name": backends[i].service, "port": int64(backends[i].port)}},
		}
		if item.Rewrite != "" {
			rule["filters"] = []interface{}{map[string]interface{}{
				"type":       "URLRewrite",
				"urlRewrite": map[string]interface{}{"path": map[string]interface{}{"type": "ReplacePrefixMatch", "replacePrefixMatch": item.Rewrite}},
			}}
		}
		if item.Timeout != "" {
			rule["timeouts"] = map[string]interface{}{"request": item.Timeout}
		}
		rules = append(rules, rule)
	}
	return map[string]interface{}{
		"parentRefs": []interface{}{map[string]interface{}{
			"group":       spec.GatewayAPIGroupVersion.Group,
			"kind":        "Gateway",
			"namespace":   parent.Namespace,
			"name":        parent.Name,
			"sectionName": parent.SectionName,
		}},
		"hostnames": hostnames,
		"rules":     rules,
	}
}
