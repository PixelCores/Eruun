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

// BuildGatewayRoute renders the route object of a component's gateway trait for
// the configured provider. The ingress provider is the only one in this phase.
func BuildGatewayRoute(component *model.ApplicationComponent, gateway spec.GatewayTraitSpec, services []spec.ServiceTraitSpec, ports []spec.Ports, cfg spec.GatewayConfig) (*unstructured.Unstructured, error) {
	if cfg.Provider != spec.GatewayProviderIngress {
		return nil, fmt.Errorf("gateway provider %q cannot render routes", cfg.Provider)
	}
	hosts, err := spec.GatewayHosts(gateway, component.Name, component.Namespace, cfg)
	if err != nil {
		return nil, err
	}
	paths := make([]networkingv1.HTTPIngressPath, 0, len(gateway.Routes))
	for i, route := range gateway.Routes {
		if route.Rewrite != "" || route.Timeout != "" {
			return nil, fmt.Errorf("routes[%d]: the ingress gateway supports neither rewrite nor timeout", i)
		}
		service, port, err := spec.ResolveGatewayBackend(route.Backend, services, ports)
		if err != nil {
			return nil, fmt.Errorf("routes[%d]: %w", i, err)
		}
		if service == "" {
			service = naming.ServiceName(component.Name, component.ResourceNameKey())
		}
		pathType := networkingv1.PathTypePrefix
		if route.EffectiveMatch() == spec.GatewayMatchExact {
			pathType = networkingv1.PathTypeExact
		}
		paths = append(paths, networkingv1.HTTPIngressPath{
			Path:     route.EffectivePath(),
			PathType: &pathType,
			Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
				Name: service,
				Port: networkingv1.ServiceBackendPort{Number: port},
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
	className := cfg.IngressClass
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&networkingv1.IngressSpec{IngressClassName: &className, Rules: rules})
	if err != nil {
		return nil, fmt.Errorf("convert ingress route: %w", err)
	}
	route, err := GatewayRouteRef(component, cfg)
	if err != nil {
		return nil, err
	}
	route.Object["spec"] = object
	return route, nil
}

// GatewayRouteRef identifies the single route object a component owns under the
// configured provider, without a spec.
func GatewayRouteRef(component *model.ApplicationComponent, cfg spec.GatewayConfig) (*unstructured.Unstructured, error) {
	if cfg.Provider != spec.GatewayProviderIngress {
		return nil, fmt.Errorf("gateway provider %q has no route resource", cfg.Provider)
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind(string(spec.KubeKindIngress)))
	route.SetName(naming.GatewayRouteName(component.Name, component.ResourceNameKey()))
	route.SetNamespace(component.Namespace)
	return route, nil
}
