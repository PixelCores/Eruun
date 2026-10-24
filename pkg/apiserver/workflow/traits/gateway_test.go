package traits

import (
	"testing"

	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestBuildGatewayRouteRendersIngress(t *testing.T) {
	cfg := spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}
	component := &model.ApplicationComponent{Name: "api", AppID: "demo", Namespace: "ws-a", ComponentType: "webservice"}
	gateway := spec.GatewayTraitSpec{
		Hosts: []string{"api-ws-a.apps.example.com", "chat-ws-a.apps.example.com"},
		Routes: []spec.GatewayRouteSpec{
			{},
			{Path: "/v1/models", Match: spec.GatewayMatchExact, Backend: &spec.GatewayBackendSpec{Service: "metrics", Port: 9100}},
		},
	}
	services := []spec.ServiceTraitSpec{
		{Ports: []spec.ServicePortTraitSpec{{Port: 8080}}},
		{Name: "metrics", Ports: []spec.ServicePortTraitSpec{{Port: 9100}}},
	}
	// The unnamed Service trait is ambiguous for the first route.
	_, err := BuildGatewayRoute(component, gateway, services, nil, cfg)
	require.ErrorContains(t, err, "routes[0]")

	services = services[1:]
	gateway.Routes[0].Backend = &spec.GatewayBackendSpec{Service: "metrics"}
	route, err := BuildGatewayRoute(component, gateway, services, nil, cfg)
	require.NoError(t, err)
	require.Equal(t, "networking.k8s.io/v1", route.GetAPIVersion())
	require.Equal(t, "Ingress", route.GetKind())
	var ingress networkingv1.Ingress
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(route.Object, &ingress))
	require.Equal(t, naming.GatewayRouteName("api", "demo"), ingress.Name)
	require.Equal(t, "ws-a", ingress.Namespace)
	require.Equal(t, "nginx", *ingress.Spec.IngressClassName)
	require.Empty(t, ingress.Annotations)
	require.Empty(t, ingress.Spec.TLS)
	require.Nil(t, ingress.Spec.DefaultBackend)
	require.Len(t, ingress.Spec.Rules, 2)
	for i, rule := range ingress.Spec.Rules {
		require.Equal(t, gateway.Hosts[i], rule.Host)
		paths := rule.HTTP.Paths
		require.Len(t, paths, 2)
		require.Equal(t, "/", paths[0].Path)
		require.Equal(t, networkingv1.PathTypePrefix, *paths[0].PathType)
		require.Equal(t, "/v1/models", paths[1].Path)
		require.Equal(t, networkingv1.PathTypeExact, *paths[1].PathType)
		for _, path := range paths {
			require.Equal(t, "metrics", path.Backend.Service.Name)
			require.Equal(t, int32(9100), path.Backend.Service.Port.Number)
		}
	}
}

func TestBuildGatewayRouteDefaultsAndRejections(t *testing.T) {
	cfg := spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}
	component := &model.ApplicationComponent{Name: "api", AppID: "demo", Namespace: "ws-a", ComponentType: "webservice"}
	ports := []spec.Ports{{Port: 8080}}
	route, err := BuildGatewayRoute(component, spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}}, nil, ports, cfg)
	require.NoError(t, err)
	var ingress networkingv1.Ingress
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(route.Object, &ingress))
	require.Equal(t, "api-ws-a.apps.example.com", ingress.Spec.Rules[0].Host)
	backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
	require.Equal(t, naming.ServiceName("api", "demo"), backend.Name)
	require.Equal(t, int32(8080), backend.Port.Number)

	for _, tt := range []struct {
		name    string
		gateway spec.GatewayTraitSpec
		cfg     spec.GatewayConfig
		want    string
	}{
		{"disabled provider", spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}}, spec.GatewayConfig{}, "cannot render routes"},
		{"foreign host", spec.GatewayTraitSpec{Hosts: []string{"api-ws-b.apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}}, cfg, `"-ws-a"`},
		{"rewrite", spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/api", Rewrite: "/"}}}, cfg, "neither rewrite nor timeout"},
		{"timeout", spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Timeout: "30s"}}}, cfg, "neither rewrite nor timeout"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildGatewayRoute(component, tt.gateway, nil, ports, tt.cfg)
			require.ErrorContains(t, err, tt.want)
		})
	}
}
