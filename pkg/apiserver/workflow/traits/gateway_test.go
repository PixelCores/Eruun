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
		{"disabled provider", spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}}, spec.GatewayConfig{}, "has no route resource"},
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

// The HTTPRoute matches design §6.5: attached to the platform Gateway's HTTPS
// listener, one rule per route, rewrite and timeout only where declared.
func TestBuildGatewayRouteRendersHTTPRoute(t *testing.T) {
	cfg := spec.GatewayConfig{Provider: spec.GatewayProviderGatewayAPI, Domain: "apps.example.com", ProxyNamespace: "istio-ingress",
		ParentRef: spec.GatewayParentRef{Namespace: "istio-ingress", Name: "eruun", SectionName: spec.GatewayHTTPSListener}}
	component := &model.ApplicationComponent{Name: "agent", AppID: "demo", Namespace: "ws-a", ComponentType: "webservice"}
	gateway := spec.GatewayTraitSpec{
		Hosts: []string{"chat-ws-a.apps.example.com"},
		Routes: []spec.GatewayRouteSpec{
			{Backend: &spec.GatewayBackendSpec{Port: 3000}},
			{Path: "/api", Rewrite: "/", Timeout: "120s", Backend: &spec.GatewayBackendSpec{Port: 8080}},
			{Path: "/healthz", Match: spec.GatewayMatchExact, Timeout: "0s", Backend: &spec.GatewayBackendSpec{Port: 8080}},
		},
	}
	ports := []spec.Ports{{Port: 3000}, {Port: 8080}}

	route, err := BuildGatewayRoute(component, gateway, nil, ports, cfg)
	require.NoError(t, err)
	service := naming.ServiceName("agent", "demo")
	require.Equal(t, map[string]interface{}{
		"apiVersion": "gateway.networking.k8s.io/v1",
		"kind":       "HTTPRoute",
		"metadata":   map[string]interface{}{"name": naming.GatewayRouteName("agent", "demo"), "namespace": "ws-a"},
		"spec": map[string]interface{}{
			"parentRefs": []interface{}{map[string]interface{}{
				"group": "gateway.networking.k8s.io", "kind": "Gateway", "namespace": "istio-ingress", "name": "eruun", "sectionName": "https",
			}},
			"hostnames": []interface{}{"chat-ws-a.apps.example.com"},
			"rules": []interface{}{
				map[string]interface{}{
					"matches":     []interface{}{map[string]interface{}{"path": map[string]interface{}{"type": "PathPrefix", "value": "/"}}},
					"backendRefs": []interface{}{map[string]interface{}{"name": service, "port": int64(3000)}},
				},
				map[string]interface{}{
					"matches":     []interface{}{map[string]interface{}{"path": map[string]interface{}{"type": "PathPrefix", "value": "/api"}}},
					"filters":     []interface{}{map[string]interface{}{"type": "URLRewrite", "urlRewrite": map[string]interface{}{"path": map[string]interface{}{"type": "ReplacePrefixMatch", "replacePrefixMatch": "/"}}}},
					"timeouts":    map[string]interface{}{"request": "120s"},
					"backendRefs": []interface{}{map[string]interface{}{"name": service, "port": int64(8080)}},
				},
				map[string]interface{}{
					"matches":     []interface{}{map[string]interface{}{"path": map[string]interface{}{"type": "Exact", "value": "/healthz"}}},
					"timeouts":    map[string]interface{}{"request": "0s"},
					"backendRefs": []interface{}{map[string]interface{}{"name": service, "port": int64(8080)}},
				},
			},
		},
	}, route.Object)
	// The payload must survive JSON round trips (task checkpoints, transport).
	require.NotPanics(t, func() { route.DeepCopy() })
}
