package v1

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var testGateway = spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}

// The component view must show the backend the route renderer deploys.
func TestConvertComponentModelsToDTOGatewayMatchesRenderedRoute(t *testing.T) {
	service := func(name string, ports ...int32) spec.ServiceTraitSpec {
		trait := spec.ServiceTraitSpec{Name: name, Type: "internal", Selector: map[string]string{"tier": "api"}}
		for _, port := range ports {
			trait.Ports = append(trait.Ports, spec.ServicePortTraitSpec{Port: port})
		}
		return trait
	}
	for _, tt := range []struct {
		name     string
		services []spec.ServiceTraitSpec
		ports    []spec.Ports
		backend  *spec.GatewayBackendSpec
		wantName string
		wantPort int32
	}{
		{
			name:     "explicit second service uses its own port",
			services: []spec.ServiceTraitSpec{service("api-v1", 8080), service("api-v2", 9090)},
			backend:  &spec.GatewayBackendSpec{Service: "api-v2"}, wantName: "api-v2", wantPort: 9090,
		},
		{
			name:     "explicit port selects a declared port",
			services: []spec.ServiceTraitSpec{service("api", 9090, 9443)},
			backend:  &spec.GatewayBackendSpec{Port: 9443}, wantName: "api", wantPort: 9443,
		},
		{
			name:     "single service supplies name and port",
			services: []spec.ServiceTraitSpec{service("api", 8080)}, wantName: "api", wantPort: 8080,
		},
		{
			name:     "external service is never a candidate",
			services: []spec.ServiceTraitSpec{{Name: "external", Type: "external", ExternalName: "example.com"}, service("api", 8080)},
			wantName: "api", wantPort: 8080,
		},
		{
			name:     "unnamed service trait is the generated service",
			services: []spec.ServiceTraitSpec{service("", 8080)},
			wantName: naming.ServiceName("backend", "demo"), wantPort: 8080,
		},
		{
			name: "properties supply the default service", ports: []spec.Ports{{Port: 9090}},
			wantName: naming.ServiceName("backend", "demo"), wantPort: 9090,
		},
		{
			name:     "several services without a backend cannot render",
			services: []spec.ServiceTraitSpec{service("api-v1", 8080), service("api-v2", 9090)},
		},
		{
			name: "no declared port cannot render",
		},
		{
			name:     "undeclared port cannot render",
			services: []spec.ServiceTraitSpec{service("api", 8080)},
			backend:  &spec.GatewayBackendSpec{Port: 9090},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gateway := spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/v1", Match: spec.GatewayMatchExact, Backend: tt.backend}}}
			component := &model.ApplicationComponent{
				Name: "backend", AppID: "demo", Namespace: "component-ns", ComponentType: config.ServerJob,
				Properties: mustJSONStruct(t, spec.Properties{Ports: tt.ports, Labels: map[string]string{"tier": "api"}}),
				Traits:     mustJSONStruct(t, spec.Traits{Service: tt.services, Gateway: &gateway}),
			}
			before, err := json.Marshal(component)
			require.NoError(t, err)
			dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{component}, testGateway)
			require.NoError(t, err)
			require.Len(t, dtos, 1)
			view := dtos[0].Gateway
			require.NotNil(t, view)
			require.Equal(t, []string{"backend-component-ns.apps.example.com"}, view.Hosts)
			require.Len(t, view.Routes, 1)
			require.Equal(t, "/v1", view.Routes[0].Path)
			require.Equal(t, spec.GatewayMatchExact, view.Routes[0].Match)
			// The public spec stays declarative; only the view receives defaults.
			require.Equal(t, gateway, *dtos[0].Traits.Gateway)

			route, err := traits.BuildGatewayRoute(component, gateway, tt.services, tt.ports, testGateway)
			if tt.wantName == "" {
				require.Error(t, err)
				require.Empty(t, view.Routes[0].Service)
				require.Zero(t, view.Routes[0].Port)
			} else {
				require.NoError(t, err)
				var ingress networkingv1.Ingress
				require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(route.Object, &ingress))
				require.Equal(t, naming.GatewayRouteName("backend", "demo"), ingress.Name)
				require.Equal(t, "component-ns", ingress.Namespace)
				require.Equal(t, "nginx", *ingress.Spec.IngressClassName)
				require.Len(t, ingress.Spec.Rules, 1)
				require.Equal(t, view.Hosts[0], ingress.Spec.Rules[0].Host)
				path := ingress.Spec.Rules[0].HTTP.Paths[0]
				require.Equal(t, view.Routes[0].Path, path.Path)
				require.Equal(t, networkingv1.PathTypeExact, *path.PathType)
				require.Equal(t, tt.wantName, path.Backend.Service.Name)
				require.Equal(t, tt.wantPort, path.Backend.Service.Port.Number)
				require.Equal(t, path.Backend.Service.Name, view.Routes[0].Service)
				require.Equal(t, path.Backend.Service.Port.Number, view.Routes[0].Port)
			}
			after, err := json.Marshal(component)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "query and rendering must not rewrite the saved component")
		})
	}
}
