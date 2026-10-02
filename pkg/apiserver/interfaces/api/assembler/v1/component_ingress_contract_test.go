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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
)

func TestConvertComponentModelsToDTOIngressMatchesRenderedBackend(t *testing.T) {
	service := func(name string, port int32) spec.ServiceTraitSpec {
		return spec.ServiceTraitSpec{
			Name: name, Type: "internal", Selector: map[string]string{"tier": "api"},
			Ports: []spec.ServicePortTraitSpec{{Port: port}},
		}
	}
	for _, tt := range []struct {
		name      string
		services  []spec.ServiceTraitSpec
		ports     []spec.Ports
		backend   spec.IngressRoute
		wantName  string
		wantPort  int32
		ambiguous bool
	}{
		{
			name:     "explicit second service uses its own port",
			services: []spec.ServiceTraitSpec{service("api-v1", 8080), service("api-v2", 9090)},
			ports:    []spec.Ports{{Port: 8080}, {Port: 9090}},
			backend:  spec.IngressRoute{ServiceName: "api-v2"}, wantName: "api-v2", wantPort: 9090,
		},
		{
			name:     "explicit port wins",
			services: []spec.ServiceTraitSpec{service("api-v1", 8080), service("api-v2", 9090)},
			backend:  spec.IngressRoute{ServiceName: "api-v2", ServicePort: 8443}, wantName: "api-v2", wantPort: 8443,
		},
		{
			name:     "single service supplies name and port",
			services: []spec.ServiceTraitSpec{service("api", 8080)}, wantName: "api", wantPort: 8080,
		},
		{
			name:     "non-external service is the default",
			services: []spec.ServiceTraitSpec{{Name: "external", Type: "external", ExternalName: "example.com"}, service("api", 8080)},
			wantName: "api", wantPort: 8080,
		},
		{
			name: "properties supply the default port", ports: []spec.Ports{{Port: 9090}},
			wantName: naming.ServiceName("backend", "demo"), wantPort: 9090,
		},
		{
			name:     "explicit generated service uses properties",
			services: []spec.ServiceTraitSpec{service("api", 8080)}, ports: []spec.Ports{{Port: 9090}},
			backend:  spec.IngressRoute{ServiceName: naming.ServiceName("backend", "demo")},
			wantName: naming.ServiceName("backend", "demo"), wantPort: 9090,
		},
		{
			name: "no declared ports uses eighty", wantName: naming.ServiceName("backend", "demo"), wantPort: 80,
		},
		{
			name:     "ambiguous input remains readable but cannot render",
			services: []spec.ServiceTraitSpec{service("api-v1", 8080), service("api-v2", 9090)},
			wantName: "api-v1", wantPort: 8080, ambiguous: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			component := &model.ApplicationComponent{
				Name: "backend", AppID: "demo", Namespace: "component-ns", ComponentType: config.ServerJob,
				Properties: mustJSONStruct(t, spec.Properties{Ports: tt.ports, Labels: map[string]string{"tier": "api"}}),
				Traits: mustJSONStruct(t, spec.Traits{
					Service: tt.services,
					Ingress: []spec.IngressTraitsSpec{{
						Name: "api-ingress", Namespace: "ignored-ns", IngressClassName: "nginx", DefaultPathType: "Exact",
						Hosts:       []string{"b.example.com", "a.example.com"},
						TLS:         []spec.IngressTLSConfig{{Hosts: []string{"b.example.com", "a.example.com"}, SecretName: "api-tls"}},
						Annotations: map[string]string{"nginx.ingress.kubernetes.io/rewrite-target": "/kept"},
						Routes:      []spec.IngressRoutes{{Backend: tt.backend, Rewrite: &spec.RewritePolicy{Type: "regex", Replacement: "/ignored"}}},
					}},
				}),
			}
			before, err := json.Marshal(component)
			require.NoError(t, err)
			dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{component})
			require.NoError(t, err)
			require.Len(t, dtos, 1)
			require.Len(t, dtos[0].Ingresses, 1)
			summary := dtos[0].Ingresses[0]
			require.Len(t, summary.Routes, 2)
			// The public spec stays declarative; only the summary receives defaults.
			require.Equal(t, tt.backend, dtos[0].Traits.Ingress[0].Routes[0].Backend)

			workload := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "backend", Image: "nginx:1"}},
			}}}}
			objects, err := traits.ApplyTraits(component, workload)
			if tt.ambiguous {
				require.Equal(t, tt.wantName, summary.Routes[0].ServiceName)
				require.Equal(t, tt.wantPort, summary.Routes[0].ServicePort)
				require.ErrorContains(t, err, `ingress backend serviceName is required when component "backend" defines multiple non-external service traits`)
			} else {
				require.NoError(t, err)
				require.Len(t, objects, 1)
				ingress, ok := objects[0].(*networkingv1.Ingress)
				require.True(t, ok)
				require.Equal(t, tt.wantName, ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Name)
				require.Equal(t, tt.wantPort, ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number)
				require.Equal(t, naming.IngressName(ingress.Name, component.ResourceNameKey()), summary.Name)
				require.Equal(t, ingress.Namespace, summary.Namespace)
				require.Equal(t, *ingress.Spec.IngressClassName, summary.IngressClassName)
				require.Equal(t, ingress.Annotations, summary.Annotations)
				require.Equal(t, "/kept", summary.Annotations["nginx.ingress.kubernetes.io/rewrite-target"])
				require.Len(t, ingress.Spec.TLS, 1)
				require.Equal(t, ingress.Spec.TLS[0].Hosts, summary.TLS[0].Hosts)
				require.Equal(t, ingress.Spec.TLS[0].SecretName, summary.TLS[0].SecretName)
				for i, rule := range ingress.Spec.Rules {
					route := summary.Routes[i]
					path := rule.HTTP.Paths[0]
					require.Equal(t, rule.Host, route.Host)
					require.Equal(t, path.Path, route.Path)
					require.Equal(t, "/", route.Path)
					require.Equal(t, string(*path.PathType), route.PathType)
					require.Equal(t, "Exact", route.PathType)
					require.Equal(t, path.Backend.Service.Name, route.ServiceName)
					require.Equal(t, path.Backend.Service.Port.Number, route.ServicePort)
				}
			}
			after, err := json.Marshal(component)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after), "query and rendering must not rewrite the saved component")
		})
	}
}

func TestConvertComponentModelsToDTOIngressPreservesRoutePresentation(t *testing.T) {
	for _, tt := range []struct {
		name             string
		defaultPathType  string
		routes           []spec.IngressRoutes
		annotations      map[string]string
		wantSummaryTypes []string
		wantRenderTypes  []string
		wantAnnotations  map[string]string
	}{
		{
			name: "nil annotations and prefix default", routes: []spec.IngressRoutes{{}},
			wantSummaryTypes: []string{"Prefix"}, wantRenderTypes: []string{"Prefix"},
		},
		{
			name: "empty annotations collapse to nil", routes: []spec.IngressRoutes{{}}, annotations: map[string]string{},
			wantSummaryTypes: []string{"Prefix"}, wantRenderTypes: []string{"Prefix"},
		},
		{
			name: "empty rewrite preserves summary empty map", routes: []spec.IngressRoutes{{Rewrite: &spec.RewritePolicy{}}},
			wantSummaryTypes: []string{"Prefix"}, wantRenderTypes: []string{"Prefix"}, wantAnnotations: map[string]string{},
		},
		{
			name: "route type wins over ingress and rewrite", defaultPathType: "Prefix",
			routes:           []spec.IngressRoutes{{PathType: "Exact", Rewrite: &spec.RewritePolicy{Type: "regex", Replacement: "/$1"}}},
			annotations:      map[string]string{"nginx.ingress.kubernetes.io/rewrite-target": "/kept"},
			wantSummaryTypes: []string{"Exact"}, wantRenderTypes: []string{"Exact"},
			wantAnnotations: map[string]string{"nginx.ingress.kubernetes.io/rewrite-target": "/kept", "nginx.ingress.kubernetes.io/use-regex": "true"},
		},
		{
			name: "route whitespace and case normalize in summary and renderer", defaultPathType: "Prefix",
			routes:           []spec.IngressRoutes{{PathType: " eXaCt "}},
			wantSummaryTypes: []string{"Exact"}, wantRenderTypes: []string{"Exact"},
		},
		{
			name: "default whitespace normalizes in summary and renderer", defaultPathType: " Exact ",
			routes:           []spec.IngressRoutes{{}},
			wantSummaryTypes: []string{"Exact"}, wantRenderTypes: []string{"Exact"},
		},
		{
			name: "invalid route uses normalized default", defaultPathType: " implementation-specific ",
			routes:           []spec.IngressRoutes{{PathType: "unknown"}},
			wantSummaryTypes: []string{"ImplementationSpecific"}, wantRenderTypes: []string{"ImplementationSpecific"},
		},
		{
			name: "invalid route falls back to prefix", routes: []spec.IngressRoutes{{PathType: "unknown"}},
			wantSummaryTypes: []string{"Prefix"}, wantRenderTypes: []string{"Prefix"},
		},
		{
			name:             "rewrite only affects current and later routes",
			routes:           []spec.IngressRoutes{{Path: "/first"}, {Path: "/second", Rewrite: &spec.RewritePolicy{Type: "regexReplace", Replacement: "/$1"}}, {Path: "/third"}},
			wantSummaryTypes: []string{"Prefix", "ImplementationSpecific", "ImplementationSpecific"},
			wantRenderTypes:  []string{"Prefix", "ImplementationSpecific", "ImplementationSpecific"},
			wantAnnotations:  map[string]string{"nginx.ingress.kubernetes.io/rewrite-target": "/$1", "nginx.ingress.kubernetes.io/use-regex": "true"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ingressSpec := spec.IngressTraitsSpec{DefaultPathType: tt.defaultPathType, Routes: tt.routes, Annotations: tt.annotations}
			before, err := json.Marshal(ingressSpec)
			require.NoError(t, err)
			component := &model.ApplicationComponent{
				Name: "backend", AppID: "demo", ComponentType: config.ServerJob,
				Traits: mustJSONStruct(t, spec.Traits{Ingress: []spec.IngressTraitsSpec{ingressSpec}}),
			}
			dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{component})
			require.NoError(t, err)
			summary := dtos[0].Ingresses[0]
			ingress, err := traits.BuildIngress(&ingressSpec)
			require.NoError(t, err)
			require.Nil(t, summary.TLS)
			require.Nil(t, ingress.Spec.TLS)
			require.Equal(t, tt.wantAnnotations, summary.Annotations)
			if len(tt.wantAnnotations) == 0 {
				require.Nil(t, ingress.Annotations)
			} else {
				require.Equal(t, tt.wantAnnotations, ingress.Annotations)
			}
			for i, route := range summary.Routes {
				require.Equal(t, tt.wantSummaryTypes[i], route.PathType)
				require.Equal(t, tt.wantRenderTypes[i], string(*ingress.Spec.Rules[0].HTTP.Paths[i].PathType))
			}
			after, err := json.Marshal(ingressSpec)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
			dtoSpec, err := json.Marshal(dtos[0].Traits.Ingress[0])
			require.NoError(t, err)
			require.Equal(t, string(before), string(dtoSpec))
		})
	}
}
