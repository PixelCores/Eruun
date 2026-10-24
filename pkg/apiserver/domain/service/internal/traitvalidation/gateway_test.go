package traitvalidation

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestValidateGateway(t *testing.T) {
	const field = "component[0].traits.gateway"
	ingress := spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}
	routes := func(n int) []spec.GatewayRouteSpec {
		result := make([]spec.GatewayRouteSpec, n)
		for i := range result {
			result[i].Path = fmt.Sprintf("/p%d", i)
		}
		return result
	}
	hosts := func(n int) []string {
		result := make([]string, n)
		for i := range result {
			result[i] = fmt.Sprintf("h%d-ws-a.apps.example.com", i)
		}
		return result
	}
	for _, tt := range []struct {
		name      string
		gateway   spec.GatewayTraitSpec
		component func(*apisv1.CreateComponentRequest)
		cfg       *spec.GatewayConfig
		want      map[string]string
	}{
		{name: "defaults", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}}},
		{
			name: "declared hosts and both match types",
			gateway: spec.GatewayTraitSpec{Hosts: hosts(8), Routes: []spec.GatewayRouteSpec{
				{Path: "/v1"}, {Path: "/v1", Match: spec.GatewayMatchExact}, {Path: "/v1/models", Backend: &spec.GatewayBackendSpec{Port: 8080}},
			}},
		},
		{
			name:      "store component",
			gateway:   spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}},
			component: func(c *apisv1.CreateComponentRequest) { c.ComponentType = config.StoreJob },
		},
		{
			name: "disabled platform gateway", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}}, cfg: &spec.GatewayConfig{},
			want: map[string]string{field: apisv1.ErrCodeGatewayNotAllowed},
		},
		{
			name:      "job component",
			gateway:   spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}},
			component: func(c *apisv1.CreateComponentRequest) { c.ComponentType = config.InstantJob },
			want:      map[string]string{field: apisv1.ErrCodeGatewayNotAllowed},
		},
		{
			name: "too many hosts", gateway: spec.GatewayTraitSpec{Hosts: hosts(9), Routes: []spec.GatewayRouteSpec{{}}},
			want: map[string]string{field + ".hosts": apisv1.ErrCodeInvalidGatewayHost},
		},
		{
			name:    "duplicate host",
			gateway: spec.GatewayTraitSpec{Hosts: []string{"api-ws-a.apps.example.com", "api-ws-a.apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}},
			want:    map[string]string{field + ".hosts[1]": apisv1.ErrCodeInvalidGatewayHost},
		},
		{
			name:    "host of another workspace",
			gateway: spec.GatewayTraitSpec{Hosts: []string{"api-ws-b.apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}},
			want:    map[string]string{field + ".hosts": apisv1.ErrCodeInvalidGatewayHost},
		},
		{
			name:    "multi-label host",
			gateway: spec.GatewayTraitSpec{Hosts: []string{"api.ws-a.apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}},
			want:    map[string]string{field + ".hosts": apisv1.ErrCodeInvalidGatewayHost},
		},
		{
			name: "no routes", gateway: spec.GatewayTraitSpec{},
			want: map[string]string{field + ".routes": apisv1.ErrCodeMissingGatewayRoutes},
		},
		{
			name: "too many routes", gateway: spec.GatewayTraitSpec{Routes: routes(17)},
			want: map[string]string{field + ".routes": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "duplicate effective route", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}, {Path: "/", Match: spec.GatewayMatchPrefix}}},
			want: map[string]string{field + ".routes[1]": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "relative path", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "api"}}},
			want: map[string]string{field + ".routes[0].path": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "path traversal", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/a/../b"}}},
			want: map[string]string{field + ".routes[0].path": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "path query", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/a?b"}}},
			want: map[string]string{field + ".routes[0].path": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "match is case sensitive", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Match: "Prefix"}}},
			want: map[string]string{field + ".routes[0].match": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "rewrite of an exact match", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/api", Match: spec.GatewayMatchExact, Rewrite: "/"}}},
			want: map[string]string{field + ".routes[0].rewrite": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "relative rewrite", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/api", Rewrite: "v1"}}},
			want: map[string]string{field + ".routes[0].rewrite": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "rewrite on ingress", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/api", Rewrite: "/"}}},
			want: map[string]string{field + ".routes[0].rewrite": apisv1.ErrCodeGatewayUnsupported},
		},
		{
			name: "timeout without unit", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Timeout: "10"}}},
			want: map[string]string{field + ".routes[0].timeout": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "timeout below one second", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Timeout: "500ms"}}},
			want: map[string]string{field + ".routes[0].timeout": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "timeout above one hour", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Timeout: "1h1s"}}},
			want: map[string]string{field + ".routes[0].timeout": apisv1.ErrCodeInvalidGatewayRoute},
		},
		{
			name: "timeout on ingress", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Timeout: "1h"}}},
			want: map[string]string{field + ".routes[0].timeout": apisv1.ErrCodeGatewayUnsupported},
		},
		{
			name: "unknown backend service", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Backend: &spec.GatewayBackendSpec{Service: "other"}}}},
			want: map[string]string{field + ".routes[0].backend": apisv1.ErrCodeInvalidGatewayBackend},
		},
		{
			name: "undeclared backend port", gateway: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Backend: &spec.GatewayBackendSpec{Port: 9090}}}},
			want: map[string]string{field + ".routes[0].backend": apisv1.ErrCodeInvalidGatewayBackend},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gateway := tt.gateway
			component := apisv1.CreateComponentRequest{
				Name: "api", ComponentType: config.ServerJob,
				Properties: apisv1.Properties{Ports: []spec.Ports{{Port: 8080}}},
				Traits:     apisv1.Traits{Gateway: &gateway},
			}
			if tt.component != nil {
				tt.component(&component)
			}
			cfg := ingress
			if tt.cfg != nil {
				cfg = *tt.cfg
			}
			got := map[string]string{}
			for _, err := range ValidateGateway(component, "ws-a", cfg, field) {
				require.NotContains(t, got, err.Field, "one error per field")
				require.NotEmpty(t, err.Message)
				got[err.Field] = err.Code
			}
			if tt.want == nil {
				tt.want = map[string]string{}
			}
			require.Equal(t, tt.want, got)
		})
	}
	require.Nil(t, ValidateGateway(apisv1.CreateComponentRequest{Name: "api", ComponentType: config.InstantJob}, "ws-a", spec.GatewayConfig{}, field))
}
