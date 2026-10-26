package validation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

// Try applies the gateway rules to workspace components with the platform
// gateway configuration and returns the declared spec unchanged.
func TestTryApplicationValidatesGatewayInWorkspace(t *testing.T) {
	ingress := spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"}
	ctx := access.WithScope(context.Background(), access.Scope{WorkspaceID: "a", Namespace: "ws-a", Role: "member"})
	request := func(traits apisv1.Traits) apisv1.CreateApplicationsRequest {
		return apisv1.CreateApplicationsRequest{
			Name: "chat", Version: "1.0.0",
			Components: []apisv1.CreateComponentRequest{{
				Name: "api", ComponentType: config.ServerJob, Image: "nginx:1.27", Replicas: 1,
				Properties: apisv1.Properties{Ports: []spec.Ports{{Port: 8080}}},
				Traits:     traits,
			}},
		}
	}
	try := func(gateway spec.GatewayConfig, traits apisv1.Traits) *apisv1.TryApplicationResponse {
		svc := &validationServiceImpl{Cfg: &config.Config{Accounts: &spec.AccountConfig{}, Gateway: gateway}}
		return svc.TryApplication(ctx, request(traits))
	}

	declared := spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Path: "/v1"}}}
	resp := try(ingress, apisv1.Traits{Gateway: &declared})
	require.True(t, resp.Valid, "%+v", resp.Errors)
	require.Equal(t, declared, *resp.NormalizedSpec.Components[0].Traits.Gateway)

	// Service trait names take their stored form before the gateway resolves them.
	padded := apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{Name: " api-svc ", Selector: map[string]string{"app": "api"}, Ports: []spec.ServicePortTraitSpec{{Port: 8080}}}},
		Gateway: &spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{Backend: &spec.GatewayBackendSpec{Service: "api-svc"}}}},
	}
	resp = try(ingress, padded)
	require.True(t, resp.Valid, "%+v", resp.Errors)
	require.Equal(t, "api-svc", resp.NormalizedSpec.Components[0].Traits.Service[0].Name)

	resp = try(spec.GatewayConfig{}, apisv1.Traits{Gateway: &declared})
	requireValidationError(t, resp.Errors, "component[0].traits.gateway", apisv1.ErrCodeGatewayNotAllowed)

	foreign := spec.GatewayTraitSpec{Hosts: []string{"api-ws-b.apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}}
	resp = try(ingress, apisv1.Traits{Gateway: &foreign})
	requireValidationError(t, resp.Errors, "component[0].traits.gateway.hosts", apisv1.ErrCodeInvalidGatewayHost)

	resp = try(ingress, apisv1.Traits{Sidecar: []spec.SidecarTraitsSpec{{Name: "proxy", Image: "busybox:1.37", Traits: spec.Traits{Gateway: &declared}}}})
	require.False(t, resp.Valid)
	found := false
	for _, err := range resp.Errors {
		found = found || err.Code == apisv1.ErrCodeGatewayNotAllowed
	}
	require.True(t, found, "nested gateway must be rejected: %+v", resp.Errors)
}
