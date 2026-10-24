package application

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestValidateComponentTraitsForWriteRejectsInvalidServiceTrait(t *testing.T) {
	err := validateComponentTraitsForWrite(config.ServerJob, apisv1.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "backend",
				Type: "internal",
				Ports: []spec.ServicePortTraitSpec{
					{Port: 8080},
				},
			},
		},
	}, "component[0].traits")

	require.Error(t, err)
	require.Contains(t, err.Error(), "selector is required")
}

func TestValidateComponentTraitsForWriteValidatesResources(t *testing.T) {
	testCases := []struct {
		name      string
		traits    apisv1.Traits
		wantError string
	}{
		{
			name: "rejects top-level cpu limit below request",
			traits: apisv1.Traits{
				Resources: &spec.ResourceTraitsSpec{CPU: "500m", CPULimit: "300m"},
			},
			wantError: "cpuLimit must be greater than or equal to cpu request",
		},
		{
			name: "rejects sidecar memory limit below request",
			traits: apisv1.Traits{
				Sidecar: []spec.SidecarTraitsSpec{
					{
						Name:  "metrics",
						Image: "metrics:v1",
						Traits: spec.Traits{
							Resources: &spec.ResourceTraitsSpec{Memory: "1Gi", MemoryLimit: "512Mi"},
						},
					},
				},
			},
			wantError: "memoryLimit must be greater than or equal to memory request",
		},
		{
			name: "allows limit-only resources",
			traits: apisv1.Traits{
				Resources: &spec.ResourceTraitsSpec{CPULimit: "300m", MemoryLimit: "512Mi"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateComponentTraitsForWrite(config.ServerJob, tc.traits, "component[0].traits")
			if tc.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

func TestValidateComponentTraitsForWriteRejectsNestedStorageSubPathConflict(t *testing.T) {
	err := validateComponentTraitsForWrite(config.ServerJob, apisv1.Traits{
		Sidecar: []spec.SidecarTraitsSpec{
			{
				Name:  "logger",
				Image: "logger:latest",
				Traits: spec.Traits{
					Storage: []spec.StorageTraitSpec{{
						Name:        "logs",
						Type:        spec.StorageTypePersistent,
						MountPath:   "/app/log",
						SubPath:     "fixed/logs",
						SubPathExpr: "$(POD_IP)/logs",
					}},
				},
			},
		},
	}, "component[0].traits")

	require.Error(t, err)
	require.Contains(t, err.Error(), "storage subPath and subPathExpr cannot both be set")
}

func TestValidateVersionUpdateWorkspaceComponentsChecksGateway(t *testing.T) {
	ctx := access.WithScope(context.Background(), access.Scope{WorkspaceID: "workspace-a", Namespace: "ws-a", Role: "member"})
	svc := &applicationsServiceImpl{Cfg: &config.Config{
		Accounts: &spec.AccountConfig{},
		Gateway:  spec.GatewayConfig{Provider: spec.GatewayProviderIngress, Domain: "apps.example.com", ProxyNamespace: "ingress-nginx", IngressClass: "nginx"},
	}}
	gateway := &spec.GatewayTraitSpec{Hosts: []string{"api-ws-b.apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}}
	components := []apisv1.CreateComponentRequest{{
		Name: "api", ComponentType: config.ServerJob,
		Properties: apisv1.Properties{Ports: []spec.Ports{{Port: 8080}}},
		Traits:     apisv1.Traits{Gateway: gateway},
	}}

	err := svc.validateVersionUpdateWorkspaceComponents(ctx, &versionUpdateRun{}, components)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.True(t, strings.HasPrefix(bcode.SafeClientMessage(err), "component[api].traits.gateway.hosts: "), bcode.SafeClientMessage(err))

	gateway.Hosts = nil
	require.NoError(t, svc.validateVersionUpdateWorkspaceComponents(ctx, &versionUpdateRun{}, components))
}
