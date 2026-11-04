package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func gatewayHostStore(namespace string, gateway spec.GatewayTraitSpec) *inMemoryAppStore {
	store := newInMemoryAppStore()
	store.apps["app-chat"] = &model.Applications{ID: "app-chat", Name: "chat", Version: "1.0.0", Namespace: namespace}
	store.components["web"] = &model.ApplicationComponent{
		Name:          "web",
		AppID:         "app-chat",
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Properties:    mustJSONStruct(&apisv1.Properties{Ports: []spec.Ports{{Port: 8080}}}),
		Traits:        mustJSONStruct(&apisv1.Traits{Gateway: &gateway}),
	}
	return store
}

func gatewayComponent(name string, gateway spec.GatewayTraitSpec) apisv1.CreateComponentRequest {
	return apisv1.CreateComponentRequest{
		Name:          name,
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Properties:    apisv1.Properties{Ports: []spec.Ports{{Port: 8080}}},
		Traits:        apisv1.Traits{Gateway: &gateway},
	}
}

// The platform gateway forwards one host to one route, so a workspace cannot
// hold two components with the same effective host, whether the host is the
// default one or declared.
func TestCreateApplicationsRejectsGatewayHostCollision(t *testing.T) {
	const namespace = "eruun-ws-0123456789abcdef0123456789abcdef"
	declared := "chat-" + namespace + ".apps.example.com"
	for _, tc := range []struct {
		name      string
		existing  spec.GatewayTraitSpec
		requested []apisv1.CreateComponentRequest
		requestNS string
		conflict  string
	}{
		{
			name:      "default hosts of same-named components, ignoring case",
			existing:  spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}},
			requested: []apisv1.CreateComponentRequest{gatewayComponent("Web", spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}})},
			conflict:  `"web-` + namespace + `"`,
		},
		{
			name:      "declared host taken by another application",
			existing:  spec.GatewayTraitSpec{Hosts: []string{declared}, Routes: []spec.GatewayRouteSpec{{}}},
			requested: []apisv1.CreateComponentRequest{gatewayComponent("api", spec.GatewayTraitSpec{Hosts: []string{declared}, Routes: []spec.GatewayRouteSpec{{}}})},
			conflict:  `"chat-` + namespace + `"`,
		},
		{
			name:     "declared host equal to another application's default host",
			existing: spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}},
			requested: []apisv1.CreateComponentRequest{gatewayComponent("api", spec.GatewayTraitSpec{
				Hosts: []string{"web-" + namespace + ".apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}},
			})},
			conflict: `"web-` + namespace + `"`,
		},
		{
			name:     "two components of the new application",
			existing: spec.GatewayTraitSpec{Hosts: []string{"other-" + namespace + ".apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}},
			requested: []apisv1.CreateComponentRequest{
				gatewayComponent("api", spec.GatewayTraitSpec{Hosts: []string{declared}, Routes: []spec.GatewayRouteSpec{{}}}),
				gatewayComponent("admin", spec.GatewayTraitSpec{Hosts: []string{declared}, Routes: []spec.GatewayRouteSpec{{}}}),
			},
			conflict: `"chat-` + namespace + `"`,
		},
		{
			name:      "same component name in another workspace",
			existing:  spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}},
			requested: []apisv1.CreateComponentRequest{gatewayComponent("web", spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}})},
			requestNS: "eruun-ws-fedcba9876543210fedcba9876543210",
		},
		{
			name:      "different hosts",
			existing:  spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}},
			requested: []apisv1.CreateComponentRequest{gatewayComponent("api", spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}})},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := gatewayHostStore(namespace, tc.existing)
			requestNS := namespace
			if tc.requestNS != "" {
				requestNS = tc.requestNS
			}
			_, err := newMockServiceWithStore(store).CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
				Namespace: requestNS, Name: "agent", Components: tc.requested,
			})
			if tc.conflict == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Contains(t, err.Error(), "duplicate gateway host label "+tc.conflict)
			require.Len(t, store.apps, 1, "the rejected application must not be created")
		})
	}
}

// A version update that gives a component another application's host is
// rejected before anything is written.
func TestUpdateVersionRejectsGatewayHostCollision(t *testing.T) {
	const namespace = "eruun-ws-0123456789abcdef0123456789abcdef"
	store := gatewayHostStore(namespace, spec.GatewayTraitSpec{Routes: []spec.GatewayRouteSpec{{}}})
	store.apps["app-agent"] = &model.Applications{ID: "app-agent", Name: "agent", Version: "1.0.0", Namespace: namespace}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-agent",
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Properties:    mustJSONStruct(&apisv1.Properties{Ports: []spec.Ports{{Port: 8080}}}),
	}
	traits := apisv1.Traits{Gateway: &spec.GatewayTraitSpec{Hosts: []string{"web-" + namespace + ".apps.example.com"}, Routes: []spec.GatewayRouteSpec{{}}}}

	_, err := newMockServiceWithStore(store).UpdateVersion(context.Background(), "app-agent", apisv1.UpdateVersionRequest{
		Version:    "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{Name: "api", Traits: &traits}},
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), `duplicate gateway host label "web-`+namespace+`"`)
	require.Equal(t, "1.0.0", store.apps["app-agent"].Version)
	require.Nil(t, store.components["api"].Traits)
}
