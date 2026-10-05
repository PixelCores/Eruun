package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestConvertComponentModelToDTOUsesServicePortsForSvcLinks(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ports []model.Ports
	}{
		{name: "service port differs from container port", ports: []model.Ports{{Port: 8080}}},
		{name: "service trait without property ports"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &model.ApplicationComponent{
				AppID:         "app-9",
				Name:          "api",
				Namespace:     "default",
				ComponentType: config.ServerJob,
				Properties:    mustJSONStruct(t, model.Properties{Ports: tc.ports}),
				Traits: mustJSONStruct(t, model.Traits{Service: []spec.ServiceTraitSpec{{
					Name:     "api-fixed",
					Type:     string(spec.ServiceAccessInternal),
					Selector: map[string]string{"app": "api"},
					Ports:    []spec.ServicePortTraitSpec{{Port: 80, TargetPort: 8080, Protocol: "TCP"}},
				}}}),
			}

			dto, err := ConvertComponentModelToDTO(component)
			require.NoError(t, err)
			require.Len(t, dto.Services, 1)
			require.Equal(t, int32(80), dto.Services[0].Ports[0].Port)
			require.Equal(t, int32(8080), dto.Services[0].Ports[0].TargetPort)
			require.Equal(t, []apisv1.ExternalLink{
				{Type: "svc", Value: "api-fixed.default.svc:80"},
			}, dto.ExternalLinks)
		})
	}
}

func TestConvertComponentModelToDTOUsesFirstExternalServiceForSvcLink(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:          "api",
		Namespace:     "team",
		ComponentType: config.ServerJob,
		Traits: mustJSONStruct(t, model.Traits{Service: []spec.ServiceTraitSpec{
			{
				Name: "external-api", Type: string(spec.ServiceAccessExternal), ExternalName: "api.example.com",
				Ports: []spec.ServicePortTraitSpec{{Port: 80, Protocol: "TCP"}, {Port: 80, Protocol: "UDP"}, {Port: 443, Protocol: "TCP"}},
			},
			{
				Name: "other-api", Type: string(spec.ServiceAccessExternal), ExternalName: "other.example.com",
				Ports: []spec.ServicePortTraitSpec{{Port: 8080, Protocol: "TCP"}},
			},
		}}),
	}

	dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{component})
	require.NoError(t, err)
	require.Len(t, dtos, 1)
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "svc", Value: "external-api.team.svc:80,443"},
	}, dtos[0].ExternalLinks)
}
