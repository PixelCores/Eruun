package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestConvertComponentModelsToDTOScopesAndOrdersCredentials(t *testing.T) {
	components := []*model.ApplicationComponent{
		{
			Name: "db-secret", Namespace: "default", ComponentType: config.SecretJob,
			Properties: mustJSONStruct(t, model.Properties{Secret: map[string]string{"z": "last", "a": "first"}}),
		},
		{
			Name: "db-secret", Namespace: "default", ComponentType: config.SecretJob,
			Properties: mustJSONStruct(t, model.Properties{Secret: map[string]string{"a": "duplicate"}}),
		},
		{
			Name: "db-secret", Namespace: "team", ComponentType: config.SecretJob,
			Properties: mustJSONStruct(t, model.Properties{Secret: map[string]string{"a": "other namespace"}}),
		},
	}
	for _, namespace := range []string{"", "team", "missing"} {
		components = append(components, &model.ApplicationComponent{
			Name: "api-" + namespace, Namespace: namespace, ComponentType: config.ServerJob,
			Traits: mustJSONStruct(t, model.Traits{EnvFrom: []spec.EnvFromSourceSpec{{
				Type: spec.StorageTypeSecret, SourceName: "db-secret",
			}}}),
		})
	}

	dtos, err := ConvertComponentModelsToDTO(components)
	require.NoError(t, err)
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envFrom", SecretName: "db-secret", Key: "a", Value: "first", Resolved: true},
		{Source: "component.envFrom", SecretName: "db-secret", Key: "z", Value: "last", Resolved: true},
	}, requireComponentByName(t, dtos, "api-").Credentials)
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envFrom", SecretName: "db-secret", Key: "a", Value: "other namespace", Resolved: true},
	}, requireComponentByName(t, dtos, "api-team").Credentials)
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envFrom", SecretName: "db-secret", Resolved: false},
	}, requireComponentByName(t, dtos, "api-missing").Credentials)
}
