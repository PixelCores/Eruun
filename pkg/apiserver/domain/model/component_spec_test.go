package model

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
)

func TestComponentSpecPreservesPersistedInput(t *testing.T) {
	properties := JSONStruct{
		"secret":  map[string]any{"token": "example-placeholder"},
		"ports":   []any{},
		"command": []any{"run", "--serve"},
	}
	traits := JSONStruct{
		"sidecar": []any{map[string]any{"name": "logger", "image": "logger:1"}},
		"envs":    []any{},
	}
	component := &ApplicationComponent{
		Name: "api", ComponentType: config.ServerJob, Image: "api:1",
		Namespace: "workspace", Replicas: 2, Properties: &properties, Traits: &traits,
	}
	result, err := component.ComponentSpec()
	require.NoError(t, err)
	require.Equal(t, "api", result.Name)
	require.Equal(t, config.ServerJob, result.ComponentType)
	require.Equal(t, "api:1", result.Image)
	require.Equal(t, "workspace", result.Namespace)
	require.EqualValues(t, 2, result.Replicas)
	require.Equal(t, map[string]string{"token": "example-placeholder"}, result.Properties.Secret)
	require.NotNil(t, result.Properties.Ports)
	require.Empty(t, result.Properties.Ports)
	require.Nil(t, result.Properties.Env)
	require.Equal(t, []string{"run", "--serve"}, result.Properties.Command)
	require.Len(t, result.Traits.Sidecar, 1)
	require.Equal(t, "logger", result.Traits.Sidecar[0].Name)
	require.Equal(t, "logger:1", result.Traits.Sidecar[0].Image)
	require.NotNil(t, result.Traits.Envs)
	require.Nil(t, result.Template)

	// Mutating the decoded input must not mutate the persistence snapshot.
	result.Properties.Secret["token"] = "changed"
	require.Equal(t, "example-placeholder", properties["secret"].(map[string]any)["token"])
}

func TestComponentSpecNilAndInvalidFields(t *testing.T) {
	var nilMap JSONStruct
	for _, component := range []*ApplicationComponent{nil, {}, {Properties: &nilMap, Traits: &nilMap}} {
		result, err := component.ComponentSpec()
		require.NoError(t, err)
		require.Equal(t, spec.Component{}, result)
	}
	for _, tc := range []struct {
		name      string
		component ApplicationComponent
	}{
		{"properties", ApplicationComponent{Properties: &JSONStruct{"ports": "invalid"}}},
		{"traits", ApplicationComponent{Traits: &JSONStruct{"sidecar": "invalid"}}},
		{"unencodable", ApplicationComponent{Properties: &JSONStruct{"invalid": func() {}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.component.Name = "broken"
			_, err := tc.component.ComponentSpec()
			require.ErrorContains(t, err, "convert component broken")
			if tc.name == "unencodable" {
				var unsupported *json.UnsupportedTypeError
				require.ErrorAs(t, err, &unsupported)
			} else {
				var invalid *json.UnmarshalTypeError
				require.ErrorAs(t, err, &invalid)
			}
		})
	}
}
