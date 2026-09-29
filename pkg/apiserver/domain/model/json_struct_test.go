package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestJSONStructBytesReturnsSerializationErrors(t *testing.T) {
	valid := JSONStruct{"name": "demo"}
	data, err := valid.Bytes()
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"demo"}`, string(data))

	invalid := JSONStruct{"invalid": func() {}}
	_, err = invalid.Bytes()
	var unsupportedType *json.UnsupportedTypeError
	require.ErrorAs(t, err, &unsupportedType)
}

func TestNewJSONStructByStructPreservesJSONContract(t *testing.T) {
	var typedNil *Properties
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"nil", nil},
		{"typed nil", typedNil},
		{"empty map", map[string]any{}},
		{"properties", Properties{Ports: []Ports{}, Secret: map[string]string{"value": "yes"}, StartTime: 9007199254740991}},
		{"nested traits", Traits{Sidecar: []spec.SidecarTraitsSpec{{Name: "logger", Image: "logger:1"}}, TargetWorkEnv: map[string]string{"zone": "a"}}},
		{"numbers", map[string]any{"int": int64(42), "negative": -5, "fraction": 1.25}},
		{"tags and custom JSON", struct {
			Name    string          `json:"displayName" yaml:"differentName"`
			Omitted string          `json:"omitted,omitempty"`
			When    time.Time       `json:"when"`
			Raw     json.RawMessage `json:"raw"`
		}{Name: "sample", When: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC), Raw: json.RawMessage(`{"enabled":true}`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewJSONStructByStruct(tc.value)
			require.NoError(t, err)
			if tc.value == nil {
				require.Nil(t, got)
				return
			}
			// Compare with the removed YAML round-trip for real field semantics.
			legacyBytes, err := yaml.Marshal(tc.value)
			require.NoError(t, err)
			var legacy JSONStruct
			require.NoError(t, yaml.Unmarshal(legacyBytes, &legacy))
			require.Equal(t, &legacy, got)
		})
	}
}

func TestNewJSONStructByStructRejectsInvalidInput(t *testing.T) {
	for _, value := range []any{map[string]any{"invalid": func() {}}, []string{"not an object"}} {
		_, err := NewJSONStructByStruct(value)
		require.Error(t, err)
	}
}

func TestJSONStructRawExtensionReturnsSerializationErrors(t *testing.T) {
	valid := JSONStruct{"name": "demo"}
	raw, err := valid.RawExtension()
	require.NoError(t, err)
	require.NotNil(t, raw)
	require.JSONEq(t, `{"name":"demo"}`, string(raw.Raw))

	invalid := JSONStruct{"invalid": func() {}}
	_, err = invalid.RawExtension()
	require.Error(t, err)
}
