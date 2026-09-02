package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
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
