package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type validateNameRequest struct {
	Name string `validate:"checkname"`
}

func TestInitValidator_Idempotent(t *testing.T) {
	require.NoError(t, InitValidator())
	require.NoError(t, InitValidator())
}

func TestInitValidator_RegistersCustomRules(t *testing.T) {
	require.NoError(t, InitValidator())

	require.NoError(t, validate.Struct(validateNameRequest{Name: "app-name"}))
	require.Error(t, validate.Struct(validateNameRequest{Name: "Invalid Name"}))
}
