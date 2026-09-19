package api

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/stretchr/testify/require"
)

func TestEvaluationExamplesUseTheSharedPublicContract(t *testing.T) {
	read := func(path string) []byte {
		raw, err := os.ReadFile("../../../../examples/agent-evaluation/" + path)
		require.NoError(t, err)
		return bytes.ReplaceAll(raw, []byte("<UPLOADED_DATASET_ID>"), []byte("11111111-1111-1111-1111-111111111111"))
	}
	for _, path := range []string{"evaluation.json", "load-test/evaluation.json", "command.json"} {
		var request jobs.SubmitRequest
		require.NoError(t, spec.DecodeJobJSON(read(path), &request), path)
		require.NoError(t, request.Normalize(), path)
	}
	var app apisv1.CreateApplicationsRequest
	require.NoError(t, json.Unmarshal(read("application.json"), &app))
	require.Len(t, app.Components, 1)
	component := &app.Components[0]
	require.NoError(t, spec.NormalizeComponentEvaluation(string(component.ComponentType), component.Image, component.Properties, &component.Traits))
	require.Equal(t, "oracle", component.Traits.Evaluation.Agent)
	require.Equal(t, []string{component.Name}, app.Workflow[0].Components)
}
