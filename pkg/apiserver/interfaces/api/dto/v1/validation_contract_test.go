package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinalizeValidationErrorsUsesCanonicalJSONPointers(t *testing.T) {
	applicationErrors := FinalizeValidationErrors([]ValidationError{
		{Field: "component[0].traits.storage[1].mountPath"},
		{Field: "workflow[0].components[0]"},
		{Field: "workflow[1].name"},
		{Field: "callback.success"},
		{Field: `component[0].traits.targetWorkEnv["bad/key~name"]`},
	}, ValidationPathApplication)
	require.Equal(t, "/components/0/traits/storage/1/mountPath", applicationErrors[0].Path)
	require.Equal(t, "/workflow/0/components/0", applicationErrors[1].Path)
	require.Equal(t, "/workflow/1/name", applicationErrors[2].Path)
	require.Equal(t, "/callback/success", applicationErrors[3].Path)
	require.Equal(t, "/components/0/traits/targetWorkEnv/bad~1key~0name", applicationErrors[4].Path)

	workflowErrors := FinalizeValidationErrors([]ValidationError{{Field: "workflow[0].name"}}, ValidationPathWorkflow)
	require.Equal(t, "/workflow/0/name", workflowErrors[0].Path)
}

func TestEmptyValidationErrorsRemainAnArray(t *testing.T) {
	require.NotNil(t, FinalizeValidationErrors(nil, ValidationPathApplication))
}
