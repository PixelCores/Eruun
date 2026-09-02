package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskDispatchRejectsMissingExecutionIdentity(t *testing.T) {
	payload := []byte(`{"taskId":"task-1","workflowId":"workflow-1","projectId":"project-1","appId":"app-1"}`)
	_, err := UnmarshalTaskDispatch(payload)
	require.ErrorContains(t, err, "invalid workflow dispatch envelope")
}

func TestTaskDispatchRoundTrip(t *testing.T) {
	want := TaskDispatch{
		Version:       taskDispatchVersion,
		TaskID:        "task-2",
		WorkflowID:    "workflow-2",
		ProjectID:     "project-2",
		AppID:         "app-2",
		RunGeneration: 7,
		RunToken:      "run-token",
	}

	payload, err := MarshalTaskDispatch(want)
	require.NoError(t, err)
	got, err := UnmarshalTaskDispatch(payload)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
