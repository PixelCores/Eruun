package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskDispatchRejectsMissingExecutionIdentity(t *testing.T) {
	payload := []byte(`{"taskId":"task-1"}`)
	_, err := UnmarshalTaskDispatch(payload)
	require.ErrorContains(t, err, "invalid workflow dispatch envelope")
}

func TestTaskDispatchRoundTrip(t *testing.T) {
	want := TaskDispatch{
		Version:       taskDispatchVersion,
		TaskID:        "task-2",
		RunGeneration: 7,
		RunToken:      "run-token",
	}

	payload, err := MarshalTaskDispatch(want)
	require.NoError(t, err)
	require.JSONEq(t, `{"version":2,"taskId":"task-2","runGeneration":7,"runToken":"run-token"}`, string(payload))
	got, err := UnmarshalTaskDispatch(payload)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
