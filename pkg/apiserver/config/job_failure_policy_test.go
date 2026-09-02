package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeJobFailurePolicy(t *testing.T) {
	tests := []struct {
		name     string
		input    WorkflowFailurePolicy
		expected WorkflowFailurePolicy
		ok       bool
	}{
		{name: "empty inherits workflow", input: "", expected: "", ok: true},
		{name: "cleanup failed override", input: " cleanup_FAILED ", expected: WorkflowFailurePolicyCleanupFailed, ok: true},
		{name: "cleanup all is not allowed", input: WorkflowFailurePolicyCleanupAll, expected: "", ok: false},
		{name: "unknown is not allowed", input: "delete_everything", expected: "", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, ok := NormalizeJobFailurePolicy(tt.input)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.expected, actual)
		})
	}
}
