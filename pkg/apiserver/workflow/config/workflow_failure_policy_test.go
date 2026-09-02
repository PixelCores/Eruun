package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeWorkflowFailurePolicyDefaultsToCleanupAll(t *testing.T) {
	policy, ok := NormalizeWorkflowFailurePolicy("")
	require.True(t, ok)
	require.Equal(t, WorkflowFailurePolicyCleanupAll, policy)
}

func TestNormalizeWorkflowFailurePolicyAllowsCleanupFailedOptOut(t *testing.T) {
	policy, ok := NormalizeWorkflowFailurePolicy(" cleanup_failed ")
	require.True(t, ok)
	require.Equal(t, WorkflowFailurePolicyCleanupFailed, policy)
}
