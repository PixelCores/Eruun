package config

import "strings"

// NormalizeWorkflowFailurePolicy returns a normalized policy and whether the input is known.
func NormalizeWorkflowFailurePolicy(policy WorkflowFailurePolicy) (WorkflowFailurePolicy, bool) {
	normalized := strings.ToLower(strings.TrimSpace(string(policy)))
	switch normalized {
	case "":
		return WorkflowFailurePolicyCleanupAll, true
	case string(WorkflowFailurePolicyCleanupFailed):
		return WorkflowFailurePolicyCleanupFailed, true
	case string(WorkflowFailurePolicyCleanupAll):
		return WorkflowFailurePolicyCleanupAll, true
	default:
		return WorkflowFailurePolicyCleanupFailed, false
	}
}
