package config

import "strings"

// NormalizeJobFailurePolicy normalizes the optional failure-policy override for an instant Job.
// An empty value inherits the workflow policy; cleanup_failed is the only explicit override.
func NormalizeJobFailurePolicy(policy WorkflowFailurePolicy) (WorkflowFailurePolicy, bool) {
	normalized := strings.ToLower(strings.TrimSpace(string(policy)))
	switch normalized {
	case "":
		return "", true
	case string(WorkflowFailurePolicyCleanupFailed):
		return WorkflowFailurePolicyCleanupFailed, true
	default:
		return "", false
	}
}
