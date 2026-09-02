package config

import "strings"

// NormalizeJobRunPolicy returns a normalized policy and whether the input is known.
func NormalizeJobRunPolicy(policy string) (JobRunPolicy, bool) {
	normalized := strings.ToLower(strings.TrimSpace(policy))
	switch normalized {
	case "":
		return JobRunPolicySkipIfCompleted, true
	case string(JobRunPolicyRecreate):
		return JobRunPolicyRecreate, true
	case string(JobRunPolicySkipIfCompleted):
		return JobRunPolicySkipIfCompleted, true
	default:
		return JobRunPolicySkipIfCompleted, false
	}
}
