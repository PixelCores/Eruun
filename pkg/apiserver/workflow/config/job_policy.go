package config

import "strings"

type JobRunPolicy string

const (
	DefaultRun                  JobRunPolicy = ""
	DefaultNotRun               JobRunPolicy = "default_not_run"
	ForceRun                    JobRunPolicy = "force_run"
	SkipRun                     JobRunPolicy = "skip"
	JobRunPolicyRecreate        JobRunPolicy = "recreate"
	JobRunPolicySkipIfCompleted JobRunPolicy = "skip_if_completed"
)

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
