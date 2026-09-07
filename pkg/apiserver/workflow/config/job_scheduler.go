package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

const (
	JobSchedulerPriority  = "priority"
	JobSchedulerFIFO      = "fifo"
	JobSchedulingQueued   = "queued"
	JobSchedulingAdmitted = "admitted"
	JobSchedulingReleased = "released"
)

// JobSchedulerPolicy governs admission to Job execution across all workers.
// It does not replace Kubernetes placement or its resource quotas.
type JobSchedulerPolicy struct {
	Strategy                      string `json:"strategy"`
	MaxConcurrentJobs             int    `json:"maxConcurrentJobs"`
	MaxConcurrentJobsPerWorkspace int    `json:"maxConcurrentJobsPerWorkspace"`
	AgingSeconds                  int    `json:"agingSeconds"`
}

func DefaultJobSchedulerPolicy() JobSchedulerPolicy {
	return JobSchedulerPolicy{
		Strategy: JobSchedulerPriority, MaxConcurrentJobs: 100,
		MaxConcurrentJobsPerWorkspace: 10, AgingSeconds: 60,
	}
}

func (p JobSchedulerPolicy) Validate() error {
	if p.Strategy != JobSchedulerPriority && p.Strategy != JobSchedulerFIFO {
		return fmt.Errorf("job scheduler strategy must be priority or fifo")
	}
	if p.MaxConcurrentJobs < 1 || p.MaxConcurrentJobs > 10000 {
		return fmt.Errorf("maxConcurrentJobs must be between 1 and 10000")
	}
	if p.MaxConcurrentJobsPerWorkspace < 1 || p.MaxConcurrentJobsPerWorkspace > p.MaxConcurrentJobs {
		return fmt.Errorf("maxConcurrentJobsPerWorkspace must be between 1 and maxConcurrentJobs")
	}
	if p.AgingSeconds < 1 || p.AgingSeconds > 86400 {
		return fmt.Errorf("agingSeconds must be between 1 and 86400")
	}
	return nil
}

func ValidateJobSchedulingClass(name string) error {
	_, err := ResolveJobSchedulingPriority(name)
	return err
}

func ResolveJobSchedulingPriority(name string) (int, error) {
	switch name {
	case "", "normal":
		return 50, nil
	case "background":
		return 0, nil
	case "high":
		return 100, nil
	default:
		return 0, fmt.Errorf("unknown job scheduling class %q", name)
	}
}

func ParseJobSchedulerPolicy(value json.RawMessage) (JobSchedulerPolicy, error) {
	p := DefaultJobSchedulerPolicy()
	if len(bytes.TrimSpace(value)) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return p, fmt.Errorf("job scheduler policy must be an object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(value, &fields); err != nil {
		return p, fmt.Errorf("decode job scheduler policy: %w", err)
	}
	for field, raw := range fields {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return p, fmt.Errorf("job scheduler policy field %s cannot be null", field)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return p, fmt.Errorf("decode job scheduler policy: %w", err)
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return p, fmt.Errorf("job scheduler policy contains trailing JSON")
	}
	return p, p.Validate()
}

func NormalizeJobSchedulerPolicyValue(value json.RawMessage) (json.RawMessage, error) {
	p, err := ParseJobSchedulerPolicy(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(p)
}
