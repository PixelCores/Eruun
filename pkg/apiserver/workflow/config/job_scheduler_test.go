package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJobSchedulerPolicy(t *testing.T) {
	p, err := ParseJobSchedulerPolicy(json.RawMessage(`{}`))
	require.NoError(t, err)
	require.Equal(t, DefaultJobSchedulerPolicy(), p)
	p, err = ParseJobSchedulerPolicy(json.RawMessage(`{"strategy":"fifo","maxConcurrentJobs":8,"maxConcurrentJobsPerWorkspace":3,"agingSeconds":2}`))
	require.NoError(t, err)
	require.Equal(t, JobSchedulerFIFO, p.Strategy)
	require.Equal(t, 8, p.MaxConcurrentJobs)
	for _, value := range []string{
		`null`, `[]`, `{"strategy":"random"}`, `{"maxConcurrentJobs":0}`,
		`{"maxConcurrentJobs":10001}`, `{"maxConcurrentJobsPerWorkspace":101}`,
		`{"agingSeconds":0}`, `{"agingSeconds":86401}`, `{"capacity":1}`, `{} {}`,
		`{"strategy":null}`, `{"agingSeconds":null}`,
	} {
		t.Run(value, func(t *testing.T) {
			_, err := ParseJobSchedulerPolicy(json.RawMessage(value))
			require.Error(t, err)
		})
	}
}

func TestJobSchedulerClasses(t *testing.T) {
	for class, want := range map[string]int{"": 50, "normal": 50, "background": 0, "high": 100} {
		got, err := ResolveJobSchedulingPriority(class)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	for _, class := range []string{"HIGH", "urgent", " normal", "100"} {
		require.Error(t, ValidateJobSchedulingClass(class))
	}
}
