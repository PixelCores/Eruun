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
		`{"maxEvaluationTimeoutSeconds":null}`, `{"maxEvaluationTimeoutSeconds":59}`,
		`{"maxEvaluationTimeoutSeconds":1209601}`,
		`{"resourceCreationQPS":0}`, `{"resourceCreationQPS":1001}`,
		`{"resourceCreationBurst":0}`, `{"resourceCreationBurst":1001}`,
		`{"maxStartingSandboxes":0}`, `{"maxStartingSandboxes":10001}`,
	} {
		t.Run(value, func(t *testing.T) {
			_, err := ParseJobSchedulerPolicy(json.RawMessage(value))
			require.Error(t, err)
		})
	}
}

func TestJobSchedulerOnlineEvaluationTimeout(t *testing.T) {
	for _, value := range []string{`{"maxEvaluationTimeoutSeconds":60}`, `{"maxEvaluationTimeoutSeconds":1209600}`} {
		_, err := ParseJobSchedulerPolicy(json.RawMessage(value))
		require.NoError(t, err)
	}
	// Existing persisted policy objects gain the supported default when read;
	// no destructive rewrite of the saved admission limits is required.
	p, err := ParseJobSchedulerPolicy(json.RawMessage(`{"maxConcurrentJobs":25,"maxConcurrentJobsPerWorkspace":5}`))
	require.NoError(t, err)
	require.EqualValues(t, MaxEvaluationTimeoutSeconds, p.MaxEvaluationTimeoutSeconds)
	require.Equal(t, 25, p.MaxConcurrentJobs)
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

func TestWorkflowLeaseReaperBatchBounds(t *testing.T) {
	for _, size := range []int{0, 1, 10000, 10001} {
		cfg := DefaultRuntimeConfig()
		cfg.LeaseReaperBatchSize = size
		if size >= 1 && size <= 10000 {
			require.Empty(t, cfg.Validate())
		} else {
			require.NotEmpty(t, cfg.Validate())
		}
	}
}
