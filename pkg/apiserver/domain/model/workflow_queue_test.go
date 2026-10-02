package model

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	gormschema "gorm.io/gorm/schema"
)

func TestWorkflowQueue_IdempotencyKeyUniqueIndex(t *testing.T) {
	parsed, err := gormschema.Parse(&WorkflowQueue{}, &sync.Map{}, gormschema.NamingStrategy{})
	require.NoError(t, err)

	index := parsed.LookIndex("idx_workflow_queue_idempotency_key")
	require.NotNil(t, index, "expected workflow queue idempotency index")
	require.Equal(t, "UNIQUE", index.Class)
	require.Len(t, index.Fields, 1)
	require.Equal(t, "idempotency_key", index.Fields[0].DBName)
}

func TestWorkflowQueue_ReaperIndex(t *testing.T) {
	parsed, err := gormschema.Parse(&WorkflowQueue{}, &sync.Map{}, gormschema.NamingStrategy{})
	require.NoError(t, err)

	index := parsed.LookIndex("idx_workflow_queue_reaper")
	require.NotNil(t, index, "expected workflow queue reaper index")
	require.Equal(t, []string{"status", "lease_expires_at"}, []string{
		index.Fields[0].DBName,
		index.Fields[1].DBName,
	})
}

func TestJobInfo_DelayRecoveryIndex(t *testing.T) {
	parsed, err := gormschema.Parse(&JobInfo{}, &sync.Map{}, gormschema.NamingStrategy{})
	require.NoError(t, err)

	index := parsed.LookIndex("idx_job_delay_pending")
	require.NotNil(t, index, "expected delayed job recovery index")
	require.Equal(t, []string{"status", "delay_state", "delay_execute_at"}, []string{
		index.Fields[0].DBName,
		index.Fields[1].DBName,
		index.Fields[2].DBName,
	})
}

func TestWorkflowQueue_RunTokenIsNotSerialized(t *testing.T) {
	payload, err := json.Marshal(WorkflowQueue{
		TaskID:        "task-1",
		RunGeneration: 2,
		RunToken:      "secret-fencing-token",
	})
	require.NoError(t, err)
	require.False(t, strings.Contains(string(payload), "secret-fencing-token"))
	require.False(t, strings.Contains(string(payload), "runToken"))
}

func TestJobTask_RunTokenIsNotSerialized(t *testing.T) {
	payload, err := json.Marshal(JobTask{
		TaskID:             "task-1",
		RunGeneration:      2,
		RunToken:           "secret-fencing-token",
		OwnerRunGeneration: 3,
		WorkerID:           "secret-worker-id",
	})
	require.NoError(t, err)
	require.False(t, strings.Contains(string(payload), "secret-fencing-token"))
	require.False(t, strings.Contains(string(payload), "RunToken"))
	require.False(t, strings.Contains(string(payload), "OwnerRunGeneration"))
	require.False(t, strings.Contains(string(payload), "secret-worker-id"))
}

func TestNormalizeVersionUpdateCleanupResolutionTaskIDs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		values []string
		want   []string
		err    string
	}{
		{name: "absent references"},
		{name: "empty references", values: []string{}},
		{name: "trim and sort", values: []string{" task-b\t", "task-a "}, want: []string{"task-a", "task-b"}},
		{name: "empty entry", values: []string{""}, err: "has an empty resolvesTaskIDs entry"},
		{name: "whitespace entry", values: []string{" \t"}, err: "has an empty resolvesTaskIDs entry"},
		{name: "self reference", values: []string{" resolver "}, err: "cannot resolve itself"},
		{name: "duplicate after trim", values: []string{"task-a", " task-a "}, err: "more than once"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := append([]string{}, tt.values...)
			result, err := NormalizeVersionUpdateCleanupResolutionTaskIDs(" resolver ", tt.values)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, result)
			require.Equal(t, original, append([]string{}, tt.values...))
		})
	}
}

func TestValidateVersionUpdateCleanupResolutionGraph(t *testing.T) {
	for _, tt := range []struct {
		name       string
		references map[string][]string
		err        string
	}{
		{name: "no tasks"},
		{name: "independent tasks", references: map[string][]string{"first": nil, "second": nil}},
		{
			name: "chain and shared target are valid references",
			references: map[string][]string{
				"first": nil, "retry": {"first"}, "resolver": {"first", "retry"},
			},
		},
		{
			name: "unknown task", references: map[string][]string{"resolver": {"missing"}},
			err: "task resolver resolves unknown StatefulSet cleanup task missing",
		},
		{
			name: "cycle", references: map[string][]string{"first": {"second"}, "second": {"first"}},
			err: "resolution graph contains a cycle",
		},
		{
			name: "cycle reached through another task",
			references: map[string][]string{
				"first": {"second"}, "second": {"third"}, "third": {"second"},
			},
			err: "resolution graph contains a cycle",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateVersionUpdateCleanupResolutionGraph(tt.references)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
		})
	}
}
