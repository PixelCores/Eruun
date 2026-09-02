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
