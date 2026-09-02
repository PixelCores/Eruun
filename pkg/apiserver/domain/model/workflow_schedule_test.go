package model

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	gormschema "gorm.io/gorm/schema"
)

func TestWorkflowSchedule_EnabledNextRunCompositeIndex(t *testing.T) {
	parsed, err := gormschema.Parse(&WorkflowSchedule{}, &sync.Map{}, gormschema.NamingStrategy{})
	require.NoError(t, err)

	index := parsed.LookIndex("idx_workflow_schedule_enabled_next_run")
	require.NotNil(t, index, "expected workflow schedule dispatch index")
	require.Empty(t, index.Class, "expected non-unique index")
	require.Len(t, index.Fields, 2)
	require.Equal(t, "enabled", index.Fields[0].DBName)
	require.Equal(t, "next_run", index.Fields[1].DBName)
}

func TestWorkflowQueue_StatusExecuteAtCompositeIndex(t *testing.T) {
	parsed, err := gormschema.Parse(&WorkflowQueue{}, &sync.Map{}, gormschema.NamingStrategy{})
	require.NoError(t, err)

	index := parsed.LookIndex("idx_workflow_queue_dispatch")
	require.NotNil(t, index, "expected workflow queue dispatch index")
	require.Empty(t, index.Class, "expected non-unique index")
	require.Len(t, index.Fields, 2)
	require.Equal(t, "status", index.Fields[0].DBName)
	require.Equal(t, "execute_at", index.Fields[1].DBName)
}
