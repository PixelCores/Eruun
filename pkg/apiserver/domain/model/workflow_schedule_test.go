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

	indexes := parsed.ParseIndexes()
	index, ok := indexes["idx_workflow_schedule_enabled_next_run"]
	require.True(t, ok, "expected workflow schedule dispatch index")
	require.Empty(t, index.Class, "expected non-unique index")
	require.Len(t, index.Fields, 2)
	require.Equal(t, "enabled", index.Fields[0].DBName)
	require.Equal(t, "next_run", index.Fields[1].DBName)
}
