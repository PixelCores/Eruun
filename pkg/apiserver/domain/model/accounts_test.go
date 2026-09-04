package model

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	gormschema "gorm.io/gorm/schema"
)

func TestSession_IdleCleanupIndex(t *testing.T) {
	parsed, err := gormschema.Parse(&Session{}, &sync.Map{}, gormschema.NamingStrategy{})
	require.NoError(t, err)

	index := parsed.LookIndex("idx_session_idle_cleanup")
	require.NotNil(t, index, "expected session idle cleanup index")
	require.Empty(t, index.Class, "expected non-unique index")
	require.Len(t, index.Fields, 1)
	require.Equal(t, "access_expires_at", index.Fields[0].DBName)
}
