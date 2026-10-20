package model

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRBACProfileEntityContract(t *testing.T) {
	p := &RBACProfile{Name: "example"}
	require.Equal(t, "eruun_rbac_profiles", p.TableName())
	require.Equal(t, "example", p.PrimaryKey())
	require.Equal(t, map[string]interface{}{"name": "example"}, p.Index())
	require.Empty(t, (&RBACProfile{}).Index())
	require.True(t, builtinModelExists(t, p.TableName()))
}
