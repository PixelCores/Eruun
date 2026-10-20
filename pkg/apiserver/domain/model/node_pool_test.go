package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodePoolEntityContract(t *testing.T) {
	p := &NodePool{Name: "example"}
	require.Equal(t, "eruun_node_pools", p.TableName())
	require.Equal(t, "example", p.PrimaryKey())
	require.Equal(t, map[string]interface{}{"name": "example"}, p.Index())
	require.Empty(t, (&NodePool{}).Index())
	require.True(t, builtinModelExists(t, p.TableName()))
}
