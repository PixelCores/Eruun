package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeSelectorProfile_EntityContract(t *testing.T) {
	profile := &NodeSelectorProfile{
		ID:   "node-1",
		Name: "gpu-nodes",
	}

	require.Equal(t, "eruun_node_selector_profiles", profile.TableName())
	require.Equal(t, "node_selector_profile", profile.ShortTableName())
	require.Equal(t, "node-1", profile.PrimaryKey())

	index := profile.Index()
	require.Equal(t, "node-1", index["id"])
	require.Equal(t, "gpu-nodes", index["name"])

	require.False(t, builtinModelExists(t, profile.TableName()), "expected model to be absent after migration to system setting")
}
