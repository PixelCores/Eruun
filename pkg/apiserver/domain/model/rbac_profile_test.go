package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRBACProfile_EntityContract(t *testing.T) {
	profile := &RBACProfile{
		ID:   "rbac-1",
		Name: "admin",
	}

	require.Equal(t, "eruun_rbac_profiles", profile.TableName())
	require.Equal(t, "rbac_profile", profile.ShortTableName())
	require.Equal(t, "rbac-1", profile.PrimaryKey())

	index := profile.Index()
	require.Equal(t, "rbac-1", index["id"])
	require.Equal(t, "admin", index["name"])

	require.False(t, builtinModelExists(t, profile.TableName()), "expected model to be absent after migration to system setting")
}
