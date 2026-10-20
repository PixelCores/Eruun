package spec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservedComponentLabels(t *testing.T) {
	for _, key := range []string{LabelManagedBy, LabelAppID, LabelComponentID, LabelComponentName, LabelShareName, LabelShareStrategy} {
		require.True(t, IsReservedComponentLabel(key), key)
	}
	require.False(t, IsReservedComponentLabel("eruun.io/pvc-role"))
	require.False(t, IsReservedComponentLabel("team"))

	require.Nil(t, ReservedComponentLabelsIn(nil))
	require.Empty(t, ReservedComponentLabelsIn(map[string]string{"team": "a"}))
	require.Equal(t, []string{LabelManagedBy, LabelShareName, LabelShareStrategy}, ReservedComponentLabelsIn(map[string]string{
		LabelShareStrategy: "ignore",
		"team":             "a",
		LabelManagedBy:     "x",
		LabelShareName:     "demo",
	}))
}
