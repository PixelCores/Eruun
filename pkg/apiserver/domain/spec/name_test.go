package spec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidAPIName(t *testing.T) {
	for name, want := range map[string]bool{
		"app-name":             true,
		"demo.app":             true,
		"Invalid Name":         false,
		"UPPER":                false,
		"a":                    false,
		strings.Repeat("a", 9): false,
		strings.Repeat("a", 8): true,
	} {
		require.Equal(t, want, ValidAPIName(name, 8), name)
	}
}
