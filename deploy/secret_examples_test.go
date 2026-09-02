package deploy_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSecretExampleContainsOnlyExplicitPlaceholders(t *testing.T) {
	raw, err := os.ReadFile("eruun-secrets.example.yaml")
	require.NoError(t, err)

	content := string(raw)
	require.GreaterOrEqual(t, strings.Count(content, "__REPLACE_WITH_STRONG_PASSWORD__"), 3)
}
