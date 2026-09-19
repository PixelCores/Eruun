package resourceimport

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeImportMode(t *testing.T) {
	mode, err := normalizeImportMode("")
	require.NoError(t, err)
	assert.Equal(t, importModeDryRun, mode)

	mode, err = normalizeImportMode("APPLY")
	require.NoError(t, err)
	assert.Equal(t, importModeApply, mode)

	_, err = normalizeImportMode("invalid")
	require.Error(t, err)
}

func TestNormalizeImportKinds(t *testing.T) {
	kinds, warnings, err := normalizeImportKinds(nil)
	require.NoError(t, err)
	assert.Len(t, kinds, len(defaultImportKinds))
	assert.NotContains(t, kinds, importKindClusterRoles)
	assert.NotContains(t, kinds, importKindClusterRoleBindings)
	assert.Empty(t, warnings)

	kinds, warnings, err = normalizeImportKinds([]string{
		"deploy", "statefulset", "svc", "ds", "job", "cj",
		"secret", "sa", "rolebinding", "clusterrole", "clusterrolebindings", "deployments",
	})
	require.NoError(t, err)
	assert.Contains(t, kinds, importKindDeployments)
	assert.Contains(t, kinds, importKindStatefulSets)
	assert.Contains(t, kinds, importKindServices)
	assert.Contains(t, kinds, importKindDaemonSets)
	assert.Contains(t, kinds, importKindJobs)
	assert.Contains(t, kinds, importKindCronJobs)
	assert.Contains(t, kinds, importKindSecrets)
	assert.Contains(t, kinds, importKindServiceAccounts)
	assert.Contains(t, kinds, importKindRoleBindings)
	assert.Contains(t, kinds, importKindClusterRoles)
	assert.Contains(t, kinds, importKindClusterRoleBindings)
	assert.NotEmpty(t, warnings)

	_, _, err = normalizeImportKinds([]string{"bad-kind"})
	require.Error(t, err)
}

func TestParseStrictResourceName(t *testing.T) {
	prefix, appID, component, ok := parseStrictResourceName("mahjongways2-26022513312d88jw-backend")
	require.True(t, ok)
	assert.Equal(t, "mahjongways2", prefix)
	assert.Equal(t, "26022513312d88jw", appID)
	assert.Equal(t, "backend", component)

	prefix, appID, component, ok = parseStrictResourceName("mahjongways2-26022513312d88jw-api-server")
	require.True(t, ok)
	assert.Equal(t, "mahjongways2", prefix)
	assert.Equal(t, "26022513312d88jw", appID)
	assert.Equal(t, "api-server", component)

	_, _, _, ok = parseStrictResourceName("redis-master-svc")
	assert.False(t, ok)

	_, _, _, ok = parseStrictResourceName("gateway-abcdefghijklmnop-api")
	assert.False(t, ok)

	_, _, _, ok = parseStrictResourceName("proxy-2601151316wv4dtu")
	assert.False(t, ok)
}

func TestLooksLikeGeneratedAppID(t *testing.T) {
	assert.True(t, looksLikeGeneratedAppID("26022513312d88jw"))
	assert.True(t, looksLikeGeneratedAppID("a1b2c3d4e5f6g7h8i9j0k1l2"))

	assert.False(t, looksLikeGeneratedAppID("master"))
	assert.False(t, looksLikeGeneratedAppID("abcdefghijklmnop"))
	assert.False(t, looksLikeGeneratedAppID("1234567890123456"))
	assert.False(t, looksLikeGeneratedAppID("abc123"))
	assert.False(t, looksLikeGeneratedAppID("abc123-foo"))
}

func TestSharedAppIDForNamespace_BoundedAndStable(t *testing.T) {
	longNamespace := strings.Repeat("verylongnamespace", 6)
	sharedID := sharedAppIDForNamespace(longNamespace)
	require.NotEmpty(t, sharedID)
	assert.LessOrEqual(t, len(sharedID), 63)
	assert.Regexp(t, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, sharedID)
	assert.Equal(t, sharedID, sharedAppIDForNamespace(longNamespace))

	otherNamespace := longNamespace + "-other"
	assert.NotEqual(t, sharedID, sharedAppIDForNamespace(otherNamespace))
}

func TestSupportsNameBasedAppInference(t *testing.T) {
	assert.True(t, supportsNameBasedAppInference(importKindDeployments))
	assert.True(t, supportsNameBasedAppInference(importKindSecrets))
	assert.True(t, supportsNameBasedAppInference(importKindIngresses))

	assert.False(t, supportsNameBasedAppInference(importKindServiceAccounts))
	assert.False(t, supportsNameBasedAppInference(importKindRoles))
	assert.False(t, supportsNameBasedAppInference(importKindRoleBindings))
	assert.False(t, supportsNameBasedAppInference(importKindClusterRoles))
	assert.False(t, supportsNameBasedAppInference(importKindClusterRoleBindings))
}
