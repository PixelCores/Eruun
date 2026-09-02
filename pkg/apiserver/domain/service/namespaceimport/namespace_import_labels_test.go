package namespaceimport

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestBuildImportLabels_UseAppIDForRBACManagedLabel(t *testing.T) {
	appID := "26022513312d88jw"
	stableKey := "stable-import-key"
	component := "backend-rbac"

	rbacLabels := buildImportLabels(&importResource{kindKey: importKindRoleBindings}, appID, stableKey, component, 0, false)
	assert.Equal(t, config.ManagedByEruun, rbacLabels[config.LabelManagedBy])
	assert.Equal(t, component, rbacLabels[config.LabelComponentName])
	assert.Equal(t, stableKey, rbacLabels[config.LabelImportAppKey])

	configMapLabels := buildImportLabels(&importResource{kindKey: importKindConfigMaps}, appID, stableKey, component, 0, false)
	assert.Equal(t, config.ManagedByEruun, configMapLabels[config.LabelManagedBy])
	assert.Equal(t, component, configMapLabels[config.LabelComponentName])
	assert.Equal(t, stableKey, configMapLabels[config.LabelImportAppKey])
}

func TestBuildImportLabels_SharedPlanUsesStandardShareLabels(t *testing.T) {
	appID := "generated-shared-app-id"
	component := "proxy-2601151316wv4dtu"

	labels := buildImportLabels(&importResource{
		kindKey: importKindDeployments,
		name:    component,
		labels:  map[string]string{},
	}, appID, "shared-default", component, 101, true)

	assert.Equal(t, appID, labels[config.LabelAppID])
	assert.Equal(t, component, labels[config.LabelComponentName])
	assert.Equal(t, "shared-default", labels[config.LabelImportAppKey])
	assert.Equal(t, component, labels[config.LabelShareName])
	assert.Equal(t, string(domainspec.ShareStrategyDefault), labels[config.LabelShareStrategy])
}

func TestBuildImportLabels_PreservesExistingShareLabels(t *testing.T) {
	labels := buildImportLabels(&importResource{
		kindKey: importKindConfigMaps,
		name:    "backend-config",
		labels: map[string]string{
			config.LabelShareName:     "shared-config",
			config.LabelShareStrategy: string(domainspec.ShareStrategyIgnore),
		},
	}, "app-id", "stable-app-key", "backend", 0, false)

	assert.Equal(t, "shared-config", labels[config.LabelShareName])
	assert.Equal(t, string(domainspec.ShareStrategyIgnore), labels[config.LabelShareStrategy])
	assert.Equal(t, "stable-app-key", labels[config.LabelImportAppKey])
}

func TestBuildImportLabels_BoundsLongComponentLabelValues(t *testing.T) {
	longName := strings.Repeat("backend-", 12)

	labels := buildImportLabels(&importResource{
		kindKey: importKindConfigMaps,
		name:    longName,
	}, "app-id", "stable-app-key", "", 0, false)

	componentLabel := labels[config.LabelComponentName]
	require.NotEmpty(t, componentLabel)
	assert.LessOrEqual(t, len(componentLabel), 63)
	assert.Regexp(t, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, componentLabel)
	assert.LessOrEqual(t, len(labels[config.LabelManagedBy]), 63)

	explicit := buildImportLabels(&importResource{
		kindKey: importKindConfigMaps,
		name:    "backend",
	}, "app-id", "stable-app-key", longName, 0, false)
	assert.LessOrEqual(t, len(explicit[config.LabelComponentName]), 63)
	assert.LessOrEqual(t, len(explicit[config.LabelManagedBy]), 63)
}

func TestBuildLabelPatch(t *testing.T) {
	labels := map[string]string{
		config.LabelManagedBy:      config.ManagedByEruun,
		"eruun.io/app-id":          "a1",
		"eruun.io/component-name":  "backend",
		"example.com/custom-label": "a1-backend",
	}

	t.Run("daemonset includes pod template labels", func(t *testing.T) {
		payload := decodePatchPayload(t, mustBuildPatch(t, importKindDaemonSets, labels, nil))
		assert.Equal(t, labels, getStringMap(t, payload, "metadata", "labels"))
		assert.Equal(t, labels, getStringMap(t, payload, "spec", "template", "metadata", "labels"))
	})

	t.Run("controller template labels keep selector keys", func(t *testing.T) {
		selector := map[string]string{
			config.LabelAppID:       "old-app",
			config.LabelComponentID: "12",
		}
		payload := decodePatchPayload(t, mustBuildPatch(t, importKindDeployments, labels, selector))
		metadataLabels := getStringMap(t, payload, "metadata", "labels")
		assert.Equal(t, "old-app", metadataLabels[config.LabelAppID])
		assert.NotContains(t, metadataLabels, config.LabelComponentID)
		assert.Equal(t, config.ManagedByEruun, metadataLabels[config.LabelManagedBy])
		templateLabels := getStringMap(t, payload, "spec", "template", "metadata", "labels")
		assert.Equal(t, "old-app", templateLabels[config.LabelAppID])
		assert.Equal(t, "12", templateLabels[config.LabelComponentID])
		assert.Equal(t, config.ManagedByEruun, templateLabels[config.LabelManagedBy])
	})

	t.Run("job patches metadata labels only", func(t *testing.T) {
		payload := decodePatchPayload(t, mustBuildPatch(t, importKindJobs, labels, nil))
		assert.Equal(t, labels, getStringMap(t, payload, "metadata", "labels"))
		_, hasSpec := payload["spec"]
		assert.False(t, hasSpec)
	})

	t.Run("cronjob includes nested jobTemplate labels", func(t *testing.T) {
		payload := decodePatchPayload(t, mustBuildPatch(t, importKindCronJobs, labels, nil))
		assert.Equal(t, labels, getStringMap(t, payload, "metadata", "labels"))
		assert.Equal(t, labels, getStringMap(t, payload, "spec", "jobTemplate", "spec", "template", "metadata", "labels"))
	})
}
