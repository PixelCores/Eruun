package naming

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceNamesUseApplicationNameAndAvoidDuplicatePrefix(t *testing.T) {
	appName := "m2605081521cctqpk"
	componentName := "m2605081521cctqpk-mysql-8"

	require.Equal(t, "m2605081521cctqpk-mysql-8", StoreServerName(componentName, appName))
	require.Equal(t, "m2605081521cctqpk-mysql-8", ServiceName(componentName, appName))
}

func TestResourceNamesAreBoundedWithStableHash(t *testing.T) {
	appName := "production-tenant-with-a-very-long-application-name"
	componentA := "api-worker-with-a-very-long-component-name-alpha"
	componentB := "api-worker-with-a-very-long-component-name-beta"

	deployA := WebServiceName(componentA, appName)
	deployB := WebServiceName(componentB, appName)

	require.LessOrEqual(t, len(deployA), maxResourceNameLength)
	require.LessOrEqual(t, len(ServiceName(componentA, appName)), maxResourceNameLength)
	require.LessOrEqual(t, len(PVCName("data-volume-with-a-very-long-name", appName)), maxResourceNameLength)
	require.Equal(t, deployA, WebServiceName(componentA, appName))
	require.NotEqual(t, deployA, deployB)
}

func TestControllerOwnedNamesReserveHashRoom(t *testing.T) {
	appName := "production-tenant-with-a-very-long-application-name"
	componentName := "database-cluster-with-a-very-long-component-name"

	require.LessOrEqual(t, len(StoreServerName(componentName, appName)), maxControllerOwnedResourceNameLength)
	require.LessOrEqual(t, len(CronJobName(componentName, appName)), maxControllerOwnedResourceNameLength)
	require.LessOrEqual(t, len(StoreServerName(componentName, appName)+"-"+strings.Repeat("a", hashSuffixLength)), maxResourceNameLength)
}

func TestBoundedLabelValue(t *testing.T) {
	value := "application-with-a-very-long-name-component-with-a-very-long-name"

	got := BoundedLabelValue(value)

	require.LessOrEqual(t, len(got), maxResourceNameLength)
	require.Equal(t, got, BoundedLabelValue(value))
}

func TestNormalizeLabelValue(t *testing.T) {
	require.Equal(t,
		"penalty-shootout-2026-m2606241344ccufxh-backend",
		NormalizeLabelValue("penalty shootout 2026-m2606241344ccufxh-backend"),
	)
	require.Equal(t, "", NormalizeLabelValue(""))
	require.Equal(t, "", NormalizeLabelValue("   "))

	value := "application with a very long display name component with a very long display name"
	got := NormalizeLabelValue(value)
	require.LessOrEqual(t, len(got), maxResourceNameLength)
	require.Equal(t, got, NormalizeLabelValue(value))
}

func TestNormalizeInvalidLabelValue(t *testing.T) {
	require.Equal(t, "v1.2.3", NormalizeInvalidLabelValue("v1.2.3"))
	require.Equal(t, "canary_A", NormalizeInvalidLabelValue("canary_A"))
	require.Equal(t, "MyValue", NormalizeInvalidLabelValue("MyValue"))
	require.Equal(t, "", NormalizeInvalidLabelValue(""))
	require.Equal(t, "penalty-shootout-2026", NormalizeInvalidLabelValue("penalty shootout 2026"))

	value := strings.Repeat("a", maxResourceNameLength+1)
	got := NormalizeInvalidLabelValue(value)
	require.LessOrEqual(t, len(got), maxResourceNameLength)
	require.Equal(t, got, NormalizeInvalidLabelValue(value))
}

func TestNormalizeLabelValuesPreservesKeys(t *testing.T) {
	labels := map[string]string{
		"frontPurchaserProductId": "Penalty Shootout 2026",
		"empty":                   "",
	}

	got := NormalizeLabelValues(labels)

	require.Equal(t, "penalty-shootout-2026", got["frontPurchaserProductId"])
	require.Equal(t, "", got["empty"])
	require.Equal(t, "Penalty Shootout 2026", labels["frontPurchaserProductId"])
}

func TestApplicationResourceKey(t *testing.T) {
	require.Equal(t, "game", ApplicationResourceKey("Game", "1.0.0", false))
	require.Equal(t, "mysql-8-0-41", ApplicationResourceKey("mysql", "8.0.41", true))
	require.Equal(t, "mysql", ApplicationResourceKey("mysql", "", true))
}
