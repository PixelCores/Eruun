package namespaceimport

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

func TestAssignResourcesToApps(t *testing.T) {
	namespace := "2601151316wv4dtu"
	resources := []*importResource{
		newDeploymentResource(t, "mahjongways2-26022513312d88jw-backend", namespace, map[string]string{"app": "mw-backend"}, "", []string{"backend-config"}, nil),
		newDeploymentResource(t, "mahjongways2-26022513312d88jw-frontend", namespace, map[string]string{"app": "mw-frontend"}, "", nil, nil),
		newDeploymentResource(t, "mahjongways2-2602280954weps7d-backend", namespace, map[string]string{"app": "mw2-backend"}, "", nil, nil),
		newDeploymentResource(t, "proxy-2601151316wv4dtu", namespace, map[string]string{"app": "proxy"}, "", nil, nil),
		newConfigMapResource(t, "backend-config", namespace),
		newServiceResource(t, "backend-svc", namespace, map[string]string{"app": "mw-backend"}),
	}

	grouped, appNames, _, warnings := assignResourcesToApps(namespace, resources)
	require.NotNil(t, grouped)
	assert.NotNil(t, warnings)

	sharedID := sharedAppIDForNamespace(namespace)
	require.Contains(t, grouped, "26022513312d88jw")
	require.Contains(t, grouped, "2602280954weps7d")
	require.Contains(t, grouped, sharedID)

	assert.Equal(t, "mahjongways2-26022513312d88jw", appNames["26022513312d88jw"])
	assert.Equal(t, "mahjongways2-2602280954weps7d", appNames["2602280954weps7d"])

	var cmAssignedApp string
	var svcAssignedApp string
	for _, res := range resources {
		switch res.name {
		case "backend-config":
			cmAssignedApp = res.appID
		case "backend-svc":
			svcAssignedApp = res.appID
		}
	}
	assert.Equal(t, "26022513312d88jw", cmAssignedApp)
	assert.Equal(t, "26022513312d88jw", svcAssignedApp)
}

func TestAssignResourcesToApps_BoundsInferredAppName(t *testing.T) {
	const (
		namespace = "2601151316wv4dtu"
		appID     = "26022513312d88jw"
	)
	longPrefix := strings.TrimSuffix(strings.Repeat("very-long-prefix-", 5), "-")
	resourceName := fmt.Sprintf("%s-%s-backend", longPrefix, appID)

	resources := []*importResource{
		newDeploymentResource(t, resourceName, namespace, map[string]string{"app": "backend"}, "", nil, nil),
	}

	_, appNames, _, warnings := assignResourcesToApps(namespace, resources)
	assert.Empty(t, warnings)
	require.Contains(t, appNames, appID)
	assert.LessOrEqual(t, len(appNames[appID]), datastore.PrimaryKeyMaxLength)
	assert.Equal(t, appNames[appID], utils.ToRFC1123Name(appNames[appID]))
}

func TestAssignResourcesToApps_AssignsSecretServiceAccountAndRBAC(t *testing.T) {
	namespace := "2601151316wv4dtu"
	deployName := "mahjongways2-26022513312d88jw-backend"
	appID := "26022513312d88jw"
	sharedID := sharedAppIDForNamespace(namespace)

	resources := []*importResource{
		newDeploymentResource(t, deployName, namespace, map[string]string{"app": "mw-backend"}, "backend-sa", nil, []string{"backend-secret"}),
		newServiceAccountResource(t, "backend-sa", namespace),
		newSecretResource(t, "backend-secret", namespace),

		newRoleResource(t, "backend-role", namespace),
		newRoleBindingResource(
			t,
			"backend-rb",
			namespace,
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "backend-role"},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa"}},
		),
		newRoleResource(t, "external-role", namespace),
		newRoleBindingResource(
			t,
			"external-rb",
			namespace,
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "external-role"},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: "other-ns"}},
		),

		newClusterRoleResource(t, "backend-cluster-role"),
		newClusterRoleBindingResource(
			t,
			"backend-crb",
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "backend-cluster-role"},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: namespace}},
		),
		newClusterRoleResource(t, "external-cluster-role"),
		newClusterRoleBindingResource(
			t,
			"external-crb",
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "external-cluster-role"},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: "other-ns"}},
		),
	}

	_, _, _, _ = assignResourcesToApps(namespace, resources)

	assert.Equal(t, appID, mustFindResource(t, resources, "backend-sa").appID)

	secretRes := mustFindResource(t, resources, "backend-secret")
	assert.Equal(t, appID, secretRes.appID)
	assert.Equal(t, deployName, secretRes.componentName)

	assert.Equal(t, appID, mustFindResource(t, resources, "backend-rb").appID)
	assert.Equal(t, sharedID, mustFindResource(t, resources, "backend-role").appID)
	assert.Equal(t, appID, mustFindResource(t, resources, "backend-crb").appID)
	assert.Equal(t, sharedID, mustFindResource(t, resources, "backend-cluster-role").appID)

	assert.Equal(t, sharedID, mustFindResource(t, resources, "external-rb").appID)
	assert.Equal(t, sharedID, mustFindResource(t, resources, "external-role").appID)
	assert.Equal(t, sharedID, mustFindResource(t, resources, "external-crb").appID)
	assert.Equal(t, sharedID, mustFindResource(t, resources, "external-cluster-role").appID)
}

func TestAssignResourcesToApps_KeepsSharedRolesForMultipleOwners(t *testing.T) {
	namespace := "2601151316wv4dtu"
	sharedID := sharedAppIDForNamespace(namespace)
	resources := []*importResource{
		newDeploymentResource(t, "mahjongways2-26022513312d88jw-backend", namespace, map[string]string{"app": "app-a"}, "app-a-sa", nil, nil),
		newDeploymentResource(t, "mahjongways2-2602280954weps7d-backend", namespace, map[string]string{"app": "app-b"}, "app-b-sa", nil, nil),
		newServiceAccountResource(t, "app-a-sa", namespace),
		newServiceAccountResource(t, "app-b-sa", namespace),
		newRoleResource(t, "shared-role", namespace),
		newRoleBindingResource(
			t,
			"app-a-binding",
			namespace,
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "shared-role"},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "app-a-sa"}},
		),
		newRoleBindingResource(
			t,
			"app-b-binding",
			namespace,
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "shared-role"},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "app-b-sa"}},
		),
	}

	_, _, _, _ = assignResourcesToApps(namespace, resources)

	assert.Equal(t, "26022513312d88jw", mustFindResource(t, resources, "app-a-binding").appID)
	assert.Equal(t, "2602280954weps7d", mustFindResource(t, resources, "app-b-binding").appID)
	assert.Equal(t, sharedID, mustFindResource(t, resources, "shared-role").appID)
}

func TestAssignResourcesToApps_AssignsStatefulSetGeneratedPVC(t *testing.T) {
	namespace := "2601151316wv4dtu"
	stsName := "mahjongways2-26022513312d88jw-store"
	pvcName := "data-" + stsName + "-0"

	resources := []*importResource{
		newStatefulSetResource(t, stsName, namespace, map[string]string{"app": "store"}, "", []string{"data"}),
		newPVCResource(t, pvcName, namespace),
	}

	grouped, _, _, _ := assignResourcesToApps(namespace, resources)
	require.NotNil(t, grouped)

	pvc := mustFindResource(t, resources, pvcName)
	assert.Equal(t, "26022513312d88jw", pvc.appID)
	assert.Equal(t, stsName, pvc.componentName)
}

func TestAssignResourcesToApps_DoesNotClaimStandalonePVCByTemplateName(t *testing.T) {
	namespace := "2601151316wv4dtu"
	stsName := "mahjongways2-26022513312d88jw-store"
	standalonePVC := "data"

	resources := []*importResource{
		newStatefulSetResource(t, stsName, namespace, map[string]string{"app": "store"}, "", []string{standalonePVC}),
		newPVCResource(t, standalonePVC, namespace),
	}

	grouped, _, _, _ := assignResourcesToApps(namespace, resources)
	require.NotNil(t, grouped)

	pvc := mustFindResource(t, resources, standalonePVC)
	assert.Equal(t, sharedAppIDForNamespace(namespace), pvc.appID)
	assert.Empty(t, pvc.componentName)
}

func TestAssignResourcesToApps_UsesImportAppKeyAsAuthoritativeID(t *testing.T) {
	namespace := "2601151316wv4dtu"
	name := "mahjongways2-26022513312d88jw-backend"
	res := newDeploymentResource(t, name, namespace, map[string]string{"app": "backend"}, "", nil, nil)
	res.labels = map[string]string{
		config.LabelImportAppKey: "26022513312d88jw",
		config.LabelAppID:        "newgeneratedappid123",
	}

	_, _, _, warnings := assignResourcesToApps(namespace, []*importResource{res})

	assert.Equal(t, "26022513312d88jw", res.appID)
	assert.True(t, res.explicitAppID)
	assert.NotEmpty(t, warnings)
}

func TestAssignResourcesToApps_ClusterScopedIgnoresForeignImportAppKey(t *testing.T) {
	namespace := "2601151316wv4dtu"
	appID := "26022513312d88jw"
	foreignSharedID := sharedAppIDForNamespace("foreign-namespace")
	clusterRoleName := "backend-cluster-role"
	sharedID := sharedAppIDForNamespace(namespace)

	resources := []*importResource{
		newDeploymentResource(t, "mahjongways2-"+appID+"-backend", namespace, map[string]string{"app": "backend"}, "backend-sa", nil, nil),
		newServiceAccountResource(t, "backend-sa", namespace),
		newClusterRoleResource(t, clusterRoleName),
		newClusterRoleBindingResource(
			t,
			"backend-crb",
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: clusterRoleName},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "backend-sa", Namespace: namespace}},
		),
	}

	clusterRole := mustFindResource(t, resources, clusterRoleName)
	if clusterRole.labels == nil {
		clusterRole.labels = map[string]string{}
	}
	clusterRole.labels[config.LabelImportAppKey] = foreignSharedID
	clusterRole.labels[config.LabelAppID] = "foreign-app-id"

	clusterBinding := mustFindResource(t, resources, "backend-crb")
	if clusterBinding.labels == nil {
		clusterBinding.labels = map[string]string{}
	}
	clusterBinding.labels[config.LabelImportAppKey] = foreignSharedID
	clusterBinding.labels[config.LabelAppID] = "foreign-app-id"

	grouped, _, _, warnings := assignResourcesToApps(namespace, resources)

	assert.Equal(t, sharedID, clusterRole.appID)
	assert.Equal(t, appID, clusterBinding.appID)
	assert.False(t, clusterBinding.explicitAppID)
	assert.NotContains(t, grouped, foreignSharedID)
	assert.NotEmpty(t, warnings)
	assert.Contains(t, strings.Join(warnings, " "), "ignored for cluster-scoped resource")
}
