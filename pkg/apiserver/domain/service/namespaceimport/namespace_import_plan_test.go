package namespaceimport

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestDedupeImportComponents_AppendsStableIndexedSuffixForDuplicates(t *testing.T) {
	components := []apisv1.CreateComponentRequest{
		{Name: "backend"},
		{Name: "backend"},
		{Name: "backend"},
	}

	deduped := dedupeImportComponents(components)
	dedupedAgain := dedupeImportComponents(components)
	require.Len(t, deduped, 3)
	require.Len(t, dedupedAgain, 3)
	assert.Equal(t, "backend", deduped[0].Name)
	assert.Equal(t, "backend-2", deduped[1].Name)
	assert.Equal(t, "backend-3", deduped[2].Name)
	assert.Equal(t, deduped, dedupedAgain)

	nameSet := make(map[string]struct{}, len(deduped))
	for _, comp := range deduped {
		assert.NotEmpty(t, comp.Name)
		assert.LessOrEqual(t, len(comp.Name), 63)
		assert.Regexp(t, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, comp.Name)
		nameSet[comp.Name] = struct{}{}
	}
	assert.Len(t, nameSet, len(deduped))
}

func TestBuildImportPlans_InjectsSharedComponentsIntoEveryApp(t *testing.T) {
	namespace := "2601151316wv4dtu"
	sharedName := "proxy-2601151316wv4dtu"
	resources := []*importResource{
		newDeploymentResource(t, "mahjongways2-26022513312d88jw-backend", namespace, map[string]string{"app": "app-a"}, "", nil, nil),
		newDeploymentResource(t, "mahjongways2-2602280954weps7d-backend", namespace, map[string]string{"app": "app-b"}, "", nil, nil),
		newDeploymentResource(t, sharedName, namespace, map[string]string{"app": "proxy"}, "", nil, nil),
	}

	grouped, appNames, appAliases, _ := assignResourcesToApps(namespace, resources)
	plans := (&namespaceImportServiceImpl{}).buildImportPlans(grouped, appNames, appAliases, sharedAppIDForNamespace(namespace))

	appAPlan := mustFindPlan(t, plans, "26022513312d88jw")
	appBPlan := mustFindPlan(t, plans, "2602280954weps7d")
	sharedPlan := mustFindPlan(t, plans, sharedAppIDForNamespace(namespace))

	assert.Contains(t, appAPlan.componentNames, sharedName)
	assert.Contains(t, appBPlan.componentNames, sharedName)
	assert.Contains(t, sharedPlan.componentNames, sharedName)

	sharedCompA := mustFindPlanComponent(t, appAPlan, sharedName)
	sharedCompB := mustFindPlanComponent(t, appBPlan, sharedName)
	sharedCompRoot := mustFindPlanComponent(t, sharedPlan, sharedName)
	require.NotNil(t, sharedCompA.Traits.Share)
	require.NotNil(t, sharedCompB.Traits.Share)
	require.NotNil(t, sharedCompRoot.Traits.Share)
	assert.Equal(t, string(domainspec.ShareStrategyDefault), sharedCompA.Traits.Share.Strategy)
	assert.Equal(t, string(domainspec.ShareStrategyDefault), sharedCompB.Traits.Share.Strategy)
	assert.Equal(t, string(domainspec.ShareStrategyDefault), sharedCompRoot.Traits.Share.Strategy)
}

func TestBuildImportPlans_InjectsSharedRBACIntoEveryApp(t *testing.T) {
	namespace := "2601151316wv4dtu"
	sharedName := "proxy-2601151316wv4dtu"
	sharedSA := "pod-labeler-sa"
	roleName := "pod-labeler-role"

	resources := []*importResource{
		newDeploymentResource(t, "mahjongways2-26022513312d88jw-backend", namespace, map[string]string{"app": "app-a"}, "", nil, nil),

		newDeploymentResource(t, sharedName, namespace, map[string]string{"app": "proxy"}, sharedSA, nil, nil),
		newServiceAccountResource(t, sharedSA, namespace),
		newRoleResourceWithRules(t, roleName, namespace, []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list"},
			},
		}),
		newRoleBindingResource(
			t,
			"pod-labeler-rolebinding",
			namespace,
			rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: roleName},
			[]rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: sharedSA}},
		),
	}

	grouped, appNames, appAliases, _ := assignResourcesToApps(namespace, resources)
	plans := (&namespaceImportServiceImpl{}).buildImportPlans(grouped, appNames, appAliases, sharedAppIDForNamespace(namespace))
	appPlan := mustFindPlan(t, plans, "26022513312d88jw")
	sharedComp := mustFindPlanComponent(t, appPlan, sharedName)

	require.NotEmpty(t, sharedComp.Traits.RBAC)
	found := false
	for _, policy := range sharedComp.Traits.RBAC {
		if policy.RoleName == roleName {
			found = true
			assert.Equal(t, roleName, policy.RoleLabels[config.LabelShareName])
			assert.Equal(t, string(domainspec.ShareStrategyDefault), policy.RoleLabels[config.LabelShareStrategy])
		}
	}
	assert.True(t, found, "expected shared component to carry role policy %s", roleName)
}

func TestBuildImportPlans_RenamesDuplicateComponentNamesInsteadOfDropping(t *testing.T) {
	namespace := "2601151316wv4dtu"
	resourceName := "mahjongways2-26022513312d88jw-backend"
	resources := []*importResource{
		newDeploymentResource(t, resourceName, namespace, map[string]string{"app": "mw-backend"}, "", nil, nil),
		newConfigMapResource(t, resourceName, namespace),
	}

	grouped, appNames, appAliases, _ := assignResourcesToApps(namespace, resources)
	plans := (&namespaceImportServiceImpl{}).buildImportPlans(grouped, appNames, appAliases, sharedAppIDForNamespace(namespace))
	plan := mustFindPlan(t, plans, "26022513312d88jw")

	require.Len(t, plan.components, 2)
	names := []string{plan.components[0].Name, plan.components[1].Name}
	assert.NotEqual(t, names[0], names[1])
	assert.Contains(t, names, resourceName)
	require.NotNil(t, plan.resourceComponentByKey)

	mapped := make(map[string]string)
	for _, res := range plan.resources {
		if res == nil || res.name != resourceName {
			continue
		}
		key := resourceResultKey(res)
		mapped[key] = plan.resourceComponentByKey[key]
	}
	assert.Len(t, mapped, 2)
	seen := make(map[string]struct{}, len(mapped))
	for _, name := range mapped {
		assert.Contains(t, names, name)
		seen[name] = struct{}{}
	}
	assert.Len(t, seen, 2)
}

func TestBuildImportPlans_PreservesStandalonePVCClaimName(t *testing.T) {
	namespace := "2601151316wv4dtu"
	appID := "26022513312d88jw"
	resourceName := "mahjongways2-" + appID + "-backend"
	claimName := "legacy-data"
	resources := []*importResource{
		newDeploymentResourceWithPVC(t, resourceName, namespace, map[string]string{"app": "backend"}, "data", claimName),
		newPVCResource(t, claimName, namespace),
	}

	grouped, appNames, appAliases, _ := assignResourcesToApps(namespace, resources)
	plans := (&namespaceImportServiceImpl{}).buildImportPlans(grouped, appNames, appAliases, sharedAppIDForNamespace(namespace))
	plan := mustFindPlan(t, plans, appID)
	component := mustFindPlanComponent(t, plan, resourceName)

	require.Len(t, component.Traits.Storage, 1)
	assert.Equal(t, "data", component.Traits.Storage[0].Name)
	assert.Equal(t, claimName, component.Traits.Storage[0].ClaimName)
	assert.False(t, component.Traits.Storage[0].TmpCreate)
}

func TestBuildImportPlans_SkipsStatefulSetWithVolumeClaimTemplates(t *testing.T) {
	namespace := "2601151316wv4dtu"
	appID := "26022513312d88jw"
	stsName := "mahjongways2-" + appID + "-store"
	resources := []*importResource{
		newStatefulSetResource(t, stsName, namespace, map[string]string{"app": "store"}, "", []string{"data"}),
		newPVCResource(t, "data-"+stsName+"-0", namespace),
	}

	grouped, appNames, appAliases, _ := assignResourcesToApps(namespace, resources)
	plans := (&namespaceImportServiceImpl{}).buildImportPlans(grouped, appNames, appAliases, sharedAppIDForNamespace(namespace))
	plan := mustFindPlan(t, plans, appID)

	require.Error(t, plan.err)
	assert.Contains(t, plan.err.Error(), "no convertible components")
	require.NotEmpty(t, plan.warnings)
	assert.Contains(t, strings.Join(plan.warnings, "\n"), "volumeClaimTemplates")
}

func TestImportComponentResourceKeys_AllowsStandalonePVCOverlap(t *testing.T) {
	current := []apisv1.CreateComponentRequest{{
		Name:          "worker",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Traits: apisv1.Traits{
			Storage: []domainspec.StorageTraitSpec{{Name: "cache", Type: config.StorageTypePersistent, MountPath: "/cache", ClaimName: "shared-cache"}},
		},
	}}
	existing := apisv1.CreateComponentRequest{
		Name:          "api",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Traits: apisv1.Traits{
			Storage: []domainspec.StorageTraitSpec{{Name: "cache", Type: config.StorageTypePersistent, MountPath: "/cache", ClaimName: "shared-cache"}},
		},
	}

	keys := importComponentResourceKeys(current, "game", config.DefaultNamespace)

	assert.False(t, importComponentConflictsWithResourceKeys(existing, "game", config.DefaultNamespace, keys))
	assert.Contains(t, inferredPrimaryImportKindsForComponent(existing), importKindPersistentVolumeClaims)
}

func TestBuildResourceComponentNameMapping_UsesConvertOrderQueues(t *testing.T) {
	namespace := "default"
	configRes := &importResource{
		kindKey:       importKindConfigMaps,
		kind:          "ConfigMap",
		namespace:     namespace,
		name:          "backend",
		componentName: "backend",
	}
	deployRes := &importResource{
		kindKey:       importKindDeployments,
		kind:          "Deployment",
		namespace:     namespace,
		name:          "backend",
		componentName: "backend",
	}

	source := []apisv1.CreateComponentRequest{
		{Name: "backend"},
		{Name: "backend"},
	}
	deduped := []apisv1.CreateComponentRequest{
		{Name: "backend"},
		{Name: "backend-x9k2m"},
	}

	mapping := buildResourceComponentNameMapping([]*importResource{deployRes, configRes}, source, deduped, source)
	require.NotNil(t, mapping)
	assert.Equal(t, "backend", mapping[resourceResultKey(configRes)])
	assert.Equal(t, "backend-x9k2m", mapping[resourceResultKey(deployRes)])
}

func TestBuildResourceComponentNameMapping_UsesResourceNameWhenComponentNameIsStale(t *testing.T) {
	namespace := "default"
	configRes := &importResource{
		kindKey:       importKindConfigMaps,
		kind:          "ConfigMap",
		namespace:     namespace,
		name:          "backend",
		componentName: "backend",
	}
	deployRes := &importResource{
		kindKey:       importKindDeployments,
		kind:          "Deployment",
		namespace:     namespace,
		name:          "backend",
		componentName: "backend-old",
	}

	source := []apisv1.CreateComponentRequest{
		{Name: "backend"},
		{Name: "backend"},
	}
	deduped := []apisv1.CreateComponentRequest{
		{Name: "backend"},
		{Name: "backend-x9k2m"},
	}

	mapping := buildResourceComponentNameMapping([]*importResource{deployRes, configRes}, source, deduped, source)
	require.NotNil(t, mapping)
	assert.Equal(t, "backend", mapping[resourceResultKey(configRes)])
	assert.Equal(t, "backend-x9k2m", mapping[resourceResultKey(deployRes)])
}

func TestBuildResourceComponentNameMapping_ExcludesSharedTemplateComponents(t *testing.T) {
	namespace := "default"
	deployRes := &importResource{
		kindKey:       importKindDeployments,
		kind:          "Deployment",
		namespace:     namespace,
		name:          "backend",
		componentName: "backend",
	}

	resourceSource := []apisv1.CreateComponentRequest{
		{
			Name:          "backend",
			Namespace:     namespace,
			ComponentType: config.ServerJob,
			Image:         "app:v1",
		},
	}
	sharedSource := apisv1.CreateComponentRequest{
		Name:          "backend",
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties: apisv1.Properties{
			Conf: map[string]string{"k": "v"},
		},
		Traits: apisv1.Traits{
			Share: &domainspec.ShareTraitSpec{Strategy: string(domainspec.ShareStrategyDefault)},
		},
	}

	// Simulate full conversion order where shared config component appears before app deployment component.
	source := []apisv1.CreateComponentRequest{
		sharedSource,
		resourceSource[0],
	}
	deduped := []apisv1.CreateComponentRequest{
		{Name: "backend"},
		{Name: "backend-9k2mx"},
	}

	mapping := buildResourceComponentNameMapping([]*importResource{deployRes}, source, deduped, resourceSource)
	require.NotNil(t, mapping)
	assert.Equal(t, "backend-9k2mx", mapping[resourceResultKey(deployRes)])
}

func TestEnsureSharedComponentsOnApp_MarksBySourceSignatureNotName(t *testing.T) {
	namespace := "default"
	source := []apisv1.CreateComponentRequest{
		{
			Name:          "backend",
			Namespace:     namespace,
			ComponentType: config.ServerJob,
			Image:         "app:v1",
		},
		{
			Name:          "backend",
			Namespace:     namespace,
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"k": "v"},
			},
			Traits: apisv1.Traits{
				Share: &domainspec.ShareTraitSpec{Strategy: string(domainspec.ShareStrategyDefault)},
			},
		},
	}
	deduped := []apisv1.CreateComponentRequest{
		{
			Name:          "backend",
			Namespace:     namespace,
			ComponentType: config.ServerJob,
			Image:         "app:v1",
		},
		{
			Name:          "backend-j2m8q",
			Namespace:     namespace,
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"k": "v"},
			},
		},
	}
	sharedSource := []apisv1.CreateComponentRequest{source[1]}

	ensureSharedComponentsOnApp(source, deduped, sharedSource, map[string]string{
		"Deployment/default/backend": deduped[0].Name,
	})

	assert.Nil(t, deduped[0].Traits.Share)
	require.NotNil(t, deduped[1].Traits.Share)
	assert.Equal(t, string(domainspec.ShareStrategyDefault), deduped[1].Traits.Share.Strategy)
}

func TestEnsureSharedComponentsOnApp_DoesNotTagLocalComponentWhenSignatureCollides(t *testing.T) {
	namespace := "default"
	source := []apisv1.CreateComponentRequest{
		{
			Name:          "backend",
			Namespace:     namespace,
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"k": "v"},
			},
		},
		{
			Name:          "backend",
			Namespace:     namespace,
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"k": "v"},
			},
		},
	}
	deduped := dedupeImportComponents(source)
	require.Len(t, deduped, 2)
	sharedSource := []apisv1.CreateComponentRequest{source[1]}

	ensureSharedComponentsOnApp(source, deduped, sharedSource, map[string]string{
		"ConfigMap/default/backend": deduped[0].Name,
	})

	assert.Nil(t, deduped[0].Traits.Share)
	require.NotNil(t, deduped[1].Traits.Share)
	assert.Equal(t, string(domainspec.ShareStrategyDefault), deduped[1].Traits.Share.Strategy)
}

func TestResolveResourceComponentName_UsesMappedNameWhenOriginalMissing(t *testing.T) {
	res := &importResource{
		kind:          "Deployment",
		namespace:     "default",
		name:          "backend",
		componentName: "backend",
	}
	componentIDByName := map[string]int{
		"backend-m2k9x": 101,
	}
	resourceComponentByKey := map[string]string{
		resourceResultKey(res): "backend-m2k9x",
	}

	name := resolveResourceComponentName(res, componentIDByName, resourceComponentByKey, nil)
	assert.Equal(t, "backend-m2k9x", name)
}

func TestResolveResourceComponentName_DependentResourceUsesWorkloadRemap(t *testing.T) {
	res := &importResource{
		kindKey:       importKindServices,
		kind:          "Service",
		namespace:     "default",
		name:          "backend-svc",
		componentName: "backend",
	}
	componentIDByName := map[string]int{
		"backend":      1,
		"backend-m2k9": 2,
	}
	workloadMap := map[string]string{
		"backend": "backend-m2k9",
	}

	name := resolveResourceComponentName(res, componentIDByName, nil, workloadMap)
	assert.Equal(t, "backend-m2k9", name)
}

func TestSanitizeImportComponentsForCreate_RemovesReservedLabels(t *testing.T) {
	components := []apisv1.CreateComponentRequest{
		{
			Name: "backend",
			Properties: apisv1.Properties{
				Labels: map[string]string{
					config.LabelManagedBy:     config.ManagedByEruun,
					config.LabelAppID:         "app",
					config.LabelComponentID:   "12",
					config.LabelComponentName: "backend",
					config.LabelImportAppKey:  "stable-app-key",
					config.LabelShareName:     "proxy",
					config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
					"team":                    "games",
				},
			},
			Traits: apisv1.Traits{
				Service: []domainspec.ServiceTraitSpec{
					{
						Name: "backend-svc",
						Selector: map[string]string{
							config.LabelAppID:         "old-app",
							config.LabelComponentName: "old-component",
							"app":                     "backend",
						},
						Labels: map[string]string{
							config.LabelManagedBy:    config.ManagedByEruun,
							config.LabelImportAppKey: "old-stable-key",
							config.LabelShareName:    "proxy",
							"owner":                  "platform",
						},
					},
				},
				Ingress: []domainspec.IngressTraitsSpec{
					{
						Name: "backend-ing",
						Label: map[string]string{
							config.LabelAppID:     "old-app",
							config.LabelShareName: "proxy",
							"expose":              "public",
						},
					},
				},
				RBAC: []domainspec.RBACPolicySpec{
					{
						ServiceAccountLabels: map[string]string{
							config.LabelAppID: "old-app",
							"team":            "games",
						},
						RoleLabels: map[string]string{
							config.LabelComponentName: "old-component",
							config.LabelShareName:     "proxy",
							"role":                    "reader",
						},
						BindingLabels: map[string]string{
							config.LabelManagedBy:     config.ManagedByEruun,
							config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
							"binding":                 "reader",
						},
						Rules: []domainspec.RBACRuleSpec{{Verbs: []string{"get"}}},
					},
				},
			},
		},
	}

	sanitized := sanitizeImportComponentsForCreate(components)
	require.Len(t, sanitized, 1)
	labels := sanitized[0].Properties.Labels
	assert.Equal(t, "games", labels["team"])
	assert.NotContains(t, labels, config.LabelManagedBy)
	assert.NotContains(t, labels, config.LabelAppID)
	assert.NotContains(t, labels, config.LabelComponentID)
	assert.NotContains(t, labels, config.LabelComponentName)
	assert.NotContains(t, labels, config.LabelImportAppKey)
	assert.NotContains(t, labels, config.LabelShareName)
	assert.NotContains(t, labels, config.LabelShareStrategy)

	serviceSelector := sanitized[0].Traits.Service[0].Selector
	assert.NotContains(t, serviceSelector, config.LabelAppID)
	assert.NotContains(t, serviceSelector, config.LabelComponentName)
	assert.Equal(t, "backend", serviceSelector["app"])

	serviceLabels := sanitized[0].Traits.Service[0].Labels
	assert.NotContains(t, serviceLabels, config.LabelManagedBy)
	assert.NotContains(t, serviceLabels, config.LabelImportAppKey)
	assert.Equal(t, "platform", serviceLabels["owner"])
	assert.Equal(t, "proxy", serviceLabels[config.LabelShareName])

	ingressLabels := sanitized[0].Traits.Ingress[0].Label
	assert.NotContains(t, ingressLabels, config.LabelAppID)
	assert.Equal(t, "public", ingressLabels["expose"])
	assert.Equal(t, "proxy", ingressLabels[config.LabelShareName])

	policy := sanitized[0].Traits.RBAC[0]
	assert.NotContains(t, policy.ServiceAccountLabels, config.LabelAppID)
	assert.Equal(t, "games", policy.ServiceAccountLabels["team"])
	assert.NotContains(t, policy.RoleLabels, config.LabelComponentName)
	assert.Equal(t, "proxy", policy.RoleLabels[config.LabelShareName])
	assert.Equal(t, "reader", policy.RoleLabels["role"])
	assert.NotContains(t, policy.BindingLabels, config.LabelManagedBy)
	assert.Equal(t, string(domainspec.ShareStrategyDefault), policy.BindingLabels[config.LabelShareStrategy])
	assert.Equal(t, "reader", policy.BindingLabels["binding"])

	// Ensure original input remains unchanged.
	assert.Contains(t, components[0].Properties.Labels, config.LabelAppID)
	assert.Contains(t, components[0].Traits.Service[0].Labels, config.LabelManagedBy)
}
