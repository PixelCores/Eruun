package application

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowtraits "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestCreateApplicationsAllowsNormalNameUsedByTemplate(t *testing.T) {
	store := newInMemoryAppStore()
	template := &model.Applications{
		ID:              "tmpl-1",
		Name:            "mysql",
		Version:         "8.0.41",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.apps[template.ID] = template

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "mysql",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "mysql-config",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"mysql.cnf": "[mysqld]\n"},
			},
			Traits: apisv1.Traits{},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEqual(t, template.ID, resp.ID)
	require.False(t, resp.TemplateEnabled)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsAllowsTemplateResourceNamesUsedByNormalApp(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "mysql",
		Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Ports: []spec.Ports{{Port: 8080}},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:            "mysql",
		Version:         "8.0.41",
		TemplateEnabled: boolPtr(true),
		Component: []apisv1.CreateComponentRequest{{
			Name:          "mysql-api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Properties: apisv1.Properties{
				Ports: []spec.Ports{{Port: 8081}},
			},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.TemplateEnabled)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsDoesNotUseTemplateVersionForResourceKey(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "mysql-8-0-41",
		Namespace: config.DefaultNamespace,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:            "mysql",
		Version:         "8.0.41",
		TemplateEnabled: boolPtr(true),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsAllowsStandalonePVCNameAcrossNamespacesAndClaimTemplates(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "alpha",
		Namespace: "team-a",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "team-a",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Traits: mustJSONStruct(&apisv1.Traits{
			Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
		}),
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "beta",
		Namespace: "team-b",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "worker",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
				},
			},
			{
				Name:          "mysql",
				ComponentType: config.StoreJob,
				Image:         "mysql:8",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("data", "", true)},
				},
			},
			{
				Name:          "mysql-replica",
				ComponentType: config.StoreJob,
				Image:         "mysql:8",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("data", "", true)},
				},
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsFromTemplateSuffixesDuplicateTargets(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "mysql",
		Version:         "8.0.41",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.components["mysql-config"] = &model.ApplicationComponent{
		Name:          "mysql-config",
		AppID:         "tmpl-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ConfJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Conf: map[string]string{"master.cnf": "[mysqld]\n"},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "game",
		Component: []apisv1.CreateComponentRequest{
			{Template: &apisv1.TemplateRef{ID: "tmpl-1", Target: "mysql-config"}},
			{Template: &apisv1.TemplateRef{ID: "tmpl-1", Target: "mysql-config"}},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	first := store.components["game-config"]
	second := store.components["game-config-1"]
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.Equal(t, resp.ID, first.AppID)
	require.Equal(t, resp.ID, second.AppID)
}

func TestResolveComponentsWithSourceIndexes(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-source-index"] = &model.Applications{
		ID:              "tmpl-source-index",
		Name:            "template",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.components["template-api"] = &model.ApplicationComponent{
		Name:          "template-api",
		AppID:         "tmpl-source-index",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}
	store.components["template-job"] = &model.ApplicationComponent{
		Name:          "template-job",
		AppID:         "tmpl-source-index",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.InstantJob,
		Image:         "busybox:latest",
		Properties:    mustJSONStruct(&apisv1.Properties{}),
		Traits:        mustJSONStruct(&apisv1.Traits{}),
	}
	svc := newMockServiceWithStore(store)

	t.Run("legacy wrapper preserves template resolution", func(t *testing.T) {
		components, err := ResolveComponents(context.Background(), svc.AppRepo, svc.ComponentRepo, config.DefaultNamespace, "cloned", []apisv1.CreateComponentRequest{{
			Name:          "legacy-job",
			ComponentType: config.InstantJob,
			Template:      &apisv1.TemplateRef{ID: "tmpl-source-index", Target: "template-job"},
		}})

		require.NoError(t, err)
		require.Len(t, components, 2)
		names := make([]string, 0, len(components))
		for _, component := range components {
			names = append(names, component.Name)
		}
		require.ElementsMatch(t, []string{"cloned-api", "legacy-job"}, names)
	})

	t.Run("direct targeted repeated and template only clones", func(t *testing.T) {
		components, sourceIndexes, err := svc.resolveComponentsWithSourceIndexes(context.Background(), config.DefaultNamespace, "cloned", []apisv1.CreateComponentRequest{
			{
				Name:          "first-job",
				ComponentType: config.InstantJob,
				Template:      &apisv1.TemplateRef{ID: "tmpl-source-index", Target: "template-job"},
			},
			{
				Name:          "direct-api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
			{
				Name:          "second-job",
				ComponentType: config.InstantJob,
				Template:      &apisv1.TemplateRef{ID: "tmpl-source-index", Target: "template-job"},
			},
		})

		require.NoError(t, err)
		require.Len(t, components, 4)
		require.Len(t, sourceIndexes, len(components))

		indexesByName := make(map[string]int, len(components))
		for i, component := range components {
			indexesByName[component.Name] = sourceIndexes[i]
		}
		require.Equal(t, 1, indexesByName["direct-api"])
		require.Equal(t, 0, indexesByName["first-job"])
		require.Equal(t, 2, indexesByName["second-job"])
		require.Equal(t, unmappedComponentSourceIndex, indexesByName["cloned-api"])
	})

	t.Run("fallback override", func(t *testing.T) {
		components, sourceIndexes, err := svc.resolveComponentsWithSourceIndexes(context.Background(), config.DefaultNamespace, "cloned", []apisv1.CreateComponentRequest{
			{
				Name:          "direct-api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
			{
				Name:          "fallback-job",
				ComponentType: config.InstantJob,
				Template:      &apisv1.TemplateRef{ID: "tmpl-source-index"},
			},
		})

		require.NoError(t, err)
		require.Len(t, components, 3)
		require.Len(t, sourceIndexes, len(components))

		indexesByName := make(map[string]int, len(components))
		for i, component := range components {
			indexesByName[component.Name] = sourceIndexes[i]
		}
		require.Equal(t, 0, indexesByName["direct-api"])
		require.Equal(t, 1, indexesByName["fallback-job"])
		require.Equal(t, unmappedComponentSourceIndex, indexesByName["cloned-api"])
	})
}

func TestCreateApplicationsAllowsTemplateVersionsWithSameName(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	resp80, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:            "mysql",
		Version:         "8.0.41",
		TemplateEnabled: boolPtr(true),
		Component: []apisv1.CreateComponentRequest{{
			Name:          "mysql-config-80",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"mysql.cnf": "[mysqld]\n"},
			},
			Traits: apisv1.Traits{},
		}},
	})
	require.NoError(t, err)

	resp84, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:            "mysql",
		Version:         "8.4.0",
		TemplateEnabled: boolPtr(true),
		Component: []apisv1.CreateComponentRequest{{
			Name:          "mysql-config-84",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"mysql.cnf": "[mysqld]\n"},
			},
			Traits: apisv1.Traits{},
		}},
	})

	require.NoError(t, err)
	require.NotEqual(t, resp80.ID, resp84.ID)
	require.True(t, resp80.TemplateEnabled)
	require.True(t, resp84.TemplateEnabled)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsUpsertsTemplateByNameAndVersion(t *testing.T) {
	store := newInMemoryAppStore()
	existing := &model.Applications{
		ID:              "tmpl-1",
		Name:            "mysql",
		Version:         "8.0.41",
		Namespace:       config.DefaultNamespace,
		Alias:           "old-alias",
		TemplateEnabled: true,
	}
	store.apps[existing.ID] = existing

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:            "mysql",
		Version:         "8.0.41",
		Alias:           "new-alias",
		TemplateEnabled: boolPtr(true),
		Component: []apisv1.CreateComponentRequest{{
			Name:          "mysql-config",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"mysql.cnf": "[mysqld]\n"},
			},
			Traits: apisv1.Traits{},
		}},
	})

	require.NoError(t, err)
	require.Equal(t, existing.ID, resp.ID)
	require.Equal(t, "new-alias", store.apps[existing.ID].Alias)
	require.Len(t, store.apps, 1)
}

func TestCreateApplicationsDoesNotUpsertTemplateAcrossNamespaces(t *testing.T) {
	store := newInMemoryAppStore()
	existing := &model.Applications{
		ID:              "tmpl-team-a",
		Name:            "mysql",
		Version:         "8.0.41",
		Namespace:       "team-a",
		Alias:           "team-a-template",
		TemplateEnabled: true,
	}
	store.apps[existing.ID] = existing

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:            "mysql",
		Version:         "8.0.41",
		Namespace:       "team-b",
		Alias:           "team-b-template",
		TemplateEnabled: boolPtr(true),
		Component: []apisv1.CreateComponentRequest{{
			Name:          "mysql-config",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"mysql.cnf": "[mysqld]\n"},
			},
			Traits: apisv1.Traits{},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEqual(t, existing.ID, resp.ID)
	require.Equal(t, "team-a-template", store.apps[existing.ID].Alias)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsRejectsTemplateRenameToExistingKey(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "mysql",
		Version:         "8.0.41",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.apps["tmpl-2"] = &model.Applications{
		ID:              "tmpl-2",
		Name:            "redis",
		Version:         "6.2.17",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		ID:              "tmpl-2",
		Name:            "mysql",
		Version:         "8.0.41",
		TemplateEnabled: boolPtr(true),
	})

	require.ErrorIs(t, err, bcode.ErrApplicationExist)
	require.Equal(t, "redis", store.apps["tmpl-2"].Name)
	require.Equal(t, "6.2.17", store.apps["tmpl-2"].Version)
}

func TestCreateApplicationsAllowsTemplateRenameToUnusedKey(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "mysql",
		Version:         "8.0.41",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}
	store.apps["tmpl-2"] = &model.Applications{
		ID:              "tmpl-2",
		Name:            "redis",
		Version:         "6.2.17",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: true,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		ID:              "tmpl-2",
		Name:            "mysql",
		Version:         "8.4.0",
		TemplateEnabled: boolPtr(true),
		Component: []apisv1.CreateComponentRequest{{
			Name:          "mysql-config-84",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"mysql.cnf": "[mysqld]\n"},
			},
			Traits: apisv1.Traits{},
		}},
	})

	require.NoError(t, err)
	require.Equal(t, "tmpl-2", resp.ID)
	require.Equal(t, "mysql", store.apps["tmpl-2"].Name)
	require.Equal(t, "8.4.0", store.apps["tmpl-2"].Version)
}

func TestCreateApplicationsFromTemplateRequiresEnable(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-1", Name: "tmpl", TemplateEnabled: false}
	store.apps[templateApp.ID] = templateApp
	store.components["tmpl-comp"] = &model.ApplicationComponent{
		Name:          "tmpl-comp",
		AppID:         templateApp.ID,
		Replicas:      1,
		ComponentType: config.StoreJob,
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "new-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-comp",
			ComponentType: config.StoreJob,
			Template:      &apisv1.TemplateRef{ID: templateApp.ID},
		}},
	}

	_, err := svc.CreateApplications(context.Background(), req)
	require.ErrorIs(t, err, bcode.ErrTemplateNotEnabled)
}

func TestCreateApplicationsFromTemplateClonesTraitsAndNames(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-2", Name: "mysql", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:       "mysql",
			ClaimName:  "mysql",
			SourceName: "tem-mysql-config",
			TmpCreate:  true,
			Size:       "1Gi",
			Type:       config.StorageTypePersistent,
		}},
		Ingress: []spec.IngressTraitsSpec{{
			Name: "mysql",
			Routes: []spec.IngressRoutes{{
				Backend: spec.IngressRoute{ServiceName: "mysql-master"},
			}},
		}},
		Service: []spec.ServiceTraitSpec{
			{
				Name: "mysql-master",
				Type: string(spec.ServiceAccessInternal),
				Labels: map[string]string{
					"name": "mysql-master",
				},
				Selector: map[string]string{
					"mysql-pod-role":          "mysql-master",
					"role":                    "mysql-master",
					config.LabelComponentName: "mysql",
				},
				Ports: []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			},
			{
				Name:     "primary",
				Type:     string(spec.ServiceAccessInternal),
				Selector: map[string]string{"mysql-pod-role": "primary"},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3307, TargetPort: 3306, Protocol: "TCP"}},
			},
		},
		RBAC: []spec.RBACPolicySpec{{
			ServiceAccount: "mysql",
			RoleName:       "mysql",
			BindingName:    "mysql",
		}},
		Envs: []spec.SimplifiedEnvSpec{{
			Name: "MYSQL_ROOT_PASSWORD",
			ValueFrom: spec.ValueSource{
				Secret: &spec.SecretSelectorSpec{
					Name: "tem-mysql-secret",
					Key:  "MYSQL_ROOT_PASSWORD",
				},
			},
		}},
		Init: []spec.InitTraitSpec{{
			Name:  "init-mysql",
			Image: "busybox:1.36",
			Properties: spec.Properties{
				Env: map[string]string{
					"DB_HOST":          "mysql-master.default.svc",
					"MASTER_ROLE_NAME": "mysql-master",
				},
				Command: []string{"sh", "-c", "mysql -h mysql-master.default.svc.cluster.local"},
			},
		}},
		Sidecar: []spec.SidecarTraitsSpec{{
			Name: "backup",
			Env: map[string]string{
				"DB_HOST": "mysql-master",
			},
			Command: []string{"sh", "-c"},
			Args:    []string{"connect mysql-master.default.svc.cluster.local"},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)

	templateProps := apisv1.Properties{
		Labels: map[string]string{
			"role":   "mysql-master",
			"stable": "db",
		},
		Env: map[string]string{
			"a":                "b",
			"DB_HOST":          "mysql-master.default.svc",
			"MYSQL_DATABASE":   "mysql",
			"MASTER_ROLE_NAME": "mysql-master",
		},
		Command: []string{"sh", "-c", "mysql -h mysql-master.default.svc.cluster.local"},
	}
	propsJSON, err := model.NewJSONStructByStruct(templateProps)
	require.NoError(t, err)

	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:latest",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}

	templateSecretProps := apisv1.Properties{Secret: map[string]string{"MYSQL_ROOT_PASSWORD": "orig"}}
	secretPropsJSON, err := model.NewJSONStructByStruct(templateSecretProps)
	require.NoError(t, err)
	store.components["mysql-secret"] = &model.ApplicationComponent{
		Name:          "mysql-secret",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties:    secretPropsJSON,
	}

	templateConfigProps := apisv1.Properties{
		Conf: map[string]string{"master.cnf": "dummy", "slave.cnf": "dummy"},
	}
	configPropsJSON, err := model.NewJSONStructByStruct(templateConfigProps)
	require.NoError(t, err)
	store.components["tem-mysql-config"] = &model.ApplicationComponent{
		Name:          "tem-mysql-config",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ConfJob,
		Properties:    configPropsJSON,
	}

	svc := newMockServiceWithStore(store)
	templateEnabled := true
	req := apisv1.CreateApplicationsRequest{
		Name:  "cloned-app",
		Alias: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "new-mysql",
				ComponentType: config.StoreJob,
				Properties: apisv1.Properties{
					Env: map[string]string{"a": "override", "NEW": "env"},
				},
				Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
			},
			{
				Name:          "new-mysql-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"MYSQL_ROOT_PASSWORD": "override-secret"},
				},
				Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tem-mysql-secret"},
			},
		},
		TemplateEnabled: &templateEnabled,
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.TemplateEnabled)

	var createdStore *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.ComponentType == config.StoreJob {
			createdStore = comp
			break
		}
	}
	require.NotNil(t, createdStore)
	require.Equal(t, "new-mysql", createdStore.Name)

	var clonedTraits apisv1.Traits
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Traits)), &clonedTraits))
	require.Len(t, clonedTraits.Storage, 1)
	require.Equal(t, "new-mysql", clonedTraits.Storage[0].Name)
	require.Equal(t, "new-mysql", clonedTraits.Storage[0].ClaimName)
	require.Equal(t, "cloned-app-config", clonedTraits.Storage[0].SourceName)

	require.Len(t, clonedTraits.Ingress, 1)
	require.Equal(t, "new-mysql", clonedTraits.Ingress[0].Name)
	require.Len(t, clonedTraits.Ingress[0].Routes, 1)
	require.Equal(t, "cloned-app-mysql-master", clonedTraits.Ingress[0].Routes[0].Backend.ServiceName)

	require.Len(t, clonedTraits.Service, 2)
	require.Equal(t, "cloned-app-mysql-master", clonedTraits.Service[0].Name)
	require.Equal(t, "cloned-app-mysql-master", clonedTraits.Service[0].Labels["name"])
	require.Equal(t, "cloned-app-mysql-master", clonedTraits.Service[0].Selector["mysql-pod-role"])
	require.Equal(t, "cloned-app-mysql-master", clonedTraits.Service[0].Selector["role"])
	require.Equal(t, "new-mysql", clonedTraits.Service[0].Selector[config.LabelComponentName])
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[1].Name)
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[1].Selector["mysql-pod-role"])

	require.Len(t, clonedTraits.RBAC, 1)
	require.Equal(t, "mysql", clonedTraits.RBAC[0].ServiceAccount)
	require.Equal(t, "mysql", clonedTraits.RBAC[0].RoleName)
	require.Equal(t, "mysql", clonedTraits.RBAC[0].BindingName)
	require.Equal(t, config.DefaultNamespace, clonedTraits.RBAC[0].Namespace)

	var clonedProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Properties)), &clonedProps))
	require.Equal(t, "override", clonedProps.Env["a"])
	require.Equal(t, "env", clonedProps.Env["NEW"])
	require.Equal(t, "cloned-app-mysql-master.default.svc", clonedProps.Env["DB_HOST"])
	require.Equal(t, "mysql", clonedProps.Env["MYSQL_DATABASE"])
	require.Equal(t, "cloned-app-mysql-master", clonedProps.Env["MASTER_ROLE_NAME"])
	require.Equal(t, "cloned-app-mysql-master", clonedProps.Labels["role"])
	require.Equal(t, "db", clonedProps.Labels["stable"])
	require.Equal(t, []string{"sh", "-c", "mysql -h cloned-app-mysql-master.default.svc.cluster.local"}, clonedProps.Command)
	require.Len(t, clonedTraits.Envs, 1)
	require.Equal(t, "new-mysql-secret", clonedTraits.Envs[0].ValueFrom.Secret.Name)
	require.Len(t, clonedTraits.Init, 1)
	require.Equal(t, "cloned-app-mysql-master.default.svc", clonedTraits.Init[0].Properties.Env["DB_HOST"])
	require.Equal(t, clonedTraits.Service[0].Selector["mysql-pod-role"], clonedTraits.Init[0].Properties.Env["MASTER_ROLE_NAME"])
	require.Equal(t, []string{"sh", "-c", "mysql -h cloned-app-mysql-master.default.svc.cluster.local"}, clonedTraits.Init[0].Properties.Command)
	require.Len(t, clonedTraits.Sidecar, 1)
	require.Equal(t, "mysql-master", clonedTraits.Sidecar[0].Env["DB_HOST"])
	require.Equal(t, []string{"connect cloned-app-mysql-master.default.svc.cluster.local"}, clonedTraits.Sidecar[0].Args)

	var createdSecret *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.ComponentType == config.SecretJob {
			createdSecret = comp
			break
		}
	}
	require.NotNil(t, createdSecret)
	require.Equal(t, "new-mysql-secret", createdSecret.Name)
	var secretProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdSecret.Properties)), &secretProps))
	require.Equal(t, "override-secret", secretProps.Secret["MYSQL_ROOT_PASSWORD"])
}

func TestCreateApplicationsFromTemplateSharesTopLevelPersistentStorageWithNestedContainers(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-3", Name: "mysql", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:      "data",
			Type:      config.StorageTypePersistent,
			TmpCreate: true,
			Size:      "1Gi",
			MountPath: "/var/lib/mysql",
		}},
		Init: []spec.InitTraitSpec{{
			Name:  "clone-mysql",
			Image: "busybox:1.36",
			Traits: apisv1.Traits{Storage: []spec.StorageTraitSpec{{
				Name:       "data",
				Type:       config.StorageTypePersistent,
				MountPath:  "/var/lib/mysql",
				SubPath:    "init",
				SourceName: "mysql",
			}}},
		}},
		Sidecar: []spec.SidecarTraitsSpec{
			{
				Name:  "backup",
				Image: "backup:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{{
						Name:      "data",
						Type:      config.StorageTypePersistent,
						MountPath: "/var/lib/mysql",
						SubPath:   "backup",
						ReadOnly:  true,
					}},
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)

	propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)

	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:latest",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "tenant-a-mysql-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "tenant-a-mysql",
				ComponentType: config.StoreJob,
				Template: &apisv1.TemplateRef{
					ID:                  templateApp.ID,
					Target:              "mysql",
					DefaultStorageClass: "tenant-a-nas",
				},
			},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var createdStore *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.ComponentType == config.StoreJob {
			createdStore = comp
			break
		}
	}
	require.NotNil(t, createdStore)

	var clonedTraits apisv1.Traits
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Traits)), &clonedTraits))
	require.Len(t, clonedTraits.Storage, 1)
	require.Equal(t, "data", clonedTraits.Storage[0].Name)
	require.True(t, clonedTraits.Storage[0].TmpCreate)
	require.Equal(t, "1Gi", clonedTraits.Storage[0].Size)
	require.Equal(t, "tenant-a-nas", clonedTraits.Storage[0].StorageClass)
	require.Empty(t, clonedTraits.Storage[0].ClaimName)

	require.Len(t, clonedTraits.Init, 1)
	require.Len(t, clonedTraits.Init[0].Traits.Storage, 1)
	require.Equal(t, "data", clonedTraits.Init[0].Traits.Storage[0].Name)
	require.True(t, clonedTraits.Init[0].Traits.Storage[0].TmpCreate)
	require.Equal(t, "init", clonedTraits.Init[0].Traits.Storage[0].SubPath)
	require.Equal(t, "tenant-a-nas", clonedTraits.Init[0].Traits.Storage[0].StorageClass)
	require.Equal(t, "tenant-a-mysql", clonedTraits.Init[0].Traits.Storage[0].SourceName)
	require.Len(t, clonedTraits.Sidecar, 1)
	require.Len(t, clonedTraits.Sidecar[0].Traits.Storage, 1)
	require.Equal(t, "data", clonedTraits.Sidecar[0].Traits.Storage[0].Name)
	require.True(t, clonedTraits.Sidecar[0].Traits.Storage[0].TmpCreate)
	require.Equal(t, "backup", clonedTraits.Sidecar[0].Traits.Storage[0].SubPath)
	require.True(t, clonedTraits.Sidecar[0].Traits.Storage[0].ReadOnly)
	require.Equal(t, "tenant-a-nas", clonedTraits.Sidecar[0].Traits.Storage[0].StorageClass)
	require.Empty(t, clonedTraits.Sidecar[0].Traits.Storage[0].ClaimName)

	workflowtraits.RegisterAllProcessors()
	workload := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: createdStore.Name, Image: createdStore.Image}},
		}}},
	}
	additional, err := workflowtraits.ApplyTraits(createdStore, workload)
	require.NoError(t, err)
	require.Empty(t, additional)
	require.Len(t, workload.Spec.VolumeClaimTemplates, 1)
	require.Equal(t, "data", workload.Spec.VolumeClaimTemplates[0].Name)
	require.NotNil(t, workload.Spec.VolumeClaimTemplates[0].Spec.StorageClassName)
	require.Equal(t, "tenant-a-nas", *workload.Spec.VolumeClaimTemplates[0].Spec.StorageClassName)
	storageRequest := workload.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "1Gi", storageRequest.String())

	require.Len(t, workload.Spec.Template.Spec.InitContainers, 1)
	requireSingleStorageMount(t, workload.Spec.Template.Spec.InitContainers[0], "clone-mysql", "init", false)
	require.Len(t, workload.Spec.Template.Spec.Containers, 2)
	requireSingleStorageMount(t, workload.Spec.Template.Spec.Containers[0], createdStore.Name, "", false)
	requireSingleStorageMount(t, workload.Spec.Template.Spec.Containers[1], "backup", "backup", true)
}

func TestRewriteTraitsForTemplateOnlySharesTopLevelPersistentStorage(t *testing.T) {
	traits := &apisv1.Traits{
		Storage: []spec.StorageTraitSpec{
			{Name: "data", Type: config.StorageTypePersistent, TmpCreate: true, Size: "1Gi"},
			{Name: "config", Type: config.StorageTypeEphemeral},
		},
		Sidecar: []spec.SidecarTraitsSpec{{
			Name: "backup",
			Traits: apisv1.Traits{Storage: []spec.StorageTraitSpec{
				{Name: "cache", Type: config.StorageTypePersistent},
				{Name: "config", Type: config.StorageTypePersistent},
			}},
		}},
	}

	require.NoError(t, rewriteTraitsForTemplate(traits, "mysql", "tenant-mysql", "tenant-app", config.DefaultNamespace, newTemplateRewriteMap(), nil, nil, nil))
	require.Len(t, traits.Sidecar, 1)
	require.Len(t, traits.Sidecar[0].Traits.Storage, 2)
	require.Equal(t, "tenant-app-cache", traits.Sidecar[0].Traits.Storage[0].Name)
	require.False(t, traits.Sidecar[0].Traits.Storage[0].TmpCreate)
	require.Equal(t, config.StorageTypePersistent, traits.Sidecar[0].Traits.Storage[1].Type)
	require.Equal(t, "tenant-app-config", traits.Sidecar[0].Traits.Storage[1].Name)
	require.False(t, traits.Sidecar[0].Traits.Storage[1].TmpCreate)
}

func requireSingleStorageMount(t *testing.T, container corev1.Container, name, subPath string, readOnly bool) {
	t.Helper()
	require.Equal(t, name, container.Name)
	require.Len(t, container.VolumeMounts, 1)
	require.Equal(t, "data", container.VolumeMounts[0].Name)
	require.Equal(t, "/var/lib/mysql", container.VolumeMounts[0].MountPath)
	require.Equal(t, subPath, container.VolumeMounts[0].SubPath)
	require.Equal(t, readOnly, container.VolumeMounts[0].ReadOnly)
}
