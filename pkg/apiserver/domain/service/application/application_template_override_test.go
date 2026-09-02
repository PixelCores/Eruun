package application

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestCreateApplicationsFromTemplatePreservesEnvOverrideValuesDuringRewrite(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-env-override", Name: "mysql-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name:     "mysql-master",
			Type:     string(config.ServiceAccessInternal),
			Selector: map[string]string{"role": "mysql-master"},
			Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	})
	require.NoError(t, err)
	propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
		Env: map[string]string{
			"DB_HOST":       "mysql-master.default.svc",
			"TEMPLATE_HOST": "mysql-master.default.svc",
		},
	})
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
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-mysql",
			ComponentType: config.StoreJob,
			Properties: apisv1.Properties{
				Env: map[string]string{"DB_HOST": "mysql-master.default.svc"},
			},
			Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
		}},
	})
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

	var clonedProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Properties)), &clonedProps))
	require.Equal(t, "mysql-master.default.svc", clonedProps.Env["DB_HOST"])
	require.Equal(t, "cloned-app-mysql-master.default.svc", clonedProps.Env["TEMPLATE_HOST"])
}

func TestCreateApplicationsFromTemplatePreservesInitEnvOverrideValuesDuringRewrite(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-init-env-override", Name: "mysql-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name:     "mysql-master",
			Type:     string(config.ServiceAccessInternal),
			Selector: map[string]string{"role": "mysql-master"},
			Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
		Init: []spec.InitTraitSpec{{
			Name:  "init-db",
			Image: "busybox:1.36",
			Properties: spec.Properties{
				Env: map[string]string{
					"DB_HOST":       "mysql-master.default.svc",
					"TEMPLATE_HOST": "mysql-master.default.svc",
				},
				Command: []string{"sh", "-c", "mysql -h mysql-master.default.svc.cluster.local"},
			},
		}},
	})
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
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-mysql",
			ComponentType: config.StoreJob,
			Traits: apisv1.Traits{
				Init: []spec.InitTraitSpec{{
					Name: "init-db",
					Properties: spec.Properties{
						Env: map[string]string{"DB_HOST": "mysql-master.default.svc"},
					},
				}},
			},
			Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
		}},
	})
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
	require.Len(t, clonedTraits.Init, 1)
	require.Equal(t, "mysql-master.default.svc", clonedTraits.Init[0].Properties.Env["DB_HOST"])
	require.Equal(t, "cloned-app-mysql-master.default.svc", clonedTraits.Init[0].Properties.Env["TEMPLATE_HOST"])
	require.Equal(t, []string{"sh", "-c", "mysql -h cloned-app-mysql-master.default.svc.cluster.local"}, clonedTraits.Init[0].Properties.Command)
}

func TestCreateApplicationsFromTemplateAppliesDefaultStorageClass(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-default-storage-class", Name: "mysql-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{
			{
				Name:      "data",
				Type:      config.StorageTypePersistent,
				MountPath: "/var/lib/mysql",
				TmpCreate: true,
				Size:      "30Gi",
			},
			{
				Name:      "cache",
				Type:      config.StorageTypeEphemeral,
				MountPath: "/cache",
			},
			{
				Name:         "explicit-data",
				Type:         config.StorageTypePersistent,
				MountPath:    "/explicit",
				StorageClass: "fast-ssd",
			},
		},
		Init: []spec.InitTraitSpec{{
			Name:  "init-db",
			Image: "busybox:1.36",
			Traits: apisv1.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "init-data",
					Type:      config.StorageTypePersistent,
					MountPath: "/init-data",
				}},
			},
		}},
		Sidecar: []spec.SidecarTraitsSpec{{
			Name:  "backup",
			Image: "busybox:1.36",
			Traits: apisv1.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "backup-data",
					Type:      config.StorageTypePersistent,
					MountPath: "/backup-data",
				}},
			},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)

	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:latest",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    mustJSONStruct(apisv1.Properties{}),
		Traits:        traitsJSON,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-mysql",
			ComponentType: config.StoreJob,
			Template: &apisv1.TemplateRef{
				ID:                  templateApp.ID,
				Target:              "mysql",
				DefaultStorageClass: " tenant-a-nas ",
			},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	createdStore := store.components["new-mysql"]
	require.NotNil(t, createdStore)
	require.Equal(t, resp.ID, createdStore.AppID)

	var clonedTraits apisv1.Traits
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Traits)), &clonedTraits))
	require.Len(t, clonedTraits.Storage, 3)
	require.Equal(t, "tenant-a-nas", clonedTraits.Storage[0].StorageClass)
	require.Empty(t, clonedTraits.Storage[1].StorageClass)
	require.Equal(t, "fast-ssd", clonedTraits.Storage[2].StorageClass)
	require.Len(t, clonedTraits.Init, 1)
	require.Len(t, clonedTraits.Init[0].Traits.Storage, 1)
	require.Equal(t, "tenant-a-nas", clonedTraits.Init[0].Traits.Storage[0].StorageClass)
	require.Len(t, clonedTraits.Sidecar, 1)
	require.Len(t, clonedTraits.Sidecar[0].Traits.Storage, 1)
	require.Equal(t, "tenant-a-nas", clonedTraits.Sidecar[0].Traits.Storage[0].StorageClass)
}

func TestCreateApplicationsFromTemplatePreservesSecretTextOverrides(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-encoded", Name: "app", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Envs: []spec.SimplifiedEnvSpec{{
			Name: "PASSWORD",
			ValueFrom: spec.ValueSource{
				Secret: &spec.SecretSelectorSpec{
					Name: "tem-app-secret",
					Key:  "PASSWORD",
				},
			},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)
	store.components["app"] = &model.ApplicationComponent{
		Name:          "app",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:latest",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}

	templateSecretProps := apisv1.Properties{
		Secret: map[string]string{
			"PASSWORD": base64.StdEncoding.EncodeToString([]byte("template-secret")),
		},
	}
	secretPropsJSON, err := model.NewJSONStructByStruct(templateSecretProps)
	require.NoError(t, err)
	store.components["tem-app-secret"] = &model.ApplicationComponent{
		Name:          "tem-app-secret",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties:    secretPropsJSON,
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "cloned-from-template",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "clone-app",
				ComponentType: config.ServerJob,
				Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "app"},
			},
			{
				Name:          "clone-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"PASSWORD": "override-secret"},
				},
				Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tem-app-secret"},
			},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var createdSecret *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.Name == "clone-secret" {
			createdSecret = comp
			break
		}
	}
	require.NotNil(t, createdSecret)
	var storedSecretProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdSecret.Properties)), &storedSecretProps))
	require.Equal(t, "override-secret", storedSecretProps.Secret["PASSWORD"])

	components, err := svc.ListApplicationComponents(context.Background(), resp.ID)
	require.NoError(t, err)
	var secretComponent *model.ApplicationComponent
	for _, component := range components {
		if component != nil && component.Name == "clone-secret" {
			secretComponent = component
			break
		}
	}
	require.NotNil(t, secretComponent)
	var decodedSecretProps model.Properties
	require.NoError(t, decodeJSONStruct(secretComponent.Properties, &decodedSecretProps))
	require.Equal(t, "override-secret", decodedSecretProps.Secret["PASSWORD"])

	dtos, err := assembler.ConvertComponentModelsToDTO(components)
	require.NoError(t, err)
	var apiComponent *apisv1.ApplicationComponent
	for _, component := range dtos {
		if component != nil && component.Name == "clone-app" {
			apiComponent = component
			break
		}
	}
	require.NotNil(t, apiComponent)
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "PASSWORD", SecretName: "clone-secret", Key: "PASSWORD", Value: "override-secret", Resolved: true},
	}, apiComponent.Credentials)
}

func TestCreateApplicationsFromTemplateKeepsBase64LookingOverrideAsText(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-encoded-override", Name: "app", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Envs: []spec.SimplifiedEnvSpec{{
			Name: "PASSWORD",
			ValueFrom: spec.ValueSource{
				Secret: &spec.SecretSelectorSpec{
					Name: "tem-app-secret",
					Key:  "PASSWORD",
				},
			},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)
	store.components["app"] = &model.ApplicationComponent{
		Name:          "app",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:latest",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}

	templateSecretProps := apisv1.Properties{
		Secret: map[string]string{
			"PASSWORD": base64.StdEncoding.EncodeToString([]byte("template-secret")),
		},
	}
	secretPropsJSON, err := model.NewJSONStructByStruct(templateSecretProps)
	require.NoError(t, err)
	store.components["tem-app-secret"] = &model.ApplicationComponent{
		Name:          "tem-app-secret",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties:    secretPropsJSON,
	}

	svc := newMockServiceWithStore(store)
	encodedOverride := base64.StdEncoding.EncodeToString([]byte("override-secret"))
	req := apisv1.CreateApplicationsRequest{
		Name: "cloned-from-template-encoded-override",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "clone-app",
				ComponentType: config.ServerJob,
				Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "app"},
			},
			{
				Name:          "clone-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"PASSWORD": encodedOverride},
				},
				Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tem-app-secret"},
			},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var createdSecret *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.Name == "clone-secret" {
			createdSecret = comp
			break
		}
	}
	require.NotNil(t, createdSecret)
	var storedSecretProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdSecret.Properties)), &storedSecretProps))
	require.Equal(t, encodedOverride, storedSecretProps.Secret["PASSWORD"])

	components, err := svc.ListApplicationComponents(context.Background(), resp.ID)
	require.NoError(t, err)
	var secretComponent *model.ApplicationComponent
	for _, component := range components {
		if component != nil && component.Name == "clone-secret" {
			secretComponent = component
			break
		}
	}
	require.NotNil(t, secretComponent)
	var decodedSecretProps model.Properties
	require.NoError(t, decodeJSONStruct(secretComponent.Properties, &decodedSecretProps))
	require.Equal(t, encodedOverride, decodedSecretProps.Secret["PASSWORD"])
}

func TestCreateApplicationsFromTemplateKeepsBase64LookingOverrideOnPlaintextSecret(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-plain-override", Name: "app", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Envs: []spec.SimplifiedEnvSpec{{
			Name: "PASSWORD",
			ValueFrom: spec.ValueSource{
				Secret: &spec.SecretSelectorSpec{
					Name: "tem-app-secret",
					Key:  "PASSWORD",
				},
			},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)
	store.components["app"] = &model.ApplicationComponent{
		Name:          "app",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:latest",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}

	templateSecretProps := apisv1.Properties{
		Secret: map[string]string{
			"USERNAME": "root",
		},
	}
	secretPropsJSON, err := model.NewJSONStructByStruct(templateSecretProps)
	require.NoError(t, err)
	store.components["tem-app-secret"] = &model.ApplicationComponent{
		Name:          "tem-app-secret",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties:    secretPropsJSON,
	}

	svc := newMockServiceWithStore(store)
	encodedOverride := base64.StdEncoding.EncodeToString([]byte("override-secret"))
	req := apisv1.CreateApplicationsRequest{
		Name: "cloned-from-template-plain-override",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "clone-app",
				ComponentType: config.ServerJob,
				Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "app"},
			},
			{
				Name:          "clone-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"PASSWORD": encodedOverride},
				},
				Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tem-app-secret"},
			},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var createdSecret *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.Name == "clone-secret" {
			createdSecret = comp
			break
		}
	}
	require.NotNil(t, createdSecret)

	var storedSecretProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdSecret.Properties)), &storedSecretProps))
	require.Equal(t, encodedOverride, storedSecretProps.Secret["PASSWORD"])
	require.Equal(t, "root", storedSecretProps.Secret["USERNAME"])

	components, err := svc.ListApplicationComponents(context.Background(), resp.ID)
	require.NoError(t, err)
	var secretComponent *model.ApplicationComponent
	for _, component := range components {
		if component != nil && component.Name == "clone-secret" {
			secretComponent = component
			break
		}
	}
	require.NotNil(t, secretComponent)

	var decodedSecretProps model.Properties
	require.NoError(t, decodeJSONStruct(secretComponent.Properties, &decodedSecretProps))
	require.Equal(t, encodedOverride, decodedSecretProps.Secret["PASSWORD"])
	require.Equal(t, "root", decodedSecretProps.Secret["USERNAME"])
}

func TestCreateApplicationsFromTemplateOverridesInitEnv(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-init", Name: "mysql", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name: "mysql-master",
			Type: string(config.ServiceAccessInternal),
			Selector: map[string]string{
				"mysql-pod-role": "mysql-master",
			},
			Ports: []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
		Init: []spec.InitTraitSpec{{
			Name:  "init-mysql",
			Image: "busybox:1.36",
			Properties: spec.Properties{
				Env: map[string]string{
					"SQL_URL":          "https://example.com/template.sql",
					"MYSQL_DATABASE":   "template-db",
					"MASTER_ROLE_NAME": "mysql-master",
				},
			},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)

	templateProps := apisv1.Properties{Env: map[string]string{"a": "b"}}
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

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-mysql",
			ComponentType: config.StoreJob,
			Traits: apisv1.Traits{
				Init: []spec.InitTraitSpec{{
					Name: "init-mysql",
					Properties: spec.Properties{
						Env: map[string]string{
							"SQL_URL":          "https://example.com/override.sql",
							"MASTER_ROLE_NAME": "custom-master",
						},
					},
				}},
			},
			Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
		}},
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
	require.Len(t, clonedTraits.Init, 1)
	require.Equal(t, "https://example.com/override.sql", clonedTraits.Init[0].Properties.Env["SQL_URL"])
	require.Equal(t, "template-db", clonedTraits.Init[0].Properties.Env["MYSQL_DATABASE"])
	require.Equal(t, "custom-master", clonedTraits.Init[0].Properties.Env["MASTER_ROLE_NAME"])
	require.Len(t, clonedTraits.Service, 1)
	require.Equal(t, "cloned-app-mysql-master", clonedTraits.Service[0].Selector["mysql-pod-role"])
}

func TestCreateApplicationsFromTemplateOverridesInitEnvDefaultFirst(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-init-default", Name: "mysql", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Init: []spec.InitTraitSpec{
			{
				Name:  "init-first",
				Image: "busybox:1.36",
				Properties: spec.Properties{
					Env: map[string]string{
						"SQL_URL": "https://example.com/first.sql",
					},
				},
			},
			{
				Name:  "init-second",
				Image: "busybox:1.36",
				Properties: spec.Properties{
					Env: map[string]string{
						"SQL_URL": "https://example.com/second.sql",
					},
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)

	templateProps := apisv1.Properties{Env: map[string]string{"a": "b"}}
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

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-mysql",
			ComponentType: config.StoreJob,
			Traits: apisv1.Traits{
				Init: []spec.InitTraitSpec{{
					Properties: spec.Properties{
						Env: map[string]string{
							"SQL_URL": "https://example.com/override.sql",
						},
					},
				}},
			},
			Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
		}},
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
	require.Len(t, clonedTraits.Init, 2)
	require.Equal(t, "https://example.com/override.sql", clonedTraits.Init[0].Properties.Env["SQL_URL"])
	require.Equal(t, "https://example.com/second.sql", clonedTraits.Init[1].Properties.Env["SQL_URL"])
}

func TestCreateApplicationsFromTemplateOverridesResources(t *testing.T) {
	tests := []struct {
		name              string
		templateResources *spec.ResourceTraitsSpec
		overrideResources *spec.ResourceTraitsSpec
		expectedResources *spec.ResourceTraitsSpec
	}{
		{
			name:              "override cpu keeps template memory and gpu",
			templateResources: &spec.ResourceTraitsSpec{CPU: "300m", Memory: "600Mi", GPU: "1"},
			overrideResources: &spec.ResourceTraitsSpec{CPU: "500m"},
			expectedResources: &spec.ResourceTraitsSpec{CPU: "500m", Memory: "600Mi", GPU: "1"},
		},
		{
			name:              "override memory keeps template cpu",
			templateResources: &spec.ResourceTraitsSpec{CPU: "300m", Memory: "600Mi"},
			overrideResources: &spec.ResourceTraitsSpec{Memory: "1Gi"},
			expectedResources: &spec.ResourceTraitsSpec{CPU: "300m", Memory: "1Gi"},
		},
		{
			name:              "create resources when template has none",
			overrideResources: &spec.ResourceTraitsSpec{CPU: "250m", Memory: "512Mi"},
			expectedResources: &spec.ResourceTraitsSpec{CPU: "250m", Memory: "512Mi"},
		},
		{
			name:              "override limits keeps template requests",
			templateResources: &spec.ResourceTraitsSpec{CPU: "160m", Memory: "260Mi", CPULimit: "300m", MemoryLimit: "600Mi"},
			overrideResources: &spec.ResourceTraitsSpec{CPULimit: "500m", MemoryLimit: "1Gi"},
			expectedResources: &spec.ResourceTraitsSpec{CPU: "160m", Memory: "260Mi", CPULimit: "500m", MemoryLimit: "1Gi"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			templateApp := &model.Applications{ID: "tmpl-resources", Name: "mysql", TemplateEnabled: true}
			store.apps[templateApp.ID] = templateApp
			store.components["mysql"] = &model.ApplicationComponent{
				Name:          "mysql",
				AppID:         templateApp.ID,
				Namespace:     config.DefaultNamespace,
				Image:         "mysql:latest",
				Replicas:      1,
				ComponentType: config.StoreJob,
				Properties:    mustJSONStruct(apisv1.Properties{}),
				Traits: mustJSONStruct(apisv1.Traits{
					Resources: tt.templateResources,
				}),
			}

			svc := newMockServiceWithStore(store)
			resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
				Name: "cloned-app",
				Component: []apisv1.CreateComponentRequest{{
					Name:          "new-mysql",
					ComponentType: config.StoreJob,
					Traits: apisv1.Traits{
						Resources: tt.overrideResources,
					},
					Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
				}},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)

			createdStore := store.components["new-mysql"]
			require.NotNil(t, createdStore)
			require.Equal(t, resp.ID, createdStore.AppID)

			var clonedTraits apisv1.Traits
			require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Traits)), &clonedTraits))
			require.Equal(t, tt.expectedResources, clonedTraits.Resources)
		})
	}
}

func TestCreateApplicationsFromTemplateMergesTargetWorkEnv(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-target-work-env", Name: "mysql", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:latest",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    mustJSONStruct(apisv1.Properties{}),
		Traits: mustJSONStruct(apisv1.Traits{
			TargetWorkEnv: map[string]string{
				"disk": "ssd",
				"zone": "east",
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-mysql",
			ComponentType: config.StoreJob,
			Traits: apisv1.Traits{
				TargetWorkEnv: map[string]string{
					"zone": "west",
					"gpu":  "true",
				},
			},
			Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	createdStore := store.components["new-mysql"]
	require.NotNil(t, createdStore)
	require.Equal(t, resp.ID, createdStore.AppID)

	var clonedTraits apisv1.Traits
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Traits)), &clonedTraits))
	require.Equal(t, map[string]string{
		"disk": "ssd",
		"zone": "west",
		"gpu":  "true",
	}, clonedTraits.TargetWorkEnv)
}
