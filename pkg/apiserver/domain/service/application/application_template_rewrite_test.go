package application

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestCreateApplicationsFromTemplateKeepsDuplicateCloneRewritesPerClone(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-duplicate-clone", Name: "mysql", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name: "mysql",
			Type: string(spec.ServiceAccessInternal),
			Selector: map[string]string{
				config.LabelComponentName: "mysql",
				"role":                    "mysql",
			},
			Ports: []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)
	templateProps := apisv1.Properties{
		Labels: map[string]string{
			"role": "mysql",
		},
	}
	propsJSON, err := model.NewJSONStructByStruct(templateProps)
	require.NoError(t, err)
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:8",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "tenant-db",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "mysql",
				ComponentType: config.StoreJob,
				Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
			},
			{
				Name:          "mysql-1",
				ComponentType: config.StoreJob,
				Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	first := findComponentByAppAndName(store, resp.ID, "mysql")
	second := findComponentByAppAndName(store, resp.ID, "mysql-1")
	require.NotNil(t, first)
	require.NotNil(t, second)

	var firstTraits, secondTraits apisv1.Traits
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, first.Traits)), &firstTraits))
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, second.Traits)), &secondTraits))
	require.Len(t, firstTraits.Service, 1)
	require.Len(t, secondTraits.Service, 1)
	require.Equal(t, "mysql", firstTraits.Service[0].Selector[config.LabelComponentName])
	require.Equal(t, "mysql", firstTraits.Service[0].Selector["role"])
	require.Equal(t, "mysql-1", secondTraits.Service[0].Selector[config.LabelComponentName])
	require.Equal(t, "mysql-1", secondTraits.Service[0].Selector["role"])

	var firstProps, secondProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, first.Properties)), &firstProps))
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, second.Properties)), &secondProps))
	require.Equal(t, "mysql", firstProps.Labels["role"])
	require.Equal(t, "mysql-1", secondProps.Labels["role"])
}

func TestCreateApplicationsFromTemplateRejectsRewrittenServiceTraitNameTooLong(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-long-service", Name: "long-service-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	templateTraits := apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name:     "shared-database-service-name-that-is-already-long",
			Type:     string(spec.ServiceAccessInternal),
			Selector: map[string]string{"role": "db"},
			Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(templateTraits)
	require.NoError(t, err)
	propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)

	store.components["db"] = &model.ApplicationComponent{
		Name:          "db",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:latest",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}

	svc := newMockServiceWithStore(store)
	_, err = svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "tenant-alpha-game-db",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "game-db-mysql-component",
			ComponentType: config.StoreJob,
			Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "db"},
		}},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "must be at most 63 characters")
}

func TestCreateApplicationsFromTemplateRejectsRewrittenServiceTraitNameCollision(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-service-collision", Name: "mysql-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name:     "primary",
				Type:     string(spec.ServiceAccessInternal),
				Selector: map[string]string{"role": "primary"},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			},
			{
				Name:     "primary",
				Type:     string(spec.ServiceAccessInternal),
				Selector: map[string]string{"role": "primary"},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3307, TargetPort: 3306, Protocol: "TCP"}},
			},
		},
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
	_, err = svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "new-mysql",
			ComponentType: config.StoreJob,
			Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
		}},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate service name")
	require.Contains(t, err.Error(), "cloned-app-primary")
}

func TestCreateApplicationsFromTemplateRejectsRewrittenServiceTraitNameCollisionAcrossComponents(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-cross-service-collision", Name: "cross-service-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	addTemplateComponent := func(name, serviceName string) {
		traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
			Service: []spec.ServiceTraitSpec{{
				Name:     serviceName,
				Type:     string(spec.ServiceAccessInternal),
				Selector: map[string]string{"role": serviceName},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			}},
		})
		require.NoError(t, err)
		propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
		require.NoError(t, err)
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         templateApp.ID,
			Namespace:     config.DefaultNamespace,
			Image:         "mysql:latest",
			Replicas:      1,
			ComponentType: config.StoreJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
	}
	addTemplateComponent("foo", "primary")
	addTemplateComponent("foo-bar", "primary")

	svc := newMockServiceWithStore(store)
	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "foo", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "foo"}},
			{Name: "foo-bar", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "foo-bar"}},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate service name")
	require.Contains(t, err.Error(), "cloned-app-primary")
}

func TestCreateApplicationsRejectsResolvedServiceTraitNameCollisionWithNonTemplateComponent(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-request-service-collision", Name: "request-service-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name:     "primary",
			Type:     string(spec.ServiceAccessInternal),
			Selector: map[string]string{"role": "primary"},
			Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
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
	_, err = svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "worker",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{Service: []spec.ServiceTraitSpec{{
					Name:     "cloned-app-primary",
					Type:     string(spec.ServiceAccessInternal),
					Selector: map[string]string{"app": "worker"},
					Ports:    []spec.ServicePortTraitSpec{{Port: 8080, TargetPort: 8080, Protocol: "TCP"}},
				}}},
			},
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"}},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate service name")
	require.Contains(t, err.Error(), "cloned-app-primary")
	require.Contains(t, err.Error(), "worker")
	require.Contains(t, err.Error(), "new-mysql")
}

func TestCreateApplicationsRejectsResolvedServiceTraitNameCollisionAcrossTemplateIDs(t *testing.T) {
	store := newInMemoryAppStore()

	addTemplateComponent := func(templateID, componentName, serviceName string) {
		templateApp := &model.Applications{ID: templateID, Name: templateID, TemplateEnabled: true}
		store.apps[templateApp.ID] = templateApp
		traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
			Service: []spec.ServiceTraitSpec{{
				Name:     serviceName,
				Type:     string(spec.ServiceAccessInternal),
				Selector: map[string]string{"role": serviceName},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			}},
		})
		require.NoError(t, err)
		propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
		require.NoError(t, err)
		store.components[templateID+"-"+componentName] = &model.ApplicationComponent{
			Name:          componentName,
			AppID:         templateApp.ID,
			Namespace:     config.DefaultNamespace,
			Image:         "mysql:latest",
			Replicas:      1,
			ComponentType: config.StoreJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
	}
	addTemplateComponent("tmpl-request-service-collision-a", "mysql", "primary")
	addTemplateComponent("tmpl-request-service-collision-b", "other", "primary")

	svc := newMockServiceWithStore(store)
	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: "tmpl-request-service-collision-a", Target: "mysql"}},
			{Name: "new", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: "tmpl-request-service-collision-b", Target: "other"}},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate service name")
	require.Contains(t, err.Error(), "cloned-app-primary")
	require.Contains(t, err.Error(), "new-mysql")
	require.Contains(t, err.Error(), `"new"`)
}

func TestCreateApplicationsFromTemplateDoesNotRewriteBareServiceNameText(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-bare-service-text", Name: "mysql-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name: "mysql",
			Type: string(spec.ServiceAccessInternal),
			Selector: map[string]string{
				"role":                    "mysql",
				config.LabelComponentName: "mysql",
			},
			Ports: []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	})
	require.NoError(t, err)
	propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
		Env: map[string]string{
			"URL":      "mysql://mysql:3306/app",
			"DNS_HOST": "mysql.default.svc",
		},
		Command: []string{"sh", "-c", "mysql -h mysql && mysql -h mysql.default.svc.cluster.local"},
		Labels:  map[string]string{"role": "mysql"},
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
			Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
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
	require.Len(t, clonedTraits.Service, 1)
	require.Equal(t, "cloned-app-mysql", clonedTraits.Service[0].Name)
	require.Equal(t, "cloned-app-mysql", clonedTraits.Service[0].Selector["role"])
	require.Equal(t, "new-mysql", clonedTraits.Service[0].Selector[config.LabelComponentName])

	var clonedProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Properties)), &clonedProps))
	require.Equal(t, "mysql://mysql:3306/app", clonedProps.Env["URL"])
	require.Equal(t, "cloned-app-mysql.default.svc", clonedProps.Env["DNS_HOST"])
	require.Equal(t, []string{"sh", "-c", "mysql -h mysql && mysql -h cloned-app-mysql.default.svc.cluster.local"}, clonedProps.Command)
	require.Equal(t, "cloned-app-mysql", clonedProps.Labels["role"])
}

func TestCreateApplicationsFromTemplatePreservesUndeclaredIngressBackend(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-shared-ingress", Name: "shared-ingress-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Ingress: []spec.IngressTraitsSpec{{
			Name: "mysql",
			Routes: []spec.IngressRoutes{{
				Backend: spec.IngressRoute{ServiceName: "mysql-master"},
			}},
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
			Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
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
	require.Len(t, clonedTraits.Ingress, 1)
	require.Equal(t, "new-mysql", clonedTraits.Ingress[0].Name)
	require.Len(t, clonedTraits.Ingress[0].Routes, 1)
	require.Equal(t, "mysql-master", clonedTraits.Ingress[0].Routes[0].Backend.ServiceName)
}

func TestCreateApplicationsFromTemplateRejectsAmbiguousIngressBackendServiceReference(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-ambiguous-ingress-service", Name: "ambiguous-ingress-service", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	addServiceComponent := func(name string) {
		traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
			Service: []spec.ServiceTraitSpec{{
				Name:     "primary",
				Type:     string(spec.ServiceAccessPublic),
				Selector: map[string]string{"role": "primary"},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			}},
		})
		require.NoError(t, err)
		propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
		require.NoError(t, err)
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         templateApp.ID,
			Namespace:     config.DefaultNamespace,
			Image:         "mysql:latest",
			Replicas:      1,
			ComponentType: config.StoreJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
	}
	addServiceComponent("mysql")
	addServiceComponent("redis")

	gatewayTraitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Ingress: []spec.IngressTraitsSpec{{
			Name: "gateway",
			Routes: []spec.IngressRoutes{{
				Backend: spec.IngressRoute{ServiceName: "primary"},
			}},
		}},
	})
	require.NoError(t, err)
	gatewayPropsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	store.components["gateway"] = &model.ApplicationComponent{
		Name:          "gateway",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:latest",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    gatewayPropsJSON,
		Traits:        gatewayTraitsJSON,
	}

	svc := newMockServiceWithStore(store)
	_, err = svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"}},
			{Name: "new-redis", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "redis"}},
			{Name: "new-gateway", ComponentType: config.ServerJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "gateway"}},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "component new-gateway traits.ingress[0].routes[0].backend.serviceName")
	require.Contains(t, err.Error(), `ambiguous template service "primary"`)
	require.Contains(t, err.Error(), "new-mysql-primary")
	require.Contains(t, err.Error(), "new-redis-primary")
}

func TestCreateApplicationsFromTemplatePreservesAmbiguousServiceDNSInText(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-ambiguous-text-service", Name: "ambiguous-text-service", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	addServiceComponent := func(name string) {
		traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
			Service: []spec.ServiceTraitSpec{{
				Name:     "primary",
				Type:     string(spec.ServiceAccessPublic),
				Selector: map[string]string{"role": "primary"},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			}},
		})
		require.NoError(t, err)
		propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
		require.NoError(t, err)
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         templateApp.ID,
			Namespace:     config.DefaultNamespace,
			Image:         "mysql:latest",
			Replicas:      1,
			ComponentType: config.StoreJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
	}
	addServiceComponent("mysql")
	addServiceComponent("redis")

	gatewayPropsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
		Env:     map[string]string{"DB_HOST": "primary.default.svc"},
		Command: []string{"sh", "-c", "connect primary.default.svc.cluster.local"},
	})
	require.NoError(t, err)
	gatewayTraitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)
	store.components["gateway"] = &model.ApplicationComponent{
		Name:          "gateway",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:latest",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    gatewayPropsJSON,
		Traits:        gatewayTraitsJSON,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"}},
			{Name: "new-redis", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "redis"}},
			{Name: "new-gateway", ComponentType: config.ServerJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "gateway"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var createdGateway *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.Name == "new-gateway" {
			createdGateway = comp
			break
		}
	}
	require.NotNil(t, createdGateway)

	var clonedProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdGateway.Properties)), &clonedProps))
	require.Equal(t, "primary.default.svc", clonedProps.Env["DB_HOST"])
	require.Equal(t, []string{"sh", "-c", "connect primary.default.svc.cluster.local"}, clonedProps.Command)
}

func TestCreateApplicationsFromTemplateRewritesNamespaceQualifiedAmbiguousServiceDNS(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-ambiguous-source-ns-service", Name: "ambiguous-source-ns-service", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	addServiceComponent := func(name, namespace string) {
		traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
			Service: []spec.ServiceTraitSpec{{
				Name:     "primary",
				Type:     string(spec.ServiceAccessPublic),
				Selector: map[string]string{"role": "primary"},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			}},
		})
		require.NoError(t, err)
		propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
		require.NoError(t, err)
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         templateApp.ID,
			Namespace:     namespace,
			Image:         "mysql:latest",
			Replicas:      1,
			ComponentType: config.StoreJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
	}
	addServiceComponent("mysql", config.DefaultNamespace)
	addServiceComponent("redis", "redis-ns")

	gatewayPropsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
		Env: map[string]string{
			"MYSQL_HOST": "primary.default.svc.cluster.local",
			"REDIS_HOST": "primary.redis-ns.svc.cluster.local",
		},
		Command: []string{"sh", "-c", "connect primary.default.svc and primary.redis-ns.svc"},
	})
	require.NoError(t, err)
	gatewayTraitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)
	store.components["gateway"] = &model.ApplicationComponent{
		Name:          "gateway",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:latest",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    gatewayPropsJSON,
		Traits:        gatewayTraitsJSON,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"}},
			{Name: "new-redis", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "redis"}},
			{Name: "new-gateway", ComponentType: config.ServerJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "gateway"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var createdGateway *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.Name == "new-gateway" {
			createdGateway = comp
			break
		}
	}
	require.NotNil(t, createdGateway)

	var clonedProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdGateway.Properties)), &clonedProps))
	require.Equal(t, "new-mysql-primary.default.svc.cluster.local", clonedProps.Env["MYSQL_HOST"])
	require.Equal(t, "new-redis-primary.default.svc.cluster.local", clonedProps.Env["REDIS_HOST"])
	require.Equal(t, []string{"sh", "-c", "connect new-mysql-primary.default.svc and new-redis-primary.default.svc"}, clonedProps.Command)
}

func TestCreateApplicationsFromTemplateRewritesExternalNameServiceDNS(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-external-name-service-dns", Name: "external-name-service-dns", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	mysqlTraitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []spec.ServiceTraitSpec{{
			Name:     "mysql",
			Type:     string(spec.ServiceAccessInternal),
			Selector: map[string]string{"role": "mysql"},
			Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	})
	require.NoError(t, err)
	mysqlPropsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	store.components["mysql"] = &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:latest",
		Replicas:      1,
		ComponentType: config.StoreJob,
		Properties:    mysqlPropsJSON,
		Traits:        mysqlTraitsJSON,
	}

	proxyTraitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name:         "mysql-proxy",
				Type:         string(spec.ServiceAccessExternal),
				ExternalName: "mysql.default.svc.cluster.local",
				Ports:        []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			},
			{
				Name:         "external-api",
				Type:         string(spec.ServiceAccessExternal),
				ExternalName: "example.org",
				Ports:        []spec.ServicePortTraitSpec{{Port: 443, TargetPort: 443, Protocol: "TCP"}},
			},
		},
	})
	require.NoError(t, err)
	proxyPropsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	store.components["proxy"] = &model.ApplicationComponent{
		Name:          "proxy",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:latest",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    proxyPropsJSON,
		Traits:        proxyTraitsJSON,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"}},
			{Name: "new-proxy", ComponentType: config.ServerJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "proxy"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var createdProxy *model.ApplicationComponent
	for _, comp := range store.components {
		if comp.AppID == resp.ID && comp.Name == "new-proxy" {
			createdProxy = comp
			break
		}
	}
	require.NotNil(t, createdProxy)

	var clonedTraits apisv1.Traits
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdProxy.Traits)), &clonedTraits))
	require.Len(t, clonedTraits.Service, 2)
	require.Equal(t, "cloned-app-mysql.default.svc.cluster.local", clonedTraits.Service[0].ExternalName)
	require.Equal(t, "example.org", clonedTraits.Service[1].ExternalName)
}

func TestCreateApplicationsFromTemplateKeepsServiceNamesOutOfResourceReferenceRewrites(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-service-resource-overlap", Name: "service-resource-overlap", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	mysqlTraits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:       "secret-volume",
			Type:       config.StorageTypeSecret,
			MountPath:  "/etc/secret",
			SourceName: "primary",
		}},
		Service: []spec.ServiceTraitSpec{{
			Name:     "primary",
			Type:     string(spec.ServiceAccessInternal),
			Labels:   map[string]string{"role": "primary"},
			Selector: map[string]string{"role": "primary"},
			Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
		EnvFrom: []spec.EnvFromSourceSpec{{
			Type:       config.VolumeTypeSecret,
			SourceName: "primary",
		}},
		Envs: []spec.SimplifiedEnvSpec{{
			Name: "PASSWORD",
			ValueFrom: spec.ValueSource{
				Secret: &spec.SecretSelectorSpec{Name: "primary", Key: "PASSWORD"},
			},
		}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(mysqlTraits)
	require.NoError(t, err)
	propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
		Labels: map[string]string{"role": "primary"},
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

	secretPropsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
		Secret: map[string]string{"PASSWORD": "template-secret"},
	})
	require.NoError(t, err)
	store.components["primary"] = &model.ApplicationComponent{
		Name:          "primary",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties:    secretPropsJSON,
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"}},
			{Name: "new-primary-secret", ComponentType: config.SecretJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "primary"}},
		},
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
	require.Len(t, clonedTraits.Service, 1)
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[0].Name)
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[0].Labels["role"])
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[0].Selector["role"])
	require.Len(t, clonedTraits.Storage, 1)
	require.Equal(t, "new-primary-secret", clonedTraits.Storage[0].SourceName)
	require.Len(t, clonedTraits.EnvFrom, 1)
	require.Equal(t, "new-primary-secret", clonedTraits.EnvFrom[0].SourceName)
	require.Len(t, clonedTraits.Envs, 1)
	require.Equal(t, "new-primary-secret", clonedTraits.Envs[0].ValueFrom.Secret.Name)

	var clonedProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Properties)), &clonedProps))
	require.Equal(t, "cloned-app-primary", clonedProps.Labels["role"])
}

func TestCreateApplicationsFromTemplatePreservesUndeclaredResourceReferenceMatchingServiceName(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-service-resource-undeclared", Name: "service-resource-undeclared", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	staticValue := "primary"
	traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:       "secret-volume",
			Type:       config.StorageTypeSecret,
			MountPath:  "/etc/secret",
			SourceName: "primary",
		}},
		Service: []spec.ServiceTraitSpec{{
			Name:     "primary",
			Type:     string(spec.ServiceAccessInternal),
			Labels:   map[string]string{"role": "primary"},
			Selector: map[string]string{"role": "primary"},
			Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
		EnvFrom: []spec.EnvFromSourceSpec{{
			Type:       config.VolumeTypeSecret,
			SourceName: "primary",
		}},
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "primary", Key: "PASSWORD"},
				},
			},
			{
				Name: "CONFIG",
				ValueFrom: spec.ValueSource{
					Config: &spec.ConfigMapSelectorSpec{Name: "primary", Key: "CONFIG"},
				},
			},
			{
				Name: "STATIC",
				ValueFrom: spec.ValueSource{
					Static: &staticValue,
				},
			},
		},
	})
	require.NoError(t, err)
	propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
		Labels: map[string]string{"role": "primary"},
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
			Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"},
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
	require.Len(t, clonedTraits.Service, 1)
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[0].Name)
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[0].Labels["role"])
	require.Equal(t, "cloned-app-primary", clonedTraits.Service[0].Selector["role"])
	require.Len(t, clonedTraits.Storage, 1)
	require.Equal(t, "primary", clonedTraits.Storage[0].SourceName)
	require.Len(t, clonedTraits.EnvFrom, 1)
	require.Equal(t, "primary", clonedTraits.EnvFrom[0].SourceName)
	require.Len(t, clonedTraits.Envs, 3)
	require.Equal(t, "primary", clonedTraits.Envs[0].ValueFrom.Secret.Name)
	require.Equal(t, "primary", clonedTraits.Envs[1].ValueFrom.Config.Name)
	require.Equal(t, "primary", *clonedTraits.Envs[2].ValueFrom.Static)

	var clonedProps apisv1.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, createdStore.Properties)), &clonedProps))
	require.Equal(t, "cloned-app-primary", clonedProps.Labels["role"])
}

func TestCreateApplicationsFromTemplateScopesDuplicateServiceTraitNames(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-duplicate-service", Name: "duplicate-service-template", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	addTemplateComponent := func(name string) {
		traitsJSON, err := model.NewJSONStructByStruct(apisv1.Traits{
			Ingress: []spec.IngressTraitsSpec{{
				Name: name,
				Routes: []spec.IngressRoutes{{
					Backend: spec.IngressRoute{ServiceName: "primary"},
				}},
			}},
			Service: []spec.ServiceTraitSpec{{
				Name:     "primary",
				Type:     string(spec.ServiceAccessPublic),
				Selector: map[string]string{"role": "primary"},
				Labels:   map[string]string{"role": "primary"},
				Ports:    []spec.ServicePortTraitSpec{{Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
			}},
		})
		require.NoError(t, err)
		propsJSON, err := model.NewJSONStructByStruct(apisv1.Properties{
			Labels: map[string]string{"role": "primary"},
		})
		require.NoError(t, err)
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         templateApp.ID,
			Namespace:     config.DefaultNamespace,
			Image:         "mysql:latest",
			Replicas:      1,
			ComponentType: config.StoreJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
	}
	addTemplateComponent("mysql")
	addTemplateComponent("redis")

	svc := newMockServiceWithStore(store)
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "game-db",
		Component: []apisv1.CreateComponentRequest{
			{Name: "new-mysql", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "mysql"}},
			{Name: "new-redis", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "redis"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	created := make(map[string]*model.ApplicationComponent)
	for _, comp := range store.components {
		if comp.AppID == resp.ID {
			created[comp.Name] = comp
		}
	}
	require.Contains(t, created, "new-mysql")
	require.Contains(t, created, "new-redis")

	assertClonedComponent := func(componentName, serviceName string) {
		component := created[componentName]
		var traits apisv1.Traits
		require.NoError(t, json.Unmarshal([]byte(mustJSON(t, component.Traits)), &traits))
		require.Len(t, traits.Service, 1)
		require.Equal(t, serviceName, traits.Service[0].Name)
		require.Equal(t, serviceName, traits.Service[0].Selector["role"])
		require.Equal(t, serviceName, traits.Service[0].Labels["role"])
		require.Len(t, traits.Ingress, 1)
		require.Len(t, traits.Ingress[0].Routes, 1)
		require.Equal(t, serviceName, traits.Ingress[0].Routes[0].Backend.ServiceName)

		var props apisv1.Properties
		require.NoError(t, json.Unmarshal([]byte(mustJSON(t, component.Properties)), &props))
		require.Equal(t, serviceName, props.Labels["role"])
	}
	assertClonedComponent("new-mysql", "new-mysql-primary")
	assertClonedComponent("new-redis", "new-redis-primary")
}

func TestRewriteTemplateServiceNameUsesHyphenTokenBoundaries(t *testing.T) {
	require.Equal(t, "new-app", rewriteTemplateServiceName("app", "app", "new-app"))
	require.Equal(t, "new-mysql", rewriteTemplateServiceName("new-mysql", "mysql", "new-mysql"))
	require.Equal(t, "new-mysql-master", rewriteTemplateServiceName("new-mysql-master", "mysql", "new-mysql"))
	require.Equal(t, "new-mysql-master", rewriteTemplateServiceName("mysql-master", "mysql", "new-mysql"))
	require.Equal(t, "new-app-master", rewriteTemplateServiceName("app-master", "app", "new-app"))
	require.Equal(t, "svc-new-app", rewriteTemplateServiceName("svc-app", "app", "new-app"))
	require.Equal(t, "new-app-mapper", rewriteTemplateServiceName("mapper", "app", "new-app"))
	require.Equal(t, "new-mysql-primary", rewriteTemplateServiceName("primary", "mysql", "new-mysql"))
	require.Equal(t, "api-api", rewriteTemplateServiceName("api", "backend", "api"))
	require.Equal(t, "new-mysql-8-master", rewriteTemplateServiceName("new-mysql-8-master", "mysql-8", "new-mysql-8"))
	require.Equal(t, "new-mysql-8-master", rewriteTemplateServiceName("mysql-8-master", "mysql-8", "new-mysql-8"))
}

func TestRewriteTemplateServiceNameForTraitUsesAppNameForLiteralInternal(t *testing.T) {
	require.Equal(t, "game-db-mysql-master", rewriteTemplateServiceNameForTrait("mysql-master", "mysql-8", "game-db-mysql-8", "game-db", string(spec.ServiceAccessInternal)))
	require.Equal(t, "game-db-mysql", rewriteTemplateServiceNameForTrait("mysql", "mysql-8", "game-db-mysql-8", "game-db", string(spec.ServiceAccessInternal)))
	require.Equal(t, "game-db-mysql-8-mysql-master", rewriteTemplateServiceNameForTrait("mysql-master", "mysql-8", "game-db-mysql-8", "game-db", ""))
	require.Equal(t, "game-db-mysql-8-mysql-master", rewriteTemplateServiceNameForTrait("mysql-master", "mysql-8", "game-db-mysql-8", "game-db", string(corev1.ServiceTypeClusterIP)))
	require.Equal(t, "game-db-mysql-8-mysql-master", rewriteTemplateServiceNameForTrait("mysql-master", "mysql-8", "game-db-mysql-8", "game-db", string(spec.ServiceAccessPublic)))
}

func TestTemplateRewriteMapUsesReferenceBoundaries(t *testing.T) {
	rewriteMap := newTemplateRewriteMap()
	require.NoError(t, addTemplateServiceRewrite(rewriteMap, templateServiceRewrite{
		oldName:         "mysql-master",
		newName:         "new-mysql-master",
		sourceNamespace: config.DefaultNamespace,
	}, config.DefaultNamespace, true))
	require.NoError(t, rewriteMap.addServiceExact("primary", "new-primary"))
	require.NoError(t, rewriteMap.addExact("mysql-master", "new-mysql-master"))
	rewriteMap.refresh()

	require.Equal(t,
		"mysql -h new-mysql-master.default.svc.cluster.local && echo mysql-mastering primary-role mysql-master",
		rewriteMap.rewriteText("mysql -h mysql-master.default.svc.cluster.local && echo mysql-mastering primary-role mysql-master"),
	)
	require.Equal(t, "new-mysql-master", rewriteMap.rewriteValue("mysql-master"))
	require.Equal(t, "new-primary", rewriteMap.rewriteValue("primary"))
	_, ok := rewriteMap.exactValue("primary")
	require.False(t, ok)
	require.Equal(t, "mysql-master", rewriteMap.rewriteText("mysql-master"))
	require.Equal(t, "mysql-mastering", rewriteMap.rewriteValue("mysql-mastering"))
}

func TestRewritePropertiesForTemplateKeepsArbitraryExactServiceEnvValues(t *testing.T) {
	rewriteMap := newTemplateRewriteMap()
	require.NoError(t, addTemplateServiceRewrite(rewriteMap, templateServiceRewrite{
		oldName:         "mysql",
		newName:         "cloned-app-mysql",
		sourceNamespace: config.DefaultNamespace,
	}, config.DefaultNamespace, true))
	require.NoError(t, addTemplateServiceRewrite(rewriteMap, templateServiceRewrite{
		oldName:         "mysql-master",
		newName:         "cloned-app-mysql-master",
		sourceNamespace: config.DefaultNamespace,
	}, config.DefaultNamespace, true))
	rewriteMap.refresh()

	props := apisv1.Properties{
		Env: map[string]string{
			"MYSQL_DATABASE":   "mysql",
			"DB_HOST":          "mysql.default.svc",
			"MASTER_ROLE_NAME": "mysql-master",
		},
	}

	rewritePropertiesForTemplate(&props, rewriteMap)

	require.Equal(t, "mysql", props.Env["MYSQL_DATABASE"])
	require.Equal(t, "cloned-app-mysql.default.svc", props.Env["DB_HOST"])
	require.Equal(t, "cloned-app-mysql-master", props.Env["MASTER_ROLE_NAME"])
}

func TestRewriteServiceSelectorValuesRewritesServiceRefsAndPreservesMismatchedLabels(t *testing.T) {
	rewriteMap := newTemplateRewriteMap()
	require.NoError(t, addTemplateComponentNameRewrite(rewriteMap, "mysql", "new-mysql"))
	require.NoError(t, addTemplateServiceRewrite(rewriteMap, templateServiceRewrite{
		oldName:         "mysql",
		newName:         "cloned-app-mysql",
		sourceNamespace: config.DefaultNamespace,
	}, config.DefaultNamespace, true))
	require.NoError(t, addTemplateServiceRewrite(rewriteMap, templateServiceRewrite{
		oldName:         "mysql-master",
		newName:         "cloned-app-mysql-master",
		sourceNamespace: config.DefaultNamespace,
	}, config.DefaultNamespace, true))
	require.NoError(t, addTemplateServiceRewrite(rewriteMap, templateServiceRewrite{
		oldName:         "backend",
		newName:         "cloned-app-backend",
		sourceNamespace: config.DefaultNamespace,
	}, config.DefaultNamespace, true))
	rewriteMap.refresh()

	selector := map[string]string{
		"name":                    "mysql",
		"role":                    "backend",
		"tier":                    "frontend",
		"mysql-pod-role":          "mysql-master",
		config.LabelComponentName: "mysql",
	}
	originalPodLabels := map[string]string{
		"name": "mysql",
		"role": "gateway",
		"tier": "frontend",
	}
	rewrittenPodLabels := map[string]string{
		"name": "cloned-app-mysql",
		"role": "cloned-app-gateway",
		"tier": "cloned-app-frontend",
	}

	rewriteServiceSelectorValues(selector, rewriteMap, originalPodLabels, rewrittenPodLabels)

	require.Equal(t, "cloned-app-mysql", selector["name"])
	require.Equal(t, "backend", selector["role"])
	require.Equal(t, "cloned-app-frontend", selector["tier"])
	require.Equal(t, "cloned-app-mysql-master", selector["mysql-pod-role"])
	require.Equal(t, "new-mysql", selector[config.LabelComponentName])
}
