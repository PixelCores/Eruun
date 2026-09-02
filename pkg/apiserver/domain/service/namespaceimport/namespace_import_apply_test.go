package namespaceimport

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestImportNamespaceResourcesPreservesEncodedSecretProvenance(t *testing.T) {
	const namespace = "tenant-a"

	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{}
	svc := &namespaceImportServiceImpl{
		KubeClient:         fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "backend-secret", Namespace: namespace}, Data: map[string][]byte{"password": []byte("secret-pwd")}}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		WorkflowRepo:       &mockWorkflowRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindSecrets},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, appService.createReqs, 1)
	require.Len(t, appService.createReqs[0].Component, 1)
	require.Equal(t, "backend-secret", appService.createReqs[0].Component[0].Name)
	require.Equal(t, "secret-pwd", appService.createReqs[0].Component[0].Properties.Secret["password"])
}

func TestImportNamespaceResources_PersistsObserveModeAtomically(t *testing.T) {
	const (
		namespace     = config.DefaultNamespace
		appID         = "26022513312d88jw"
		componentName = "backend"
	)
	deploymentName := "mahjongways2-" + appID + "-" + componentName

	props, err := model.NewJSONStructByStruct(apisv1.Properties{
		Labels: map[string]string{"app": componentName},
	})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.components[componentName] = &model.ApplicationComponent{
		ID:            101,
		AppID:         appID,
		Name:          componentName,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}

	workflowRepo := &namespaceImportWorkflowRepoStub{}
	appService := &namespaceImportAppServiceStub{
		listApps: []*apisv1.ApplicationBase{
			{
				ID:        appID,
				Name:      "mahjongways2-" + appID,
				Namespace: namespace,
			},
		},
	}
	store.apps[appID] = &model.Applications{
		ID:        appID,
		Name:      "mahjongways2-" + appID,
		Namespace: namespace,
	}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": componentName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		WorkflowRepo:       workflowRepo,
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Len(t, appService.createReqs, 1)

	assert.True(t, resp.Apps[0].WorkflowDisabled)
	assert.Equal(t, 1, resp.Summary.AppsApplied)
	assert.Equal(t, appID, appService.createReqs[0].ID)
	assert.True(t, appService.createReqs[0].ImportAsObserve)
	assert.Empty(t, workflowRepo.findByAppIDs, "workflow disabling belongs to the application transaction")
}

func TestImportNamespaceResources_ApplyResolvesEffectiveAppID(t *testing.T) {
	const (
		namespace     = config.DefaultNamespace
		parsedAppID   = "26022513312d88jw"
		componentName = "backend"
	)
	deploymentName := "mahjongways2-" + parsedAppID + "-" + componentName
	appName := "mahjongways2-" + parsedAppID

	cases := []struct {
		name             string
		listApps         []*apisv1.ApplicationBase
		generatedID      string
		wantCreateReqID  string
		wantEffectiveApp string
	}{
		{
			name:             "create missing app",
			generatedID:      "n9j4k1m7p2q8r5t6",
			wantCreateReqID:  "",
			wantEffectiveApp: "n9j4k1m7p2q8r5t6",
		},
		{
			name: "refresh existing app",
			listApps: []*apisv1.ApplicationBase{
				{
					ID:        "x2w4v6u8t0s1r3q5",
					Name:      appName,
					Namespace: namespace,
				},
			},
			wantCreateReqID:  "x2w4v6u8t0s1r3q5",
			wantEffectiveApp: "x2w4v6u8t0s1r3q5",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			for _, app := range tc.listApps {
				if app == nil {
					continue
				}
				store.apps[app.ID] = &model.Applications{
					ID:        app.ID,
					Name:      app.Name,
					Namespace: app.Namespace,
				}
			}
			workflowRepo := &namespaceImportWorkflowRepoStub{}
			appService := &namespaceImportAppServiceStub{
				listApps:    tc.listApps,
				generatedID: tc.generatedID,
			}
			svc := &namespaceImportServiceImpl{
				KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      deploymentName,
						Namespace: namespace,
					},
					Spec: appsv1.DeploymentSpec{
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": componentName},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{Name: componentName, Image: "nginx:1.27"},
								},
							},
						},
					},
				}),
				ApplicationService: appService,
				ValidationService:  NewValidationService(),
				AppRepo:            &mockAppRepo{store: store},
				WorkflowRepo:       workflowRepo,
				ComponentRepo:      &mockComponentRepo{store: store},
			}

			resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
				Namespace:    namespace,
				Mode:         importModeApply,
				IncludeKinds: []string{importKindDeployments},
			})
			require.NoError(t, err)
			require.Len(t, resp.Apps, 1)
			require.Len(t, resp.ResourceResults, 1)
			require.Len(t, appService.createReqs, 1)

			assert.Equal(t, tc.wantCreateReqID, appService.createReqs[0].ID)
			assert.Equal(t, tc.wantEffectiveApp, resp.Apps[0].AppID)
			assert.Equal(t, tc.wantEffectiveApp, resp.ResourceResults[0].AppID)
			assert.Empty(t, workflowRepo.findByAppIDs)

			deploy, getErr := svc.KubeClient.AppsV1().Deployments(namespace).Get(context.Background(), deploymentName, metav1.GetOptions{})
			require.NoError(t, getErr)
			assert.Equal(t, tc.wantEffectiveApp, deploy.Labels[config.LabelAppID])
			assert.Equal(t, tc.wantEffectiveApp, deploy.Spec.Template.Labels[config.LabelAppID])
		})
	}
}

func TestImportNamespaceResources_ApplyWithFilteredKindsPreservesExistingOmittedComponents(t *testing.T) {
	const (
		namespace     = config.DefaultNamespace
		parsedAppID   = "26022513312d88jw"
		componentName = "backend"
		existingAppID = "existing-app-id"
	)
	deploymentName := "mahjongways2-" + parsedAppID + "-" + componentName
	appName := "mahjongways2-" + parsedAppID

	props, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []domainspec.ServiceTraitSpec{
			{
				Name: "existing-config-svc",
				Selector: map[string]string{
					config.LabelAppID:         existingAppID,
					config.LabelComponentName: "existing-config",
					"app":                     "existing-config",
				},
				Labels: map[string]string{
					config.LabelManagedBy: config.ManagedByEruun,
					"owner":               "platform",
				},
				Ports: []domainspec.ServicePortTraitSpec{
					{Port: 80, TargetPort: 80},
				},
			},
		},
	})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.apps[existingAppID] = &model.Applications{
		ID:        existingAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components["existing-config"] = &model.ApplicationComponent{
		ID:            201,
		AppID:         existingAppID,
		Name:          "existing-config",
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        traits,
	}

	appService := &namespaceImportAppServiceStub{}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": componentName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, appService.createReqs, 1)
	require.Len(t, resp.Apps, 1)

	assert.Equal(t, existingAppID, appService.createReqs[0].ID)
	assert.Equal(t, 1, resp.Summary.ComponentsPlanned)
	assert.Equal(t, 1, resp.Summary.ComponentsApplied)
	componentNames := make([]string, 0, len(appService.createReqs[0].Component))
	for _, comp := range appService.createReqs[0].Component {
		componentNames = append(componentNames, comp.Name)
	}
	assert.Contains(t, componentNames, "existing-config")
	assert.Contains(t, componentNames, deploymentName)

	var preserved *apisv1.CreateComponentRequest
	for i := range appService.createReqs[0].Component {
		if appService.createReqs[0].Component[i].Name == "existing-config" {
			preserved = &appService.createReqs[0].Component[i]
			break
		}
	}
	require.NotNil(t, preserved)
	require.Len(t, preserved.Traits.Service, 1)
	assert.Equal(t, existingAppID, preserved.Traits.Service[0].Selector[config.LabelAppID])
	assert.Equal(t, "existing-config", preserved.Traits.Service[0].Selector[config.LabelComponentName])
	assert.Equal(t, "existing-config", preserved.Traits.Service[0].Selector["app"])
	assert.NotContains(t, preserved.Traits.Service[0].Labels, config.LabelManagedBy)
	assert.Equal(t, "platform", preserved.Traits.Service[0].Labels["owner"])
}

func TestImportNamespaceResources_ApplyWithFilteredKindsDropsStaleIncludedComponents(t *testing.T) {
	const (
		namespace      = config.DefaultNamespace
		parsedAppID    = "26022513312d88jw"
		existingAppID  = "existing-app-id"
		staleConfig    = "mahjongways2-26022513312d88jw-legacy-config"
		currentConfig  = "mahjongways2-26022513312d88jw-current-config"
		keepDeployment = "mahjongways2-26022513312d88jw-backend"
	)
	appName := "mahjongways2-" + parsedAppID

	props, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.apps[existingAppID] = &model.Applications{
		ID:        existingAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components[staleConfig] = &model.ApplicationComponent{
		ID:            301,
		AppID:         existingAppID,
		Name:          staleConfig,
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        traits,
	}
	store.components[currentConfig] = &model.ApplicationComponent{
		ID:            302,
		AppID:         existingAppID,
		Name:          currentConfig,
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        traits,
	}
	store.components[keepDeployment] = &model.ApplicationComponent{
		ID:            303,
		AppID:         existingAppID,
		Name:          keepDeployment,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}

	appService := &namespaceImportAppServiceStub{}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      currentConfig,
				Namespace: namespace,
			},
			Data: map[string]string{"key": "value"},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindConfigMaps},
	})
	require.NoError(t, err)
	require.Len(t, appService.createReqs, 1)
	require.Len(t, resp.Apps, 1)

	assert.Equal(t, existingAppID, appService.createReqs[0].ID)
	componentNames := make([]string, 0, len(appService.createReqs[0].Component))
	for _, comp := range appService.createReqs[0].Component {
		componentNames = append(componentNames, comp.Name)
	}
	assert.Contains(t, componentNames, currentConfig)
	assert.Contains(t, componentNames, keepDeployment)
	assert.NotContains(t, componentNames, staleConfig)
}

func TestImportNamespaceResources_ApplyWithTraitKindsKeepsExistingWorkloadComponents(t *testing.T) {
	const (
		namespace      = config.DefaultNamespace
		parsedAppID    = "26022513312d88jw"
		existingAppID  = "existing-app-id"
		staleConfig    = "mahjongways2-26022513312d88jw-legacy-config"
		currentConfig  = "mahjongways2-26022513312d88jw-current-config"
		keepDeployment = "mahjongways2-26022513312d88jw-backend"
	)
	appName := "mahjongways2-" + parsedAppID

	props, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	configTraits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)
	workloadTraits, err := model.NewJSONStructByStruct(apisv1.Traits{
		Service: []domainspec.ServiceTraitSpec{
			{
				Name: "backend-svc",
				Selector: map[string]string{
					"app": "backend",
				},
				Ports: []domainspec.ServicePortTraitSpec{
					{Port: 80, TargetPort: 80},
				},
			},
		},
	})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.apps[existingAppID] = &model.Applications{
		ID:        existingAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components[staleConfig] = &model.ApplicationComponent{
		ID:            401,
		AppID:         existingAppID,
		Name:          staleConfig,
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        configTraits,
	}
	store.components[currentConfig] = &model.ApplicationComponent{
		ID:            402,
		AppID:         existingAppID,
		Name:          currentConfig,
		Namespace:     namespace,
		ComponentType: config.ConfJob,
		Properties:    props,
		Traits:        configTraits,
	}
	store.components[keepDeployment] = &model.ApplicationComponent{
		ID:            403,
		AppID:         existingAppID,
		Name:          keepDeployment,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        workloadTraits,
	}

	appService := &namespaceImportAppServiceStub{}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      currentConfig,
				Namespace: namespace,
			},
			Data: map[string]string{"key": "value"},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindConfigMaps, importKindServices},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Lenf(t, appService.createReqs, 1, "app error: %s", resp.Apps[0].Error)

	assert.Equal(t, existingAppID, appService.createReqs[0].ID)
	componentNames := make([]string, 0, len(appService.createReqs[0].Component))
	for _, comp := range appService.createReqs[0].Component {
		componentNames = append(componentNames, comp.Name)
	}
	assert.Contains(t, componentNames, currentConfig)
	assert.Contains(t, componentNames, keepDeployment)
	assert.NotContains(t, componentNames, staleConfig)
}

func TestImportNamespaceResources_ApplyWithDeploymentsOnlyKeepsAmbiguousExistingServerJob(t *testing.T) {
	const (
		namespace       = config.DefaultNamespace
		parsedAppID     = "26022513312d88jw"
		existingAppID   = "existing-app-id"
		currentWorkload = "mahjongways2-26022513312d88jw-current"
		legacyWorkload  = "mahjongways2-26022513312d88jw-legacy"
	)
	appName := "mahjongways2-" + parsedAppID

	props, err := model.NewJSONStructByStruct(apisv1.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)

	store := newInMemoryAppStore()
	store.apps[existingAppID] = &model.Applications{
		ID:        existingAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components[currentWorkload] = &model.ApplicationComponent{
		ID:            501,
		AppID:         existingAppID,
		Name:          currentWorkload,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}
	store.components[legacyWorkload] = &model.ApplicationComponent{
		ID:            502,
		AppID:         existingAppID,
		Name:          legacyWorkload,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}

	appService := &namespaceImportAppServiceStub{}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      currentWorkload,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "current"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "current", Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Lenf(t, appService.createReqs, 1, "app error: %s", resp.Apps[0].Error)

	componentNames := make([]string, 0, len(appService.createReqs[0].Component))
	for _, comp := range appService.createReqs[0].Component {
		componentNames = append(componentNames, comp.Name)
	}
	assert.Contains(t, componentNames, currentWorkload)
	assert.Contains(t, componentNames, legacyWorkload)
}

func TestImportNamespaceResources_SelectorManagedAppIDMissingInDBFailsApply(t *testing.T) {
	const (
		namespace      = config.DefaultNamespace
		selectorAppID  = "a1b2c3d4e5f6g7h8"
		componentID    = "12"
		componentName  = "backend"
		deploymentName = "mahjongways2-26022513312d88jw-backend"
	)

	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{
		generatedID: "newgeneratedapp1234",
	}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						config.LabelAppID:       selectorAppID,
						config.LabelComponentID: componentID,
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							config.LabelAppID:         selectorAppID,
							config.LabelComponentID:   componentID,
							config.LabelComponentName: componentName,
							"app":                     componentName,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Len(t, resp.ResourceResults, 1)
	assert.Empty(t, appService.createReqs)
	assert.Equal(t, 0, resp.Summary.AppsApplied)
	assert.Contains(t, resp.Apps[0].Error, "selector-managed appID")
	assert.Contains(t, resp.Apps[0].Error, "not found in database")
	assert.Equal(t, importResourceStatusFailed, resp.ResourceResults[0].Status)
}

func TestImportNamespaceResources_SelectorManagedComponentIDUsesResolvedMetadataLabel(t *testing.T) {
	const (
		namespace      = config.DefaultNamespace
		selectorAppID  = "a1b2c3d4e5f6g7h8"
		oldComponentID = "12"
		newComponentID = 101
		componentName  = "backend"
		deploymentName = "mahjongways2-26022513312d88jw-backend"
		appName        = "mahjongways2-26022513312d88jw"
	)

	store := newInMemoryAppStore()
	props, err := model.NewJSONStructByStruct(apisv1.Properties{
		Labels: map[string]string{"app": componentName},
	})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(apisv1.Traits{})
	require.NoError(t, err)
	store.apps[selectorAppID] = &model.Applications{
		ID:        selectorAppID,
		Name:      appName,
		Namespace: namespace,
	}
	store.components["backend"] = &model.ApplicationComponent{
		ID:            newComponentID,
		AppID:         selectorAppID,
		Name:          componentName,
		Namespace:     namespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Properties:    props,
		Traits:        traits,
	}

	appService := &namespaceImportAppServiceStub{
		listApps: []*apisv1.ApplicationBase{
			{ID: selectorAppID, Name: appName, Namespace: namespace},
		},
	}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						config.LabelAppID:       selectorAppID,
						config.LabelComponentID: oldComponentID,
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							config.LabelAppID:         selectorAppID,
							config.LabelComponentID:   oldComponentID,
							config.LabelComponentName: componentName,
							"app":                     componentName,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Len(t, resp.ResourceResults, 1)
	require.Len(t, appService.createReqs, 1)

	assert.Equal(t, selectorAppID, appService.createReqs[0].ID)
	assert.Equal(t, importResourceStatusLabeled, resp.ResourceResults[0].Status)

	deploy, getErr := svc.KubeClient.AppsV1().Deployments(namespace).Get(context.Background(), deploymentName, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(t, "101", deploy.Labels[config.LabelComponentID])
	assert.Equal(t, oldComponentID, deploy.Spec.Template.Labels[config.LabelComponentID])
}

func TestImportNamespaceResources_ApplySharedRBACWithoutComponentsStillLabelsResources(t *testing.T) {
	const namespace = config.DefaultNamespace

	appService := &namespaceImportAppServiceStub{}
	store := newInMemoryAppStore()
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(
			&rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shared-role",
					Namespace: namespace,
				},
				Rules: []rbacv1.PolicyRule{{
					Verbs:     []string{"get"},
					Resources: []string{"pods"},
				}},
			},
			&rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shared-rolebinding",
					Namespace: namespace,
				},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "Role",
					Name:     "shared-role",
				},
				Subjects: []rbacv1.Subject{{
					Kind: rbacv1.ServiceAccountKind,
					Name: "missing-sa",
				}},
			},
		),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindRoles, importKindRoleBindings},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Len(t, resp.ResourceResults, 2)
	require.Len(t, appService.createReqs, 1)

	assert.Empty(t, resp.Apps[0].Error)
	assert.Equal(t, 1, resp.Summary.AppsApplied)
	assert.Equal(t, 2, resp.Summary.ResourcesLabeledSuccess)
	for _, item := range resp.ResourceResults {
		assert.Equal(t, importResourceStatusLabeled, item.Status)
	}
}

func TestImportNamespaceResources_FailsPlanWhenValidationServiceMissing(t *testing.T) {
	const (
		namespace     = config.DefaultNamespace
		componentName = "backend"
		deployment    = "mahjongways2-26022513312d88jw-backend"
	)

	appService := &namespaceImportAppServiceStub{}
	store := newInMemoryAppStore()
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deployment,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": componentName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Len(t, resp.ResourceResults, 1)

	assert.Empty(t, appService.createReqs)
	assert.Contains(t, resp.Apps[0].Error, "validation service is nil")
	assert.Equal(t, importResourceStatusFailed, resp.ResourceResults[0].Status)
}

func TestImportNamespaceResources_SelectorManagedAppIDNameMismatchFailsApply(t *testing.T) {
	const (
		namespace      = config.DefaultNamespace
		selectorAppID  = "a1b2c3d4e5f6g7h8"
		componentID    = "12"
		componentName  = "backend"
		deploymentName = "backend-service"
		existingName   = "existing-app-name"
	)

	store := newInMemoryAppStore()
	store.apps[selectorAppID] = &model.Applications{
		ID:        selectorAppID,
		Name:      existingName,
		Namespace: namespace,
	}
	appService := &namespaceImportAppServiceStub{}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						config.LabelAppID:       selectorAppID,
						config.LabelComponentID: componentID,
					},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							config.LabelAppID:         selectorAppID,
							config.LabelComponentID:   componentID,
							config.LabelComponentName: componentName,
							"app":                     componentName,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Len(t, resp.ResourceResults, 1)

	assert.Empty(t, appService.createReqs)
	assert.Contains(t, resp.Apps[0].Error, "selector-managed appID")
	assert.Contains(t, resp.Apps[0].Error, "refusing fallback rename")
	assert.Equal(t, importResourceStatusFailed, resp.ResourceResults[0].Status)
}

func TestImportNamespaceResources_ApplyUsesAppRepoFullIndexForExistingApp(t *testing.T) {
	const (
		namespace     = config.DefaultNamespace
		parsedAppID   = "26022513312d88jw"
		componentName = "backend"
	)

	deploymentName := "mahjongways2-" + parsedAppID + "-" + componentName
	targetName := "mahjongways2-" + parsedAppID
	targetID := "existing-target-id"

	store := newInMemoryAppStore()
	// Simulate a namespace with many apps where existing target app may not be present
	// in a paginated first page.
	for idx := 0; idx < 12; idx++ {
		id := fmt.Sprintf("existing-%d", idx)
		name := fmt.Sprintf("app-%d", idx)
		if idx == 11 {
			id = targetID
			name = targetName
		}
		store.apps[id] = &model.Applications{
			ID:        id,
			Name:      name,
			Namespace: namespace,
		}
	}

	appService := &namespaceImportAppServiceStub{}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": componentName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	require.Len(t, appService.createReqs, 1)

	assert.Equal(t, targetID, appService.createReqs[0].ID)
	assert.Equal(t, targetID, resp.Apps[0].AppID)
}

func TestLoadExistingAppIndex_RejectsDuplicateNamesInNamespace(t *testing.T) {
	const (
		namespace = config.DefaultNamespace
		appName   = "mahjongways2-26022513312d88jw"
	)

	store := newInMemoryAppStore()
	store.apps["existing-1"] = &model.Applications{
		ID:        "existing-1",
		Name:      appName,
		Namespace: namespace,
	}
	store.apps["existing-2"] = &model.Applications{
		ID:        "existing-2",
		Name:      appName,
		Namespace: namespace,
	}

	svc := &namespaceImportServiceImpl{
		AppRepo: &mockAppRepo{store: store},
	}

	_, _, _, err := svc.loadExistingAppIndex(context.Background(), namespace)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate application name")
	assert.Contains(t, err.Error(), appName)
	assert.Contains(t, err.Error(), namespace)
}

func TestImportNamespaceResources_ApplyReimportUsesStableImportAppKey(t *testing.T) {
	const (
		namespace     = config.DefaultNamespace
		parsedAppID   = "26022513312d88jw"
		componentName = "backend"
		generatedID   = "n9j4k1m7p2q8r5t6"
	)
	deploymentName := "mahjongways2-" + parsedAppID + "-" + componentName
	appName := "mahjongways2-" + parsedAppID

	firstStore := newInMemoryAppStore()
	firstSvc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": componentName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: &namespaceImportAppServiceStub{
			generatedID: generatedID,
		},
		ValidationService: NewValidationService(),
		AppRepo:           &mockAppRepo{store: firstStore},
		ComponentRepo:     &mockComponentRepo{store: firstStore},
	}

	_, err := firstSvc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)

	deploy, getErr := firstSvc.KubeClient.AppsV1().Deployments(namespace).Get(context.Background(), deploymentName, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(t, parsedAppID, deploy.Labels[config.LabelImportAppKey])
	assert.Equal(t, parsedAppID, deploy.Spec.Template.Labels[config.LabelImportAppKey])
	assert.Equal(t, generatedID, deploy.Labels[config.LabelAppID])

	secondAppSvc := &namespaceImportAppServiceStub{
		listApps: []*apisv1.ApplicationBase{
			{ID: generatedID, Name: appName, Namespace: namespace},
		},
	}
	secondStore := newInMemoryAppStore()
	secondStore.apps[generatedID] = &model.Applications{
		ID:        generatedID,
		Name:      appName,
		Namespace: namespace,
	}
	secondSvc := &namespaceImportServiceImpl{
		KubeClient:         firstSvc.KubeClient,
		ApplicationService: secondAppSvc,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: secondStore},
		ComponentRepo:      &mockComponentRepo{store: secondStore},
	}

	resp, err := secondSvc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, secondAppSvc.createReqs, 1)
	assert.Equal(t, generatedID, secondAppSvc.createReqs[0].ID)
	require.Len(t, resp.Apps, 1)
	assert.Equal(t, generatedID, resp.Apps[0].AppID)
}

func TestImportNamespaceResources_ApplyRecomputesIDForCollapsedAppNames(t *testing.T) {
	const (
		namespace   = config.DefaultNamespace
		generatedID = "generated-collapsed-id"
	)

	firstDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backend-one",
			Namespace: namespace,
			Labels: map[string]string{
				config.LabelImportAppKey: "AppA",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "backend-one"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "backend-one", Image: "nginx:1.27"},
					},
				},
			},
		},
	}
	secondDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backend-two",
			Namespace: namespace,
			Labels: map[string]string{
				config.LabelImportAppKey: "appa",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "backend-two"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "backend-two", Image: "nginx:1.27"},
					},
				},
			},
		},
	}

	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{
		generatedID:  generatedID,
		persistStore: store,
	}
	svc := &namespaceImportServiceImpl{
		KubeClient:         fake.NewSimpleClientset(firstDeployment, secondDeployment),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: append([]string(nil), allImportKinds...),
	})
	require.NoError(t, err)
	require.Len(t, appService.createReqs, 2)

	assert.Empty(t, appService.createReqs[0].ID)
	assert.Equal(t, generatedID, appService.createReqs[1].ID)

	emptyIDCount := 0
	for _, req := range appService.createReqs {
		if strings.TrimSpace(req.ID) == "" {
			emptyIDCount++
		}
	}
	assert.Equal(t, 1, emptyIDCount)
	secondReqComponentNames := make([]string, 0, len(appService.createReqs[1].Component))
	for _, component := range appService.createReqs[1].Component {
		secondReqComponentNames = append(secondReqComponentNames, component.Name)
	}
	assert.ElementsMatch(t, []string{"backend-one", "backend-two"}, secondReqComponentNames)

	require.Len(t, resp.Apps, 2)
	assert.Equal(t, generatedID, resp.Apps[0].AppID)
	assert.Equal(t, generatedID, resp.Apps[1].AppID)
}

func TestImportNamespaceResources_ApplyStripsReservedLabelsBeforeCreate(t *testing.T) {
	const (
		namespace     = config.DefaultNamespace
		parsedAppID   = "26022513312d88jw"
		componentName = "backend"
	)
	deploymentName := "mahjongways2-" + parsedAppID + "-" + componentName

	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{
		listApps: []*apisv1.ApplicationBase{
			{
				ID:        "existing-app-id",
				Name:      "mahjongways2-" + parsedAppID,
				Namespace: namespace,
			},
		},
	}
	store.apps["existing-app-id"] = &model.Applications{
		ID:        "existing-app-id",
		Name:      "mahjongways2-" + parsedAppID,
		Namespace: namespace,
	}
	svc := &namespaceImportServiceImpl{
		KubeClient: fake.NewSimpleClientset(&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deploymentName,
				Namespace: namespace,
				Labels: map[string]string{
					config.LabelAppID:         parsedAppID,
					config.LabelComponentName: deploymentName,
					config.LabelManagedBy:     config.ManagedByEruun,
					"team":                    "gaming",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app":                     componentName,
							config.LabelAppID:         parsedAppID,
							config.LabelComponentName: deploymentName,
							config.LabelManagedBy:     config.ManagedByEruun,
							"team":                    "gaming",
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: componentName, Image: "nginx:1.27"},
						},
					},
				},
			},
		}),
		ApplicationService: appService,
		ValidationService:  NewValidationService(),
		AppRepo:            &mockAppRepo{store: store},
		ComponentRepo:      &mockComponentRepo{store: store},
	}

	resp, err := svc.ImportNamespaceResources(context.Background(), apisv1.ImportNamespaceApplicationsRequest{
		Namespace:    namespace,
		Mode:         importModeApply,
		IncludeKinds: []string{importKindDeployments},
	})
	require.NoError(t, err)
	require.Len(t, appService.createReqs, 1)
	require.NotEmpty(t, appService.createReqs[0].Component)

	labels := appService.createReqs[0].Component[0].Properties.Labels
	assert.NotContains(t, labels, config.LabelManagedBy)
	assert.NotContains(t, labels, config.LabelAppID)
	assert.NotContains(t, labels, config.LabelComponentID)
	assert.NotContains(t, labels, config.LabelComponentName)
	assert.NotContains(t, labels, config.LabelShareName)
	assert.NotContains(t, labels, config.LabelShareStrategy)
	assert.Equal(t, "gaming", labels["team"])
	assert.Equal(t, 1, resp.Summary.AppsApplied)
}
