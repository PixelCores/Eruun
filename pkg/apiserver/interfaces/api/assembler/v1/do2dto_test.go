package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestConvertComponentModelToDTOStatusDefault(t *testing.T) {
	component := &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Equal(t, string(config.ComponentStatusNotDeploy), dto.Status)
	require.Nil(t, dto.ExternalLinks)
}

func TestConvertWorkflowStepsPreservesProperties(t *testing.T) {
	steps := &model.WorkflowSteps{
		FailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupAll,
		Steps: []*model.WorkflowStep{
			{
				Name:         "deploy-nginx",
				WorkflowType: config.JobDeploy,
				Properties: []model.Policies{
					{Policies: []string{"nginx"}},
				},
			},
			{
				Name:         "log-archive-upload",
				WorkflowType: config.JobLogArchiveUpload,
				Mode:         config.WorkflowModeStepByStep,
				Properties: []model.Policies{
					{Policies: []string{"api"}, Path: "/var/log/api", Container: "api"},
					{Policies: []string{"worker"}, Path: "/var/log/worker"},
				},
				SubSteps: []*model.WorkflowSubStep{
					{
						Name:         "archive-sidecar",
						WorkflowType: config.JobLogArchiveUpload,
						Properties: []model.Policies{
							{Policies: []string{"sidecar"}, Path: "/var/log/sidecar", Container: "logs"},
						},
					},
				},
			},
		},
	}
	raw, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	failurePolicy, details, err := convertWorkflowSteps(raw)
	require.NoError(t, err)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, failurePolicy)
	require.Len(t, details, 2)

	require.Equal(t, []string{"nginx"}, details[0].Components)
	require.Equal(t, []apisv1.WorkflowProperties{
		{Policies: []string{"nginx"}},
	}, details[0].Properties)
	require.Equal(t, []string{"api", "worker"}, details[1].Components)
	require.Equal(t, []apisv1.WorkflowProperties{
		{Policies: []string{"api"}, Path: "/var/log/api", Container: "api"},
		{Policies: []string{"worker"}, Path: "/var/log/worker"},
	}, details[1].Properties)
	require.Len(t, details[1].SubSteps, 1)
	require.Equal(t, []string{"sidecar"}, details[1].SubSteps[0].Components)
	require.Equal(t, []apisv1.WorkflowProperties{
		{Policies: []string{"sidecar"}, Path: "/var/log/sidecar", Container: "logs"},
	}, details[1].SubSteps[0].Properties)
}

func TestConvertWorkflowStepsDefaultsMissingFailurePolicyToCleanupAll(t *testing.T) {
	raw, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "deploy-nginx",
			WorkflowType: config.JobDeploy,
			Properties:   []model.Policies{{Policies: []string{"nginx"}}},
		}},
	})
	require.NoError(t, err)

	failurePolicy, details, err := convertWorkflowSteps(raw)
	require.NoError(t, err)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, failurePolicy)
	require.Len(t, details, 1)
}

func TestConvertComponentModelToDTOOmitsLegacySecretMeta(t *testing.T) {
	propsJSON, err := model.NewJSONStructByStruct(&model.Properties{
		Secret: map[string]string{"password": "secret-pwd"},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "db-secret",
		Namespace:     "default",
		ComponentType: config.SecretJob,
		Properties:    propsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"password": "secret-pwd"}, dto.Properties.Secret)
}

func TestConvertComponentModelToDTOIngressLinks(t *testing.T) {
	traits := model.Traits{
		Ingress: []model.IngressTraitsSpec{
			{
				Routes: []model.IngressRoutes{
					{Host: "example.com", Path: "/"},
					{Host: "example.com", Path: "/api"},
				},
			},
		},
	}
	properties := model.Properties{
		Ports: []model.Ports{{Port: 80}},
	}
	traitsJSON, err := model.NewJSONStructByStruct(&traits)
	require.NoError(t, err)
	propsJSON, err := model.NewJSONStructByStruct(&properties)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            2,
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    propsJSON,
		Traits:        traitsJSON,
		Status:        "Running",
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Equal(t, "Running", dto.Status)
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "ingress", Value: "example.com/"},
		{Type: "ingress", Value: "example.com/api"},
	}, dto.ExternalLinks)
}

func TestConvertComponentModelToDTOExpandsIngressLevelHosts(t *testing.T) {
	traits := model.Traits{
		Ingress: []model.IngressTraitsSpec{
			{
				Hosts: []string{" api.example.com ", "admin.example.com", " "},
				Routes: []model.IngressRoutes{
					{
						Path: "/v1",
						Backend: model.IngressRoute{
							ServiceName: "api-svc",
							ServicePort: 8080,
						},
					},
					{
						Host: "route.example.com",
						Path: "/route",
						Backend: model.IngressRoute{
							ServiceName: "route-svc",
							ServicePort: 9090,
						},
					},
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(&traits)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            3,
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Len(t, dto.Ingresses, 1)
	require.Equal(t, []apisv1.ComponentIngressRouteInfo{
		{Host: "api.example.com", Path: "/v1", PathType: "Prefix", ServiceName: "api-svc", ServicePort: 8080},
		{Host: "admin.example.com", Path: "/v1", PathType: "Prefix", ServiceName: "api-svc", ServicePort: 8080},
		{Host: "route.example.com", Path: "/route", PathType: "Prefix", ServiceName: "route-svc", ServicePort: 9090},
	}, dto.Ingresses[0].Routes)
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "ingress", Value: "api.example.com/v1"},
		{Type: "ingress", Value: "admin.example.com/v1"},
		{Type: "ingress", Value: "route.example.com/route"},
	}, dto.ExternalLinks)
	require.Equal(t, naming.IngressName("backend-ingress", "app-1"), dto.Ingresses[0].Name)
	require.Equal(t, "default", dto.Ingresses[0].Namespace)
}

func TestConvertComponentModelToDTOUsesDeployedIngressIdentity(t *testing.T) {
	traits := model.Traits{
		Ingress: []model.IngressTraitsSpec{
			{
				Name:      "Custom Route",
				Namespace: "custom-ns",
				Routes: []model.IngressRoutes{
					{Host: "example.com", Path: "/"},
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(&traits)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            4,
		AppID:         "App-1",
		Name:          "Gateway",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Len(t, dto.Ingresses, 1)
	require.Equal(t, naming.IngressName("Custom Route", "App-1"), dto.Ingresses[0].Name)
	require.Equal(t, "default", dto.Ingresses[0].Namespace)
}

func TestConvertComponentModelToDTOUsesDefaultNamespaceForBlankComponentIngress(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Ingress: []model.IngressTraitsSpec{
			{
				Namespace: "custom-ns",
				Routes: []model.IngressRoutes{
					{Host: "example.com", Path: "/"},
				},
			},
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            5,
		AppID:         "app-1",
		Name:          "gateway",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Len(t, dto.Ingresses, 1)
	require.Equal(t, config.DefaultNamespace, dto.Ingresses[0].Namespace)
}

func TestConvertComponentModelToDTOSkipsIngressForUnsupportedComponentTypes(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(&model.Traits{
		Ingress: []model.IngressTraitsSpec{
			{
				Routes: []model.IngressRoutes{
					{Host: "config.example.com", Path: "/"},
				},
			},
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            6,
		AppID:         "app-1",
		Name:          "app-config",
		Namespace:     "default",
		ComponentType: config.ConfJob,
		Traits:        traitsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Empty(t, dto.Ingresses)
	require.Empty(t, dto.ExternalLinks)
}

func TestConvertComponentModelToDTOSvcLinks(t *testing.T) {
	properties := model.Properties{
		Ports: []model.Ports{{Port: 80}, {Port: 443}},
	}
	propsJSON, err := model.NewJSONStructByStruct(&properties)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            3,
		AppID:         "app-9",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    propsJSON,
		Status:        "Running",
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Equal(t, "Running", dto.Status)
	expectedName := naming.ServiceName("api", "app-9")
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "svc", Value: expectedName + ".default.svc:80,443"},
	}, dto.ExternalLinks)
}

func TestConvertComponentModelToDTOUsesResourceAppNameForGeneratedSvcLinks(t *testing.T) {
	properties := model.Properties{
		Ports: []model.Ports{{Port: 9090}},
	}
	propsJSON, err := model.NewJSONStructByStruct(&properties)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:              3,
		AppID:           "opaque-app-id",
		ResourceAppName: "m2605081521cctqpk",
		Name:            "proxy",
		Namespace:       "paas-game-review",
		ComponentType:   config.ServerJob,
		Properties:      propsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)

	expectedName := naming.ServiceName("proxy", "m2605081521cctqpk")
	require.Len(t, dto.Services, 1)
	require.Equal(t, expectedName, dto.Services[0].Name)
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "svc", Value: expectedName + ".paas-game-review.svc:9090"},
	}, dto.ExternalLinks)
}

func TestConvertComponentModelToDTOUsesSharedResourceKeyForGeneratedNames(t *testing.T) {
	properties := mustJSONStruct(t, model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	traits := mustJSONStruct(t, model.Traits{
		Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)},
		Ingress: []spec.IngressTraitsSpec{
			{
				Routes: []spec.IngressRoutes{
					{Host: "web.example.com", Path: "/"},
				},
			},
		},
	})

	component := &model.ApplicationComponent{
		ID:              4,
		AppID:           "opaque-app-id",
		ResourceAppName: "demo",
		Name:            "web",
		Namespace:       "default",
		ComponentType:   config.ServerJob,
		Properties:      properties,
		Traits:          traits,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)

	require.Len(t, dto.Services, 1)
	require.Equal(t, "web", dto.Services[0].Name)
	require.Len(t, dto.Ingresses, 1)
	require.Equal(t, "web-ingress", dto.Ingresses[0].Name)
	require.Equal(t, "web", dto.Ingresses[0].Routes[0].ServiceName)
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "ingress", Value: "web.example.com/"},
	}, dto.ExternalLinks)
}

func TestConvertComponentModelToDTOUsesExplicitServiceTraitNameForSvcLink(t *testing.T) {
	properties := model.Properties{
		Ports: []model.Ports{{Port: 80}},
	}
	traits := model.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "api-fixed",
				Type: string(spec.ServiceAccessInternal),
				Ports: []spec.ServicePortTraitSpec{
					{Port: 80, TargetPort: 80, Protocol: "TCP"},
				},
			},
		},
	}
	propsJSON, err := model.NewJSONStructByStruct(&properties)
	require.NoError(t, err)
	traitsJSON, err := model.NewJSONStructByStruct(&traits)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            4,
		AppID:         "app-9",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "svc", Value: "api-fixed.default.svc:80"},
	}, dto.ExternalLinks)
}

func TestConvertComponentModelToDTOPrefersNonExternalServiceTraitForSvcLink(t *testing.T) {
	properties := model.Properties{
		Ports: []model.Ports{{Port: 80}},
	}
	traits := model.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name:         "external-api",
				Type:         string(spec.ServiceAccessExternal),
				ExternalName: "example.com",
			},
			{
				Name: "internal-api",
				Type: string(spec.ServiceAccessInternal),
				Ports: []spec.ServicePortTraitSpec{
					{Port: 80, TargetPort: 80, Protocol: "TCP"},
				},
			},
		},
	}
	propsJSON, err := model.NewJSONStructByStruct(&properties)
	require.NoError(t, err)
	traitsJSON, err := model.NewJSONStructByStruct(&traits)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            5,
		AppID:         "app-9",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Equal(t, []apisv1.ExternalLink{
		{Type: "svc", Value: "internal-api.default.svc:80"},
	}, dto.ExternalLinks)
}

func TestConvertComponentModelToDTOSkipsServiceSummariesForNonServiceDeployComponents(t *testing.T) {
	properties := mustJSONStruct(t, model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	traits := mustJSONStruct(t, model.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "job-svc",
				Type: string(spec.ServiceAccessInternal),
				Ports: []spec.ServicePortTraitSpec{
					{Port: 80, TargetPort: 8080},
				},
			},
		},
	})

	testCases := []struct {
		name          string
		componentType config.JobType
	}{
		{name: "instant job", componentType: config.InstantJob},
		{name: "scheduled job", componentType: config.ScheduledJob},
		{name: "cloud job", componentType: config.CloudJob},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			component := &model.ApplicationComponent{
				ID:            6,
				AppID:         "app-9",
				Name:          "job-runner",
				Namespace:     "default",
				ComponentType: tc.componentType,
				Properties:    properties,
				Traits:        traits,
			}

			dto, err := ConvertComponentModelToDTO(component)
			require.NoError(t, err)
			require.Nil(t, dto.Services)
			require.Nil(t, dto.ExternalLinks)
		})
	}
}

func TestConvertComponentModelToDTOAppliesIngressRouteDefaults(t *testing.T) {
	traits := mustJSONStruct(t, model.Traits{
		Ingress: []spec.IngressTraitsSpec{
			{
				DefaultPathType: "Exact",
				Routes: []spec.IngressRoutes{
					{Host: "api.example.com"},
				},
			},
		},
	})
	component := &model.ApplicationComponent{
		ID:            7,
		AppID:         "app-9",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits:        traits,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Len(t, dto.Ingresses, 1)
	require.Equal(t, []apisv1.ComponentIngressRouteInfo{
		{
			Host:        "api.example.com",
			Path:        "/",
			PathType:    "Exact",
			ServiceName: naming.ServiceName("api", "app-9"),
			ServicePort: 80,
		},
	}, dto.Ingresses[0].Routes)
}

func TestConvertComponentModelToDTOAppliesIngressRewriteDefaultsToSummary(t *testing.T) {
	properties := mustJSONStruct(t, model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	traits := mustJSONStruct(t, model.Traits{
		Ingress: []spec.IngressTraitsSpec{
			{
				Annotations: map[string]string{
					"example.com/custom": "keep",
				},
				Routes: []spec.IngressRoutes{
					{
						Host: "api.example.com",
						Path: "/api(/.*)",
						Rewrite: &spec.RewritePolicy{
							Type:        "regexReplace",
							Replacement: "/$1",
						},
					},
				},
			},
		},
	})
	component := &model.ApplicationComponent{
		ID:            8,
		AppID:         "app-9",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    properties,
		Traits:        traits,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Len(t, dto.Ingresses, 1)
	require.Equal(t, map[string]string{
		"example.com/custom":                         "keep",
		"nginx.ingress.kubernetes.io/rewrite-target": "/$1",
		"nginx.ingress.kubernetes.io/use-regex":      "true",
	}, dto.Ingresses[0].Annotations)
	require.Equal(t, []apisv1.ComponentIngressRouteInfo{
		{
			Host:        "api.example.com",
			Path:        "/api(/.*)",
			PathType:    "ImplementationSpecific",
			ServiceName: naming.ServiceName("api", "app-9"),
			ServicePort: 8080,
			Rewrite: &spec.RewritePolicy{
				Type:        "regexReplace",
				Replacement: "/$1",
			},
		},
	}, dto.Ingresses[0].Routes)
}

func TestConvertComponentModelToDTOLastAbnormal(t *testing.T) {
	component := &model.ApplicationComponent{
		ID:           9,
		AppID:        "app-9",
		Name:         "api",
		Namespace:    "default",
		LastAbnormal: "container=api reason=CrashLoopBackOff",
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Equal(t, component.LastAbnormal, dto.LastAbnormal)
}

func TestConvertComponentModelToDTOPopulatesSidecars(t *testing.T) {
	traits := model.Traits{
		Sidecar: []model.SidecarSpec{
			{
				Name:    "log-agent",
				Image:   "vector:0.36",
				Command: []string{"vector"},
				Args:    []string{"--config", "/etc/vector/vector.yaml"},
				Env: map[string]string{
					"VECTOR_LOG": "info",
				},
			},
		},
	}
	traitsJSON, err := model.NewJSONStructByStruct(&traits)
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            10,
		AppID:         "app-10",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}

	dto, err := ConvertComponentModelToDTO(component)
	require.NoError(t, err)
	require.Len(t, dto.Sidecars, 1)
	require.Equal(t, "log-agent", dto.Sidecars[0].Name)
	require.Equal(t, "vector:0.36", dto.Sidecars[0].Image)
	require.Equal(t, []string{"vector"}, dto.Sidecars[0].Command)
	require.Equal(t, []string{"--config", "/etc/vector/vector.yaml"}, dto.Sidecars[0].Args)
	require.Equal(t, map[string]string{"VECTOR_LOG": "info"}, dto.Sidecars[0].Env)
	require.Equal(t, dto.Traits.Sidecar, dto.Sidecars)
}

func TestConvertComponentModelsToDTOAddsResourceDetailsAndCredentials(t *testing.T) {
	secretProps := mustJSONStruct(t, model.Properties{
		Secret: map[string]string{
			"password": "secret-pwd",
			"username": "root",
		},
	})
	apiProps := mustJSONStruct(t, model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	apiTraits := mustJSONStruct(t, model.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "api-svc",
				Type: string(spec.ServiceAccessInternal),
				Ports: []spec.ServicePortTraitSpec{
					{Name: "http", Port: 80, TargetPort: 8080, Protocol: "TCP"},
				},
			},
		},
		Ingress: []spec.IngressTraitsSpec{
			{
				Routes: []spec.IngressRoutes{
					{Host: "api.example.com", Path: "/api"},
				},
			},
		},
		Resources: &spec.ResourceTraitsSpec{CPU: "500m", Memory: "256Mi", CPULimit: "750m", MemoryLimit: "512Mi"},
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "DB_PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
				},
			},
		},
		Init: []spec.InitTraitSpec{
			{
				Name: "init-db",
				Traits: spec.Traits{
					Storage: []spec.StorageTraitSpec{
						{Name: "db-secret", Type: config.StorageTypeSecret, MountPath: "/secrets"},
					},
				},
			},
		},
		Sidecar: []spec.SidecarTraitsSpec{
			{
				Name:  "metrics",
				Image: "metrics:1.0",
				Traits: spec.Traits{
					Resources: &spec.ResourceTraitsSpec{CPU: "100m"},
					EnvFrom: []spec.EnvFromSourceSpec{
						{Type: "secret", SourceName: "db-secret"},
					},
				},
			},
		},
	})

	dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "db-secret",
			Namespace:     "default",
			ComponentType: config.SecretJob,
			Properties:    secretProps,
		},
		{
			ID:            2,
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Properties:    apiProps,
			Traits:        apiTraits,
		},
	})
	require.NoError(t, err)

	api := requireComponentByName(t, dtos, "api")
	require.Equal(t, []apisv1.ComponentServiceInfo{
		{
			Name:      "api-svc",
			Namespace: "default",
			Type:      string(spec.ServiceAccessInternal),
			Ports: []apisv1.ComponentServicePortInfo{
				{Name: "http", Port: 80, TargetPort: 8080, Protocol: "TCP"},
			},
		},
	}, api.Services)
	require.Len(t, api.Ingresses, 1)
	require.Equal(t, naming.IngressName("api-ingress", "app-1"), api.Ingresses[0].Name)
	require.Equal(t, "default", api.Ingresses[0].Namespace)
	require.Equal(t, []apisv1.ComponentIngressRouteInfo{
		{Host: "api.example.com", Path: "/api", PathType: "Prefix", ServiceName: "api-svc", ServicePort: 80},
	}, api.Ingresses[0].Routes)
	require.ElementsMatch(t, []apisv1.ComponentResourceConfig{
		{Scope: "main", Name: "api", CPU: "500m", Memory: "256Mi", CPULimit: "750m", MemoryLimit: "512Mi"},
		{Scope: "sidecar", Name: "metrics", CPU: "100m"},
	}, api.ResourceConfigs)
	require.ElementsMatch(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Value: "secret-pwd", Resolved: true},
		{Source: "component.init[init-db].storage", SecretName: "db-secret", Key: "password", Value: "secret-pwd", Resolved: true},
		{Source: "component.init[init-db].storage", SecretName: "db-secret", Key: "username", Value: "root", Resolved: true},
		{Source: "component.sidecar[metrics].envFrom", SecretName: "db-secret", Key: "password", Value: "secret-pwd", Resolved: true},
		{Source: "component.sidecar[metrics].envFrom", SecretName: "db-secret", Key: "username", Value: "root", Resolved: true},
	}, api.Credentials)
}

func TestConvertComponentModelsToDTOReportsUnresolvedCredentials(t *testing.T) {
	secretProps := mustJSONStruct(t, model.Properties{
		Secret: map[string]string{"username": "root"},
	})
	apiTraits := mustJSONStruct(t, model.Traits{
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "DB_PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
				},
			},
			{
				Name: "API_TOKEN",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "missing-secret", Key: "token"},
				},
			},
		},
	})

	dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "db-secret",
			Namespace:     "default",
			ComponentType: config.SecretJob,
			Properties:    secretProps,
		},
		{
			ID:            2,
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Traits:        apiTraits,
		},
	})
	require.NoError(t, err)

	api := requireComponentByName(t, dtos, "api")
	require.ElementsMatch(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Resolved: false},
		{Source: "component.envs", EnvName: "API_TOKEN", SecretName: "missing-secret", Key: "token", Resolved: false},
	}, api.Credentials)
}

func TestConvertComponentModelsToDTOTreatsEmptyCredentialValuesAsUnresolved(t *testing.T) {
	secretProps := mustJSONStruct(t, model.Properties{
		Secret: map[string]string{
			"password": "",
			"username": "root",
		},
	})
	apiTraits := mustJSONStruct(t, model.Traits{
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "DB_PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
				},
			},
		},
		EnvFrom: []spec.EnvFromSourceSpec{
			{
				Type:       config.StorageTypeSecret,
				SourceName: "db-secret",
			},
		},
	})

	dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "db-secret",
			Namespace:     "default",
			ComponentType: config.SecretJob,
			Properties:    secretProps,
		},
		{
			ID:            2,
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Traits:        apiTraits,
		},
	})
	require.NoError(t, err)

	api := requireComponentByName(t, dtos, "api")
	require.ElementsMatch(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Resolved: false},
		{Source: "component.envFrom", SecretName: "db-secret", Key: "password", Resolved: false},
		{Source: "component.envFrom", SecretName: "db-secret", Key: "username", Value: "root", Resolved: true},
	}, api.Credentials)
}

func TestConvertComponentModelsToDTOSkipsWholeSecretCredentialsWhenSecretHasNoKeys(t *testing.T) {
	secretProps := mustJSONStruct(t, model.Properties{
		Secret: map[string]string{},
	})
	apiTraits := mustJSONStruct(t, model.Traits{
		EnvFrom: []spec.EnvFromSourceSpec{
			{
				Type:       config.StorageTypeSecret,
				SourceName: "db-secret",
			},
		},
	})

	dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "db-secret",
			Namespace:     "default",
			ComponentType: config.SecretJob,
			Properties:    secretProps,
		},
		{
			ID:            2,
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Traits:        apiTraits,
		},
	})
	require.NoError(t, err)

	api := requireComponentByName(t, dtos, "api")
	require.Nil(t, api.Credentials)
}

func TestConvertComponentModelsToDTOSkipsEncodedCredentialValues(t *testing.T) {
	secretProps := mustJSONStruct(t, model.Properties{
		Secret: map[string]string{"password": "c2VjcmV0LXB3ZA=="},
	})
	apiTraits := mustJSONStruct(t, model.Traits{
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "DB_PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
				},
			},
		},
	})

	dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "db-secret",
			Namespace:     "default",
			ComponentType: config.SecretJob,
			Properties:    secretProps,
		},
		{
			ID:            2,
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Traits:        apiTraits,
		},
	})
	require.NoError(t, err)

	api := requireComponentByName(t, dtos, "api")
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Value: "c2VjcmV0LXB3ZA==", Resolved: true},
	}, api.Credentials)
}

func TestConvertComponentModelsToDTOKeepsManualBase64LikeValuesResolved(t *testing.T) {
	secretProps := mustJSONStruct(t, model.Properties{
		Secret: map[string]string{"password": "dGVzdA=="},
	})
	apiTraits := mustJSONStruct(t, model.Traits{
		Envs: []spec.SimplifiedEnvSpec{
			{
				Name: "DB_PASSWORD",
				ValueFrom: spec.ValueSource{
					Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
				},
			},
		},
	})

	dtos, err := ConvertComponentModelsToDTO([]*model.ApplicationComponent{
		{
			ID:            1,
			AppID:         "app-1",
			Name:          "db-secret",
			Namespace:     "default",
			ComponentType: config.SecretJob,
			Properties:    secretProps,
		},
		{
			ID:            2,
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Traits:        apiTraits,
		},
	})
	require.NoError(t, err)

	api := requireComponentByName(t, dtos, "api")
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Value: "dGVzdA==", Resolved: true},
	}, api.Credentials)
}

func mustJSONStruct(t *testing.T, value interface{}) *model.JSONStruct {
	t.Helper()
	result, err := model.NewJSONStructByStruct(value)
	require.NoError(t, err)
	return result
}

func requireComponentByName(t *testing.T, components []*apisv1.ApplicationComponent, name string) *apisv1.ApplicationComponent {
	t.Helper()
	for _, component := range components {
		if component != nil && component.Name == name {
			return component
		}
	}
	require.Failf(t, "component not found", "name=%s", name)
	return nil
}
