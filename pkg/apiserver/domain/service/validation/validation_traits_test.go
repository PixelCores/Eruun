package validation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestValidationService_TryApplication_InvalidProbeConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Probes: []spec.ProbeTraitsSpec{
						{
							Type: "liveness",
							// Missing probe method (exec, httpGet, or tcpSocket)
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to invalid probe config")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidProbeConfig {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid probe config error")
}

func TestValidationService_TryApplication_ValidProbeConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Probes: []spec.ProbeTraitsSpec{
						{
							Type: "liveness",
							HTTPGet: &spec.HTTPGetProbe{
								Path: "/health",
								Port: 8080,
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	// Should not have probe-related errors
	for _, err := range resp.Errors {
		assert.NotEqual(t, apisv1.ErrCodeInvalidProbeConfig, err.Code, "Should not have probe config error")
	}
}

func TestValidationService_TryApplication_ResourcesLimitMustNotBeBelowRequest(t *testing.T) {
	testCases := []struct {
		name          string
		resources     spec.ResourceTraitsSpec
		wantValid     bool
		expectedField string
	}{
		{
			name:          "cpu limit below request",
			resources:     spec.ResourceTraitsSpec{CPU: "500m", CPULimit: "300m"},
			wantValid:     false,
			expectedField: "component[0].traits.resources.cpuLimit",
		},
		{
			name:          "memory limit below request",
			resources:     spec.ResourceTraitsSpec{Memory: "1Gi", MemoryLimit: "512Mi"},
			wantValid:     false,
			expectedField: "component[0].traits.resources.memoryLimit",
		},
		{
			name:      "cpu limit equals request",
			resources: spec.ResourceTraitsSpec{CPU: "500m", CPULimit: "500m"},
			wantValid: true,
		},
		{
			name:      "cpu limit above request",
			resources: spec.ResourceTraitsSpec{CPU: "500m", CPULimit: "1"},
			wantValid: true,
		},
		{
			name:      "memory limit above request",
			resources: spec.ResourceTraitsSpec{Memory: "512Mi", MemoryLimit: "1Gi"},
			wantValid: true,
		},
		{
			name:      "cpu limit without request",
			resources: spec.ResourceTraitsSpec{CPULimit: "300m"},
			wantValid: true,
		},
		{
			name:      "memory limit without request",
			resources: spec.ResourceTraitsSpec{MemoryLimit: "512Mi"},
			wantValid: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &validationServiceImpl{}
			ctx := context.Background()
			req := apisv1.CreateApplicationsRequest{
				Name:      "my-app",
				Namespace: "default",
				Component: []apisv1.CreateComponentRequest{
					{
						Name:          "backend",
						ComponentType: config.ServerJob,
						Image:         "nginx:latest",
						Traits: apisv1.Traits{
							Resources: &tc.resources,
						},
					},
				},
			}

			resp := svc.TryApplication(ctx, req)

			if tc.wantValid {
				require.True(t, resp.Valid, "expected valid resources trait: %+v", resp.Errors)
				return
			}
			require.False(t, resp.Valid, "expected invalid resources trait")
			requireValidationError(t, resp.Errors, tc.expectedField, apisv1.ErrCodeInvalidTraitConfig)
		})
	}
}

func TestValidationService_TryApplication_NestedTraitForbidden(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Sidecar: []spec.SidecarTraitsSpec{
						{
							Name:  "sidecar-1",
							Image: "busybox:latest",
							Traits: spec.Traits{
								// Nested sidecar is forbidden
								Sidecar: []spec.SidecarTraitsSpec{
									{
										Name:  "nested-sidecar",
										Image: "busybox:latest",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to nested sidecar")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeNestedTraitForbidden {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected nested trait forbidden error")
}

func TestValidationService_TryApplication_InvalidStorageType(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{
						{
							Type:      "invalid-type",
							MountPath: "/data",
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to invalid storage type")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidStorageType {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid storage type error")
}

func TestValidationService_TryApplication_MissingRBACRules(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					RBAC: []spec.RBACPolicySpec{
						{
							ServiceAccount: "my-sa",
							Rules:          []spec.RBACRuleSpec{}, // Empty rules
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing RBAC rules")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingRBACRules {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing RBAC rules error")
}

func TestValidationService_TryApplication_MissingIngressRoutes(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Ingress: []spec.IngressTraitsSpec{
						{
							Name:   "my-ingress",
							Routes: []spec.IngressRoutes{}, // Empty routes
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing ingress routes")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingIngressRoutes {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing ingress routes error")
}

// ==================== Additional Test Cases ====================

func TestValidationService_TryApplication_MultipleProbeTypes(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	// Test that multiple probe methods in one probe config is invalid
	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Probes: []spec.ProbeTraitsSpec{
						{
							Type: "liveness",
							HTTPGet: &spec.HTTPGetProbe{
								Path: "/health",
								Port: 8080,
							},
							TCPSocket: &spec.TCPSocketProbe{
								Port: 8080,
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to multiple probe methods")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidProbeConfig {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid probe config error")
}

func TestValidationService_TryApplication_MissingStorageMountPath(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{
						{
							Type:      "persistent",
							Name:      "data",
							MountPath: "", // Missing mount path
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing mountPath")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingRequiredField && err.Field == "component[0].traits.storage[0].mountPath" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing mountPath error")
}

func TestValidationService_TryApplication_StorageSubPathAndSubPathExprMutuallyExclusive(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{
						{
							Type:        "persistent",
							Name:        "logs",
							MountPath:   "/app/log",
							SubPath:     "fixed/logs",
							SubPathExpr: "$(POD_IP)/logs",
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to mutually exclusive subPath fields")
	requireValidationError(t, resp.Errors, "component[0].traits.storage[0].subPathExpr", apisv1.ErrCodeInvalidTraitConfig)
}

func TestValidationService_TryApplication_InvalidStorageSize(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{
						{
							Type:      "persistent",
							Name:      "data",
							MountPath: "/data",
							TmpCreate: true,
							Size:      "invalid-size", // Invalid size format
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to invalid storage size")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidStorageSize {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid storage size error")
}

func TestValidationService_TryApplication_MissingRBACVerbs(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					RBAC: []spec.RBACPolicySpec{
						{
							ServiceAccount: "my-sa",
							Rules: []spec.RBACRuleSpec{
								{
									APIGroups: []string{""},
									Resources: []string{"pods"},
									Verbs:     []string{}, // Empty verbs
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing RBAC verbs")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingRBACVerbs {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing RBAC verbs error")
}

func TestValidationService_TryApplication_ValidRBACConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					RBAC: []spec.RBACPolicySpec{
						{
							ServiceAccount: "pod-reader",
							Rules: []spec.RBACRuleSpec{
								{
									APIGroups: []string{""},
									Resources: []string{"pods"},
									Verbs:     []string{"get", "list", "watch"},
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	// Should not have RBAC-related errors
	for _, err := range resp.Errors {
		assert.NotEqual(t, apisv1.ErrCodeMissingRBACRules, err.Code, "Should not have missing RBAC rules error")
		assert.NotEqual(t, apisv1.ErrCodeMissingRBACVerbs, err.Code, "Should not have missing RBAC verbs error")
	}
}

func TestValidationService_TryApplication_AllowsMissingIngressServiceName(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Ingress: []spec.IngressTraitsSpec{
						{
							Name: "my-ingress",
							Routes: []spec.IngressRoutes{
								{
									Path: "/",
									Backend: spec.IngressRoute{
										ServiceName: "", // Missing service name
										ServicePort: 80,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.True(t, resp.Valid, "Expected valid when service name is omitted")
}

func TestValidationService_TryApplication_RejectsMissingIngressServiceNameWithMultipleServices(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name: "backend-v1",
							Type: "internal",
							Selector: map[string]string{
								"version": "v1",
							},
							Ports: []spec.ServicePortTraitSpec{
								{Port: 8080},
							},
						},
						{
							Name: "backend-v2",
							Type: "internal",
							Selector: map[string]string{
								"version": "v2",
							},
							Ports: []spec.ServicePortTraitSpec{
								{Port: 8081},
							},
						},
					},
					Ingress: []spec.IngressTraitsSpec{
						{
							Name: "my-ingress",
							Routes: []spec.IngressRoutes{
								{
									Path: "/",
									Backend: spec.IngressRoute{
										ServicePort: 8080,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid, "Expected invalid when ingress serviceName is ambiguous")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingServiceName &&
			err.Field == "component[0].traits.ingress[0].routes[0].backend.serviceName" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing ingress backend serviceName validation error")
}

func TestValidationService_TryApplication_ValidIngressConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Ingress: []spec.IngressTraitsSpec{
						{
							Name:             "my-ingress",
							IngressClassName: "nginx",
							Hosts:            []string{"api.example.com"},
							Routes: []spec.IngressRoutes{
								{
									Path: "/v1",
									Backend: spec.IngressRoute{
										ServiceName: "api-service",
										ServicePort: 8080,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	// Should not have ingress-related errors
	for _, err := range resp.Errors {
		assert.NotEqual(t, apisv1.ErrCodeMissingIngressRoutes, err.Code, "Should not have missing routes error")
		assert.NotEqual(t, apisv1.ErrCodeMissingServiceName, err.Code, "Should not have missing service name error")
	}
}

func TestValidationService_TryApplication_InvalidIngressNameAndHosts(t *testing.T) {
	testCases := []struct {
		name          string
		mutate        func(*spec.IngressTraitsSpec)
		expectedField string
		expectedCode  string
	}{
		{
			name: "invalid ingress name",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.Name = "Bad_Ingress"
			},
			expectedField: "component[0].traits.ingress[0].name",
			expectedCode:  apisv1.ErrCodeInvalidNameFormat,
		},
		{
			name: "host with trailing dot",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.Hosts = []string{"api.example.com."}
			},
			expectedField: "component[0].traits.ingress[0].hosts[0]",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
		{
			name: "host with surrounding whitespace",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.Hosts = []string{" api.example.com "}
			},
			expectedField: "component[0].traits.ingress[0].hosts[0]",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
		{
			name: "whitespace-only host",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.Hosts = []string{"   "}
			},
			expectedField: "component[0].traits.ingress[0].hosts[0]",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
		{
			name: "route host with trailing dot",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.Routes[0].Host = "api.example.com."
			},
			expectedField: "component[0].traits.ingress[0].routes[0].host",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
		{
			name: "route host with surrounding whitespace",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.Routes[0].Host = " api.example.com "
			},
			expectedField: "component[0].traits.ingress[0].routes[0].host",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
		{
			name: "tls host with uppercase character",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.TLS = []spec.IngressTLSConfig{{
					SecretName: "api-tls",
					Hosts:      []string{"API.example.com"},
				}}
			},
			expectedField: "component[0].traits.ingress[0].tls[0].hosts[0]",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
		{
			name: "tls host with surrounding whitespace",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.TLS = []spec.IngressTLSConfig{{
					SecretName: "api-tls",
					Hosts:      []string{" api.example.com "},
				}}
			},
			expectedField: "component[0].traits.ingress[0].tls[0].hosts[0]",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
		{
			name: "ip literal host",
			mutate: func(ingress *spec.IngressTraitsSpec) {
				ingress.Hosts = []string{"192.168.1.10"}
			},
			expectedField: "component[0].traits.ingress[0].hosts[0]",
			expectedCode:  apisv1.ErrCodeInvalidTraitConfig,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &validationServiceImpl{}
			ingress := spec.IngressTraitsSpec{
				Name:  "my-ingress",
				Hosts: []string{"api.example.com"},
				Routes: []spec.IngressRoutes{
					{
						Path: "/v1",
						Backend: spec.IngressRoute{
							ServiceName: "api-service",
							ServicePort: 8080,
						},
					},
				},
			}
			tc.mutate(&ingress)

			resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
				Name:      "my-app",
				Namespace: "default",
				Component: []apisv1.CreateComponentRequest{
					{
						Name:          "backend",
						ComponentType: config.ServerJob,
						Image:         "nginx:latest",
						Traits: apisv1.Traits{
							Ingress: []spec.IngressTraitsSpec{ingress},
						},
					},
				},
			})

			require.False(t, resp.Valid)
			found := false
			for _, err := range resp.Errors {
				if err.Field == tc.expectedField && err.Code == tc.expectedCode {
					found = true
					break
				}
			}
			require.True(t, found, "expected %s/%s in errors: %+v", tc.expectedField, tc.expectedCode, resp.Errors)
		})
	}
}

func TestValidationService_TryApplication_ValidWildcardIngressHost(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Ingress: []spec.IngressTraitsSpec{
						{
							Name:  "my-ingress",
							Hosts: []string{"*.example.com"},
							Routes: []spec.IngressRoutes{
								{
									Path: "/v1",
									Backend: spec.IngressRoute{
										ServiceName: "api-service",
										ServicePort: 8080,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	require.True(t, resp.Valid, "expected wildcard ingress host to be valid: %+v", resp.Errors)
}

func TestValidationService_TryApplication_InitContainerMissingImage(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Init: []spec.InitTraitSpec{
						{
							Name:  "init-container",
							Image: "", // Missing image
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing init container image")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingImage {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing image error for init container")
}

func TestValidationService_TryApplication_SidecarMissingImage(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Sidecar: []spec.SidecarTraitsSpec{
						{
							Name:  "sidecar",
							Image: "", // Missing image
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing sidecar image")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingImage {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing image error for sidecar")
}

func TestValidationService_TryApplication_ValidServiceTrait(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name: "backend",
							Type: "internal",
							Selector: map[string]string{
								"app": "backend",
							},
							Ports: []spec.ServicePortTraitSpec{
								{Port: 8080, TargetPort: 8080, Protocol: "TCP"},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid, "Expected valid application config")
	assert.Empty(t, resp.Errors, "Expected no validation errors")
}

func TestValidationService_TryApplication_InvalidServiceTraitMissingPorts(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name: "backend",
							Type: "internal",
							Selector: map[string]string{
								"app": "backend",
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingRequiredField && err.Field == "component[0].traits.service[0].ports" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing service ports error")
}

func TestValidationService_TryApplication_InvalidServiceTraitHeadlessType(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name:     "backend",
							Type:     "node",
							Headless: true,
							Selector: map[string]string{
								"app": "backend",
							},
							Ports: []spec.ServicePortTraitSpec{
								{Port: 8080},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidTraitConfig && err.Field == "component[0].traits.service[0].headless" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected headless/type conflict error")
}

func TestValidationService_TryApplication_ValidServiceTraitLegacyType(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name: "backend",
							Type: "ClusterIP",
							Selector: map[string]string{
								"app": "backend",
							},
							Ports: []spec.ServicePortTraitSpec{
								{Port: 8080},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid, "Expected legacy service type to remain compatible")
}

func TestValidationService_TryApplication_InvalidReservedServiceTraitLabels(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name: "backend",
							Type: "internal",
							Selector: map[string]string{
								"app": "backend",
							},
							Labels: map[string]string{
								config.LabelAppID: "evil-app",
							},
							Ports: []spec.ServicePortTraitSpec{
								{Port: 8080},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid, "Expected invalid when service trait overrides reserved labels")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidTraitConfig &&
			err.Field == "component[0].traits.service[0].labels."+config.LabelAppID {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected reserved service trait label validation error")
}

func TestValidationService_TryApplication_InvalidReservedIngressTraitLabel(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Ingress: []spec.IngressTraitsSpec{
						{
							Name: "backend",
							Label: map[string]string{
								config.LabelComponentName: "custom-backend",
							},
							Routes: []spec.IngressRoutes{
								{
									Path: "/",
									Backend: spec.IngressRoute{
										ServiceName: "backend",
										ServicePort: 8080,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid, "Expected invalid when ingress trait overrides reserved labels")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidTraitConfig &&
			err.Field == "component[0].traits.ingress[0].label."+config.LabelComponentName {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected reserved ingress trait label validation error")
}

func TestValidationService_TryApplication_InvalidExternalServiceTraitMissingExternalName(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name: "backend-external",
							Type: "external",
							Ports: []spec.ServicePortTraitSpec{
								{Port: 443},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingRequiredField && err.Field == "component[0].traits.service[0].externalName" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing externalName error")
}

func TestValidationService_TryApplication_ValidExternalServiceTrait(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name:         "backend-external",
							Type:         "external",
							ExternalName: "example.org",
							Ports: []spec.ServicePortTraitSpec{
								{Port: 443},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid, "Expected external service trait to be valid with externalName")
}

func TestValidationService_TryApplication_InvalidServiceTraitNameFormat(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Service: []spec.ServiceTraitSpec{
						{
							Name: "Backend_Service",
							Type: "internal",
							Selector: map[string]string{
								"app": "backend",
							},
							Ports: []spec.ServicePortTraitSpec{
								{Port: 8080},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidNameFormat && err.Field == "component[0].traits.service[0].name" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid service name format error")
}

func TestValidateComponentTraitsForWriteRejectsInvalidServiceTrait(t *testing.T) {
	err := validateComponentTraitsForWrite(config.ServerJob, apisv1.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "backend",
				Type: "internal",
				Ports: []spec.ServicePortTraitSpec{
					{Port: 8080},
				},
			},
		},
	}, "component[0].traits")

	require.Error(t, err)
	require.Contains(t, err.Error(), "selector is required")
}

func TestValidateComponentTraitsForWriteValidatesResources(t *testing.T) {
	testCases := []struct {
		name      string
		traits    apisv1.Traits
		wantError string
	}{
		{
			name: "rejects top-level cpu limit below request",
			traits: apisv1.Traits{
				Resources: &spec.ResourceTraitsSpec{CPU: "500m", CPULimit: "300m"},
			},
			wantError: "cpuLimit must be greater than or equal to cpu request",
		},
		{
			name: "rejects sidecar memory limit below request",
			traits: apisv1.Traits{
				Sidecar: []spec.SidecarTraitsSpec{
					{
						Name:  "metrics",
						Image: "metrics:v1",
						Traits: spec.Traits{
							Resources: &spec.ResourceTraitsSpec{Memory: "1Gi", MemoryLimit: "512Mi"},
						},
					},
				},
			},
			wantError: "memoryLimit must be greater than or equal to memory request",
		},
		{
			name: "allows limit-only resources",
			traits: apisv1.Traits{
				Resources: &spec.ResourceTraitsSpec{CPULimit: "300m", MemoryLimit: "512Mi"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateComponentTraitsForWrite(config.ServerJob, tc.traits, "component[0].traits")
			if tc.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

func TestValidateComponentTraitsForWriteRejectsReservedIngressLabels(t *testing.T) {
	err := validateComponentTraitsForWrite(config.ServerJob, apisv1.Traits{
		Ingress: []spec.IngressTraitsSpec{
			{
				Label: map[string]string{
					config.LabelComponentName: "custom-backend",
				},
				Routes: []spec.IngressRoutes{
					{
						Path: "/",
						Backend: spec.IngressRoute{
							ServiceName: "backend",
							ServicePort: 8080,
						},
					},
				},
			},
		},
	}, "component[0].traits")

	require.Error(t, err)
	require.Contains(t, err.Error(), "traits.ingress.label")
	require.Contains(t, err.Error(), config.LabelComponentName)
}

func TestValidateComponentTraitsForWriteRejectsAmbiguousIngressBackend(t *testing.T) {
	err := validateComponentTraitsForWrite(config.ServerJob, apisv1.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "backend-v1",
				Type: "internal",
				Selector: map[string]string{
					"version": "v1",
				},
				Ports: []spec.ServicePortTraitSpec{
					{Port: 8080},
				},
			},
			{
				Name: "backend-v2",
				Type: "internal",
				Selector: map[string]string{
					"version": "v2",
				},
				Ports: []spec.ServicePortTraitSpec{
					{Port: 8081},
				},
			},
		},
		Ingress: []spec.IngressTraitsSpec{
			{
				Routes: []spec.IngressRoutes{
					{
						Path: "/",
						Backend: spec.IngressRoute{
							ServicePort: 8080,
						},
					},
				},
			},
		},
	}, "component[0].traits")

	require.Error(t, err)
	require.Contains(t, err.Error(), "serviceName is required")
}

func TestValidateComponentTraitsForWriteRejectsNestedStorageSubPathConflict(t *testing.T) {
	err := validateComponentTraitsForWrite(config.ServerJob, apisv1.Traits{
		Sidecar: []spec.SidecarTraitsSpec{
			{
				Name:  "logger",
				Image: "logger:latest",
				Traits: spec.Traits{
					Storage: []spec.StorageTraitSpec{{
						Name:        "logs",
						Type:        config.StorageTypePersistent,
						MountPath:   "/app/log",
						SubPath:     "fixed/logs",
						SubPathExpr: "$(POD_IP)/logs",
					}},
				},
			},
		},
	}, "component[0].traits")

	require.Error(t, err)
	require.Contains(t, err.Error(), "storage subPath and subPathExpr cannot both be set")
}

// ==================== TryWorkflow Tests ====================
