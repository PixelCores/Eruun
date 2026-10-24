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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
				Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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

func TestValidationService_TryApplication_MultipleProbeTypes(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	// Test that multiple probe methods in one probe config is invalid
	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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

func TestValidationService_TryApplication_InitContainerMissingImage(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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

func TestValidationService_TryApplication_RejectsKubernetesServiceType(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Components: []apisv1.CreateComponentRequest{
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
	assert.False(t, resp.Valid, "Kubernetes service types are not part of the trait vocabulary")
}

func TestValidationService_TryApplication_InvalidReservedServiceTraitLabels(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Components: []apisv1.CreateComponentRequest{
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
								spec.LabelAppID: "evil-app",
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
			err.Field == "component[0].traits.service[0].labels."+spec.LabelAppID {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected reserved service trait label validation error")
}

func TestValidationService_TryApplication_InvalidExternalServiceTraitMissingExternalName(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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
		Components: []apisv1.CreateComponentRequest{
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

// ==================== TryWorkflow Tests ====================
