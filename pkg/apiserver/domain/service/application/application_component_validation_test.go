package application

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

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
					spec.LabelComponentName: "custom-backend",
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
	require.Contains(t, err.Error(), spec.LabelComponentName)
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
						Type:        spec.StorageTypePersistent,
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
