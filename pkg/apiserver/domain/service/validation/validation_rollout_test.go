package validation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestValidationService_TryApplication_RolloutValid(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()
	maxSurge := intstr.FromString("25%")
	maxUnavailable := intstr.FromInt32(0)

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Namespace:     "default",
				Traits: apisv1.Traits{
					Rollout: &spec.RolloutTraitSpec{
						Type: "RollingUpdate",
						RollingUpdate: &spec.RolloutRollingUpdateSpec{
							MaxSurge:       &maxSurge,
							MaxUnavailable: &maxUnavailable,
						},
					},
				},
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy", WorkflowType: config.JobDeploy, Components: []string{"backend"}},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid)
	assert.Empty(t, resp.Errors)
}

func TestValidationService_TryApplication_RolloutRollingUpdateRequiresConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Namespace:     "default",
				Traits: apisv1.Traits{
					Rollout: &spec.RolloutTraitSpec{Type: "RollingUpdate"},
				},
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy", WorkflowType: config.JobDeploy, Components: []string{"backend"}},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)

	found := false
	for _, err := range resp.Errors {
		if err.Field == "component[0].traits.rollout.rollingUpdate" &&
			err.Code == apisv1.ErrCodeMissingRequiredField {
			found = true
			assert.Contains(t, err.Message, "requires rollingUpdate")
			break
		}
	}
	assert.True(t, found, "Expected missing rollingUpdate validation error")
}

func TestValidationService_TryApplication_RolloutRollingUpdateRequiresFields(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()
	maxSurge := intstr.FromString("25%")
	maxUnavailable := intstr.FromInt32(0)

	testCases := []struct {
		name          string
		rollingUpdate *spec.RolloutRollingUpdateSpec
		expectedField string
		expectedMsg   string
	}{
		{
			name:          "empty rollingUpdate",
			rollingUpdate: &spec.RolloutRollingUpdateSpec{},
			expectedField: "component[0].traits.rollout.rollingUpdate.maxSurge",
			expectedMsg:   "requires rollingUpdate.maxSurge",
		},
		{
			name: "missing maxSurge",
			rollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxUnavailable: &maxUnavailable,
			},
			expectedField: "component[0].traits.rollout.rollingUpdate.maxSurge",
			expectedMsg:   "requires rollingUpdate.maxSurge",
		},
		{
			name: "missing maxUnavailable",
			rollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxSurge: &maxSurge,
			},
			expectedField: "component[0].traits.rollout.rollingUpdate.maxUnavailable",
			expectedMsg:   "requires rollingUpdate.maxUnavailable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := apisv1.CreateApplicationsRequest{
				Name:      "demo-app",
				Namespace: "default",
				Component: []apisv1.CreateComponentRequest{
					{
						Name:          "backend",
						ComponentType: config.ServerJob,
						Image:         "nginx:latest",
						Namespace:     "default",
						Traits: apisv1.Traits{
							Rollout: &spec.RolloutTraitSpec{
								Type:          "RollingUpdate",
								RollingUpdate: tc.rollingUpdate,
							},
						},
					},
				},
				WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
					{Name: "deploy", WorkflowType: config.JobDeploy, Components: []string{"backend"}},
				},
			}

			resp := svc.TryApplication(ctx, req)
			assert.False(t, resp.Valid)

			found := false
			for _, err := range resp.Errors {
				if err.Field == tc.expectedField && err.Code == apisv1.ErrCodeMissingRequiredField {
					found = true
					assert.Contains(t, err.Message, tc.expectedMsg)
					break
				}
			}
			assert.True(t, found, "Expected missing field validation error")
		})
	}
}

func TestValidationService_TryApplication_RolloutRejectsStringZeroVariants(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	deployMaxSurge := intstr.FromString("00%")
	deployMaxUnavailable := intstr.FromString("-0%")
	statefulMaxUnavailable := intstr.FromString("-0%")
	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Namespace:     "default",
				Traits: apisv1.Traits{
					Rollout: &spec.RolloutTraitSpec{
						Type: "RollingUpdate",
						RollingUpdate: &spec.RolloutRollingUpdateSpec{
							MaxSurge:       &deployMaxSurge,
							MaxUnavailable: &deployMaxUnavailable,
						},
					},
				},
			},
			{
				Name:          "store",
				ComponentType: config.StoreJob,
				Image:         "mysql:8",
				Namespace:     "default",
				Traits: apisv1.Traits{
					Rollout: &spec.RolloutTraitSpec{
						Type: "RollingUpdate",
						RollingUpdate: &spec.RolloutRollingUpdateSpec{
							MaxUnavailable: &statefulMaxUnavailable,
						},
					},
				},
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy", WorkflowType: config.JobDeploy, Components: []string{"backend", "store"}},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)

	foundDeploymentZero := false
	foundStatefulSetZero := false
	for _, err := range resp.Errors {
		switch {
		case err.Field == "component[0].traits.rollout.rollingUpdate" &&
			err.Code == apisv1.ErrCodeInvalidTraitConfig:
			foundDeploymentZero = true
			assert.Contains(t, err.Message, "cannot both be 0")
		case err.Field == "component[1].traits.rollout.rollingUpdate.maxUnavailable" &&
			err.Code == apisv1.ErrCodeInvalidTraitConfig:
			foundStatefulSetZero = true
			assert.Contains(t, err.Message, "must be greater than 0")
		}
	}
	assert.True(t, foundDeploymentZero, "Expected deployment both-zero validation error")
	assert.True(t, foundStatefulSetZero, "Expected statefulset maxUnavailable zero validation error")
}

func TestValidationService_TryApplication_RolloutRejectsNumericStrings(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()
	intMaxSurge := intstr.FromInt32(1)
	intMaxUnavailable := intstr.FromInt32(0)
	stringMaxSurge := intstr.FromString("1")
	stringMaxUnavailable := intstr.FromString("0")

	testCases := []struct {
		name          string
		componentType config.JobType
		rollingUpdate *spec.RolloutRollingUpdateSpec
		expectedField string
	}{
		{
			name:          "deployment maxSurge numeric string",
			componentType: config.ServerJob,
			rollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxSurge:       &stringMaxSurge,
				MaxUnavailable: &intMaxUnavailable,
			},
			expectedField: "component[0].traits.rollout.rollingUpdate.maxSurge",
		},
		{
			name:          "deployment maxUnavailable numeric string",
			componentType: config.ServerJob,
			rollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxSurge:       &intMaxSurge,
				MaxUnavailable: &stringMaxUnavailable,
			},
			expectedField: "component[0].traits.rollout.rollingUpdate.maxUnavailable",
		},
		{
			name:          "statefulset maxUnavailable numeric string",
			componentType: config.StoreJob,
			rollingUpdate: &spec.RolloutRollingUpdateSpec{
				MaxUnavailable: &stringMaxSurge,
			},
			expectedField: "component[0].traits.rollout.rollingUpdate.maxUnavailable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := apisv1.CreateApplicationsRequest{
				Name:      "demo-app",
				Namespace: "default",
				Component: []apisv1.CreateComponentRequest{
					{
						Name:          "backend",
						ComponentType: tc.componentType,
						Image:         "nginx:latest",
						Namespace:     "default",
						Traits: apisv1.Traits{
							Rollout: &spec.RolloutTraitSpec{
								Type:          "RollingUpdate",
								RollingUpdate: tc.rollingUpdate,
							},
						},
					},
				},
				WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
					{Name: "deploy", WorkflowType: config.JobDeploy, Components: []string{"backend"}},
				},
			}

			resp := svc.TryApplication(ctx, req)
			assert.False(t, resp.Valid)

			found := false
			for _, err := range resp.Errors {
				if err.Field == tc.expectedField && err.Code == apisv1.ErrCodeInvalidTraitConfig {
					found = true
					assert.Contains(t, err.Message, "percentage string")
					break
				}
			}
			assert.True(t, found, "Expected numeric string validation error")
		})
	}
}

func TestValidationService_TryApplication_RolloutRejectsUnsupportedComponent(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "task",
				ComponentType: config.InstantJob,
				Image:         "busybox:1.36",
				Namespace:     "default",
				Traits: apisv1.Traits{
					Rollout: &spec.RolloutTraitSpec{Type: "RollingUpdate"},
				},
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{Name: "run", Components: []string{"task"}},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)
	assert.NotEmpty(t, resp.Errors)
	assert.Equal(t, apisv1.ErrCodeInvalidTraitConfig, resp.Errors[0].Code)
	assert.Contains(t, resp.Errors[0].Message, "webservice and store")
}

func TestValidationService_TryApplication_RolloutNestedRejected(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Namespace:     "default",
				Traits: apisv1.Traits{
					Sidecar: []spec.SidecarTraitsSpec{
						{
							Name:  "agent",
							Image: "busybox:1.36",
							Traits: spec.Traits{
								Rollout: &spec.RolloutTraitSpec{Type: "RollingUpdate"},
							},
						},
					},
				},
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy", WorkflowType: config.JobDeploy, Components: []string{"backend"}},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid)
	assert.NotEmpty(t, resp.Errors)
	assert.Equal(t, apisv1.ErrCodeInvalidTraitConfig, resp.Errors[0].Code)
	assert.Contains(t, resp.Errors[0].Message, "workload-level")
}
