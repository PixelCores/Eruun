package validation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestValidationService_TryApplication_ValidEnvFromConfig(t *testing.T) {
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
					EnvFrom: []spec.EnvFromSourceSpec{
						{
							Type:       "configMap",
							SourceName: "app-config",
						},
						{
							Type:       "secret",
							SourceName: "app-secrets",
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	// Should not have envFrom-related errors
	for _, err := range resp.Errors {
		assert.NotEqual(t, apisv1.ErrCodeInvalidEnvFromType, err.Code, "Should not have envFrom type error")
		assert.NotEqual(t, apisv1.ErrCodeMissingRequiredField, err.Code, "Should not have missing field error for envFrom")
	}
}

func TestValidationService_TryApplication_InvalidEnvFromType(t *testing.T) {
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
					EnvFrom: []spec.EnvFromSourceSpec{
						{
							Type:       "invalid-type",
							SourceName: "app-config",
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to invalid envFrom type")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidEnvFromType {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid envFrom type error")
}

func TestValidationService_TryApplication_ValidEnvsConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	staticValue := "production"
	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Envs: []spec.SimplifiedEnvSpec{
						{
							Name: "APP_ENV",
							ValueFrom: spec.ValueSource{
								Static: &staticValue,
							},
						},
						{
							Name: "DB_PASSWORD",
							ValueFrom: spec.ValueSource{
								Secret: &spec.SecretSelectorSpec{
									Name: "db-credentials",
									Key:  "password",
								},
							},
						},
						{
							Name: "DB_HOST",
							ValueFrom: spec.ValueSource{
								Config: &spec.ConfigMapSelectorSpec{
									Name: "db-config",
									Key:  "host",
								},
							},
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	// Should not have envs-related errors
	for _, err := range resp.Errors {
		if err.Field != "" && (err.Field == "component[0].traits.envs[0]" ||
			err.Field == "component[0].traits.envs[1]" ||
			err.Field == "component[0].traits.envs[2]") {
			t.Errorf("Unexpected error for envs: %+v", err)
		}
	}
}

func TestValidationService_TryApplication_InvalidEnvValueSource(t *testing.T) {
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
					Envs: []spec.SimplifiedEnvSpec{
						{
							Name:      "APP_ENV",
							ValueFrom: spec.ValueSource{}, // No value source specified
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing env value source")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidEnvValueSource {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid env value source error")
}

func TestValidationService_TryApplication_TargetWorkEnvValid(t *testing.T) {
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
					TargetWorkEnv: map[string]string{"app": "lab"},
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

func TestValidationService_TryApplication_TargetWorkEnvNestedRejected(t *testing.T) {
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
								TargetWorkEnv: map[string]string{"app": "lab"},
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
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidTraitConfig && err.Field == "component[0].traits.sidecar[0].traits.targetWorkEnv" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected nested targetWorkEnv invalid trait config error")
}

func TestValidationService_TryApplication_TargetWorkEnvInvalidSelector(t *testing.T) {
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
					TargetWorkEnv: map[string]string{
						"bad key": "lab",
						"app":     "",
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

	foundInvalidKey := false
	foundEmptyValue := false
	for _, err := range resp.Errors {
		switch err.Field {
		case `component[0].traits.targetWorkEnv["bad key"]`:
			foundInvalidKey = true
		case `component[0].traits.targetWorkEnv["app"]`:
			foundEmptyValue = true
		}
	}
	assert.True(t, foundInvalidKey, "Expected invalid targetWorkEnv key error")
	assert.True(t, foundEmptyValue, "Expected empty targetWorkEnv value error")
}

func TestValidationService_TryApplication_TargetWorkEnvStringRejected(t *testing.T) {
	var req apisv1.CreateApplicationsRequest

	err := json.Unmarshal([]byte(`{
		"name": "demo-app",
		"namespace": "default",
		"component": [
			{
				"name": "backend",
				"type": "server",
				"image": "nginx:latest",
				"namespace": "default",
				"traits": {
					"targetWorkEnv": "lab"
				}
			}
		],
		"workflow": [
			{"name": "deploy", "components": ["backend"]}
		]
	}`), &req)

	assert.Error(t, err)
}
