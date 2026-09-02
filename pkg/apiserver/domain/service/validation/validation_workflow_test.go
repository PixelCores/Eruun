package validation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func tryWorkflowRequestFromUpdateJSON(t *testing.T, input string) apisv1.TryWorkflowRequest {
	t.Helper()
	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(input), &req))
	return apisv1.TryWorkflowRequest{
		WorkflowID:    req.WorkflowID,
		Name:          req.Name,
		Alias:         req.Alias,
		WorkflowType:  req.WorkflowType,
		Callback:      req.Callback,
		FailurePolicy: req.FailurePolicy,
		Workflow:      req.Workflow,
	}
}

func TestValidationService_TryApplication_RejectsInvalidWorkflowCallback(t *testing.T) {
	svc := &validationServiceImpl{}
	req := validCallbackTryApplicationRequest()
	req.WorkflowCallback = &apisv1.WorkflowCallback{
		Methods: map[string]string{"success": "PATCH"},
	}

	resp := svc.TryApplication(context.Background(), req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow.callback", apisv1.ErrCodeInvalidTraitConfig)
}

func TestValidationService_TryApplication_WorkflowCallbackOverridesInvalidAppCallback(t *testing.T) {
	svc := &validationServiceImpl{
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}),
	}
	req := validCallbackTryApplicationRequest()
	req.Callback = &apisv1.WorkflowCallback{
		Methods: map[string]string{"success": "PATCH"},
	}
	req.WorkflowCallback = &apisv1.WorkflowCallback{
		Success: "http://127.0.0.1:8080/callback",
	}

	resp := svc.TryApplication(context.Background(), req)

	require.True(t, resp.Valid, "expected workflow callback to override app callback: %+v", resp.Errors)
}

func TestValidationService_TryWorkflowRejectsInvalidCallbackMethod(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "test-app", Namespace: "default"}
	store.components["backend"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	resp := svc.TryWorkflow(context.Background(), "app-1", apisv1.TryWorkflowRequest{
		Name: "test-workflow",
		Callback: &apisv1.WorkflowCallback{
			Methods: map[string]string{"success": "PATCH"},
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-backend",
			WorkflowType: config.JobDeploy,
			Mode:         "StepByStep",
			Components:   []string{"backend"},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "callback", apisv1.ErrCodeInvalidTraitConfig)
}

func TestValidationService_TryWorkflowRejectsInvalidFailurePolicy(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "test-app", Namespace: "default"}
	store.components["backend"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	resp := svc.TryWorkflow(context.Background(), "app-1", apisv1.TryWorkflowRequest{
		Name:          "test-workflow",
		FailurePolicy: "delete_everything",
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-backend",
			WorkflowType: config.JobDeploy,
			Mode:         "StepByStep",
			Components:   []string{"backend"},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "failurePolicy", apisv1.ErrCodeInvalidWorkflowFailurePolicy)
}

func TestValidationService_TryWorkflowRejectsInvalidCallbackURL(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "test-app", Namespace: "default"}
	store.components["backend"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.DefaultURLSecurityPolicy()),
		AppRepo:                   repos.AppRepo,
		ComponentRepo:             repos.ComponentRepo,
	}

	resp := svc.TryWorkflow(context.Background(), "app-1", apisv1.TryWorkflowRequest{
		Name: "test-workflow",
		Callback: &apisv1.WorkflowCallback{
			Success: "ftp://example.com/callback",
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-backend",
			WorkflowType: config.JobDeploy,
			Mode:         "StepByStep",
			Components:   []string{"backend"},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "callback", apisv1.ErrCodeInvalidTraitConfig)
}

func TestValidationService_TryApplication_WorkflowUsesResolvedTemplateComponentNames(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "nginx",
		Namespace:       "default",
		Version:         "1.0.0",
		TemplateEnabled: true,
	}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "tmpl-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	appSvc := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: appSvc.AppRepo, ComponentRepo: appSvc.ComponentRepo}

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "game",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{{
			Template: &apisv1.TemplateRef{ID: "tmpl-1", Target: "api"},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-step",
			WorkflowType: config.JobDeploy,
			Mode:         "StepByStep",
			Components:   []string{"api"},
		}},
	})

	require.False(t, resp.Valid)
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeComponentNotFound {
			found = true
			assert.Contains(t, err.Message, "api")
		}
		require.NotEqual(t, apisv1.ErrCodeInvalidComponentType, err.Code)
		require.NotEqual(t, apisv1.ErrCodeMissingImage, err.Code)
	}
	assert.True(t, found, "expected workflow to validate against resolved component names")
}

func TestValidationService_TryApplication_WorkflowComponentNotFound(t *testing.T) {
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
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "deploy-step",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"backend", "non-existent-component"},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to component not found")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeComponentNotFound {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected component not found error")
}

func TestValidationService_TryApplication_InvalidWorkflowMode(t *testing.T) {
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
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "deploy-step",
				WorkflowType: config.JobDeploy,
				Mode:         "invalid-mode",
				Components:   []string{"backend"},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to invalid workflow mode")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidWorkflowMode {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid workflow mode error")
}

func TestValidationService_TryApplication_WorkflowSubStepComponentNotFound(t *testing.T) {
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
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "deploy-step",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"backend"},
				SubSteps: []apisv1.CreateWorkflowSubStepRequest{
					{
						Name:         "sub-step",
						WorkflowType: config.JobDeploy,
						Components:   []string{"non-existent-component"},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to component not found in substep")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeComponentNotFound {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected component not found error")
}

func TestValidationService_TryApplication_DuplicateWorkflowStepName(t *testing.T) {
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
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "deploy-step",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"backend"},
			},
			{
				Name:         "deploy-step", // Duplicate
				WorkflowType: config.JobDeploy,
				Mode:         "DAG",
				Components:   []string{"backend"},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to duplicate workflow step name")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeDuplicateWorkflowStep {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected duplicate workflow step error")
}

func TestValidationService_TryApplication_EmptyWorkflowStep(t *testing.T) {
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
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "empty-step",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{}, // No components and no substeps
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to empty workflow step")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeWorkflowStepNoComponent {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected workflow step no component error")
}

func TestValidationService_TryWorkflow_EmptyWorkflow(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.TryWorkflowRequest{
		Name:     "test-workflow",
		Workflow: []apisv1.CreateWorkflowStepRequest{},
	}

	resp := svc.TryWorkflow(ctx, "", req)

	assert.False(t, resp.Valid, "Expected invalid due to empty workflow")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingRequiredField && err.Field == "workflow" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing workflow error")
}

func TestValidationService_TryWorkflow_ValidWorkflow(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	// Note: Without appID, component validation will skip
	req := apisv1.TryWorkflowRequest{
		Name: "test-workflow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "step1",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"backend"},
			},
		},
	}

	// Without appID, no component validation
	resp := svc.TryWorkflow(ctx, "", req)

	// Will have component not found error since no appID provided
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeComponentNotFound {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected component not found error when appID is empty")
}

func TestValidationService_TryWorkflow_UnsupportedJobTypeIsRejected(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.TryWorkflowRequest{
		Name: "test-workflow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "unsupported-step",
				WorkflowType: config.JobType("unsupported_job_type"),
				Mode:         "DAG",
			},
		},
	}

	resp := svc.TryWorkflow(ctx, "", req)
	assert.False(t, resp.Valid, "Expected unsupported jobType to be rejected")
	assert.NotEmpty(t, resp.Errors)
	assert.Equal(t, apisv1.ErrCodeInvalidWorkflowStepType, resp.Errors[0].Code)
}

func TestValidationService_TryWorkflow_UnsupportedWorkflowTypeIsRejected(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "test-app", Namespace: "default"}
	store.components["backend"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	resp := svc.TryWorkflow(context.Background(), "app-1", apisv1.TryWorkflowRequest{
		Name:         "test-workflow",
		WorkflowType: config.WorkflowTaskType("not-a-task"),
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-backend",
			WorkflowType: config.JobDeploy,
			Mode:         "StepByStep",
			Components:   []string{"backend"},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflowType", apisv1.ErrCodeInvalidWorkflowStepType)
}

func TestValidationService_TryApplication_LogArchiveUploadRequiresPath(t *testing.T) {
	svc := &validationServiceImpl{}

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "log-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "archive-api",
			WorkflowType: config.JobLogArchiveUpload,
			Mode:         "StepByStep",
			Components:   []string{"api"},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].properties.path", apisv1.ErrCodeMissingRequiredField)
}

func TestValidationService_TryApplication_LogArchiveUploadAllowsNameBasedStep(t *testing.T) {
	svc := &validationServiceImpl{}

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "log-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "api",
			WorkflowType: config.JobLogArchiveUpload,
			Mode:         "StepByStep",
			Properties: apisv1.WorkflowProperties{
				Path: "/var/log/api",
			},
		}},
	})

	require.True(t, resp.Valid, "expected name-based log archive step to pass: %+v", resp.Errors)
}

func TestValidationService_TryApplication_LogArchiveUploadRejectsNonPodComponent(t *testing.T) {
	svc := &validationServiceImpl{}

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "log-app",
		Namespace: "default",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "app-config",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"app.yaml": "debug: true"},
			},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "archive-config",
			WorkflowType: config.JobLogArchiveUpload,
			Mode:         "StepByStep",
			Components:   []string{"app-config"},
			Properties: apisv1.WorkflowProperties{
				Path: "/var/log/app",
			},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].components[0]", apisv1.ErrCodeInvalidWorkflowStepType)
}

func TestValidationService_TryWorkflow_LogArchiveUploadSubStepRequiresPath(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "log-app", Namespace: "default"}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	resp := svc.TryWorkflow(context.Background(), "app-1", apisv1.TryWorkflowRequest{
		Name: "log-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name: "archive-group",
			Mode: "StepByStep",
			SubSteps: []apisv1.CreateWorkflowSubStepRequest{{
				Name:         "archive-api",
				WorkflowType: config.JobLogArchiveUpload,
				Components:   []string{"api"},
			}},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].subSteps[0].properties.path", apisv1.ErrCodeMissingRequiredField)
}

func TestValidationService_TryWorkflow_LogArchiveUploadAllowsNameBasedStep(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "log-app", Namespace: "default"}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	resp := svc.TryWorkflow(context.Background(), "app-1", apisv1.TryWorkflowRequest{
		Name: "log-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "api",
			WorkflowType: config.JobLogArchiveUpload,
			Mode:         "StepByStep",
			Properties: apisv1.WorkflowProperties{
				Path: "/var/log/api",
			},
		}},
	})

	require.True(t, resp.Valid, "expected name-based log archive step to pass: %+v", resp.Errors)
}

func TestValidationService_TryWorkflow_LogArchiveUploadRejectsNonPodComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "log-app", Namespace: "default"}
	store.components["app-config"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "app-config",
		Namespace:     "default",
		ComponentType: config.ConfJob,
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	resp := svc.TryWorkflow(context.Background(), "app-1", apisv1.TryWorkflowRequest{
		Name: "log-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "archive-config",
			WorkflowType: config.JobLogArchiveUpload,
			Mode:         "StepByStep",
			Components:   []string{"app-config"},
			Properties: apisv1.WorkflowProperties{
				Path: "/var/log/app",
			},
		}},
	})

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].components[0]", apisv1.ErrCodeInvalidWorkflowStepType)
}

func TestValidationService_TryWorkflow_PropertiesArrayValidatesEveryLogArchivePath(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "log-app", Namespace: "default"}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	store.components["worker"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "worker",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	req := tryWorkflowRequestFromUpdateJSON(t, `{
		"name": "log-flow",
		"steps": [{
			"name": "archive",
			"workflowType": "log_archive_upload",
			"mode": "StepByStep",
			"properties": [
				{"policies": ["api"], "path": "/var/log/api"},
				{"policies": ["worker"]}
			]
		}]
	}`)

	resp := svc.TryWorkflow(context.Background(), "app-1", req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].properties[1].path", apisv1.ErrCodeMissingRequiredField)
}

func TestValidationService_TryWorkflow_PropertiesArrayValidatesEveryComponentRef(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "log-app", Namespace: "default"}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	req := tryWorkflowRequestFromUpdateJSON(t, `{
		"name": "log-flow",
		"steps": [{
			"name": "archive",
			"workflowType": "log_archive_upload",
			"mode": "StepByStep",
			"properties": [
				{"policies": ["api"], "path": "/var/log/api"},
				{"policies": ["missing"], "path": "/var/log/missing"}
			]
		}]
	}`)

	resp := svc.TryWorkflow(context.Background(), "app-1", req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].properties[1].policies[0]", apisv1.ErrCodeComponentNotFound)
}

func TestValidationService_TryWorkflow_PropertiesArrayRejectsDuplicatePolicies(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "workflow-app", Namespace: "default"}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	req := tryWorkflowRequestFromUpdateJSON(t, `{
		"name": "deploy-flow",
		"steps": [{
			"name": "deploy-api",
			"workflowType": "deploy",
			"mode": "StepByStep",
			"properties": [
				{"policies": ["api"]},
				{"policies": ["API"]}
			]
		}]
	}`)

	resp := svc.TryWorkflow(context.Background(), "app-1", req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].properties[1].policies[0]", apisv1.ErrCodeDuplicateComponent)
}

func TestValidationService_TryWorkflow_PropertiesArrayRejectsComponentsMismatch(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "workflow-app", Namespace: "default"}
	for _, name := range []string{"api", "worker", "cache"} {
		store.components[name] = &model.ApplicationComponent{
			AppID:         "app-1",
			Name:          name,
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	req := tryWorkflowRequestFromUpdateJSON(t, `{
		"name": "deploy-flow",
		"steps": [{
			"name": "deploy-components",
			"workflowType": "deploy",
			"mode": "StepByStep",
			"components": ["api", "worker"],
			"properties": [
				{"policies": ["api"]},
				{"policies": ["cache"]}
			]
		}]
	}`)

	resp := svc.TryWorkflow(context.Background(), "app-1", req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].components[1]", apisv1.ErrCodeInvalidWorkflowStepType)
}

func TestValidationService_TryWorkflow_PropertiesArrayAllowsMatchingComponents(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "workflow-app", Namespace: "default"}
	for _, name := range []string{"api", "worker"} {
		store.components[name] = &model.ApplicationComponent{
			AppID:         "app-1",
			Name:          name,
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	req := tryWorkflowRequestFromUpdateJSON(t, `{
		"name": "deploy-flow",
		"steps": [{
			"name": "deploy-components",
			"workflowType": "deploy",
			"mode": "StepByStep",
			"components": ["Worker", "API"],
			"properties": [
				{"policies": ["api"]},
				{"policies": ["worker"]}
			]
		}]
	}`)

	resp := svc.TryWorkflow(context.Background(), "app-1", req)

	require.True(t, resp.Valid, "expected properties policies to match explicit components: %+v", resp.Errors)
}

func TestValidationService_TryWorkflow_SubStepPropertiesArrayValidatesEveryLogArchivePath(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "log-app", Namespace: "default"}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	store.components["worker"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "worker",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	req := tryWorkflowRequestFromUpdateJSON(t, `{
		"name": "log-flow",
		"steps": [{
			"name": "archive-group",
			"mode": "StepByStep",
			"subSteps": [{
				"name": "archive-sub",
				"workflowType": "log_archive_upload",
				"properties": [
					{"policies": ["api"], "path": "/var/log/api"},
					{"policies": ["worker"]}
				]
			}]
		}]
	}`)

	resp := svc.TryWorkflow(context.Background(), "app-1", req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "workflow[0].subSteps[0].properties[1].path", apisv1.ErrCodeMissingRequiredField)
}

func TestValidationService_TryWorkflow_SinglePropertiesArrayMergesExplicitComponents(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "log-app", Namespace: "default"}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	repos := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: repos.AppRepo, ComponentRepo: repos.ComponentRepo}

	req := tryWorkflowRequestFromUpdateJSON(t, `{
		"name": "log-flow",
		"steps": [{
			"name": "archive-api",
			"workflowType": "log_archive_upload",
			"mode": "StepByStep",
			"components": ["api"],
			"properties": [{"path": "/var/log/api"}]
		}]
	}`)

	resp := svc.TryWorkflow(context.Background(), "app-1", req)

	require.True(t, resp.Valid, "expected single properties array to merge explicit components: %+v", resp.Errors)
}

func TestValidationService_TryWorkflow_MissingJobTypeUsesDeployDefault(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.TryWorkflowRequest{
		Name: "test-workflow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:       "missing-job-type",
				Mode:       "StepByStep",
				Components: []string{"backend"},
			},
		},
	}

	resp := svc.TryWorkflow(ctx, "", req)
	assert.False(t, resp.Valid, "Expected component validation to still run")
	assert.NotEmpty(t, resp.Errors)
	for _, validationErr := range resp.Errors {
		assert.NotEqual(t, apisv1.ErrCodeInvalidWorkflowStepType, validationErr.Code)
	}
	assert.Equal(t, apisv1.ErrCodeComponentNotFound, resp.Errors[0].Code)
}
