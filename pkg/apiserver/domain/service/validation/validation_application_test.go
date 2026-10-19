package validation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestValidationService_TryApplication_ValidConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Version:   "1.0.0",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Namespace:     "default",
				Replicas:      1,
			},
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "deploy-step",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"backend"},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.True(t, resp.Valid, "Expected valid application config")
	assert.Empty(t, resp.Errors, "Expected no validation errors")
	require.NotNil(t, resp.Errors)
	require.NotNil(t, resp.NormalizedSpec)
	require.NotNil(t, resp.Plan)
	require.Len(t, resp.Plan.Actions, 2)
	raw, err := json.Marshal(resp.NormalizedSpec)
	require.NoError(t, err)
	var resubmitted apisv1.CreateApplicationsRequest
	require.NoError(t, json.Unmarshal(raw, &resubmitted))
	require.Equal(t, resp.NormalizedSpec.Components, resubmitted.Components)
	require.Equal(t, resp.NormalizedSpec.Workflow, resubmitted.Workflow)
}

func TestValidationService_TryApplication_EmitsComponentsArray(t *testing.T) {
	tests := []struct {
		name  string
		req   apisv1.CreateApplicationsRequest
		valid bool
	}{
		{name: "omitted components", req: apisv1.CreateApplicationsRequest{Name: "ab"}, valid: true},
		{name: "explicit empty components", req: apisv1.CreateApplicationsRequest{Name: "ab", Components: []apisv1.CreateComponentRequest{}}, valid: true},
		{name: "name at write limit", req: apisv1.CreateApplicationsRequest{Name: strings.Repeat("a", 31)}, valid: true},
		{name: "dotted application name", req: apisv1.CreateApplicationsRequest{Name: "demo.app"}, valid: true},
		{name: "repository error", req: apisv1.CreateApplicationsRequest{ID: "missing", Name: "ab"}, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := (&validationServiceImpl{}).TryApplication(context.Background(), tt.req)
			require.Equal(t, tt.valid, resp.Valid, "%+v", resp.Errors)
			require.NotNil(t, resp.NormalizedSpec)
			require.NotNil(t, resp.NormalizedSpec.Components)
			raw, err := json.Marshal(resp.NormalizedSpec)
			require.NoError(t, err)
			require.Contains(t, string(raw), `"components":[]`)
		})
	}
}

func TestValidationService_TryApplication_RejectsInvalidAppCallbackURL(t *testing.T) {
	svc := NewValidationService(nil, newTestURLSecurityPolicyProvider(t, spec.DefaultURLSecurityPolicy()), nil, nil)
	req := validCallbackTryApplicationRequest()
	req.Callback = &apisv1.WorkflowCallback{
		Success: "ftp://example.com/callback",
	}

	resp := svc.TryApplication(context.Background(), req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "callback", apisv1.ErrCodeInvalidTraitConfig)
}

func TestValidationService_TryApplication_RejectsAppCallbackWhenURLPolicyUnavailable(t *testing.T) {
	svc := &validationServiceImpl{}
	req := validCallbackTryApplicationRequest()
	req.Callback = &apisv1.WorkflowCallback{
		Success: "https://example.com/callback",
	}

	resp := svc.TryApplication(context.Background(), req)

	require.False(t, resp.Valid)
	requireValidationError(t, resp.Errors, "callback", apisv1.ErrCodeInvalidTraitConfig)
}

func TestValidationService_TryApplication_CreateChecksExistingAppResourceCollision(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["existing-app"] = &model.Applications{
		ID:        "existing-app",
		Name:      "game",
		Namespace: "default",
		Version:   "1.0.0",
	}
	store.components["existing-api"] = &model.ApplicationComponent{
		AppID:         "existing-app",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	svc := NewValidationService(nil, nil, repository.NewApplicationRepository(store), repository.NewComponentRepository(store))

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "game",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
		},
	})

	require.False(t, resp.Valid, "expected dry-run create to reject existing app resource collision")
	requireValidationError(t, resp.Errors, "component", apisv1.ErrCodeInvalidTraitConfig)
}

func TestValidationService_TryApplication_UpsertSkipsCurrentAppResources(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "default",
		Version:   "1.0.0",
	}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "app-1",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	appSvc := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: appSvc.AppRepo, ComponentRepo: appSvc.ComponentRepo}

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
		},
	})

	require.True(t, resp.Valid, "expected dry-run upsert to ignore the current app resources: %+v", resp.Errors)
}

func TestValidationService_TryApplication_UpsertUsesPersistedNamespaceWhenOmitted(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "team-a",
		Version:   "2.0.0",
	}
	store.apps["other-app"] = &model.Applications{
		ID:        "other-app",
		Name:      "demo",
		Namespace: "default",
		Version:   "1.0.0",
	}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "other-app",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	appSvc := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: appSvc.AppRepo, ComponentRepo: appSvc.ComponentRepo}

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		ID:   "app-1",
		Name: "demo",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
		},
	})

	require.True(t, resp.Valid, "expected dry-run upsert to inherit stored namespace: %+v", resp.Errors)
}

func TestValidationService_TryApplication_UpsertUsesExplicitNamespaceOverride(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "team-a",
		Version:   "2.0.0",
	}
	store.apps["other-app"] = &model.Applications{
		ID:        "other-app",
		Name:      "demo",
		Namespace: "default",
		Version:   "1.0.0",
	}
	store.components["api"] = &model.ApplicationComponent{
		AppID:         "other-app",
		Name:          "api",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	appSvc := newMockServiceWithStore(store)
	svc := &validationServiceImpl{AppRepo: appSvc.AppRepo, ComponentRepo: appSvc.ComponentRepo}

	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
		},
	})

	require.False(t, resp.Valid, "expected explicit namespace override to participate in resource validation")
	require.NotEmpty(t, resp.Errors)
}

func TestValidationService_TryApplication_InvalidName(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	testCases := []struct {
		name        string
		appName     string
		expectedErr string
	}{
		{"empty name", "", apisv1.ErrCodeMissingRequiredField},
		{"name too short", "a", apisv1.ErrCodeNameTooShort},
		{"name beyond write limit", strings.Repeat("a", 32), apisv1.ErrCodeNameTooLong},
		{"uppercase canonical name", "DemoApp", apisv1.ErrCodeInvalidNameFormat},
		{"invalid characters", "My_App", apisv1.ErrCodeInvalidNameFormat},
		{"starts with hyphen", "-app", apisv1.ErrCodeInvalidNameFormat},
		{"ends with hyphen", "app-", apisv1.ErrCodeInvalidNameFormat},
		{"empty subdomain segment", "demo..app", apisv1.ErrCodeInvalidNameFormat},
		{"subdomain segment starts with hyphen", "demo.-app", apisv1.ErrCodeInvalidNameFormat},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := apisv1.CreateApplicationsRequest{
				Name:       tc.appName,
				Components: []apisv1.CreateComponentRequest{},
			}

			resp := svc.TryApplication(ctx, req)

			assert.False(t, resp.Valid, "Expected invalid application config")
			assert.NotEmpty(t, resp.Errors, "Expected validation errors")

			found := false
			for _, err := range resp.Errors {
				if err.Code == tc.expectedErr && err.Field == "name" {
					found = true
					break
				}
			}
			assert.True(t, found, "Expected error code %s for field 'name'", tc.expectedErr)
		})
	}
}

func TestValidationService_TryApplication_DuplicateComponentName(t *testing.T) {
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
			},
			{
				Name:          "backend", // Duplicate
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to duplicate component")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeDuplicateComponent {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected duplicate component error")
}

func TestValidationService_TryApplication_DuplicateGeneratedResourceName(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "game",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
			{
				Name:          "game-api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to duplicate generated deployment name")
	found := false
	for _, err := range resp.Errors {
		if err.Field == "component" && err.Code == apisv1.ErrCodeInvalidTraitConfig {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected generated resource name collision error")
}

func TestValidationService_TryApplication_RejectsForceShareServiceNameCollision(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "game",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)},
					Service: []spec.ServiceTraitSpec{{
						Name:     "shared-api",
						Type:     string(spec.ServiceAccessInternal),
						Selector: map[string]string{"app": "api"},
						Ports:    []spec.ServicePortTraitSpec{{Port: 8080, TargetPort: 8080, Protocol: "TCP"}},
					}},
				},
			},
			{
				Name:          "worker",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)},
					Service: []spec.ServiceTraitSpec{{
						Name:     "shared-api",
						Type:     string(spec.ServiceAccessInternal),
						Selector: map[string]string{"app": "worker"},
						Ports:    []spec.ServicePortTraitSpec{{Port: 8081, TargetPort: 8081, Protocol: "TCP"}},
					}},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to force-share service name collision")
	found := false
	for _, err := range resp.Errors {
		if err.Field == "component" && err.Code == apisv1.ErrCodeInvalidTraitConfig {
			assert.Contains(t, err.Message, "duplicate service name")
			found = true
			break
		}
	}
	assert.True(t, found, "Expected force-share service name collision error")
}

func TestValidationService_TryApplication_AllowsUnknownShareStrategyServiceNameCollision(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "game",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Share: &spec.ShareTraitSpec{Strategy: "future-default"},
					Service: []spec.ServiceTraitSpec{{
						Name:     "shared-api",
						Type:     string(spec.ServiceAccessInternal),
						Selector: map[string]string{"app": "api"},
						Ports:    []spec.ServicePortTraitSpec{{Port: 8080, TargetPort: 8080, Protocol: "TCP"}},
					}},
				},
			},
			{
				Name:          "worker",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Share: &spec.ShareTraitSpec{Strategy: "future-default"},
					Service: []spec.ServiceTraitSpec{{
						Name:     "shared-api",
						Type:     string(spec.ServiceAccessInternal),
						Selector: map[string]string{"app": "worker"},
						Ports:    []spec.ServicePortTraitSpec{{Port: 8081, TargetPort: 8081, Protocol: "TCP"}},
					}},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.True(t, resp.Valid, "Expected unknown share strategy to use default duplicate-safe behavior, errors: %+v", resp.Errors)
}

func TestValidationService_TryApplication_AllowsStandalonePVCReuse(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "game",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
				},
			},
			{
				Name:          "worker",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Init: []spec.InitTraitSpec{{
						Name:  "init-cache",
						Image: "busybox:latest",
						Traits: spec.Traits{
							Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("init-cache", "shared-cache", false)},
						},
					}},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.True(t, resp.Valid, "Expected valid because standalone PVCs may be shared")
}

func TestValidationService_TryApplication_MissingImage(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "", // Missing image
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing image")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingImage {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing image error")
}

func TestValidationService_TryApplication_InvalidComponentType(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: "invalid-type",
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to invalid component type")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidComponentType {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid component type error")
}

func TestValidationService_TryApplication_JobProperties(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	testCases := []struct {
		name          string
		component     apisv1.CreateComponentRequest
		expectedCode  string
		expectedField string
	}{
		{
			name: "invalid runPolicy",
			component: apisv1.CreateComponentRequest{
				Name:          "job",
				ComponentType: config.InstantJob,
				Image:         "busybox:latest",
				Properties: apisv1.Properties{
					RunPolicy: "invalid",
				},
			},
			expectedCode:  apisv1.ErrCodeInvalidJobRunPolicy,
			expectedField: "component[0].properties.runPolicy",
		},
		{
			name: "instant job cleanup all failure policy",
			component: apisv1.CreateComponentRequest{
				Name:          "job",
				ComponentType: config.InstantJob,
				Image:         "busybox:latest",
				Properties: apisv1.Properties{
					FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupAll),
				},
			},
			expectedCode:  apisv1.ErrCodeInvalidJobFailurePolicy,
			expectedField: "component[0].properties.failurePolicy",
		},
		{
			name: "instant job unknown failure policy",
			component: apisv1.CreateComponentRequest{
				Name:          "job",
				ComponentType: config.InstantJob,
				Image:         "busybox:latest",
				Properties: apisv1.Properties{
					FailurePolicy: jobFailurePolicyPointer("unknown"),
				},
			},
			expectedCode:  apisv1.ErrCodeInvalidJobFailurePolicy,
			expectedField: "component[0].properties.failurePolicy",
		},
		{
			name: "init container failure policy",
			component: apisv1.CreateComponentRequest{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Init: []spec.InitTraitSpec{{
						Name:  "migrate",
						Image: "busybox:latest",
						Properties: spec.Properties{
							FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
						},
					}},
				},
			},
			expectedCode:  apisv1.ErrCodeInvalidJobFailurePolicy,
			expectedField: "component[0].traits.init[0].properties.failurePolicy",
		},
		{
			name: "instant job invalid startTime",
			component: apisv1.CreateComponentRequest{
				Name:          "job",
				ComponentType: config.InstantJob,
				Image:         "busybox:latest",
				Properties: apisv1.Properties{
					StartTime: -1,
				},
			},
			expectedCode:  apisv1.ErrCodeInvalidJobStartTime,
			expectedField: "component[0].properties.startTime",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := apisv1.CreateApplicationsRequest{
				Name:       "my-app",
				Namespace:  "default",
				Components: []apisv1.CreateComponentRequest{tc.component},
			}
			resp := svc.TryApplication(ctx, req)
			assert.False(t, resp.Valid, "Expected invalid due to job property validation")
			found := false
			for _, err := range resp.Errors {
				if err.Code == tc.expectedCode && err.Field == tc.expectedField {
					found = true
					break
				}
			}
			assert.True(t, found, "Expected error code %s for field %s", tc.expectedCode, tc.expectedField)
		})
	}
}

func TestValidationService_TryApplication_JobAllowsFailurePolicyOptOut(t *testing.T) {
	svc := &validationServiceImpl{}
	resp := svc.TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{{
			Name:          "job",
			ComponentType: config.InstantJob,
			Image:         "busybox:latest",
			Properties: apisv1.Properties{
				RunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
				FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
			},
		}},
	})

	require.True(t, resp.Valid, "unexpected validation errors: %+v", resp.Errors)
}

func TestValidationService_TryApplication_JobFailurePolicyPresence(t *testing.T) {
	tests := []struct {
		name          string
		componentType config.JobType
		policy        *workflowconfig.WorkflowFailurePolicy
		valid         bool
	}{
		{name: "job omitted inherits", componentType: config.InstantJob, valid: true},
		{name: "job explicit empty inherits", componentType: config.InstantJob, policy: jobFailurePolicyPointer("   "), valid: true},
		{name: "non job explicit empty rejected", componentType: config.ServerJob, policy: jobFailurePolicyPointer(""), valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := (&validationServiceImpl{}).TryApplication(context.Background(), apisv1.CreateApplicationsRequest{
				Name:      "my-app",
				Namespace: "default",
				Components: []apisv1.CreateComponentRequest{{
					Name:          "component",
					ComponentType: tt.componentType,
					Image:         "image:latest",
					Properties:    apisv1.Properties{FailurePolicy: tt.policy},
				}},
			})
			require.Equal(t, tt.valid, resp.Valid, "unexpected validation errors: %+v", resp.Errors)
			if !tt.valid {
				requireValidationError(t, resp.Errors, "component[0].properties.failurePolicy", apisv1.ErrCodeInvalidJobFailurePolicy)
			}
		})
	}
}

func jobFailurePolicyPointer(policy workflowconfig.WorkflowFailurePolicy) *workflowconfig.WorkflowFailurePolicy {
	return &policy
}

func TestValidationService_TryApplication_JobAllowsStartTime(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "job",
				ComponentType: config.InstantJob,
				Image:         "busybox:latest",
				Properties: apisv1.Properties{
					StartTime: time.Now().Unix() + 60,
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid, "Expected valid when job uses startTime")
	assert.Len(t, resp.Errors, 0)
}

func TestValidationService_TryApplication_ApprovalStepValid(t *testing.T) {
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
			},
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:     "approval-step",
				Mode:     "StepByStep",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &apisv1.WorkflowStepApproval{
					NotifyURL: "https://example.com/approval",
					Method:    "POST",
					Message:   "please approve",
				},
			},
			{
				Name:         "deploy-step",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"backend"},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid, "Expected approval step config to be valid")
	assert.Empty(t, resp.Errors)
}

func TestValidationService_TryApplication_ApprovalStepInvalidConfig(t *testing.T) {
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
			},
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:       "approval-step",
				Mode:       "StepByStep",
				StepType:   config.WorkflowStepTypeApproval,
				Components: []string{"backend"},
				Approval: &apisv1.WorkflowStepApproval{
					NotifyURL: "ftp://example.com/approval",
					Method:    "PATCH",
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid, "Expected invalid approval step config")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidApprovalConfig {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected invalid approval config error")
}

func TestValidationService_TryApplication_CompleteValidConfig(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	// Complete valid configuration with all traits
	staticValue := "production"
	req := apisv1.CreateApplicationsRequest{
		Name:        "demo-app",
		Namespace:   "default",
		Version:     "1.0.0",
		Project:     "demo-project",
		Description: "A complete demo application",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "app-config",
				ComponentType: config.ConfJob,
				Namespace:     "default",
				Replicas:      1,
				Properties: apisv1.Properties{
					Conf: map[string]string{
						"database.host": "mysql.default.svc",
						"database.port": "3306",
					},
				},
			},
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "myregistry/backend:v1.0.0",
				Namespace:     "default",
				Replicas:      2,
				Properties: apisv1.Properties{
					Ports: []spec.Ports{{Port: 8080}},
					Env: map[string]string{
						"APP_ENV": "production",
					},
				},
				Traits: apisv1.Traits{
					Probes: []spec.ProbeTraitsSpec{
						{
							Type: "liveness",
							HTTPGet: &spec.HTTPGetProbe{
								Path: "/healthz",
								Port: 8080,
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
						},
						{
							Type: "readiness",
							HTTPGet: &spec.HTTPGetProbe{
								Path: "/ready",
								Port: 8080,
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       5,
						},
					},
					Resources: &spec.ResourceTraitsSpec{
						CPU:    "500m",
						Memory: "512Mi",
					},
					Envs: []spec.SimplifiedEnvSpec{
						{
							Name: "APP_ENV",
							ValueFrom: spec.ValueSource{
								Static: &staticValue,
							},
						},
					},
					Storage: []spec.StorageTraitSpec{
						{
							Type:      "persistent",
							Name:      "data",
							MountPath: "/data",
							TmpCreate: true,
							Size:      "10Gi",
						},
					},
				},
			},
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "config-step",
				WorkflowType: config.JobDeploy,
				Mode:         "StepByStep",
				Components:   []string{"app-config"},
			},
			{
				Name:         "deploy-backend",
				WorkflowType: config.JobDeploy,
				Mode:         "DAG",
				Components:   []string{"backend"},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.True(t, resp.Valid, "Expected valid application config")
	if !resp.Valid {
		for _, err := range resp.Errors {
			t.Logf("Validation error: field=%s code=%s message=%s", err.Field, err.Code, err.Message)
		}
	}
	assert.Empty(t, resp.Errors, "Expected no validation errors")
}

func TestValidationService_TryApplication_MissingEnvSourceName(t *testing.T) {
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
					EnvFrom: []spec.EnvFromSourceSpec{
						{
							Type:       "configMap",
							SourceName: "", // Missing source name
						},
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	assert.False(t, resp.Valid, "Expected invalid due to missing sourceName")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingRequiredField && err.Field == "component[0].traits.envFrom[0].sourceName" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected missing sourceName error")
}

func TestValidationService_TryApplication_NestedInitForbidden(t *testing.T) {
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
							Name:  "init-1",
							Image: "busybox:latest",
							Traits: spec.Traits{
								// Nested init is forbidden
								Init: []spec.InitTraitSpec{
									{
										Name:  "nested-init",
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

	assert.False(t, resp.Valid, "Expected invalid due to nested init")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeNestedTraitForbidden {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected nested trait forbidden error")
}

func TestValidationService_TryApplication_UnsupportedWorkflowJobTypeIsRejected(t *testing.T) {
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
			},
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "unsupported-step",
				WorkflowType: config.JobType("unsupported_job_type"),
				Mode:         "DAG",
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid, "Expected unsupported jobType to be rejected")
	assert.NotEmpty(t, resp.Errors)
	assert.Equal(t, apisv1.ErrCodeInvalidWorkflowStepType, resp.Errors[0].Code)
}

func TestValidationService_TryApplication_ConfigTypeNoImageRequired(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	// Config type component should not require an image
	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "app-config",
				ComponentType: config.ConfJob,
				Image:         "", // No image required for config type
				Properties: apisv1.Properties{
					Conf: map[string]string{
						"key": "value",
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	// Should not have missing image error for config type
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingImage {
			t.Errorf("Should not require image for config type component")
		}
	}
}

func TestValidationService_TryApplication_SecretTypeNoImageRequired(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	// Secret type component should not require an image
	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "app-secret",
				ComponentType: config.SecretJob,
				Image:         "", // No image required for secret type
				Properties: apisv1.Properties{
					Secret: map[string]string{
						"password": "secret123",
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)

	// Should not have missing image error for secret type
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeMissingImage {
			t.Errorf("Should not require image for secret type component")
		}
	}
}

func TestValidationService_TryApplication_AllowsEmptySecretValues(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name:      "my-app",
		Namespace: "default",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "app-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{
						"password": "",
						"username": "root",
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid, "Expected empty secret values to remain valid")
	for _, err := range resp.Errors {
		assert.NotEqual(t, "component[0].properties.secret.password", err.Field)
	}
}

func TestValidationService_TryApplication_InvalidReservedPropertiesLabels(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Properties: apisv1.Properties{
					Labels: map[string]string{
						config.LabelComponentName: "custom-backend",
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.False(t, resp.Valid, "Expected invalid when overriding reserved labels")
	found := false
	for _, err := range resp.Errors {
		if err.Code == apisv1.ErrCodeInvalidTraitConfig &&
			err.Field == "component[0].properties.labels."+config.LabelComponentName {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected reserved label override validation error")
}

func TestValidationService_TryApplication_ValidCustomPropertiesLabels(t *testing.T) {
	svc := &validationServiceImpl{}
	ctx := context.Background()

	req := apisv1.CreateApplicationsRequest{
		Name: "my-app",
		Components: []apisv1.CreateComponentRequest{
			{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Properties: apisv1.Properties{
					Labels: map[string]string{
						"app.kubernetes.io/part-of": "my-app",
					},
				},
			},
		},
	}

	resp := svc.TryApplication(ctx, req)
	assert.True(t, resp.Valid, "Expected custom labels to remain valid")
	assert.Empty(t, resp.Errors, "Expected no validation errors for custom labels")
}
