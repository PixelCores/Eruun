package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestPrepareComponentsPersistsJobFailurePolicyOptOut(t *testing.T) {
	components, err := prepareComponents("app-1", "default", []apisv1.CreateComponentRequest{{
		Name:          "mysql-update-job",
		ComponentType: config.InstantJob,
		Image:         "skeema-tool:latest",
		Properties: apisv1.Properties{
			RunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
			FailurePolicy: jobFailurePolicyPointer(" cleanup_FAILED "),
		},
	}})
	require.NoError(t, err)
	require.Len(t, components, 1)

	var properties apisv1.Properties
	require.NoError(t, decodeJSONStruct(components[0].Properties, &properties))
	require.Equal(t, string(workflowconfig.JobRunPolicyRecreate), properties.RunPolicy)
	require.NotNil(t, properties.FailurePolicy)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupFailed, *properties.FailurePolicy)
}

func TestPrepareComponentsRejectsInvalidJobFailurePolicy(t *testing.T) {
	tests := []struct {
		name          string
		componentType config.JobType
		policy        workflowconfig.WorkflowFailurePolicy
	}{
		{name: "cleanup all", componentType: config.InstantJob, policy: workflowconfig.WorkflowFailurePolicyCleanupAll},
		{name: "unknown", componentType: config.InstantJob, policy: "unknown"},
		{name: "non job", componentType: config.ServerJob, policy: workflowconfig.WorkflowFailurePolicyCleanupFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := prepareComponents("app-1", "default", []apisv1.CreateComponentRequest{{
				Name:          "component",
				ComponentType: tt.componentType,
				Image:         "image:latest",
				Properties: apisv1.Properties{
					FailurePolicy: jobFailurePolicyPointer(tt.policy),
				},
			}})
			require.ErrorIs(t, err, bcode.ErrInvalidProperties)
			require.Contains(t, err.Error(), "failurePolicy")
		})
	}
}

func TestPrepareComponentsRejectsNestedJobFailurePolicy(t *testing.T) {
	_, err := prepareComponents("app-1", "default", []apisv1.CreateComponentRequest{{
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
	}})
	require.ErrorIs(t, err, bcode.ErrInvalidProperties)
	require.Contains(t, err.Error(), "traits.init[0].properties.failurePolicy")
}

func TestUpdateVersionAddPersistsJobFailurePolicyOptOut(t *testing.T) {
	store := versionUpdateFailurePolicyStore()
	svc := newMockServiceWithStore(store)
	replicas := int32(1)

	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Action:        "add",
			Name:          "mysql-update-job",
			ComponentType: config.InstantJob,
			Image:         "skeema-tool:latest",
			Replicas:      &replicas,
			Properties: &apisv1.Properties{
				RunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
				FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
			},
		}},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)

	var properties apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["mysql-update-job"].Properties, &properties))
	require.Equal(t, string(workflowconfig.JobRunPolicyRecreate), properties.RunPolicy)
	require.NotNil(t, properties.FailurePolicy)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupFailed, *properties.FailurePolicy)
}

func TestUpdateVersionUpdatePersistsJobFailurePolicyOptOut(t *testing.T) {
	store := versionUpdateFailurePolicyStore()
	store.components["existing-job"] = &model.ApplicationComponent{
		Name:          "existing-job",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "skeema-tool:latest",
		Properties:    mustJSONStruct(&apisv1.Properties{}),
	}
	svc := newMockServiceWithStore(store)

	_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name: "existing-job",
			Properties: &apisv1.Properties{
				RunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
				FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
			},
		}},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)

	var properties apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["existing-job"].Properties, &properties))
	require.Equal(t, string(workflowconfig.JobRunPolicyRecreate), properties.RunPolicy)
	require.NotNil(t, properties.FailurePolicy)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupFailed, *properties.FailurePolicy)
}

func TestUpdateVersionRejectsInvalidJobFailurePolicy(t *testing.T) {
	tests := []struct {
		name          string
		componentType config.JobType
		policy        workflowconfig.WorkflowFailurePolicy
	}{
		{name: "cleanup all", componentType: config.InstantJob, policy: workflowconfig.WorkflowFailurePolicyCleanupAll},
		{name: "non job", componentType: config.ServerJob, policy: workflowconfig.WorkflowFailurePolicyCleanupFailed},
		{name: "non job whitespace", componentType: config.ServerJob, policy: "   "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := versionUpdateFailurePolicyStore()
			svc := newMockServiceWithStore(store)
			_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version: "1.1.0",
				Components: []apisv1.ComponentUpdateSpec{{
					Action:        "add",
					Name:          "new-component",
					ComponentType: tt.componentType,
					Image:         "image:latest",
					Properties: &apisv1.Properties{
						FailurePolicy: jobFailurePolicyPointer(tt.policy),
					},
				}},
				AutoExec: boolPtr(false),
			})
			require.ErrorIs(t, err, bcode.ErrInvalidProperties)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			require.Nil(t, store.components["new-component"])
		})
	}
}

func TestUpdateVersionCanonicalizesWhitespaceJobFailurePolicyWithoutPseudoChange(t *testing.T) {
	store := versionUpdateFailurePolicyStore()
	store.components["existing-job"] = &model.ApplicationComponent{
		Name:          "existing-job",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "skeema-tool:latest",
		Properties:    mustJSONStruct(&apisv1.Properties{}),
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name: "existing-job",
			Properties: &apisv1.Properties{
				FailurePolicy: jobFailurePolicyPointer("   "),
			},
		}},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)
	require.Empty(t, resp.UpdatedComponents)

	var properties apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["existing-job"].Properties, &properties))
	require.Nil(t, properties.FailurePolicy)
}

func TestUpdateVersionExplicitEmptyClearsJobFailurePolicyOverride(t *testing.T) {
	store := versionUpdateFailurePolicyStore()
	store.components["existing-job"] = &model.ApplicationComponent{
		Name:          "existing-job",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "skeema-tool:latest",
		Properties: mustJSONStruct(&apisv1.Properties{
			FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
		}),
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name: "existing-job",
			Properties: &apisv1.Properties{
				FailurePolicy: jobFailurePolicyPointer(""),
			},
		}},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"existing-job"}, resp.UpdatedComponents)

	var properties apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["existing-job"].Properties, &properties))
	require.Nil(t, properties.FailurePolicy)
}

func TestUpdateVersionOmittedPropertiesPreservesJobFailurePolicyOverride(t *testing.T) {
	store := versionUpdateFailurePolicyStore()
	store.components["existing-job"] = &model.ApplicationComponent{
		Name:          "existing-job",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "skeema-tool:v1",
		Properties: mustJSONStruct(&apisv1.Properties{
			Env:           map[string]string{"SCHEMA": "game"},
			RunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
			FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
		}),
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:  "existing-job",
			Image: "skeema-tool:v2",
		}},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"existing-job"}, resp.UpdatedComponents)
	require.Equal(t, "skeema-tool:v2", store.components["existing-job"].Image)

	var properties apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["existing-job"].Properties, &properties))
	require.Equal(t, map[string]string{"SCHEMA": "game"}, properties.Env)
	require.Equal(t, string(workflowconfig.JobRunPolicyRecreate), properties.RunPolicy)
	require.NotNil(t, properties.FailurePolicy)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupFailed, *properties.FailurePolicy)
}

func TestUpdateVersionEmptyPropertiesClearsJobFailurePolicyOverride(t *testing.T) {
	store := versionUpdateFailurePolicyStore()
	store.components["existing-job"] = &model.ApplicationComponent{
		Name:          "existing-job",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "skeema-tool:latest",
		Properties: mustJSONStruct(&apisv1.Properties{
			Env:           map[string]string{"SCHEMA": "game"},
			RunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
			FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
		}),
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{{
			Name:       "existing-job",
			Properties: &apisv1.Properties{},
		}},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"existing-job"}, resp.UpdatedComponents)

	var properties apisv1.Properties
	require.NoError(t, decodeJSONStruct(store.components["existing-job"].Properties, &properties))
	require.Empty(t, properties.Env)
	require.Empty(t, properties.RunPolicy)
	require.Nil(t, properties.FailurePolicy)
}

func TestCreateApplicationsFromTemplateMergesJobFailurePolicyOverride(t *testing.T) {
	tests := []struct {
		name           string
		templatePolicy *workflowconfig.WorkflowFailurePolicy
		overridePolicy *workflowconfig.WorkflowFailurePolicy
		expectedPolicy *workflowconfig.WorkflowFailurePolicy
	}{
		{
			name:           "request sets override",
			overridePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
			expectedPolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
		},
		{
			name:           "omitted request preserves template override",
			templatePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
			expectedPolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
		},
		{
			name:           "explicit empty clears template override",
			templatePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
			overridePolicy: jobFailurePolicyPointer("   "),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := templateFailurePolicyStore(config.InstantJob, tt.templatePolicy)
			svc := newMockServiceWithStore(store)
			resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
				Name: "cloned-app",
				Component: []apisv1.CreateComponentRequest{{
					Name:       "mysql-update-job",
					Template:   &apisv1.TemplateRef{ID: "tmpl-1", Target: "template-component"},
					Properties: apisv1.Properties{FailurePolicy: tt.overridePolicy},
				}},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)

			created := findCreatedFailurePolicyComponent(store, resp.ID)
			require.NotNil(t, created)
			var properties apisv1.Properties
			require.NoError(t, decodeJSONStruct(created.Properties, &properties))
			if tt.expectedPolicy == nil {
				require.Nil(t, properties.FailurePolicy)
				return
			}
			require.NotNil(t, properties.FailurePolicy)
			require.Equal(t, *tt.expectedPolicy, *properties.FailurePolicy)
		})
	}
}

func TestCreateApplicationsFromTemplateRejectsInvalidJobFailurePolicyOverride(t *testing.T) {
	tests := []struct {
		name          string
		componentType config.JobType
		policy        workflowconfig.WorkflowFailurePolicy
	}{
		{name: "cleanup all", componentType: config.InstantJob, policy: workflowconfig.WorkflowFailurePolicyCleanupAll},
		{name: "unknown", componentType: config.InstantJob, policy: "unknown"},
		{name: "non job", componentType: config.ServerJob, policy: workflowconfig.WorkflowFailurePolicyCleanupFailed},
		{name: "non job empty", componentType: config.ServerJob, policy: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := templateFailurePolicyStore(tt.componentType, nil)
			svc := newMockServiceWithStore(store)
			resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
				Name: "cloned-app",
				Component: []apisv1.CreateComponentRequest{{
					Name:       "component",
					Template:   &apisv1.TemplateRef{ID: "tmpl-1", Target: "template-component"},
					Properties: apisv1.Properties{FailurePolicy: jobFailurePolicyPointer(tt.policy)},
				}},
			})
			require.Nil(t, resp)
			require.ErrorIs(t, err, bcode.ErrInvalidProperties)
			require.Contains(t, err.Error(), "failurePolicy")
		})
	}
}

func TestCreateApplicationsFromTemplateRejectsNestedJobFailurePolicyOverride(t *testing.T) {
	store := templateFailurePolicyStore(config.InstantJob, nil)
	svc := newMockServiceWithStore(store)

	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "cloned-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:     "mysql-update-job",
			Template: &apisv1.TemplateRef{ID: "tmpl-1", Target: "template-component"},
			Traits: apisv1.Traits{Init: []spec.InitTraitSpec{{
				Name: "migrate",
				Properties: spec.Properties{
					FailurePolicy: jobFailurePolicyPointer(workflowconfig.WorkflowFailurePolicyCleanupFailed),
				},
			}}},
		}},
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrInvalidProperties)
	require.Contains(t, err.Error(), "component[0].traits.init[0].properties.failurePolicy")
}

func templateFailurePolicyStore(componentType config.JobType, policy *workflowconfig.WorkflowFailurePolicy) *inMemoryAppStore {
	store := newInMemoryAppStore()
	store.apps["tmpl-1"] = &model.Applications{
		ID:              "tmpl-1",
		Name:            "template",
		Namespace:       "default",
		Version:         "1.0.0",
		TemplateEnabled: true,
	}
	store.components["template-component"] = &model.ApplicationComponent{
		Name:          "template-component",
		AppID:         "tmpl-1",
		Namespace:     "default",
		ComponentType: componentType,
		Image:         "image:latest",
		Properties: mustJSONStruct(&apisv1.Properties{
			FailurePolicy: policy,
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}
	return store
}

func findCreatedFailurePolicyComponent(store *inMemoryAppStore, appID string) *model.ApplicationComponent {
	for _, component := range store.components {
		if component != nil && component.AppID == appID {
			return component
		}
	}
	return nil
}

func jobFailurePolicyPointer(policy workflowconfig.WorkflowFailurePolicy) *workflowconfig.WorkflowFailurePolicy {
	return &policy
}

func versionUpdateFailurePolicyStore() *inMemoryAppStore {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		Name:  "demo-workflow",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{
			Name:         "backend",
			WorkflowType: config.JobDeploy,
		}}}),
	}
	return store
}
