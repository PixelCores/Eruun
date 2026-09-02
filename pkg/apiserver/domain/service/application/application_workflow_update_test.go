package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"

	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateApplicationWorkflowCreatesWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["config"] = &model.ApplicationComponent{Name: "config", AppID: "app-1"}
	store.components["mysql-primary"] = &model.ApplicationComponent{Name: "mysql-primary", AppID: "app-1"}
	store.components["mysql-replica"] = &model.ApplicationComponent{Name: "mysql-replica", AppID: "app-1"}
	store.components["dashboard"] = &model.ApplicationComponent{Name: "dashboard", AppID: "app-1"}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name:  "custom-flow",
		Alias: "primary-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "prepare-config", WorkflowType: config.JobDeploy, Mode: "StepByStep", Components: []string{"config"}},
			{Name: "databases", WorkflowType: config.JobDeploy, Mode: "DAG", Components: []string{"mysql-primary", "mysql-replica"}},
			{Name: "deploy-dashboard", WorkflowType: config.JobDeploy, Components: []string{"dashboard"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.WorkflowID)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	require.Equal(t, "custom-flow", stored.Name)
	require.Equal(t, "primary-flow", stored.Alias)

	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 3)
	require.Equal(t, config.WorkflowFailurePolicyCleanupAll, steps.FailurePolicy)
	require.Equal(t, config.WorkflowModeDAG, steps.Steps[1].Mode)
	require.ElementsMatch(t, []string{"mysql-primary", "mysql-replica"}, steps.Steps[1].Properties[0].Policies)
}

func TestUpdateApplicationWorkflowStoresAndEchoesFailurePolicy(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1", ComponentType: config.ServerJob}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		Name:          "custom-flow",
		FailurePolicy: config.WorkflowFailurePolicyCleanupAll,
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
		}},
	})
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Equal(t, config.WorkflowFailurePolicyCleanupAll, steps.FailurePolicy)

	dto, err := assembler.ConvertWorkflowModelToDTO(stored)
	require.NoError(t, err)
	require.Equal(t, config.WorkflowFailurePolicyCleanupAll, dto.FailurePolicy)
}

func TestUpdateApplicationWorkflowPreservesExistingFailurePolicyWhenOmitted(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1", ComponentType: config.ServerJob}
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		FailurePolicy: config.WorkflowFailurePolicyCleanupAll,
		Steps: []*model.WorkflowStep{{
			Name:         "old-deploy-web",
			WorkflowType: config.JobDeploy,
			Properties:   []model.Policies{{Policies: []string{"web"}}},
		}},
	})
	require.NoError(t, err)
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Name:  "custom-flow",
		Steps: stepsJSON,
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		WorkflowID: "wf-1",
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, "wf-1", resp.WorkflowID)

	stored := store.workflows["wf-1"]
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Equal(t, config.WorkflowFailurePolicyCleanupAll, steps.FailurePolicy)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, "deploy-web", steps.Steps[0].Name)
}

func TestUpdateApplicationWorkflowExplicitlyResetsFailurePolicy(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1", ComponentType: config.ServerJob}
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		FailurePolicy: config.WorkflowFailurePolicyCleanupAll,
		Steps: []*model.WorkflowStep{{
			Name:         "old-deploy-web",
			WorkflowType: config.JobDeploy,
			Properties:   []model.Policies{{Policies: []string{"web"}}},
		}},
	})
	require.NoError(t, err)
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Name:  "custom-flow",
		Steps: stepsJSON,
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		WorkflowID:       "wf-1",
		FailurePolicy:    config.WorkflowFailurePolicyCleanupFailed,
		FailurePolicySet: true,
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, "wf-1", resp.WorkflowID)

	stored := store.workflows["wf-1"]
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Equal(t, config.WorkflowFailurePolicyCleanupFailed, steps.FailurePolicy)
}

func TestUpdateApplicationWorkflowCreatesWorkflowWithCleanupFailedOptOut(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1", ComponentType: config.ServerJob}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		Name:             "custom-flow",
		FailurePolicy:    config.WorkflowFailurePolicyCleanupFailed,
		FailurePolicySet: true,
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
		}},
	})
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Equal(t, config.WorkflowFailurePolicyCleanupFailed, steps.FailurePolicy)
}

func TestUpdateApplicationWorkflowNormalizesComponentRefs(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["api"] = &model.ApplicationComponent{Name: "api", AppID: "app-1"}
	store.components["worker"] = &model.ApplicationComponent{Name: "worker", AppID: "app-1"}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		Name: "custom-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy",
			WorkflowType: config.JobDeploy,
			Components:   []string{" API ", "api"},
			Properties: apisv1.WorkflowProperties{
				Policies: []string{" Worker "},
			},
		}},
	})
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, []string{"api", "worker"}, steps.Steps[0].Properties[0].Policies)
}

func TestUpdateApplicationWorkflowCanonicalizesMixedCaseComponentRefs(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["web"] = &model.ApplicationComponent{
		Name:          "Web",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}
	store.components["worker"] = &model.ApplicationComponent{
		Name:          "Worker",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "custom-flow",
		"workflowType": "workflow",
		"steps": [
			{
				"name": "deploy-web",
				"workflowType": "deploy",
				"components": [" web ", "WEB"],
				"properties": [
					{"policies": ["WEB"]}
				]
			},
			{
				"name": "deploy-group",
				"workflowType": "deploy",
				"subSteps": [
					{
						"name": "deploy-worker",
						"workflowType": "deploy",
						"components": ["worker"],
						"properties": [
							{"policies": ["WORKER"]}
						]
					}
				]
			},
			{
				"name": "web",
				"workflowType": "log_archive_upload",
				"properties": {
					"path": "/var/log/web"
				}
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 3)
	require.Equal(t, []model.Policies{{Policies: []string{"Web"}}}, steps.Steps[0].Properties)
	require.Len(t, steps.Steps[1].SubSteps, 1)
	require.Equal(t, []model.Policies{{Policies: []string{"Worker"}}}, steps.Steps[1].SubSteps[0].Properties)
	require.Equal(t, []model.Policies{{Policies: []string{"Web"}, Path: "/var/log/web"}}, steps.Steps[2].Properties)
}

func TestUpdateApplicationWorkflowRejectsUnsupportedJobType(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1"}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "custom-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "unsupported-step", WorkflowType: config.JobType("unsupported_job_type"), Mode: "DAG"},
			{Name: "deploy-web", WorkflowType: config.JobDeploy, Components: []string{"web"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "unsupported_job_type")
}

func TestUpdateApplicationWorkflowBlockedWhenWorkflowTaskActive(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	store.tasks["task-active"] = &model.WorkflowQueue{
		TaskID:          "task-active",
		AppID:           "app-1",
		Status:          config.StatusWaitingApprove,
		ApprovalPending: true,
	}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "blocked-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	_, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
}

func TestUpdateApplicationWorkflowSetsWorkflowType(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name:         "update-flow",
		WorkflowType: config.WorkflowTaskTypeUpdate,
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	require.Equal(t, config.WorkflowTaskTypeUpdate, stored.WorkflowType)
}

func TestUpdateApplicationWorkflowAllowsLogArchiveWorkflowType(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["api"] = &model.ApplicationComponent{Name: "api", AppID: "app-1", ComponentType: config.ServerJob, Image: "nginx:latest"}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name:         "log-archive-upload",
		WorkflowType: config.WorkflowTaskTypeLogArchiveUpload,
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "archive-api",
				WorkflowType: config.JobLogArchiveUpload,
				Components:   []string{"api"},
				Properties: apisv1.WorkflowProperties{
					Path: "/var/log/api",
				},
			},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	require.Equal(t, config.WorkflowTaskTypeLogArchiveUpload, stored.WorkflowType)
}

func TestUpdateApplicationWorkflowPreservesLogArchivePathForNameBasedStep(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["api"] = &model.ApplicationComponent{Name: "api", AppID: "app-1", ComponentType: config.ServerJob, Image: "nginx:latest"}

	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		Name:         "log-archive-upload",
		WorkflowType: config.WorkflowTaskTypeLogArchiveUpload,
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name:         "api",
			WorkflowType: config.JobLogArchiveUpload,
			Properties: apisv1.WorkflowProperties{
				Path:      "/var/log/api",
				Container: "api",
			},
		}},
	})
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, []model.Policies{{
		Policies:  []string{"api"},
		Path:      "/var/log/api",
		Container: "api",
	}}, steps.Steps[0].Properties)
}

func TestUpdateApplicationWorkflowPreservesLogArchivePathForNameBasedSubStep(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["api"] = &model.ApplicationComponent{Name: "api", AppID: "app-1", ComponentType: config.ServerJob, Image: "nginx:latest"}

	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		Name:         "log-archive-upload",
		WorkflowType: config.WorkflowTaskTypeLogArchiveUpload,
		Workflow: []apisv1.CreateWorkflowStepRequest{{
			Name: "archive-group",
			Mode: string(config.WorkflowModeStepByStep),
			SubSteps: []apisv1.CreateWorkflowSubStepRequest{{
				Name:         "api",
				WorkflowType: config.JobLogArchiveUpload,
				Properties: apisv1.WorkflowProperties{
					Path: "/var/log/api",
				},
			}},
		}},
	})
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 1)
	require.Len(t, steps.Steps[0].SubSteps, 1)
	require.Equal(t, []model.Policies{{
		Policies: []string{"api"},
		Path:     "/var/log/api",
	}}, steps.Steps[0].SubSteps[0].Properties)
}

func TestUpdateApplicationWorkflowRejectsLogArchiveNonPodComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["config"] = &model.ApplicationComponent{Name: "config", AppID: "app-1", ComponentType: config.ConfJob}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name:         "log-archive-upload",
		WorkflowType: config.WorkflowTaskTypeLogArchiveUpload,
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "archive-config",
				WorkflowType: config.JobLogArchiveUpload,
				Components:   []string{"config"},
				Properties: apisv1.WorkflowProperties{
					Path: "/var/log/api",
				},
			},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.True(t, errors.Is(err, bcode.ErrWorkflowConfig))
	require.Contains(t, err.Error(), "does not use pods")
}

func TestUpdateApplicationWorkflowUpdatesExisting(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	existing := &model.Workflow{
		ID:        "wf-1",
		Name:      "existing",
		AppID:     "app-1",
		ProjectID: "proj-1",
		Alias:     "old",
	}
	store.workflows[existing.ID] = existing
	store.components["dashboard"] = &model.ApplicationComponent{Name: "dashboard", AppID: "app-1"}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		WorkflowID:   "wf-1",
		Alias:        "updated-alias",
		WorkflowType: config.WorkflowTaskTypeUpdate,
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-dashboard", WorkflowType: config.JobDeploy, Mode: "StepByStep", Components: []string{"dashboard"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "wf-1", resp.WorkflowID)

	stored := store.workflows["wf-1"]
	require.Equal(t, "updated-alias", stored.Alias)
	require.Equal(t, config.WorkflowTaskTypeUpdate, stored.WorkflowType)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, "deploy-dashboard", steps.Steps[0].Name)
	require.Equal(t, config.WorkflowModeStepByStep, steps.Steps[0].Mode)
}

func TestUpdateApplicationWorkflowCreatesNewWhenWorkflowIDMissing(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}

	existing := &model.Workflow{
		ID:        "wf-1",
		Name:      "default-flow",
		AppID:     "app-1",
		ProjectID: "proj-1",
	}
	store.workflows[existing.ID] = existing

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "custom-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEqual(t, existing.ID, resp.WorkflowID)

	newWorkflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, newWorkflow)
	require.Equal(t, "custom-flow", newWorkflow.Name)
	require.Equal(t, "default-flow", store.workflows["wf-1"].Name)
}

func TestUpdateApplicationWorkflowInheritsMetadata(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Namespace: "app-ns",
		Project:   "proj-app",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	store.workflows["wf-legacy"] = &model.Workflow{
		ID:          "wf-legacy",
		Name:        "demoapp-workflow",
		AppID:       "app-1",
		Namespace:   "legacy-ns",
		ProjectID:   "legacy-proj",
		Description: "legacy-desc",
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "another-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	created := store.workflows[resp.WorkflowID]
	require.NotNil(t, created)
	require.Equal(t, "legacy-ns", created.Namespace)
	require.Equal(t, "legacy-proj", created.ProjectID)
	require.Equal(t, "legacy-desc", created.Description)
}

func TestUpdateApplicationWorkflowDefaultsMetadataFromApp(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Namespace: "app-ns",
		Project:   "proj-app",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateApplicationWorkflowRequest{
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	created := store.workflows[resp.WorkflowID]
	require.NotNil(t, created)
	require.Equal(t, "app-ns", created.Namespace)
	require.Equal(t, "proj-app", created.ProjectID)
}

func TestUpdateApplicationWorkflowGeneratesUniqueName(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	store.workflows["wf-default"] = &model.Workflow{
		ID:        "wf-default",
		Name:      "demoapp-workflow",
		AppID:     "app-1",
		ProjectID: "proj-1",
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateApplicationWorkflowRequest{
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEqual(t, "wf-default", resp.WorkflowID)

	created := store.workflows[resp.WorkflowID]
	require.NotNil(t, created)
	require.Equal(t, "demoapp-workflow-1", created.Name)
}

func TestUpdateApplicationWorkflowMissingComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["config"] = &model.ApplicationComponent{Name: "config", AppID: "app-1"}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "bad-flow",
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "missing", WorkflowType: config.JobDeploy, Components: []string{"not-found"}},
		},
	}

	_, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.True(t, errors.Is(err, bcode.ErrWorkflowConfig))
	require.Contains(t, err.Error(), "not found")
}
