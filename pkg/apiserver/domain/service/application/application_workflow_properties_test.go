package application

import (
	"context"
	"encoding/json"

	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"

	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateApplicationWorkflowPreservesComponentsForEmptyPropertiesArray(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1"}
	store.components["worker"] = &model.ApplicationComponent{Name: "worker", AppID: "app-1"}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "custom-flow",
		"steps": [
			{
				"name": "deploy-web",
				"workflowType": "deploy",
				"components": [" Web "],
				"properties": []
			},
			{
				"name": "deploy-group",
				"workflowType": "deploy",
				"subSteps": [
					{
						"name": "deploy-worker",
						"workflowType": "deploy",
						"components": ["WORKER"],
						"properties": []
					}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 2)
	require.Equal(t, []model.Policies{{Policies: []string{"web"}}}, steps.Steps[0].Properties)
	require.Len(t, steps.Steps[1].SubSteps, 1)
	require.Equal(t, []model.Policies{{Policies: []string{"worker"}}}, steps.Steps[1].SubSteps[0].Properties)
}

func TestUpdateApplicationWorkflowValidatesComponentsForEmptyPropertiesArray(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "custom-flow",
		"steps": [
			{
				"name": "deploy-web",
				"workflowType": "deploy",
				"components": ["web"],
				"properties": []
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), `component "web" not found`)
}

func TestUpdateApplicationWorkflowAcceptsReadResponsePropertiesArray(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	for _, name := range []string{"api", "worker", "sidecar"} {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "log-archive-upload",
		"workflowType": "log_archive_upload",
		"steps": [
			{
				"name": "archive-pods",
				"workflowType": "log_archive_upload",
				"components": [" API ", "WORKER"],
				"properties": [
					{"policies": [" API ", "api"], "path": "/var/log/api", "container": "api"},
					{"policies": ["Worker"], "path": "/var/log/worker"}
				],
				"subSteps": [
					{
						"name": "archive-sidecar",
						"workflowType": "log_archive_upload",
						"components": [" SideCar "],
						"properties": [
							{"policies": ["SideCar"], "path": "/var/log/sidecar", "container": "logs"}
						]
					}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, []model.Policies{
		{Policies: []string{"api"}, Path: "/var/log/api", Container: "api"},
		{Policies: []string{"worker"}, Path: "/var/log/worker"},
	}, steps.Steps[0].Properties)
	require.Len(t, steps.Steps[0].SubSteps, 1)
	require.Equal(t, []model.Policies{
		{Policies: []string{"sidecar"}, Path: "/var/log/sidecar", Container: "logs"},
	}, steps.Steps[0].SubSteps[0].Properties)
}

func TestUpdateApplicationWorkflowRejectsComponentsMismatchForPropertiesArray(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	for _, name := range []string{"api", "worker", "cache"} {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "deploy-flow",
		"workflowType": "workflow",
		"steps": [
			{
				"name": "deploy-components",
				"workflowType": "deploy",
				"components": ["api", "worker"],
				"properties": [
					{"policies": ["api"]},
					{"policies": ["cache"]}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Contains(t, err.Error(), "components must match properties policies")
}

func TestUpdateApplicationWorkflowAcceptsPropertiesArrayWithoutExplicitComponents(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	for _, name := range []string{"api", "worker"} {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "deploy-flow",
		"workflowType": "workflow",
		"steps": [
			{
				"name": "deploy-components",
				"workflowType": "deploy",
				"properties": [
					{"policies": ["API"]},
					{"policies": ["worker"]}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	steps := decodeWorkflowSteps(t, stored.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, []model.Policies{
		{Policies: []string{"api"}},
		{Policies: []string{"worker"}},
	}, steps.Steps[0].Properties)
}

func TestUpdateApplicationWorkflowRejectsAmbiguousPropertiesArray(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "log-archive-upload",
		"workflowType": "log_archive_upload",
		"steps": [
			{
				"name": "archive-pods",
				"workflowType": "log_archive_upload",
				"components": ["api"],
				"properties": [
					{"policies": ["  "], "path": "/var/log/api"},
					{"policies": ["api"], "path": "/var/log/api2"}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "properties entries require policies")
}

func TestUpdateApplicationWorkflowRejectsDuplicatePropertiesArrayPolicies(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "deploy-flow",
		"workflowType": "workflow",
		"steps": [
			{
				"name": "deploy-api",
				"workflowType": "deploy",
				"properties": [
					{"policies": ["api"]},
					{"policies": ["API"]}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "duplicate policy")
}

func TestUpdateApplicationWorkflowRejectsDuplicateSubStepPropertiesArrayPolicies(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "deploy-flow",
		"workflowType": "workflow",
		"steps": [
			{
				"name": "deploy-group",
				"mode": "StepByStep",
				"subSteps": [
					{
						"name": "deploy-api",
						"workflowType": "deploy",
						"properties": [
							{"policies": ["api"]},
							{"policies": ["API"]}
						]
					}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "duplicate policy")
}

func TestUpdateApplicationWorkflowRejectsSubStepComponentsMismatchForPropertiesArray(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	for _, name := range []string{"api", "worker", "cache"} {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
		}
	}

	var req apisv1.UpdateApplicationWorkflowRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "deploy-flow",
		"workflowType": "workflow",
		"steps": [
			{
				"name": "deploy-group",
				"mode": "StepByStep",
				"subSteps": [
					{
						"name": "deploy-sub",
						"workflowType": "deploy",
						"components": ["api", "worker"],
						"properties": [
							{"policies": ["api"]},
							{"policies": ["cache"]}
						]
					}
				]
			}
		]
	}`), &req))

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Contains(t, err.Error(), "components must match properties policies")
}
