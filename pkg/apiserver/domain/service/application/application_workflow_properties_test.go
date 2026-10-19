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
		"workflow": [
			{
				"name": "deploy-web",
				"jobType": "deploy",
				"components": [" Web "],
				"properties": []
			},
			{
				"name": "deploy-group",
				"jobType": "deploy",
				"subSteps": [
					{
						"name": "deploy-worker",
						"jobType": "deploy",
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
		"workflow": [
			{
				"name": "deploy-web",
				"jobType": "deploy",
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
		"name": "deploy-pods",
		"workflow": [
			{
				"name": "deploy-pods",
				"jobType": "deploy",
				"components": [" API ", "WORKER"],
				"properties": [
					{"policies": [" API ", "api"]},
					{"policies": ["Worker"]}
				],
				"subSteps": [
					{
						"name": "deploy-sidecar",
						"jobType": "deploy",
						"components": [" SideCar "],
						"properties": [
							{"policies": ["SideCar"]}
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
		{Policies: []string{"api"}},
		{Policies: []string{"worker"}},
	}, steps.Steps[0].Properties)
	require.Len(t, steps.Steps[0].SubSteps, 1)
	require.Equal(t, []model.Policies{
		{Policies: []string{"sidecar"}},
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
		"workflow": [
			{
				"name": "deploy-components",
				"jobType": "deploy",
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
		"workflow": [
			{
				"name": "deploy-components",
				"jobType": "deploy",
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
		"name": "deploy-pods",
		"workflow": [
			{
				"name": "deploy-pods",
				"jobType": "deploy",
				"components": ["api"],
				"properties": [
					{"policies": ["  "]},
					{"policies": ["api"]}
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
		"workflow": [
			{
				"name": "deploy-api",
				"jobType": "deploy",
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
		"workflow": [
			{
				"name": "deploy-group",
				"mode": "StepByStep",
				"subSteps": [
					{
						"name": "deploy-api",
						"jobType": "deploy",
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
		"workflow": [
			{
				"name": "deploy-group",
				"mode": "StepByStep",
				"subSteps": [
					{
						"name": "deploy-sub",
						"jobType": "deploy",
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
