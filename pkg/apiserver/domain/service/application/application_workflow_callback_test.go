package application

import (
	"context"

	"errors"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestUpdateApplicationWorkflowStoresCallback(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name:  "callback-flow",
		Alias: "flow",
		Callback: &apisv1.WorkflowCallback{
			Success: "https://example.com/success",
			Failure: "https://example.com/failure",
			Methods: map[string]string{
				"success": "POST",
				"failure": "PUT",
			},
			Headers: map[string]string{
				"X-Trace": "trace-123",
			},
			TimeoutSeconds: 5,
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	require.NotNil(t, stored.Callback)
	var callback apisv1.WorkflowCallback
	err = decodeJSONStruct(stored.Callback, &callback)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/success", callback.Success)
	require.Equal(t, "https://example.com/failure", callback.Failure)
	require.Equal(t, "POST", callback.Methods["success"])
	require.Equal(t, int64(5), callback.TimeoutSeconds)
}

func TestUpdateApplicationWorkflowClearsCallbackWithSQLStylePut(t *testing.T) {
	store := newInMemoryAppStore()
	store.skipNilCallbackOnPut = true
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	oldCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: "https://example.com/old"})
	require.NoError(t, err)
	oldSteps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "old",
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
		}},
	})
	require.NoError(t, err)
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		Name:         "callback-flow",
		AppID:        "app-1",
		Namespace:    config.DefaultNamespace,
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps:        oldSteps,
		Callback:     oldCallback,
	}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		WorkflowID: "wf-1",
		Callback:   &apisv1.WorkflowCallback{},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "wf-1", resp.WorkflowID)
	require.Nil(t, store.workflows["wf-1"].Callback)
}

func TestUpdateApplicationWorkflowRejectsPrivateCallbackURLByDefault(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "private-callback-default-block",
		Callback: &apisv1.WorkflowCallback{
			Success: "http://127.0.0.1:8080/callback",
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	_, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
}

func TestUpdateApplicationWorkflowFailsWhenURLSecurityPolicyUnavailable(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	svc := newMockServiceWithStore(store)
	svc.URLSecurityPolicyProvider = nil

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "callback-policy-unavailable",
		Callback: &apisv1.WorkflowCallback{
			Success: "https://example.com/callback",
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	_, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrURLSecurityPolicyUnavailable)
}

func TestUpdateApplicationWorkflowAllowsPrivateCallbackURLWhenEnabled(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	setTestURLSecurityPolicy(t, store, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "private-callback-allowed",
		Callback: &apisv1.WorkflowCallback{
			Success: "http://127.0.0.1:8080/callback",
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.WorkflowID)
}

func TestUpdateApplicationWorkflowCapsCallbackTimeout(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "callback-timeout-cap",
		Callback: &apisv1.WorkflowCallback{
			Success:        "https://example.com/success",
			TimeoutSeconds: int64((96 * time.Hour) / time.Second),
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	resp, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.NoError(t, err)

	stored := store.workflows[resp.WorkflowID]
	require.NotNil(t, stored)
	require.NotNil(t, stored.Callback)
	var callback apisv1.WorkflowCallback
	err = decodeJSONStruct(stored.Callback, &callback)
	require.NoError(t, err)
	require.Equal(t, int64((workflowconfig.DefaultWorkflowCallbackTimeoutMax / time.Second)), callback.TimeoutSeconds)
}

func TestUpdateApplicationWorkflowRejectsInvalidCallbackMethod(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.components["nginx"] = &model.ApplicationComponent{Name: "nginx", AppID: "app-1"}
	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateApplicationWorkflowRequest{
		Name: "bad-callback",
		Callback: &apisv1.WorkflowCallback{
			Success: "https://example.com/success",
			Methods: map[string]string{
				"success": "PATCH",
			},
		},
		Workflow: []apisv1.CreateWorkflowStepRequest{
			{Name: "deploy-nginx", WorkflowType: config.JobDeploy, Components: []string{"nginx"}},
		},
	}

	_, err := svc.UpdateApplicationWorkflow(context.Background(), "app-1", req)
	require.Error(t, err)
	require.True(t, errors.Is(err, bcode.ErrWorkflowConfig))
}
