package application

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	account "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestGetApplicationSpecReturnsCreateCompatibleCanonicalState(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "demo", Namespace: "default", Version: "1.2.3", TemplateEnabled: true,
		Callback: mustJSONStruct(&model.WorkflowCallback{Success: "https://example.com/app-success"}),
	}
	store.components["web"] = &model.ApplicationComponent{
		AppID: "app-1", Name: "web", Namespace: "default", ComponentType: config.ServerJob,
		Image: "nginx:1.27", Replicas: 2,
		Properties: mustJSONStruct(&apisv1.Properties{}),
		Traits:     mustJSONStruct(&apisv1.Traits{}),
	}
	steps := &model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name: "deploy", WorkflowType: config.JobDeploy,
		Properties: []model.Policies{{Policies: []string{"web"}}},
	}}}
	store.workflows["wf-1"] = &model.Workflow{
		ID: "wf-1", AppID: "app-1", Name: "demo-default", WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(steps), Callback: mustJSONStruct(&model.WorkflowCallback{Failure: "https://example.com/workflow-failure"}),
	}

	spec, err := newMockServiceWithStore(store).GetApplicationSpec(context.Background(), "app-1")
	require.NoError(t, err)
	require.Equal(t, "app-1", spec.ID)
	require.Len(t, spec.Components, 1)
	require.Equal(t, "nginx:1.27", spec.Components[0].Image)
	require.Len(t, spec.Workflow, 1)
	require.Equal(t, config.JobDeploy, spec.Workflow[0].WorkflowType)
	require.NotNil(t, spec.Callback)
	require.Equal(t, "https://example.com/app-success", spec.Callback.Success)
	require.Empty(t, spec.Callback.Failure)

	raw, err := json.Marshal(spec)
	require.NoError(t, err)
	var resubmitted apisv1.CreateApplicationsRequest
	require.NoError(t, json.Unmarshal(raw, &resubmitted))
	require.Equal(t, spec.ID, resubmitted.ID)
	require.Equal(t, spec.Components, resubmitted.Components)
	require.Len(t, resubmitted.Workflow, 1)
}

func TestGetApplicationSpecRejectsNonNativeApplication(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", ManagementMode: config.ManagementModeObserve}

	_, err := newMockServiceWithStore(store).GetApplicationSpec(context.Background(), "app-1")
	require.Error(t, err)
}

func TestGetApplicationSpecRejectsViewerBecauseSpecCanContainCredentials(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo"}
	ctx := account.WithScope(context.Background(), account.Scope{Role: "viewer"})

	_, err := newMockServiceWithStore(store).GetApplicationSpec(ctx, "app-1")
	require.ErrorIs(t, err, bcode.ErrForbidden)
}
