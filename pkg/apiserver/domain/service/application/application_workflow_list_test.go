package application

import (
	"context"

	"errors"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestConvertWorkflowStepByTemplatePhasesGrouping(t *testing.T) {
	components := []apisv1.CreateComponentRequest{
		{Name: "cloud-bootstrap", ComponentType: config.CloudJob},
		{Name: "frontend", ComponentType: config.ServerJob},
		{Name: "shared-config", ComponentType: config.ConfJob},
		{Name: "batch-runner", ComponentType: config.InstantJob},
		{Name: "shared-secret", ComponentType: config.SecretJob},
		{Name: "mysql", ComponentType: config.StoreJob},
		{Name: "nightly", ComponentType: config.ScheduledJob},
		{Name: "web-access", ComponentType: config.Service},
		{Name: "legacy", ComponentType: config.JobType("legacy")},
	}

	steps := convertWorkflowStepByTemplatePhases(components)
	require.Len(t, steps.Steps, 5)

	require.Equal(t, "phase-1-job", steps.Steps[0].Name)
	require.Equal(t, "phase-2-config-secret", steps.Steps[1].Name)
	require.Equal(t, "phase-3-store", steps.Steps[2].Name)
	require.Equal(t, "phase-4-job", steps.Steps[3].Name)
	require.Equal(t, "phase-5-webservice", steps.Steps[4].Name)

	for _, step := range steps.Steps {
		require.Equal(t, config.WorkflowModeDAG, step.Mode)
		require.Equal(t, config.JobDeploy, step.WorkflowType)
	}

	require.ElementsMatch(t, []string{"cloud-bootstrap"}, steps.Steps[0].ComponentNames())
	require.ElementsMatch(t, []string{"shared-config", "shared-secret"}, steps.Steps[1].ComponentNames())
	require.ElementsMatch(t, []string{"mysql"}, steps.Steps[2].ComponentNames())
	require.ElementsMatch(t, []string{"batch-runner"}, steps.Steps[3].ComponentNames())
	require.ElementsMatch(t, []string{"frontend", "nightly", "web-access", "legacy"}, steps.Steps[4].ComponentNames())
}

func TestListApplicationWorkflows(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Project: "proj-1",
	}
	store.workflows["wf-old"] = &model.Workflow{
		ID:        "wf-old",
		AppID:     "app-1",
		ProjectID: "proj-1",
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(1, 0),
		},
	}
	store.workflows["wf-new"] = &model.Workflow{
		ID:        "wf-new",
		AppID:     "app-1",
		ProjectID: "proj-1",
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(10, 0),
		},
	}

	svc := newMockServiceWithStore(store)
	list, err := svc.ListApplicationWorkflows(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, list, 2)
	require.Equal(t, "wf-new", list[0].ID)
	require.Equal(t, "wf-old", list[1].ID)
}

func TestListApplicationWorkflowsMissingApp(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)
	_, err := svc.ListApplicationWorkflows(context.Background(), "missing")
	require.Error(t, err)
	require.True(t, errors.Is(err, bcode.ErrApplicationNotExist))
}
