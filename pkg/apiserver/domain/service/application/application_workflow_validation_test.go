package application

import (
	"context"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/stretchr/testify/require"
)

func TestWorkflowWritesValidateModeAndStepNames(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		duplicate bool
		stepType  config.WorkflowStepType
		wantError string
	}{
		{name: "default mode"},
		{name: "sequential mode", mode: "StepByStep"},
		{name: "dag mode", mode: "DAG"},
		{name: "invalid mode", mode: "parallel", wantError: "invalid workflow mode"},
		{name: "mode is case sensitive", mode: "dag", wantError: "invalid workflow mode"},
		{name: "duplicate step names", duplicate: true, wantError: "duplicate workflow step name"},
		{name: "invalid step type", stepType: "unknown", wantError: "invalid stepType"},
	} {
		for _, operation := range []string{"create", "update"} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				store := newInMemoryAppStore()
				svc := newMockServiceWithStore(store)
				steps := []apisv1.CreateWorkflowStepRequest{{
					Name: "deploy-web", Mode: tc.mode, StepType: tc.stepType,
					WorkflowType: config.JobDeploy, Components: []string{"web"},
				}}
				if tc.duplicate {
					steps = append(steps, steps[0])
				}
				var err error
				if operation == "create" {
					_, err = svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
						Name: "demo", Namespace: config.DefaultNamespace, Version: "1.0.0", Workflow: steps,
						Components: []apisv1.CreateComponentRequest{{Name: "web", ComponentType: config.ServerJob, Image: "nginx:1.27", Replicas: 1}},
					})
				} else {
					store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: config.DefaultNamespace}
					store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1", ComponentType: config.ServerJob}
					_, err = svc.UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{Name: "deploy", Workflow: steps})
				}
				if tc.wantError == "" {
					require.NoError(t, err)
					require.NotEmpty(t, store.workflows)
					return
				}
				require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
				require.Contains(t, err.Error(), tc.wantError)
				require.Empty(t, store.workflows, "invalid workflow must not be persisted")
				if operation == "create" {
					require.Empty(t, store.apps, "invalid application workflow must fail before writes")
				}
			})
		}
	}
}
