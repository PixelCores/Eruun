package application

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// TestApplicationWritesRejectWhatTryRejects covers requests that Try used to
// reject while application creation stored them and failed during deployment.
func TestApplicationWritesRejectWhatTryRejects(t *testing.T) {
	web := func(traits apisv1.Traits) apisv1.CreateComponentRequest {
		return apisv1.CreateComponentRequest{Name: "web", ComponentType: config.ServerJob, Image: "nginx:1.27", Replicas: 1, Traits: traits}
	}
	for _, tc := range []struct {
		name      string
		component apisv1.CreateComponentRequest
		workflow  []apisv1.CreateWorkflowStepRequest
		wantErr   error
		wantField string
	}{
		{
			name:      "unknown probe type",
			component: web(apisv1.Traits{Probes: []spec.ProbeTraitsSpec{{Type: "foo", HTTPGet: &spec.HTTPGetProbe{Port: 8080}}}}),
			wantErr:   bcode.ErrApplicationConfig, wantField: "component[0].traits.probes[0].type",
		},
		{
			name: "empty exec command next to httpGet",
			component: web(apisv1.Traits{Probes: []spec.ProbeTraitsSpec{{Type: "liveness",
				Exec: &spec.ExecProbe{}, HTTPGet: &spec.HTTPGetProbe{Port: 8080}}}}),
			wantErr: bcode.ErrApplicationConfig, wantField: "component[0].traits.probes[0].exec.command",
		},
		{
			name:      "env without value source",
			component: web(apisv1.Traits{Envs: []spec.SimplifiedEnvSpec{{Name: "MODE"}}}),
			wantErr:   bcode.ErrApplicationConfig, wantField: "component[0].traits.envs[0].valueFrom",
		},
		{
			name:      "scheduledjob without schedule",
			component: apisv1.CreateComponentRequest{Name: "cron", ComponentType: config.ScheduledJob, Image: "busybox:1.36"},
			wantErr:   bcode.ErrInvalidProperties, wantField: "component[0].properties.schedule",
		},
		{
			name:      "approval notify URL without http scheme",
			component: web(apisv1.Traits{}),
			workflow: []apisv1.CreateWorkflowStepRequest{{Name: "approve", StepType: config.WorkflowStepTypeApproval,
				Approval: &apisv1.WorkflowStepApproval{NotifyURL: "ftp://example.com/hook"}}},
			wantErr: bcode.ErrWorkflowConfig, wantField: "workflow[0].approval.notifyUrl",
		},
		{
			name:      "workflow step name outside DNS label format",
			component: web(apisv1.Traits{}),
			workflow: []apisv1.CreateWorkflowStepRequest{{Name: "deploy_web", WorkflowType: config.JobDeploy,
				Components: []string{"web"}}},
			wantErr: bcode.ErrWorkflowConfig, wantField: "workflow[0].name",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			svc := newMockServiceWithStore(store)
			_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
				Name: "demo", Namespace: config.DefaultNamespace, Version: "1.0.0",
				Components: []apisv1.CreateComponentRequest{tc.component}, Workflow: tc.workflow,
			})
			require.ErrorIs(t, err, tc.wantErr)
			require.Contains(t, err.Error(), tc.wantField)
			require.Empty(t, store.apps, "rejected application must not be persisted")
		})
	}
}
