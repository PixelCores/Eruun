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

func TestUpdateVersionRejectsTraitsThatTryRejects(t *testing.T) {
	for _, tc := range []struct {
		name      string
		traits    apisv1.Traits
		wantField string
	}{
		{"unknown probe type", apisv1.Traits{Probes: []spec.ProbeTraitsSpec{{Type: "foo", HTTPGet: &spec.HTTPGetProbe{Port: 8080}}}}, ".traits.probes[0].type"},
		{"storage without mountPath", apisv1.Traits{Storage: []spec.StorageTraitSpec{{Name: "data", Type: spec.StorageTypeEphemeral}}}, ".traits.storage[0].mountPath"},
		{"env without value source", apisv1.Traits{Envs: []spec.SimplifiedEnvSpec{{Name: "MODE"}}}, ".traits.envs[0].valueFrom"},
	} {
		for _, action := range []spec.ComponentAction{spec.ComponentActionUpdate, spec.ComponentActionAdd} {
			t.Run(string(action)+"/"+tc.name, func(t *testing.T) {
				store := newInMemoryAppStore()
				store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Version: "1.0.0", Namespace: config.DefaultNamespace}
				store.components["backend"] = &model.ApplicationComponent{Name: "backend", AppID: "app-1", Namespace: config.DefaultNamespace,
					ComponentType: config.ServerJob, Image: "example/app:v1", Replicas: 1,
					Properties: mustJSONStruct(&apisv1.Properties{}), Traits: mustJSONStruct(&apisv1.Traits{})}
				store.workflows["wf-1"] = &model.Workflow{ID: "wf-1", AppID: "app-1"}
				name := "backend"
				if action == spec.ComponentActionAdd {
					name = "worker"
				}
				traits := tc.traits
				replicas := int32(1)
				resp, err := newMockServiceWithStore(store).UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
					Version: "1.1.0",
					Components: []apisv1.ComponentUpdateSpec{{Action: string(action), Name: name, ComponentType: config.ServerJob,
						Image: "example/app:v2", Replicas: &replicas, Traits: &traits}},
					AutoExec: boolPtr(false),
				})
				require.Nil(t, resp)
				require.ErrorIs(t, err, bcode.ErrApplicationConfig)
				require.ErrorContains(t, err, "component["+name+"]"+tc.wantField)
				require.Equal(t, "1.0.0", store.apps["app-1"].Version)
				require.Equal(t, "example/app:v1", store.components["backend"].Image)
				require.NotContains(t, store.components, "worker")
				require.Empty(t, store.tasks)
			})
		}
	}
}

func TestDiffUpdateVersionRejectsSourceTraitsThatTryRejects(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["source-app"] = &model.Applications{ID: "source-app", Name: "source", Version: "2.0.0", Namespace: config.DefaultNamespace}
	store.apps["target-app"] = &model.Applications{ID: "target-app", Name: "target", Version: "1.0.0", Namespace: config.DefaultNamespace}
	store.components["api"] = &model.ApplicationComponent{Name: "api", AppID: "source-app", Namespace: config.DefaultNamespace,
		ComponentType: config.ServerJob, Image: "api:v2", Replicas: 1, Properties: mustJSONStruct(&apisv1.Properties{}),
		Traits: mustJSONStruct(&apisv1.Traits{Storage: []spec.StorageTraitSpec{{Name: "data", Type: spec.StorageTypeEphemeral}}})}
	store.components["API"] = &model.ApplicationComponent{Name: "API", AppID: "target-app", Namespace: config.DefaultNamespace,
		ComponentType: config.ServerJob, Image: "api:v1", Replicas: 1, Properties: mustJSONStruct(&apisv1.Properties{}),
		Traits: mustJSONStruct(&apisv1.Traits{})}

	resp, err := newMockServiceWithStore(store).DiffUpdateVersion(context.Background(), "target-app", apisv1.DiffUpdateVersionRequest{
		SourceAppID: "source-app", AutoExec: boolPtr(false),
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.ErrorContains(t, err, ".traits.storage[0].mountPath")
	require.Equal(t, "1.0.0", store.apps["target-app"].Version)
	require.Equal(t, "api:v1", store.components["API"].Image)
	require.Empty(t, store.tasks)
}

func TestUpdateApplicationWorkflowRejectsStepNameOutsideDNSLabel(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Namespace: config.DefaultNamespace}
	store.components["web"] = &model.ApplicationComponent{Name: "web", AppID: "app-1", ComponentType: config.ServerJob}
	_, err := newMockServiceWithStore(store).UpdateApplicationWorkflow(context.Background(), "app-1", apisv1.UpdateApplicationWorkflowRequest{
		Name:     "deploy",
		Workflow: []apisv1.CreateWorkflowStepRequest{{Name: "deploy_web", WorkflowType: config.JobDeploy, Components: []string{"web"}}},
	})
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.ErrorContains(t, err, "workflow[0].name")
	require.Empty(t, store.workflows, "invalid workflow must not be persisted")
}
