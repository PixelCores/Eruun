package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGenerateJobTasksPropagatesResourceErrors(t *testing.T) {
	traitsPlu.RegisterAllProcessors()
	storage := spec.StorageTraitSpec{Name: "data", Type: "persistent", MountPath: "/data", Size: "1Gi"}
	conflict := storage
	conflict.Size = "2Gi"
	conflictTraits, err := model.NewJSONStructByStruct(spec.Traits{Storage: []spec.StorageTraitSpec{storage}, Sidecar: []spec.SidecarTraitsSpec{{Name: "helper", Image: "busybox:1.36", Traits: spec.Traits{Storage: []spec.StorageTraitSpec{conflict}}}}})
	require.NoError(t, err)
	for _, tt := range []struct {
		name       string
		kind       config.JobType
		properties *model.JSONStruct
		traits     *model.JSONStruct
		reason     string
	}{
		{"deployment conflict", config.ServerJob, &model.JSONStruct{"ports": []map[string]interface{}{{"port": 80}}}, conflictTraits, "conflicting additional object"},
		{"statefulset conflict", config.StoreJob, nil, conflictTraits, "conflicting additional object"},
		{"instant conflict", config.InstantJob, nil, conflictTraits, "conflicting additional object"},
		{"one time conflict", config.InstantJob, &model.JSONStruct{"startTime": 123}, conflictTraits, "conflicting additional object"},
		{"cron conflict", config.ScheduledJob, &model.JSONStruct{"schedule": "0 * * * *"}, conflictTraits, "conflicting additional object"},
		{"corrupt properties", config.ServerJob, &model.JSONStruct{"ports": "invalid"}, nil, "decode component properties"},
		{"invalid cron", config.ScheduledJob, &model.JSONStruct{"schedule": "invalid"}, nil, "normalize cron schedule"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			component := &model.ApplicationComponent{Name: "broken", AppID: "app", Namespace: "default", Image: "nginx:1.27", ComponentType: tt.kind, Properties: tt.properties, Traits: tt.traits}
			steps, err := model.NewJSONStructByStruct(model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "deploy", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"healthy", "broken"}}}}}})
			require.NoError(t, err)
			task := &model.WorkflowQueue{TaskID: "task", AppID: "app", WorkflowID: "workflow", Status: config.StatusRunning}
			store := &fakeDataStore{workflow: &model.Workflow{ID: task.WorkflowID, Steps: steps}, components: []*model.ApplicationComponent{{Name: "healthy", Namespace: "default", Image: "nginx:1.27", ComponentType: config.ServerJob}, component}}
			executions := mustGenerateJobTasks(t, context.Background(), task, store, 60)
			requireWorkflowGenerationFailed(t, executions, tt.reason)
			require.Equal(t, 1, countJobs(executions[0].Jobs), "partial workload and Service jobs must be discarded")
			// Exercise the state-machine entry point, including recovery beyond
			// the single replacement failure step and its persisted JobInfo.
			for _, currentStep := range []int{0, 3} {
				task.CurrentStep = currentStep
				persisted := &controllerTestStore{
					application: &model.Applications{ID: "app", Name: "app"},
					workflow:    store.workflow,
					components:  store.components,
				}
				client := fake.NewSimpleClientset()
				controller := newTestWorkflowController(t, task, client, persisted)
				controller.workspaceManager = &workspace.Manager{}
				controller.workspace = &model.Workspace{ID: "workspace", Namespace: "tenant"}
				controller.accountConfig = &spec.AccountConfig{}
				require.ErrorContains(t, controller.run(context.Background(), 1), tt.reason)
				require.Equal(t, config.StatusFailed, controller.snapshotTask().Status)
				require.Contains(t, controller.snapshotTerminalReason(), tt.reason)
				require.Equal(t, config.StatusFailed, persisted.task.Status)
				require.Len(t, persisted.jobs, 1)
				require.Equal(t, string(config.StatusFailed), persisted.jobs[0].Status)
				require.Contains(t, persisted.jobs[0].Error, tt.reason)
				require.Empty(t, client.Actions())
			}
		})
	}
}

func TestStepBuildersDiscardPartialGeneration(t *testing.T) {
	components := map[string]*model.ApplicationComponent{
		"healthy": {Name: "healthy", Image: "nginx:1.27", ComponentType: config.ServerJob},
		"broken":  {Name: "broken", ComponentType: config.ServerJob, Properties: &model.JSONStruct{"ports": "invalid"}},
	}
	for _, mode := range []config.WorkflowMode{config.WorkflowModeStepByStep, config.WorkflowModeDAG} {
		for _, substeps := range []bool{false, true} {
			t.Run(string(mode)+map[bool]string{true: " substeps", false: " components"}[substeps], func(t *testing.T) {
				step := &model.WorkflowStep{Name: "deploy", Mode: mode, WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"healthy", "broken"}}}}
				if substeps {
					step.Properties = nil
					step.SubSteps = []*model.WorkflowSubStep{{Name: "healthy", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"healthy"}}}}, {Name: "broken", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"broken"}}}}}
				}
				groups, err := buildWorkflowStepExecutionGroups(context.Background(), &model.WorkflowSteps{Steps: []*model.WorkflowStep{step}}, components, &model.WorkflowQueue{}, 60)
				require.ErrorContains(t, err, "decode component properties")
				require.Nil(t, groups)
			})
		}
	}
}

func TestGenerateJobTasksKeepsEmptyAndUnreferencedComponents(t *testing.T) {
	for _, steps := range []model.WorkflowSteps{{}, {Steps: []*model.WorkflowStep{{Name: "empty", WorkflowType: config.JobDeploy}}}, {Steps: []*model.WorkflowStep{{Name: "deploy", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"healthy"}}}}}}} {
		encoded, err := model.NewJSONStructByStruct(steps)
		require.NoError(t, err)
		store := &fakeDataStore{workflow: &model.Workflow{ID: "workflow", Steps: encoded}, components: []*model.ApplicationComponent{{Name: "healthy", Image: "nginx:1.27", ComponentType: config.ServerJob}, {Name: "unreferenced", ComponentType: config.ServerJob, Properties: &model.JSONStruct{"ports": "invalid"}}}}
		executions := mustGenerateJobTasks(t, context.Background(), &model.WorkflowQueue{AppID: "app", WorkflowID: "workflow"}, store, 60)
		if len(steps.Steps) == 0 || len(steps.Steps[0].Properties) == 0 {
			require.Empty(t, executions)
		} else {
			require.Len(t, executions, 1)
			require.Equal(t, "healthy", executions[0].Name)
		}
	}
}

func TestGenerationFailurePersistenceErrorStopsTerminalization(t *testing.T) {
	steps, err := model.NewJSONStructByStruct(model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "broken", WorkflowType: config.JobDeploy}}})
	require.NoError(t, err)
	writeErr := errors.New("failure record unavailable")
	store := &controllerTestStore{
		application:   &model.Applications{ID: "app", Name: "app"},
		workflow:      &model.Workflow{ID: "workflow", Steps: steps},
		components:    []*model.ApplicationComponent{{Name: "broken", AppID: "app", ComponentType: config.ServerJob, Properties: &model.JSONStruct{"ports": "invalid"}}},
		jobInfoAddErr: writeErr,
	}
	task := &model.WorkflowQueue{TaskID: "task", AppID: "app", WorkflowID: "workflow", Status: config.StatusRunning}
	controller := newTestWorkflowController(t, task, fake.NewSimpleClientset(), store)
	require.ErrorIs(t, controller.run(context.Background(), 1), writeErr)
	require.Equal(t, config.StatusRunning, store.task.Status)
	require.Empty(t, store.jobs)
}
