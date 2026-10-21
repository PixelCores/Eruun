package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
)

type evaluationWorkflowStore struct{ fakeDataStore }

func (s *evaluationWorkflowStore) Get(ctx context.Context, entity datastore.Entity) error {
	switch value := entity.(type) {
	case *model.SystemSetting:
		if value.Type == model.SystemSettingTypeWorkflowScheduler {
			value.Value, _ = json.Marshal(workflowconfig.DefaultJobSchedulerPolicy())
			return nil
		}
		return s.fakeDataStore.Get(ctx, entity)
	case *model.JobArtifact:
		value.WorkspaceID, value.Kind, value.Digest = "space", "dataset", "digest"
		return nil
	case *model.Workspace:
		value.Namespace = "space-ns"
		return nil
	default:
		return s.fakeDataStore.Get(ctx, entity)
	}
}

func TestWorkflowEvaluationJobsHaveIndependentExecutionsAndRecoverSnapshots(t *testing.T) {
	ctx := context.Background()
	traits, err := model.NewJSONStructByStruct(spec.Traits{Evaluation: &spec.EvaluationTraitSpec{
		Env: "ack", Agent: "oracle", TaskPackageID: "11111111-1111-1111-1111-111111111111",
	}})
	require.NoError(t, err)
	properties, err := model.NewJSONStructByStruct(spec.Properties{Labels: map[string]string{
		"team": "Model Team", workspace.EvaluationRunnerLabel: "false", "eruun.io/task-id": "spoof",
	}})
	require.NoError(t, err)
	steps, err := model.NewJSONStructByStruct(model.WorkflowSteps{Steps: []*model.WorkflowStep{
		{Name: "first", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"benchmark"}}}},
		{Name: "second", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"benchmark"}}}},
	}})
	require.NoError(t, err)
	store := &evaluationWorkflowStore{fakeDataStore: fakeDataStore{
		workflow:    &model.Workflow{ID: "wf", Steps: steps},
		application: &model.Applications{ID: "app", Name: "models", WorkspaceID: "space", Namespace: "space-ns"},
		components:  []*model.ApplicationComponent{{Name: "benchmark", AppID: "app", Namespace: "space-ns", ComponentType: config.InstantJob, Properties: properties, Traits: traits}},
	}}
	parent := &model.WorkflowQueue{AppID: "app", WorkflowID: "wf", WorkspaceID: "space", TaskID: "parent", RunGeneration: 1}
	cfg := &config.Config{Workflow: workflowconfig.DefaultRuntimeConfig(), Jobs: &spec.JobsRuntimeConfig{RunnerImage: "example.com/runner:0.22.0", APIURL: "https://api.example.com"}}
	executions, err := GenerateJobTasks(ctx, parent, store, 3600, cfg)
	require.NoError(t, err)
	require.Len(t, executions, 2)
	a, b := executions[0].Jobs[config.JobPriorityNormal][0], executions[1].Jobs[config.JobPriorityNormal][0]
	require.Equal(t, string(config.JobEval), a.JobType)
	require.Equal(t, "app", a.AppID)
	require.NotEqual(t, a.Name, b.Name, "repeating a component in a later step must create a distinct workload")
	require.NotEqual(t, a.ExecutionKey, b.ExecutionKey)
	require.NotEqual(t, a.EvaluationInfo, b.EvaluationInfo, "capabilities are per execution")
	for _, task := range []*model.JobTask{a, b} {
		workload := task.JobInfo.(*batchv1.Job)
		for _, labels := range []map[string]string{workload.Labels, workload.Spec.Template.Labels} {
			require.Equal(t, "model-team", labels["team"])
			require.Equal(t, config.ManagedByEruun, labels[spec.LabelManagedBy])
			require.Equal(t, "app", labels[spec.LabelAppID])
		}
		require.NotContains(t, workload.Labels, workspace.EvaluationRunnerLabel)
		require.NotContains(t, workload.Labels, "eruun.io/task-id")
		require.Equal(t, "true", workload.Spec.Template.Labels[workspace.EvaluationRunnerLabel])
		require.Equal(t, parent.TaskID, workload.Spec.Template.Labels["eruun.io/task-id"])
		require.Equal(t, task.ExecutionKey, workload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
		require.Equal(t, parent.TaskID, workload.Spec.Template.Annotations[config.AnnotationJobTaskID])
		require.Equal(t, cfg.Jobs.RunnerImage, workload.Spec.Template.Spec.Containers[0].Image)
		var payload map[string]any
		for _, env := range workload.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "ERUUN_JOB_CONFIG" {
				require.NoError(t, json.Unmarshal([]byte(env.Value), &payload))
			}
		}
		require.Equal(t, task.ExecutionKey, payload["executionKey"])
		key := task.ExecutionKey
		store.jobInfos = append(store.jobInfos, &model.JobInfo{Type: task.JobType, AppID: task.AppID, WorkspaceID: task.WorkspaceID,
			TaskID: parent.TaskID, ExecutionKey: &key, RunGeneration: 1, Attempt: 1, Status: string(config.StatusCompleted), EvaluationInfo: task.EvaluationInfo})
	}
	parent.RunGeneration = 2
	// Completed executions are recoverable even when the Runner is unconfigured.
	recovered, err := GenerateJobTasks(ctx, parent, store, 3600)
	require.NoError(t, err)
	require.Len(t, recovered, 2)
	for i, original := range []*model.JobTask{a, b} {
		task := recovered[i].Jobs[config.JobPriorityNormal][0]
		require.Equal(t, config.StatusCompleted, task.Status)
		require.Equal(t, original.ExecutionKey, task.ExecutionKey)
		require.Equal(t, original.EvaluationInfo, task.EvaluationInfo)
		require.EqualValues(t, 1, task.RunGeneration)
		require.EqualValues(t, 2, task.OwnerRunGeneration)
	}
}

func TestStandaloneJobStagesCompileToExecutionsAndRestoreEvaluations(t *testing.T) {
	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "member"})
	command := json.RawMessage(`{"image":"busybox:1.37.0","command":["true"],"timeoutSeconds":60}`)
	evaluation := spec.Traits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", Agent: "oracle", TaskPackageID: "11111111-1111-1111-1111-111111111111"}}
	declaration := spec.JobSpec{Name: "pipeline", Volume: &spec.JobVolume{Size: "1Gi", AccessMode: "ReadWriteMany"}, Stages: []spec.JobStage{
		{Name: "prepare", Steps: []spec.JobStep{{Name: "fetch", Type: "command", Spec: command}}},
		{Name: "evaluate", Steps: []spec.JobStep{
			{Name: "first", Type: "job", Traits: evaluation},
			{Name: "second", Type: "job", Traits: evaluation},
		}},
		{Name: "finish", Steps: []spec.JobStep{{Name: "publish", Type: "command", Spec: command}}},
	}}
	raw, err := json.Marshal(declaration)
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: "stages-task", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob, JobSpec: string(raw), RunGeneration: 1}
	store := &evaluationWorkflowStore{}
	cfg := &config.Config{Workflow: workflowconfig.DefaultRuntimeConfig(), Jobs: &spec.JobsRuntimeConfig{RunnerImage: "example.com/runner:0.22.0", APIURL: "https://api.example.com"}}
	executions, err := GenerateJobTasks(ctx, parent, store, 3600, cfg)
	require.NoError(t, err)
	require.Len(t, executions, 3)
	require.Equal(t, "prepare", executions[0].Name)
	require.Equal(t, config.WorkflowModeStepByStep, executions[0].Mode, "the PVC does not turn a single step into DAG")
	require.Len(t, executions[0].Jobs[config.JobPriorityMaxHigh], 1)
	require.Equal(t, string(config.JobDeployPVC), executions[0].Jobs[config.JobPriorityMaxHigh][0].JobType)
	require.Len(t, executions[0].Jobs[config.JobPriorityNormal], 1)
	require.Equal(t, "evaluate", executions[1].Name)
	require.Equal(t, config.WorkflowModeDAG, executions[1].Mode)
	require.Len(t, executions[1].Jobs[config.JobPriorityNormal], 2)
	require.Equal(t, "finish", executions[2].Name)
	require.Equal(t, config.WorkflowModeStepByStep, executions[2].Mode)
	seen := map[string]bool{}
	for _, execution := range executions {
		require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupFailed, execution.FailurePolicy)
		for _, tasks := range execution.Jobs {
			for _, task := range tasks {
				require.NotEmpty(t, task.ExecutionKey)
				require.False(t, seen[task.ExecutionKey])
				seen[task.ExecutionKey] = true
			}
		}
	}
	evaluations := executions[1].Jobs[config.JobPriorityNormal]
	require.NotEqual(t, evaluations[0].EvaluationInfo, evaluations[1].EvaluationInfo)
	for _, task := range evaluations {
		workload := task.JobInfo.(*batchv1.Job)
		require.Equal(t, "runner", workload.Spec.Template.Spec.Containers[0].Name)
		for _, volume := range workload.Spec.Template.Spec.Volumes {
			require.NotEqual(t, "workspace", volume.Name)
		}
		require.EqualValues(t, 0, *workload.Spec.BackoffLimit, "evaluation Runners never replay trials through Pod retries")
		key := task.ExecutionKey
		store.jobInfos = append(store.jobInfos, &model.JobInfo{Type: task.JobType, TaskID: parent.TaskID, WorkspaceID: "space",
			ExecutionKey: &key, RunGeneration: 1, Status: string(config.StatusCompleted), EvaluationInfo: task.EvaluationInfo})
	}
	parent.RunGeneration = 2
	recovered, err := GenerateJobTasks(ctx, parent, store, 3600)
	require.NoError(t, err)
	require.Len(t, recovered, 3)
	for index, original := range evaluations {
		task := recovered[1].Jobs[config.JobPriorityNormal][index]
		require.Equal(t, config.StatusCompleted, task.Status)
		require.Equal(t, original.ExecutionKey, task.ExecutionKey)
		require.Equal(t, original.EvaluationInfo, task.EvaluationInfo)
	}
}
