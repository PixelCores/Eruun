package workflow

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
)

type evaluationWorkflowStore struct{ fakeDataStore }

func (s *evaluationWorkflowStore) Get(ctx context.Context, entity datastore.Entity) error {
	switch value := entity.(type) {
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
	steps, err := model.NewJSONStructByStruct(model.WorkflowSteps{Steps: []*model.WorkflowStep{
		{Name: "first", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"benchmark"}}}},
		{Name: "second", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"benchmark"}}}},
	}})
	require.NoError(t, err)
	store := &evaluationWorkflowStore{fakeDataStore: fakeDataStore{
		workflow:    &model.Workflow{ID: "wf", Steps: steps},
		application: &model.Applications{ID: "app", Name: "models", WorkspaceID: "space", Namespace: "space-ns"},
		components:  []*model.ApplicationComponent{{Name: "benchmark", AppID: "app", Namespace: "space-ns", ComponentType: config.InstantJob, Traits: traits}},
	}}
	parent := &model.WorkflowQueue{AppID: "app", WorkflowID: "wf", WorkspaceID: "space", TaskID: "parent", RunGeneration: 1}
	cfg := &config.Config{Jobs: &spec.JobsRuntimeConfig{RunnerImage: "example.com/runner:0.22.0", APIURL: "https://api.example.com"}}
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
