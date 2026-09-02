package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestApplyWorkflowExecutionIdentityIsGenerationScopedAndDeterministic(t *testing.T) {
	newExecutions := func() []StepExecution {
		return []StepExecution{{Jobs: map[int][]*model.JobTask{10: {{Name: "api", JobType: "deploy", RetryCount: 1}}}}}
	}
	first := newExecutions()
	second := newExecutions()
	task := &model.WorkflowQueue{TaskID: "task-1", RunGeneration: 3, RunToken: "run-3"}

	applyWorkflowExecutionIdentity(first, task)
	applyWorkflowExecutionIdentity(second, task)

	firstJob := first[0].Jobs[10][0]
	secondJob := second[0].Jobs[10][0]
	require.Len(t, firstJob.ExecutionKey, 64)
	require.Equal(t, firstJob.ExecutionKey, secondJob.ExecutionKey)
	require.Equal(t, uint64(3), firstJob.RunGeneration)
	require.Equal(t, "run-3", firstJob.RunToken)
	require.Equal(t, uint(2), firstJob.Attempt)

	third := newExecutions()
	task.RunGeneration = 4
	task.RunToken = "run-4"
	applyWorkflowExecutionIdentity(third, task)
	require.NotEqual(t, firstJob.ExecutionKey, third[0].Jobs[10][0].ExecutionKey)
}

func TestFailedWorkflowGenerationExecutionCarriesGenerationIdentity(t *testing.T) {
	firstTask := &model.WorkflowQueue{TaskID: "task-generation-failure", RunGeneration: 3}
	first := failedWorkflowGenerationExecution(firstTask, int64(config.DefaultJobTaskTimeout), errors.New("invalid workflow"))
	firstJob := first.Jobs[config.JobPriorityLow][0]

	repeated := failedWorkflowGenerationExecution(firstTask, int64(config.DefaultJobTaskTimeout), errors.New("invalid workflow"))
	repeatedJob := repeated.Jobs[config.JobPriorityLow][0]
	require.Len(t, firstJob.ExecutionKey, 64)
	require.Equal(t, firstJob.ExecutionKey, repeatedJob.ExecutionKey)
	require.Equal(t, uint64(3), firstJob.RunGeneration)
	require.Equal(t, uint(1), firstJob.Attempt)

	secondTask := &model.WorkflowQueue{TaskID: firstTask.TaskID, RunGeneration: 4}
	second := failedWorkflowGenerationExecution(secondTask, int64(config.DefaultJobTaskTimeout), errors.New("invalid workflow"))
	secondJob := second.Jobs[config.JobPriorityLow][0]
	require.NotEqual(t, firstJob.ExecutionKey, secondJob.ExecutionKey)
	require.Equal(t, uint64(4), secondJob.RunGeneration)
}

func TestRestoreCommittedJobExecutionUsesLatestCommittedGenerationRegardlessOfListOrder(t *testing.T) {
	const priority = 10
	newExecutions := func() []StepExecution {
		return []StepExecution{{Jobs: map[int][]*model.JobTask{
			priority: {{Name: "api", JobType: string(config.JobDeploy)}},
		}}}
	}
	task := &model.WorkflowQueue{TaskID: "task-latest-committed", RunGeneration: 3, RunToken: "run-3"}
	latestKey := workflowExecutionKey(task.TaskID, 2, 0, priority, 0, "api", string(config.JobDeploy))
	oldestKey := workflowExecutionKey(task.TaskID, 1, 0, priority, 0, "api", string(config.JobDeploy))
	latest := &model.JobInfo{
		TaskID: task.TaskID, Status: string(config.StatusCompleted), ExecutionKey: &latestKey, RunGeneration: 2,
	}
	oldest := &model.JobInfo{
		TaskID: task.TaskID, Status: string(config.StatusFailed), ExecutionKey: &oldestKey, RunGeneration: 1,
	}

	for _, test := range []struct {
		name string
		jobs []*model.JobInfo
	}{
		{name: "latest first", jobs: []*model.JobInfo{latest, oldest}},
		{name: "latest last", jobs: []*model.JobInfo{oldest, latest}},
	} {
		t.Run(test.name, func(t *testing.T) {
			executions := newExecutions()
			applyWorkflowExecutionIdentity(executions, task)
			store := &controllerTestStore{jobs: test.jobs}

			err := restoreCommittedJobExecutions(context.Background(), executions, task, store)

			require.NoError(t, err)
			jobTask := executions[0].Jobs[priority][0]
			require.Equal(t, uint64(2), jobTask.RunGeneration)
			require.Equal(t, latestKey, jobTask.ExecutionKey)
			require.Equal(t, config.StatusCompleted, jobTask.Status)
		})
	}
}

func TestPersistWorkflowTaskSnapshotRejectsMatchingProgressFromStaleGeneration(t *testing.T) {
	latest := &model.WorkflowQueue{
		TaskID:        "task-1",
		Status:        config.StatusRunning,
		CurrentStep:   2,
		RunGeneration: 2,
		RunToken:      "run-new",
		WorkerID:      "worker-new",
	}
	store := &controllerTestStore{task: latest}
	ctl := &WorkflowCtl{Store: store}
	stale := *latest
	stale.RunGeneration = 1
	stale.RunToken = "run-old"
	stale.WorkerID = "worker-old"

	authoritative, err := ctl.persistWorkflowTaskSnapshot(context.Background(), stale, config.StatusRunning, map[string]interface{}{
		"status":       stale.Status,
		"current_step": stale.CurrentStep,
	})

	require.NoError(t, err)
	require.NotNil(t, authoritative)
	require.Equal(t, uint64(2), authoritative.RunGeneration)
	require.Equal(t, "run-new", authoritative.RunToken)
}
