package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
)

func TestBuildDatabaseResetStepExecutionAggregatesComponents(t *testing.T) {
	componentMap := map[string]*model.ApplicationComponent{
		"mysql": {
			AppID:         "app-1",
			Name:          "mysql",
			Namespace:     "default",
			ComponentType: config.StoreJob,
			Replicas:      1,
		},
		"redis": {
			AppID:         "app-1",
			Name:          "redis",
			Namespace:     "default",
			ComponentType: config.StoreJob,
			Replicas:      1,
		},
		"api": {
			AppID:         "app-1",
			Name:          "api",
			Namespace:     "default",
			ComponentType: config.ServerJob,
			Replicas:      1,
		},
	}
	task := &model.WorkflowQueue{
		TaskID:     "task-1",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
	}

	executions := buildWorkflowStepExecutions(context.Background(), 0, &model.WorkflowStep{
		Name:         "database-reset",
		WorkflowType: config.JobDatabaseReset,
		Mode:         config.WorkflowModeStepByStep,
		Properties: []model.Policies{{
			Policies:   []string{"mysql", "redis"},
			InitSQLURL: "https://files.example/game-1.0.8.sql",
		}},
	}, componentMap, task, int64(config.DefaultJobTaskTimeout))

	require.Len(t, executions, 1)
	require.Equal(t, config.WorkflowModeStepByStep, executions[0].Mode)
	var jobs []*model.JobTask
	for _, bucket := range executions[0].Jobs {
		jobs = append(jobs, bucket...)
	}
	require.Len(t, jobs, 1)
	require.Equal(t, string(config.JobDatabaseReset), jobs[0].JobType)
	info, ok := jobs[0].JobInfo.(*job.DatabaseResetJobInfo)
	require.True(t, ok)
	require.Len(t, info.DatabaseComponents, 2)
	require.Equal(t, "mysql", info.DatabaseComponents[0].Name)
	require.Equal(t, "redis", info.DatabaseComponents[1].Name)
	require.Empty(t, info.RestartComponents)
	require.Equal(t, "https://files.example/game-1.0.8.sql", info.InitSQLURL)
	require.Equal(t, "step:0/component:0", info.ExecutionKey)
}

func TestBuildDatabaseResetStepsAssignDistinctExecutionKeys(t *testing.T) {
	componentMap := databaseResetExecutionKeyComponentMap()
	task := &model.WorkflowQueue{
		TaskID:     "task-1",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
	}
	groups := buildWorkflowStepExecutionGroups(context.Background(), &model.WorkflowSteps{Steps: []*model.WorkflowStep{
		{
			Name:         "reset-mysql",
			WorkflowType: config.JobDatabaseReset,
			Properties:   []model.Policies{{Policies: []string{"mysql"}}},
		},
		{
			Name:         "reset-redis",
			WorkflowType: config.JobDatabaseReset,
			Properties:   []model.Policies{{Policies: []string{"redis"}}},
		},
	}}, componentMap, task, int64(config.DefaultJobTaskTimeout))

	require.Len(t, groups, 2)
	require.Equal(t, "step:0/component:0", requireDatabaseResetExecutionKey(t, groups[0]))
	require.Equal(t, "step:1/component:0", requireDatabaseResetExecutionKey(t, groups[1]))
}

func TestBuildDatabaseResetSubStepsAssignDistinctExecutionKeys(t *testing.T) {
	componentMap := databaseResetExecutionKeyComponentMap()
	task := &model.WorkflowQueue{
		TaskID:     "task-1",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
	}
	tests := []struct {
		name string
		mode config.WorkflowMode
	}{
		{name: "sequential", mode: config.WorkflowModeStepByStep},
		{name: "parallel", mode: config.WorkflowModeDAG},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executions := buildWorkflowStepExecutions(context.Background(), 2, &model.WorkflowStep{
				Name: "reset-stores",
				Mode: test.mode,
				SubSteps: []*model.WorkflowSubStep{
					{
						Name:         "reset-mysql",
						WorkflowType: config.JobDatabaseReset,
						Properties:   []model.Policies{{Policies: []string{"mysql"}}},
					},
					{
						Name:         "reset-redis",
						WorkflowType: config.JobDatabaseReset,
						Properties:   []model.Policies{{Policies: []string{"redis"}}},
					},
				},
			}, componentMap, task, int64(config.DefaultJobTaskTimeout))

			var keys []string
			for _, execution := range executions {
				for _, jobTask := range execution.Jobs[config.JobPriorityLow] {
					info, ok := jobTask.JobInfo.(*job.DatabaseResetJobInfo)
					require.True(t, ok)
					keys = append(keys, info.ExecutionKey)
				}
			}
			require.ElementsMatch(t, []string{
				"step:2/substep:0/component:0",
				"step:2/substep:1/component:0",
			}, keys)
		})
	}
}

func databaseResetExecutionKeyComponentMap() map[string]*model.ApplicationComponent {
	return map[string]*model.ApplicationComponent{
		"mysql": {
			AppID:         "app-1",
			Name:          "mysql",
			Namespace:     "default",
			ComponentType: config.StoreJob,
			Replicas:      1,
		},
		"redis": {
			AppID:         "app-1",
			Name:          "redis",
			Namespace:     "default",
			ComponentType: config.StoreJob,
			Replicas:      1,
		},
	}
}

func requireDatabaseResetExecutionKey(t *testing.T, executions []StepExecution) string {
	t.Helper()
	require.Len(t, executions, 1)
	jobs := executions[0].Jobs[config.JobPriorityLow]
	require.Len(t, jobs, 1)
	info, ok := jobs[0].JobInfo.(*job.DatabaseResetJobInfo)
	require.True(t, ok)
	return info.ExecutionKey
}
