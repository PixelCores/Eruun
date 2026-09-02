package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
)

func TestBuildLogArchiveUploadStepExecutionCreatesOneJobPerComponent(t *testing.T) {
	componentMap := map[string]*model.ApplicationComponent{
		"api": {
			AppID:           "app-1",
			ResourceAppName: "demo",
			Name:            "api",
			Namespace:       "default",
			ComponentType:   config.ServerJob,
			Replicas:        1,
		},
		"worker": {
			AppID:           "app-1",
			ResourceAppName: "demo",
			Name:            "worker",
			Namespace:       "default",
			ComponentType:   config.InstantJob,
			Replicas:        1,
		},
	}
	task := &model.WorkflowQueue{
		TaskID:     "task-1",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
	}

	executions := buildWorkflowStepExecutions(context.Background(), 0, &model.WorkflowStep{
		Name:         "log-archive-upload",
		WorkflowType: config.JobLogArchiveUpload,
		Mode:         config.WorkflowModeStepByStep,
		Properties: []model.Policies{
			{
				Policies:  []string{"api"},
				Path:      "/var/log/api",
				Container: "api",
			},
			{
				Policies:  []string{"worker"},
				Path:      "/var/log/worker",
				Container: "worker",
			},
		},
	}, componentMap, task, int64(config.DefaultJobTaskTimeout))

	require.Len(t, executions, 2)
	require.Equal(t, "api", executions[0].Name)
	require.Equal(t, "worker", executions[1].Name)
	expectedPaths := map[string]string{
		"api":    "/var/log/api",
		"worker": "/var/log/worker",
	}
	expectedContainers := map[string]string{
		"api":    "api",
		"worker": "worker",
	}
	for _, execution := range executions {
		require.Equal(t, config.WorkflowModeStepByStep, execution.Mode)
		jobs := flattenJobBuckets(execution.Jobs)
		require.Len(t, jobs, 1)
		require.Equal(t, string(config.JobLogArchiveUpload), jobs[0].JobType)
		info, ok := jobs[0].JobInfo.(*job.LogArchiveUploadJobInfo)
		require.True(t, ok)
		require.NotNil(t, info.Component)
		require.Equal(t, jobs[0].Name, info.Component.Name)
		require.Equal(t, expectedPaths[jobs[0].Name], info.Path)
		require.Equal(t, expectedContainers[jobs[0].Name], info.Container)
	}
}

func TestGenerateJobTasksRejectsLogArchiveUploadNonPodComponent(t *testing.T) {
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name:         "log-archive-upload",
		WorkflowType: config.JobLogArchiveUpload,
		Mode:         config.WorkflowModeStepByStep,
		Properties: []model.Policies{{
			Policies: []string{"config"},
			Path:     "/var/log/app",
		}},
	}}})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "workflow-1", Steps: stepsJSON},
		components: []*model.ApplicationComponent{{
			AppID:         "app-1",
			Name:          "config",
			Namespace:     "default",
			ComponentType: config.ConfJob,
		}},
	}
	task := &model.WorkflowQueue{
		TaskID:     "task-1",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	requireWorkflowGenerationFailed(t, executions, "does not use pods")
}

func TestGenerateJobTasksPreservesLogArchivePathForNameBasedStep(t *testing.T) {
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name:         "api",
		WorkflowType: config.JobLogArchiveUpload,
		Mode:         config.WorkflowModeStepByStep,
		Properties: []model.Policies{{
			Path:      "/var/log/api",
			Container: "api",
		}},
	}}})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "workflow-1", Steps: stepsJSON},
		components: []*model.ApplicationComponent{{
			AppID:           "app-1",
			ResourceAppName: "demo",
			Name:            "api",
			Namespace:       "default",
			ComponentType:   config.ServerJob,
		}},
	}
	task := &model.WorkflowQueue{
		TaskID:     "task-1",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 1)
	require.Equal(t, "api", executions[0].Name)
	jobs := flattenJobBuckets(executions[0].Jobs)
	require.Len(t, jobs, 1)
	require.Equal(t, string(config.JobLogArchiveUpload), jobs[0].JobType)
	info, ok := jobs[0].JobInfo.(*job.LogArchiveUploadJobInfo)
	require.True(t, ok)
	require.Equal(t, "/var/log/api", info.Path)
	require.Equal(t, "api", info.Container)
}

func flattenJobBuckets(buckets map[int][]*model.JobTask) []*model.JobTask {
	var jobs []*model.JobTask
	for _, bucket := range buckets {
		jobs = append(jobs, bucket...)
	}
	return jobs
}
