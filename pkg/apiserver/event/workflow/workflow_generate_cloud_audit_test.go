package workflow

import (
	"context"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
)

func TestGenerateJobTasksCloudJob(t *testing.T) {
	cloudProps, err := model.NewJSONStructByStruct(model.Properties{
		Cloud: &spec.CloudSpec{
			Provider: "aliyun",
			Action:   "create-ecs",
			Params: map[string]interface{}{
				"region": "cn-hangzhou",
			},
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "infra-cloud",
		AppID:         "app-cloud",
		Namespace:     "default",
		ComponentType: config.CloudJob,
		Properties:    cloudProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "infra-cloud", WorkflowType: config.JobDeploy},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{
			ID:    "wf-cloud",
			Steps: stepsJSON,
		},
		components: []*model.ApplicationComponent{component},
	}

	task := &model.WorkflowQueue{
		WorkflowID:   "wf-cloud",
		AppID:        "app-cloud",
		ProjectID:    "proj-cloud",
		WorkflowName: "cloud-workflow",
		TaskID:       "task-cloud",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 1)
	exec := executions[0]
	require.Equal(t, "infra-cloud", exec.Name)
	require.Equal(t, 1, countJobs(exec.Jobs))

	jobs := exec.Jobs[config.JobPriorityNormal]
	require.Len(t, jobs, 1)
	require.Equal(t, string(config.JobDeployCloud), jobs[0].JobType)
	require.Equal(t, int64(config.CloudJobTimeout), jobs[0].Timeout)

	info, ok := jobs[0].JobInfo.(*workflowjob.CloudJobInfo)
	require.True(t, ok)
	require.Equal(t, "aliyun", info.Provider)
	require.Equal(t, "create-ecs", info.Action)
	require.Equal(t, "cn-hangzhou", info.Params["region"])
	require.Equal(t, "step:0/component:0", info.ExecutionKey)
}

func TestGenerateJobTasksCloudJobWithoutCloudPropertiesStillEnqueuesJob(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:          "infra-cloud-invalid",
		AppID:         "app-cloud",
		Namespace:     "default",
		ComponentType: config.CloudJob,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "infra-cloud-invalid", WorkflowType: config.JobDeploy},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{
			ID:    "wf-cloud-invalid",
			Steps: stepsJSON,
		},
		components: []*model.ApplicationComponent{component},
	}

	task := &model.WorkflowQueue{
		WorkflowID:   "wf-cloud-invalid",
		AppID:        "app-cloud",
		ProjectID:    "proj-cloud",
		WorkflowName: "cloud-workflow",
		TaskID:       "task-cloud",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 1)
	exec := executions[0]
	require.Equal(t, "infra-cloud-invalid", exec.Name)
	require.Equal(t, 1, countJobs(exec.Jobs))

	jobs := exec.Jobs[config.JobPriorityNormal]
	require.Len(t, jobs, 1)
	require.Equal(t, string(config.JobDeployCloud), jobs[0].JobType)
	require.Equal(t, int64(config.CloudJobTimeout), jobs[0].Timeout)

	info, ok := jobs[0].JobInfo.(*workflowjob.CloudJobInfo)
	require.True(t, ok)
	require.Empty(t, info.Provider)
	require.Empty(t, info.Action)
	require.Nil(t, info.Params)
	require.Equal(t, "step:0/component:0", info.ExecutionKey)
}

func TestGenerateJobTasksCloudJobUsesDedicatedTimeout(t *testing.T) {
	cloudProps, err := model.NewJSONStructByStruct(model.Properties{
		Cloud: &spec.CloudSpec{
			Provider: "aliyun",
			Action:   "create-ecs",
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "infra-cloud-timeout",
		AppID:         "app-cloud",
		Namespace:     "default",
		ComponentType: config.CloudJob,
		Properties:    cloudProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "infra-cloud-timeout", WorkflowType: config.JobDeploy},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{
			ID:    "wf-cloud-timeout",
			Steps: stepsJSON,
		},
		components: []*model.ApplicationComponent{component},
	}

	task := &model.WorkflowQueue{
		WorkflowID:   "wf-cloud-timeout",
		AppID:        "app-cloud",
		ProjectID:    "proj-cloud",
		WorkflowName: "cloud-workflow",
		TaskID:       "task-cloud",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, 1)
	require.Len(t, executions, 1)
	exec := executions[0]
	jobs := exec.Jobs[config.JobPriorityNormal]
	require.Len(t, jobs, 1)
	require.Equal(t, int64(config.CloudJobTimeout), jobs[0].Timeout)
}

func TestGenerateJobTasksCloudJobAssignsDistinctExecutionKeysAcrossRepeatedComponents(t *testing.T) {
	cloudProps, err := model.NewJSONStructByStruct(model.Properties{
		Cloud: &spec.CloudSpec{
			Provider: "aliyun",
			Action:   "create-ecs",
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "infra-cloud-repeat",
		AppID:         "app-cloud",
		Namespace:     "default",
		ComponentType: config.CloudJob,
		Properties:    cloudProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "first", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"infra-cloud-repeat"}}}},
			{Name: "second", WorkflowType: config.JobDeploy, Properties: []model.Policies{{Policies: []string{"infra-cloud-repeat"}}}},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-cloud-repeat", Steps: stepsJSON},
		components: []*model.ApplicationComponent{
			component,
		},
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-cloud-repeat",
		AppID:      "app-cloud",
		TaskID:     "task-cloud-repeat",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)

	firstInfo, ok := executions[0].Jobs[config.JobPriorityNormal][0].JobInfo.(*workflowjob.CloudJobInfo)
	require.True(t, ok)
	secondInfo, ok := executions[1].Jobs[config.JobPriorityNormal][0].JobInfo.(*workflowjob.CloudJobInfo)
	require.True(t, ok)
	require.Equal(t, "step:0/component:0", firstInfo.ExecutionKey)
	require.Equal(t, "step:1/component:0", secondInfo.ExecutionKey)
	require.NotEqual(t, firstInfo.ExecutionKey, secondInfo.ExecutionKey)
}
