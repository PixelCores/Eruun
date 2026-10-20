package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestGetTaskStatusIncludesAllComponents(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:       "web",
				Properties: []model.Policies{{Policies: []string{"web"}}},
			},
			{
				Name:       "database",
				Properties: []model.Policies{{Policies: []string{"db"}}},
			},
			{
				SubSteps: []*model.WorkflowSubStep{
					{
						Name:       "cache",
						Properties: []model.Policies{{Policies: []string{"cache"}}},
					},
				},
			},
		},
	}
	stepsStruct, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-1",
		WorkflowID:   "wf-1",
		WorkflowName: "deploy",
		AppID:        "app-1",
		Status:       config.StatusRunning,
	}

	store := &statusDataStore{
		task: task,
		workflow: &model.Workflow{
			ID:    "wf-1",
			Name:  "deploy",
			AppID: "app-1",
			Steps: stepsStruct,
		},
		jobs: []*model.JobInfo{
			{
				TaskID:      "task-1",
				ServiceName: "web",
				Status:      string(config.StatusRunning),
			},
			{
				TaskID:      "task-1",
				ServiceName: "db",
				Status:      string(config.StatusFailed),
				Error:       "deploy failed",
			},
		},
	}

	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}
	resp, err := svc.GetTaskStatus(context.Background(), "task-1")
	require.NoError(t, err)

	require.Equal(t, "task-1", resp.TaskID)
	require.Equal(t, 3, len(resp.Components))

	byName := map[string]string{}
	for _, c := range resp.Components {
		byName[c.Name] = c.Status
	}

	require.Equal(t, string(config.StatusRunning), byName["web"])
	require.Equal(t, string(config.StatusFailed), byName["db"])
	require.Equal(t, string(config.StatusWaiting), byName["cache"])
}

func TestGetTaskStatusExcludesApprovalStepNames(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: "https://example.com/approval",
				},
			},
			{
				Name:       "deploy-web",
				Properties: []model.Policies{{Policies: []string{"web"}}},
			},
		},
	}
	stepsStruct, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		TaskID:       "task-approval-components",
		WorkflowID:   "wf-approval-components",
		WorkflowName: "deploy",
		AppID:        "app-1",
		Status:       config.StatusRunning,
	}
	store := &statusDataStore{
		task: task,
		workflow: &model.Workflow{
			ID:    "wf-approval-components",
			Name:  "deploy",
			AppID: "app-1",
			Steps: stepsStruct,
		},
	}

	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}
	resp, err := svc.GetTaskStatus(context.Background(), "task-approval-components")
	require.NoError(t, err)

	names := make(map[string]struct{}, len(resp.Components))
	for _, component := range resp.Components {
		names[component.Name] = struct{}{}
	}

	_, hasWeb := names["web"]
	_, hasApprovalName := names["manual-check"]
	require.True(t, hasWeb)
	require.False(t, hasApprovalName)
	require.Equal(t, 1, len(resp.Components))
}

func TestGetTaskStagesReturnsJobDetails(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:       "task-2",
		WorkflowID:   "wf-2",
		WorkflowName: "release",
		AppID:        "app-2",
		Status:       config.StatusRunning,
		Type:         config.WorkflowTaskTypeWorkflow,
	}
	store := &statusDataStore{
		task: task,
		jobs: []*model.JobInfo{
			{
				ID:          10,
				TaskID:      "task-2",
				ServiceName: "web",
				Type:        "deploy",
				Status:      string(config.StatusRunning),
				Info:        "apply deployment",
			},
			{
				ID:          11,
				TaskID:      "task-2",
				ServiceName: "db",
				Type:        "pvc",
				Status:      string(config.StatusFailed),
				Error:       "pvc timeout",
			},
		},
	}

	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}
	resp, err := svc.GetTaskStages(context.Background(), "task-2")
	require.NoError(t, err)

	require.Equal(t, "task-2", resp.TaskID)
	require.Equal(t, string(config.StatusRunning), resp.Status)
	require.Equal(t, "wf-2", resp.WorkflowID)
	require.Equal(t, "release", resp.WorkflowName)
	require.Equal(t, "app-2", resp.AppID)
	require.Equal(t, config.WorkflowTaskTypeWorkflow, resp.Type)
	require.Len(t, resp.Stages, 2)

	require.Equal(t, 10, resp.Stages[0].ID)
	require.Equal(t, "web", resp.Stages[0].Name)
	require.Equal(t, "[deploy]", resp.Stages[0].Type)
	require.Len(t, resp.Stages[0].Info, 1)
	require.Equal(t, "deploy", resp.Stages[0].Info[0].Type)
	require.Equal(t, "apply deployment", resp.Stages[0].Info[0].Message)
	require.Len(t, resp.Stages[0].Error, 0)

	require.Equal(t, 11, resp.Stages[1].ID)
	require.Equal(t, "db", resp.Stages[1].Name)
	require.Equal(t, "[pvc]", resp.Stages[1].Type)
	require.Len(t, resp.Stages[1].Info, 0)
	require.Len(t, resp.Stages[1].Error, 1)
	require.Equal(t, "db", resp.Stages[1].Error[0].Component)
	require.Equal(t, "pvc timeout", resp.Stages[1].Error[0].Message)
}

func TestGetTaskStagesMergesSameName(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:       "task-3",
		WorkflowID:   "wf-3",
		WorkflowName: "deploy",
		AppID:        "app-3",
		Status:       config.StatusRunning,
		Type:         config.WorkflowTaskTypeWorkflow,
	}
	store := &statusDataStore{
		task: task,
		jobs: []*model.JobInfo{
			{
				ID:          20,
				TaskID:      "task-3",
				ServiceName: "nginx",
				Type:        "service_deploy",
				Status:      string(config.StatusRunning),
				Info:        "svc: nginx.default.svc:80",
				StartTime:   100,
				EndTime:     110,
			},
			{
				ID:          21,
				TaskID:      "task-3",
				ServiceName: "nginx",
				Type:        "deploy",
				Status:      string(config.StatusFailed),
				Error:       "deployment failed",
				StartTime:   90,
				EndTime:     130,
			},
		},
	}

	svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}, ScheduleLocker: locker.NewNoopLocker("test-app-schedule")}
	resp, err := svc.GetTaskStages(context.Background(), "task-3")
	require.NoError(t, err)

	require.Equal(t, "task-3", resp.TaskID)
	require.Len(t, resp.Stages, 1)

	stage := resp.Stages[0]
	require.Equal(t, 20, stage.ID)
	require.Equal(t, "nginx", stage.Name)
	require.Equal(t, "[service_deploy,deploy]", stage.Type)
	require.Equal(t, string(config.StatusFailed), stage.Status)
	require.Len(t, stage.Info, 1)
	require.Equal(t, "service_deploy", stage.Info[0].Type)
	require.Equal(t, "svc: nginx.default.svc:80", stage.Info[0].Message)
	require.Len(t, stage.Error, 1)
	require.Equal(t, "nginx", stage.Error[0].Component)
	require.Equal(t, "deployment failed", stage.Error[0].Message)
	require.Equal(t, int64(90), stage.StartTime)
	require.Equal(t, int64(130), stage.EndTime)
}

func TestGetTaskStagesAggregatesTerminalTaskStatus(t *testing.T) {
	cases := []struct {
		name     string
		jobs     []*model.JobInfo
		expected string
	}{
		{
			name: "all_completed",
			jobs: []*model.JobInfo{
				{TaskID: "task-4", ServiceName: "web", Status: string(config.StatusCompleted)},
				{TaskID: "task-4", ServiceName: "db", Status: string(config.StatusCompleted)},
			},
			expected: string(config.StatusCompleted),
		},
		{
			name: "failed_overrides_completed",
			jobs: []*model.JobInfo{
				{TaskID: "task-4", ServiceName: "web", Status: string(config.StatusCompleted)},
				{TaskID: "task-4", ServiceName: "db", Status: string(config.StatusFailed)},
			},
			expected: string(config.StatusFailed),
		},
		{
			name: "skipped_and_passed_treated_as_completed",
			jobs: []*model.JobInfo{
				{TaskID: "task-4", ServiceName: "web", Status: string(config.StatusSkipped)},
				{TaskID: "task-4", ServiceName: "db", Status: string(config.StatusPassed)},
			},
			expected: string(config.StatusCompleted),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &model.WorkflowQueue{
				TaskID:       "task-4",
				WorkflowID:   "wf-4",
				WorkflowName: "deploy",
				AppID:        "app-4",
				Status:       config.StatusRunning,
				Type:         config.WorkflowTaskTypeWorkflow,
			}
			store := &statusDataStore{
				task: task,
				jobs: tc.jobs,
			}

			svc := &workflowServiceImpl{Store: store, Cfg: &config.Config{AllowPrivateURLTargets: true}}
			resp, err := svc.GetTaskStages(context.Background(), "task-4")
			require.NoError(t, err)
			require.Equal(t, tc.expected, resp.Status)
		})
	}
}

func TestEnsureAppWorkflowIdleBlocksRunningTask(t *testing.T) {
	store := &statusDataStore{
		tasks: []*model.WorkflowQueue{
			{TaskID: "task-1", AppID: "app-1", Status: config.StatusRunning},
		},
	}

	err := EnsureAppWorkflowIdle(context.Background(), store, "app-1")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
}

func TestEnsureAppWorkflowIdleBlocksCancellingTask(t *testing.T) {
	store := &statusDataStore{
		tasks: []*model.WorkflowQueue{
			{TaskID: "task-2", AppID: "app-2", Status: config.StatusCancelled},
		},
		jobs: []*model.JobInfo{
			{TaskID: "task-2", Status: string(config.StatusRunning)},
		},
	}

	err := EnsureAppWorkflowIdle(context.Background(), store, "app-2")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskCancelling)
}

func TestEnsureAppWorkflowIdleAllowsCancelledTaskWhenJobsDone(t *testing.T) {
	store := &statusDataStore{
		tasks: []*model.WorkflowQueue{
			{TaskID: "task-3", AppID: "app-3", Status: config.StatusCancelled},
		},
		jobs: []*model.JobInfo{
			{TaskID: "task-3", Status: string(config.StatusCancelled)},
		},
	}

	err := EnsureAppWorkflowIdle(context.Background(), store, "app-3")
	require.NoError(t, err)
}

func TestEnsureAppWorkflowIdleIgnoresFutureExecuteAt(t *testing.T) {
	store := &statusDataStore{
		tasks: []*model.WorkflowQueue{
			{
				TaskID:    "task-4",
				AppID:     "app-4",
				Status:    config.StatusWaiting,
				ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
			},
		},
	}

	err := EnsureAppWorkflowIdle(context.Background(), store, "app-4")
	require.NoError(t, err)
}

func TestEnsureAppWorkflowIdleBlocksFutureExecuteAtWithActiveJobs(t *testing.T) {
	store := &statusDataStore{
		tasks: []*model.WorkflowQueue{
			{
				TaskID:    "task-4-cleanup",
				AppID:     "app-4",
				Status:    config.StatusWaiting,
				ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
			},
		},
		jobs: []*model.JobInfo{
			{
				TaskID:       "task-4-cleanup",
				Type:         string(config.JobCleanupResources),
				Status:       string(config.StatusQueued),
				InternalInfo: `{"source":"version_update_remove"}`,
				ServiceName:  "api",
			},
		},
	}

	err := EnsureAppWorkflowIdle(context.Background(), store, "app-4")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
}

func TestEnsureAppWorkflowIdleBlocksRunningTaskAfterTerminalTask(t *testing.T) {
	store := &statusDataStore{
		tasks: []*model.WorkflowQueue{
			{TaskID: "task-5-1", AppID: "app-5", Status: config.StatusCompleted},
			{TaskID: "task-5-2", AppID: "app-5", Status: config.StatusRunning},
		},
	}

	err := EnsureAppWorkflowIdle(context.Background(), store, "app-5")
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
}
