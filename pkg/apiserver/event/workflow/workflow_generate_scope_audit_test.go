package workflow

import (
	"context"

	"github.com/stretchr/testify/require"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestGenerateJobTasksUsesPersistedCleanupJobsForRemovedComponents(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:            "api",
		AppID:           "app-reset",
		Namespace:       "default",
		Image:           "nginx:1.27",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		ResourceAppName: "demo-app",
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "deploy-worker",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"worker"}}},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	worker := &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-reset",
		Namespace:     "default",
		Image:         "worker:1.0",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: []*model.ApplicationComponent{worker},
		jobInfos: []*model.JobInfo{{
			ID:           1,
			Type:         string(config.JobCleanupResources),
			WorkflowID:   "wf-reset",
			ProductID:    "proj-reset",
			AppID:        "app-reset",
			TaskID:       "task-reset",
			Status:       string(config.StatusQueued),
			Info:         "cleanup resources: default/api",
			InternalInfo: versionUpdateCleanupMarker(),
			ServiceName:  "api",
		}},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
		CleanupInfo: mustVersionUpdateCleanupInfo(t, model.VersionUpdateCleanupComponent{
			Component:             component,
			ResourceAppName:       component.ResourceAppName,
			InsertBeforeStepIndex: 1,
		}),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)

	require.Equal(t, "worker", executions[0].Name)
	require.NotEmpty(t, executions[0].Jobs[config.JobPriorityNormal])

	cleanupStep := executions[1]
	require.Equal(t, versionUpdateCleanupStepName, cleanupStep.Name)
	require.Equal(t, config.WorkflowModeStepByStep, cleanupStep.Mode)
	cleanupJobs := cleanupStep.Jobs[config.JobPriorityLow]
	require.Len(t, cleanupJobs, 1)
	require.Equal(t, string(config.JobCleanupResources), cleanupJobs[0].JobType)
	require.Equal(t, "api", cleanupJobs[0].Name)
	require.Equal(t, "demo-app", cleanupJobs[0].ResourceAppName)
	cleanupComponent, ok := cleanupJobs[0].JobInfo.(*model.ApplicationComponent)
	require.True(t, ok)
	require.Equal(t, "api", cleanupComponent.Name)
}

func TestGenerateJobTasksPlacesPersistedCleanupByWorkflowStepIndexAfterExpandedStep(t *testing.T) {
	serverProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	removed := &model.ApplicationComponent{
		Name:            "legacy-api",
		AppID:           "app-reset",
		Namespace:       "default",
		Image:           "nginx:1.27",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		ResourceAppName: "demo-app",
	}
	components := []*model.ApplicationComponent{
		{Name: "api", AppID: "app-reset", Namespace: "default", Image: "nginx:1.27", Replicas: 1, ComponentType: config.ServerJob, Properties: serverProps},
		{Name: "worker", AppID: "app-reset", Namespace: "default", Image: "nginx:1.27", Replicas: 1, ComponentType: config.ServerJob, Properties: serverProps},
		{Name: "frontend", AppID: "app-reset", Namespace: "default", Image: "nginx:1.27", Replicas: 1, ComponentType: config.ServerJob, Properties: serverProps},
	}
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "deploy-backend-pair",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"api", "worker"}}},
			},
			{
				Name:         "deploy-frontend",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"frontend"}}},
			},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: components,
		jobInfos: []*model.JobInfo{{
			ID:           1,
			Type:         string(config.JobCleanupResources),
			WorkflowID:   "wf-reset",
			AppID:        "app-reset",
			TaskID:       "task-reset",
			Status:       string(config.StatusQueued),
			Info:         "cleanup resources: default/legacy-api",
			InternalInfo: versionUpdateCleanupMarker(),
			ServiceName:  "legacy-api",
		}},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
		CleanupInfo: mustVersionUpdateCleanupInfo(t, model.VersionUpdateCleanupComponent{
			Component:             removed,
			ResourceAppName:       removed.ResourceAppName,
			InsertBeforeStepIndex: 1,
		}),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 4)
	require.Equal(t, []string{"api", "worker", versionUpdateCleanupStepName, "frontend"}, []string{
		executions[0].Name,
		executions[1].Name,
		executions[2].Name,
		executions[3].Name,
	})

	cleanupJobs := executions[2].Jobs[config.JobPriorityLow]
	require.Len(t, cleanupJobs, 1)
	require.Equal(t, "legacy-api", cleanupJobs[0].Name)
}

func TestGenerateJobTasksPlacesPersistedCleanupAfterApprovalGate(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:            "api",
		AppID:           "app-reset",
		Namespace:       "default",
		Image:           "nginx:1.27",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		ResourceAppName: "demo-app",
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "approve-release",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					Method: "POST",
				},
			},
			{
				Name:         "deploy-worker",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"worker"}}},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	worker := &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-reset",
		Namespace:     "default",
		Image:         "worker:1.0",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: []*model.ApplicationComponent{worker},
		jobInfos: []*model.JobInfo{{
			ID:           1,
			Type:         string(config.JobCleanupResources),
			WorkflowID:   "wf-reset",
			ProductID:    "proj-reset",
			AppID:        "app-reset",
			TaskID:       "task-reset",
			Status:       string(config.StatusQueued),
			Info:         "cleanup resources: default/api",
			InternalInfo: versionUpdateCleanupMarker(),
			ServiceName:  "api",
		}},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
		CleanupInfo: mustVersionUpdateCleanupInfo(t, model.VersionUpdateCleanupComponent{
			Component:             component,
			ResourceAppName:       component.ResourceAppName,
			InsertBeforeStepIndex: 1,
		}),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 3)
	require.Equal(t, "approve-release", executions[0].Name)
	require.Equal(t, config.WorkflowStepTypeApproval, executions[0].StepType)
	require.Equal(t, versionUpdateCleanupStepName, executions[1].Name)
	require.Equal(t, "worker", executions[2].Name)
}

func TestGenerateJobTasksCleanupOnlySkipsDeployStepsAfterApprovalGate(t *testing.T) {
	api := &model.ApplicationComponent{
		Name:            "api",
		AppID:           "app-reset",
		Namespace:       "default",
		Image:           "nginx:1.27",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		ResourceAppName: "demo-app",
	}
	worker := &model.ApplicationComponent{
		Name:            "worker",
		AppID:           "app-reset",
		Namespace:       "default",
		Image:           "worker:1.0",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		ResourceAppName: "demo-app",
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "approve-release",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					Method: "POST",
				},
			},
			{Name: "api", WorkflowType: config.JobDeploy},
			{Name: "worker", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: []*model.ApplicationComponent{
			api,
			worker,
		},
		jobInfos: []*model.JobInfo{
			{
				ID:           1,
				Type:         string(config.JobCleanupResources),
				WorkflowID:   "wf-reset",
				ProductID:    "proj-reset",
				AppID:        "app-reset",
				TaskID:       "task-reset",
				Status:       string(config.StatusQueued),
				Info:         "cleanup resources: default/api",
				InternalInfo: versionUpdateCleanupMarker(),
				ServiceName:  "api",
			},
			{
				ID:           2,
				Type:         string(config.JobCleanupResources),
				WorkflowID:   "wf-reset",
				ProductID:    "proj-reset",
				AppID:        "app-reset",
				TaskID:       "task-reset",
				Status:       string(config.StatusQueued),
				Info:         "cleanup resources: default/worker",
				InternalInfo: versionUpdateCleanupMarker(),
				ServiceName:  "worker",
			},
		},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
		CleanupInfo: mustVersionUpdateCleanupOnlyInfo(t,
			model.VersionUpdateCleanupComponent{
				Component:             api,
				ResourceAppName:       api.ResourceAppName,
				InsertBeforeStepIndex: 1,
			},
			model.VersionUpdateCleanupComponent{
				Component:             worker,
				ResourceAppName:       worker.ResourceAppName,
				InsertBeforeStepIndex: 1,
			},
		),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)
	require.Equal(t, "approve-release", executions[0].Name)
	require.Equal(t, config.WorkflowStepTypeApproval, executions[0].StepType)
	require.Equal(t, versionUpdateCleanupStepName, executions[1].Name)

	cleanupJobs := executions[1].Jobs[config.JobPriorityLow]
	require.Len(t, cleanupJobs, 2)
	require.ElementsMatch(t, []string{"api", "worker"}, []string{cleanupJobs[0].Name, cleanupJobs[1].Name})
}

func TestGenerateJobTasksVersionRestartOnlyKeepsLeadingApprovalAndSkipsDeploy(t *testing.T) {
	api := &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-restart",
		Namespace:     "default",
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	worker := &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-restart",
		Namespace:     "default",
		Image:         "worker:1.0",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "approve-release",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					Method: "POST",
				},
			},
			{Name: "api", WorkflowType: config.JobDeploy},
			{Name: "worker", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-restart", Steps: stepsJSON},
		components: []*model.ApplicationComponent{api, worker},
	}
	task := &model.WorkflowQueue{
		WorkflowID:         "wf-restart",
		AppID:              "app-restart",
		ProjectID:          "proj-restart",
		WorkflowName:       "restart-workflow",
		TaskID:             "task-restart",
		ResourceActionInfo: mustVersionUpdateResourceActionInfo(t, true, "api"),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)
	require.Equal(t, "approve-release", executions[0].Name)
	require.Equal(t, config.WorkflowStepTypeApproval, executions[0].StepType)
	require.Equal(t, versionUpdateRestartStepName, executions[1].Name)

	restartJobs := executions[1].Jobs[config.JobPriorityLow]
	require.Len(t, restartJobs, 1)
	require.Equal(t, "api", restartJobs[0].Name)
	require.Equal(t, string(config.JobVersionRestart), restartJobs[0].JobType)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), restartJobs[0].Timeout)
	require.NotNil(t, restartJobs[0].JobInfo)
}

func TestGenerateJobTasksVersionRestartAppendsAfterDeploy(t *testing.T) {
	api := &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-restart",
		Namespace:     "default",
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	worker := &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-restart",
		Namespace:     "default",
		Image:         "worker:1.0",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "worker", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-restart", Steps: stepsJSON},
		components: []*model.ApplicationComponent{api, worker},
	}
	task := &model.WorkflowQueue{
		WorkflowID:         "wf-restart",
		AppID:              "app-restart",
		ProjectID:          "proj-restart",
		WorkflowName:       "restart-workflow",
		TaskID:             "task-restart",
		ResourceActionInfo: mustVersionUpdateResourceActionInfo(t, false, "api"),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)
	require.Equal(t, "worker", executions[0].Name)
	require.NotEmpty(t, executions[0].Jobs[config.JobPriorityNormal])
	require.Equal(t, versionUpdateRestartStepName, executions[1].Name)

	restartJobs := executions[1].Jobs[config.JobPriorityLow]
	require.Len(t, restartJobs, 1)
	require.Equal(t, "api", restartJobs[0].Name)
	require.Equal(t, string(config.JobVersionRestart), restartJobs[0].JobType)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), restartJobs[0].Timeout)
}

func TestGenerateJobTasksVersionUpdateImageReadyTimeoutTargetsWorkloadJobs(t *testing.T) {
	api := &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "api:v2",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	worker := &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "worker:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	mysql := &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "mysql:8.0",
		Replicas:      1,
		ComponentType: config.StoreJob,
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "api", WorkflowType: config.JobDeploy},
			{Name: "worker", WorkflowType: config.JobDeploy},
			{Name: "mysql", WorkflowType: config.JobDeploy},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-version", Steps: stepsJSON},
		components: []*model.ApplicationComponent{api, worker, mysql},
	}
	task := &model.WorkflowQueue{
		WorkflowID:         "wf-version",
		AppID:              "app-version",
		ProjectID:          "proj-version",
		WorkflowName:       "version-workflow",
		TaskID:             "task-version",
		ResourceActionInfo: mustVersionUpdateImageReadyActionInfo(t, 300, "api", "mysql"),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	apiDeploy := requireWorkflowJob(t, executions, "api", config.JobDeploy)
	workerDeploy := requireWorkflowJob(t, executions, "worker", config.JobDeploy)
	mysqlDeploy := requireWorkflowJob(t, executions, "mysql", config.JobDeployStore)

	require.Equal(t, int64(300), apiDeploy.Timeout)
	require.Equal(t, int64(config.DeployTimeout), workerDeploy.Timeout)
	require.Equal(t, int64(300), mysqlDeploy.Timeout)
}

func TestGenerateJobTasksDefaultExecutionScopeRunsFullWorkflow(t *testing.T) {
	backend := &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "backend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	frontend := &model.ApplicationComponent{
		Name:          "frontend",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "frontend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	socket := &model.ApplicationComponent{
		Name:          "socket",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "socket:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	mysql := &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "mysql:8.0",
		Replicas:      1,
		ComponentType: config.StoreJob,
	}
	redis := &model.ApplicationComponent{
		Name:          "redis",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "redis:7",
		Replicas:      1,
		ComponentType: config.StoreJob,
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "phase-store",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeDAG,
				Properties:   []model.Policies{{Policies: []string{"mysql", "redis"}}},
			},
			{
				Name:         "phase-web",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeDAG,
				Properties:   []model.Policies{{Policies: []string{"backend", "frontend", "socket"}}},
			},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-version", Steps: stepsJSON},
		components: []*model.ApplicationComponent{backend, frontend, socket, mysql, redis},
	}
	task := &model.WorkflowQueue{
		WorkflowID:         "wf-version",
		AppID:              "app-version",
		ProjectID:          "proj-version",
		WorkflowName:       "version-workflow",
		TaskID:             "task-version",
		ResourceActionInfo: mustVersionUpdateMarkerInfo(t),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	requireWorkflowJob(t, executions, "backend", config.JobDeploy)
	requireWorkflowJob(t, executions, "frontend", config.JobDeploy)
	requireWorkflowJob(t, executions, "socket", config.JobDeploy)
	requireWorkflowJob(t, executions, "mysql", config.JobDeployStore)
	requireWorkflowJob(t, executions, "redis", config.JobDeployStore)
}

func TestGenerateJobTasksChangedComponentsExecutionScopeFiltersWorkflowPolicies(t *testing.T) {
	backend := &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "backend:v2",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	frontend := &model.ApplicationComponent{
		Name:          "frontend",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "frontend:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	socket := &model.ApplicationComponent{
		Name:          "socket",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "socket:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	mysql := &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "mysql:8.0",
		Replicas:      1,
		ComponentType: config.StoreJob,
	}
	redis := &model.ApplicationComponent{
		Name:          "redis",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "redis:7",
		Replicas:      1,
		ComponentType: config.StoreJob,
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "approve-release",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: "https://example.com/approve",
				},
			},
			{
				Name:         "phase-store",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeDAG,
				Properties:   []model.Policies{{Policies: []string{"mysql", "redis"}}},
			},
			{
				Name:         "phase-web",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeDAG,
				Properties:   []model.Policies{{Policies: []string{"backend", "frontend", "socket"}}},
			},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-version", Steps: stepsJSON},
		components: []*model.ApplicationComponent{backend, frontend, socket, mysql, redis},
	}
	task := &model.WorkflowQueue{
		WorkflowID:         "wf-version",
		AppID:              "app-version",
		ProjectID:          "proj-version",
		WorkflowName:       "version-workflow",
		TaskID:             "task-version",
		ResourceActionInfo: mustVersionUpdateExecutionScopeActionInfo(t, "backend"),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.NotEmpty(t, executions)
	require.Equal(t, "approve-release", executions[0].Name)
	require.Equal(t, config.WorkflowStepTypeApproval, executions[0].StepType)
	requireWorkflowJob(t, executions, "backend", config.JobDeploy)

	names := workflowJobNames(executions)
	require.NotContains(t, names, "frontend")
	require.NotContains(t, names, "socket")
	require.NotContains(t, names, "mysql")
	require.NotContains(t, names, "redis")
}

func TestGenerateJobTasksChangedComponentsExecutionScopeKeepsInstantJob(t *testing.T) {
	mysqlUpdateJob := &model.ApplicationComponent{
		Name:          "mysql-update-job",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "skeema-tool:1.12.3-centos",
		ComponentType: config.InstantJob,
	}
	backend := &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "backend:v2",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "phase-components",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeDAG,
				Properties:   []model.Policies{{Policies: []string{"mysql-update-job", "backend"}}},
			},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-version", Steps: stepsJSON},
		components: []*model.ApplicationComponent{mysqlUpdateJob, backend},
	}
	task := &model.WorkflowQueue{
		WorkflowID:         "wf-version",
		AppID:              "app-version",
		ProjectID:          "proj-version",
		WorkflowName:       "version-workflow",
		TaskID:             "task-version",
		ResourceActionInfo: mustVersionUpdateExecutionScopeActionInfo(t, "mysql-update-job"),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	instantJob := requireWorkflowJob(t, executions, "mysql-update-job", config.JobDeployInstant)
	require.NotNil(t, instantJob.JobInfo)

	names := workflowJobNames(executions)
	require.Contains(t, names, "mysql-update-job")
	require.NotContains(t, names, "backend")
}

func TestGenerateJobTasksChangedComponentsExecutionScopeSkipsNonDeployJobs(t *testing.T) {
	mysql := &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-version",
		Namespace:     "default",
		Image:         "mysql:8.0",
		Replicas:      1,
		ComponentType: config.StoreJob,
	}

	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "phase-store",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeDAG,
				Properties:   []model.Policies{{Policies: []string{"mysql"}}},
			},
			{
				Name:         "reset-mysql",
				WorkflowType: config.JobDatabaseReset,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"mysql"}}},
			},
			{
				Name:         "archive-mysql",
				WorkflowType: config.JobLogArchiveUpload,
				Mode:         config.WorkflowModeStepByStep,
				Properties: []model.Policies{{
					Policies: []string{"mysql"},
					Path:     "/var/log/mysql",
				}},
			},
			{
				Name: "maintenance",
				Mode: config.WorkflowModeStepByStep,
				SubSteps: []*model.WorkflowSubStep{
					{
						Name:         "reset-mysql-substep",
						WorkflowType: config.JobDatabaseReset,
						Properties:   []model.Policies{{Policies: []string{"mysql"}}},
					},
					{
						Name:         "archive-mysql-substep",
						WorkflowType: config.JobLogArchiveUpload,
						Properties: []model.Policies{{
							Policies: []string{"mysql"},
							Path:     "/var/log/mysql",
						}},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-version", Steps: stepsJSON},
		components: []*model.ApplicationComponent{mysql},
	}
	task := &model.WorkflowQueue{
		WorkflowID:         "wf-version",
		AppID:              "app-version",
		ProjectID:          "proj-version",
		WorkflowName:       "version-workflow",
		TaskID:             "task-version",
		ResourceActionInfo: mustVersionUpdateExecutionScopeActionInfo(t, "mysql"),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	requireWorkflowJob(t, executions, "mysql", config.JobDeployStore)

	names := workflowJobNames(executions)
	require.Contains(t, names, "mysql")

	for _, execution := range executions {
		for _, jobs := range execution.Jobs {
			for _, job := range jobs {
				require.NotEqual(t, string(config.JobDatabaseReset), job.JobType)
				require.NotEqual(t, string(config.JobLogArchiveUpload), job.JobType)
			}
		}
	}
}

func TestGenerateJobTasksIgnoresUnmarkedPersistedCleanupJobs(t *testing.T) {
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: nil,
		jobInfos: []*model.JobInfo{{
			ID:           1,
			Type:         string(config.JobCleanupResources),
			WorkflowID:   "wf-reset",
			AppID:        "app-reset",
			TaskID:       "task-reset",
			Status:       string(config.StatusQueued),
			InternalInfo: `{"source":"other"}`,
			ServiceName:  "api",
		}},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Empty(t, executions)
}

func TestGenerateJobTasksWithApprovalStep(t *testing.T) {
	serverProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "web",
		AppID:         "app-approval",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    serverProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "manual-check",
				StepType: config.WorkflowStepTypeApproval,
				Mode:     config.WorkflowModeStepByStep,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: "https://example.com/hooks/approval",
					Message:   "approve rollout",
				},
			},
			{
				Name:         "deploy-web",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"web"}}},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{
			ID:    "wf-approval",
			Steps: stepsJSON,
		},
		components: []*model.ApplicationComponent{component},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-approval",
		AppID:        "app-approval",
		ProjectID:    "proj-approval",
		WorkflowName: "approval-flow",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)
	require.Equal(t, config.WorkflowStepTypeApproval, executions[0].StepType)
	require.NotNil(t, executions[0].Approval)
	require.Equal(t, "manual-check", executions[0].Name)
	require.Nil(t, executions[0].Jobs)
	require.Equal(t, config.WorkflowStepTypeComponent, executions[1].StepType)
	require.NotNil(t, executions[1].Jobs)
}
