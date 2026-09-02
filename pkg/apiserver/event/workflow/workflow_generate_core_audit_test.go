package workflow

import (
	"context"

	"fmt"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"testing"

	corev1 "k8s.io/api/core/v1"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	wfNaming "github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGenerateJobTasksPropagatesWorkflowFailurePolicy(t *testing.T) {
	serverProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	serverComponent := &model.ApplicationComponent{
		Name:          "server",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    serverProps,
	}
	steps := &model.WorkflowSteps{
		FailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupAll,
		Steps: []*model.WorkflowStep{{
			Name:         "server",
			WorkflowType: config.JobDeploy,
		}},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-1", Steps: stepsJSON},
		components: []*model.ApplicationComponent{
			serverComponent,
		},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		ProjectID:    "proj-1",
		WorkflowName: "test-workflow",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.NotEmpty(t, executions)
	for _, execution := range executions {
		require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, execution.FailurePolicy)
	}
}

func TestGenerateJobTasksDefaultsMissingFailurePolicyToCleanupAll(t *testing.T) {
	serverProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	serverComponent := &model.ApplicationComponent{
		Name:          "server",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    serverProps,
	}
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "server",
			WorkflowType: config.JobDeploy,
		}},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-1", Steps: stepsJSON},
		components: []*model.ApplicationComponent{
			serverComponent,
		},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		ProjectID:    "proj-1",
		WorkflowName: "test-workflow",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.NotEmpty(t, executions)
	for _, execution := range executions {
		require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, execution.FailurePolicy)
	}
}

func TestBuildWorkflowFailureCleanupJobsCoversAllComponents(t *testing.T) {
	store := &fakeDataStore{
		application: &model.Applications{ID: "app-1", Name: "DemoApp"},
		components: []*model.ApplicationComponent{
			{Name: "worker", AppID: "app-1", Namespace: "default", ComponentType: config.ServerJob},
			{Name: "api", AppID: "app-1", Namespace: "default", ComponentType: config.ServerJob},
		},
	}
	task := &model.WorkflowQueue{
		TaskID:        "task-1",
		WorkflowID:    "wf-1",
		AppID:         "app-1",
		ProjectID:     "proj-1",
		RunGeneration: 7,
		RunToken:      "token-7",
		WorkerID:      "worker-a",
	}

	jobs, err := buildWorkflowFailureCleanupJobs(context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	require.Equal(t, []string{"api", "worker"}, []string{jobs[0].Name, jobs[1].Name})
	for jobIndex, cleanupJob := range jobs {
		require.Equal(t, string(config.JobCleanupResources), cleanupJob.JobType)
		component, ok := cleanupJob.JobInfo.(*model.ApplicationComponent)
		require.True(t, ok)
		require.Equal(t, "app-1", component.AppID)
		require.NotEmpty(t, component.ResourceAppName)
		require.Equal(t, task.RunGeneration, cleanupJob.RunGeneration)
		require.Equal(t, task.RunToken, cleanupJob.RunToken)
		require.Equal(t, task.WorkerID, cleanupJob.WorkerID)
		require.Equal(t, uint(1), cleanupJob.Attempt)
		require.Equal(t, workflowJobExecutionKey(task, -1, config.JobPriorityLow, jobIndex, cleanupJob.Name, cleanupJob.JobType), cleanupJob.ExecutionKey)
		require.Len(t, cleanupJob.ExecutionKey, 64)
	}
}

func TestBuildWorkflowFailureCleanupJobsSkipsAdoptedApplication(t *testing.T) {
	sourceUID := "deployment-uid"
	store := &fakeDataStore{
		application: &model.Applications{
			ID:             "app-1",
			Name:           "ImportedApp",
			ManagementMode: config.ManagementModeAdopted,
		},
		components: []*model.ApplicationComponent{{
			Name:              "api",
			AppID:             "app-1",
			Namespace:         "default",
			ComponentType:     config.ServerJob,
			SourceWorkloadUID: &sourceUID,
		}},
	}
	task := &model.WorkflowQueue{TaskID: "task-1", AppID: "app-1"}

	jobs, err := buildWorkflowFailureCleanupJobs(context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestWorkflowFailureCleanupAllPreservesClaimNamePVC(t *testing.T) {
	ctx := context.Background()
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)

	traits, err := model.NewJSONStructByStruct(spec.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:      "logs",
			Type:      config.StorageTypePersistent,
			MountPath: "/logs",
			ClaimName: "default-logs-pvc",
		}},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		application: &model.Applications{ID: "app-1", Name: "DemoApp"},
		components: []*model.ApplicationComponent{
			{ID: 2, Name: "worker", AppID: "app-1", Namespace: "default", ComponentType: config.ServerJob},
			{ID: 1, Name: "api", AppID: "app-1", Namespace: "default", ComponentType: config.ServerJob, Image: "nginx:latest", Replicas: 1, Traits: traits},
		},
	}
	task := &model.WorkflowQueue{
		TaskID:     "task-cleanup-all",
		WorkflowID: "wf-cleanup-all",
		AppID:      "app-1",
		ProjectID:  "proj-1",
	}

	jobs, err := buildWorkflowFailureCleanupJobs(ctx, task, store, int64(config.DefaultJobTaskTimeout))
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	require.Equal(t, []string{"api", "worker"}, []string{jobs[0].Name, jobs[1].Name})
	require.Equal(t, string(config.JobCleanupResources), jobs[0].JobType)

	deployName := wfNaming.WebServiceName("api", "DemoApp")
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "default-logs-pvc", Namespace: "default"}},
	)
	ctl := workflowjob.NewCleanupResourcesJobCtl(jobs[0], client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	_, err = client.AppsV1().Deployments("default").Get(ctx, deployName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "default-logs-pvc", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components[1].Status)
}

func TestGenerateJobTasksSequential(t *testing.T) {
	serverProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	configProps, err := model.NewJSONStructByStruct(model.Properties{
		Conf: map[string]string{"config": "value"},
	})
	require.NoError(t, err)

	serverComponent := &model.ApplicationComponent{
		Name:          "server",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    serverProps,
	}

	configComponent := &model.ApplicationComponent{
		Name:          "config",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ConfJob,
		Properties:    configProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "server", WorkflowType: config.JobDeploy},
			{Name: "config", WorkflowType: config.JobDeploy},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	workflow := &model.Workflow{
		ID:    "wf-1",
		Steps: stepsJSON,
	}

	store := &fakeDataStore{
		workflow:   workflow,
		components: []*model.ApplicationComponent{serverComponent, configComponent},
	}

	task := &model.WorkflowQueue{
		WorkflowID:   "wf-1",
		AppID:        "app-1",
		ProjectID:    "proj-1",
		WorkflowName: "test-workflow",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)

	first := executions[0]
	require.Equal(t, "server", first.Name)
	require.Equal(t, config.WorkflowModeStepByStep, first.Mode)
	require.Len(t, first.Jobs[config.JobPriorityHigh], 1)
	require.Equal(t, string(config.JobDeployService), first.Jobs[config.JobPriorityHigh][0].JobType)
	require.Len(t, first.Jobs[config.JobPriorityNormal], 1)
	require.Equal(t, string(config.JobDeploy), first.Jobs[config.JobPriorityNormal][0].JobType)

	second := executions[1]
	require.Equal(t, "config", second.Name)
	require.Equal(t, config.WorkflowModeStepByStep, second.Mode)
	require.Len(t, second.Jobs[config.JobPriorityMaxHigh], 1)
	cmJob := second.Jobs[config.JobPriorityMaxHigh][0]
	require.Equal(t, configComponent.Name, cmJob.Name)
	cmInput, ok := cmJob.JobInfo.(*model.ConfigMapInput)
	require.True(t, ok)
	require.Equal(t, cmJob.Name, cmInput.Name)
}

func TestGenerateJobTasksParallel(t *testing.T) {
	frontendProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	require.NoError(t, err)

	backendProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 8081}},
	})
	require.NoError(t, err)

	frontend := &model.ApplicationComponent{
		Name:          "frontend",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    frontendProps,
	}

	backend := &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    backendProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "apply-services",
				WorkflowType: config.JobDeploy,
				Mode:         config.WorkflowModeDAG,
				Properties:   []model.Policies{{Policies: []string{"frontend", "backend"}}},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	workflow := &model.Workflow{
		ID:    "wf-2",
		Steps: stepsJSON,
	}

	store := &fakeDataStore{
		workflow:   workflow,
		components: []*model.ApplicationComponent{frontend, backend},
	}

	task := &model.WorkflowQueue{
		WorkflowID:   "wf-2",
		AppID:        "app-1",
		ProjectID:    "proj-1",
		WorkflowName: "parallel-workflow",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 1)

	parallel := executions[0]
	require.Equal(t, config.WorkflowModeDAG, parallel.Mode)
	require.Equal(t, "apply-services", parallel.Name)

	jobs := parallel.Jobs[config.JobPriorityNormal]
	require.GreaterOrEqual(t, len(jobs), 2)
	deployCount := 0
	for _, job := range jobs {
		if job.JobType == string(config.JobDeploy) {
			deployCount++
		}
	}
	require.Equal(t, 2, deployCount)
}

func TestGenerateJobTasksResetWorkflowCleanupThenDeploy(t *testing.T) {
	serverProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "web",
		AppID:         "app-reset",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    serverProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "cleanup-web",
				WorkflowType: config.JobCleanupResources,
				Mode:         config.WorkflowModeStepByStep,
				Properties:   []model.Policies{{Policies: []string{"web"}}},
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
		workflow:   &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: []*model.ApplicationComponent{component},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)

	cleanupJobs := executions[0].Jobs[config.JobPriorityLow]
	require.Len(t, cleanupJobs, 1)
	require.Equal(t, "web", executions[0].Name)
	require.Equal(t, string(config.JobCleanupResources), cleanupJobs[0].JobType)
	cleanupComponent, ok := cleanupJobs[0].JobInfo.(*model.ApplicationComponent)
	require.True(t, ok)
	require.Equal(t, "web", cleanupComponent.Name)

	deployJobs := executions[1].Jobs[config.JobPriorityNormal]
	require.NotEmpty(t, deployJobs)
	require.Equal(t, "web", executions[1].Name)
	require.Equal(t, string(config.JobDeploy), deployJobs[0].JobType)
}

func TestGenerateJobTasksPlacesCleanupMetadataListFailureBeforeNormalSteps(t *testing.T) {
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "deploy-worker",
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
			Properties:   []model.Policies{{Policies: []string{"worker"}}},
		}},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: []*model.ApplicationComponent{{
			Name:          "worker",
			AppID:         "app-reset",
			Namespace:     "default",
			Image:         "worker:1.0",
			Replicas:      1,
			ComponentType: config.ServerJob,
		}},
		jobInfoListErr: fmt.Errorf("list cleanup failed"),
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
		CleanupInfo: mustVersionUpdateCleanupInfo(t, model.VersionUpdateCleanupComponent{
			Component: &model.ApplicationComponent{
				Name:          "api",
				AppID:         "app-reset",
				Namespace:     "default",
				ComponentType: config.ServerJob,
			},
			InsertBeforeStepIndex: 0,
		}),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)
	require.Equal(t, versionUpdateCleanupStepName, executions[0].Name)
	cleanupJobs := executions[0].Jobs[config.JobPriorityLow]
	require.Len(t, cleanupJobs, 1)
	require.Equal(t, string(config.JobCleanupResources), cleanupJobs[0].JobType)
	require.Contains(t, fmt.Sprint(cleanupJobs[0].JobInfo), "list cleanup failed")
	require.Equal(t, "worker", executions[1].Name)
}

func TestGenerateJobTasksPlacesInvalidCleanupPayloadFailureBeforeNormalSteps(t *testing.T) {
	stepsJSON, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "deploy-worker",
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
			Properties:   []model.Policies{{Policies: []string{"worker"}}},
		}},
	})
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-reset", Steps: stepsJSON},
		components: []*model.ApplicationComponent{{
			Name:          "worker",
			AppID:         "app-reset",
			Namespace:     "default",
			Image:         "worker:1.0",
			Replicas:      1,
			ComponentType: config.ServerJob,
		}},
		jobInfos: []*model.JobInfo{{
			ID:           1,
			Type:         string(config.JobCleanupResources),
			WorkflowID:   "wf-reset",
			AppID:        "app-reset",
			TaskID:       "task-reset",
			Status:       string(config.StatusQueued),
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
			Component: &model.ApplicationComponent{
				AppID:         "app-reset",
				Namespace:     "default",
				ComponentType: config.ServerJob,
			},
			InsertBeforeStepIndex: 0,
		}),
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 2)
	require.Equal(t, versionUpdateCleanupStepName, executions[0].Name)
	cleanupJobs := executions[0].Jobs[config.JobPriorityLow]
	require.Len(t, cleanupJobs, 1)
	require.Equal(t, string(config.JobCleanupResources), cleanupJobs[0].JobType)
	require.Contains(t, fmt.Sprint(cleanupJobs[0].JobInfo), "component name is empty")
	require.Equal(t, "worker", executions[1].Name)
}

func TestGenerateJobTasksRejectsUnsupportedWorkflowJobType(t *testing.T) {
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "unsupported-step",
				WorkflowType: config.JobType("unsupported_job_type"),
				Mode:         config.WorkflowModeDAG,
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-reset-all", Steps: stepsJSON},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-reset-all",
		AppID:        "app-reset",
		ProjectID:    "proj-reset",
		WorkflowName: "reset-workflow",
		TaskID:       "task-reset",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	requireWorkflowGenerationFailed(t, executions, "unsupported_job_type")
}

func TestGenerateJobTasksSubStepByStepGroupsComponents(t *testing.T) {
	serverProps, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	api := &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-substep",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    serverProps,
	}
	worker := &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-substep",
		Namespace:     "default",
		Image:         "nginx:1.21",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    serverProps,
	}

	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name: "deploy-group",
				Mode: config.WorkflowModeStepByStep,
				SubSteps: []*model.WorkflowSubStep{
					{
						Name:         "deploy-substep",
						WorkflowType: config.JobDeploy,
						Properties:   []model.Policies{{Policies: []string{"api", "worker"}}},
					},
				},
			},
		},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow:   &model.Workflow{ID: "wf-substep-group", Steps: stepsJSON},
		components: []*model.ApplicationComponent{api, worker},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-substep-group",
		AppID:        "app-substep",
		ProjectID:    "proj-substep",
		WorkflowName: "deploy-workflow",
		TaskID:       "task-substep",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 1)
	require.Equal(t, "deploy-substep", executions[0].Name)
	require.Equal(t, config.WorkflowModeStepByStep, executions[0].Mode)

	deployJobs := executions[0].Jobs[config.JobPriorityNormal]
	require.Len(t, deployJobs, 2)
	require.Equal(t, "api", deployJobs[0].Name)
	require.Equal(t, "worker", deployJobs[1].Name)
	for _, deployJob := range deployJobs {
		require.Equal(t, string(config.JobDeploy), deployJob.JobType)
	}
}

func TestGenerateJobTasksUsesApplicationNameForResourceNames(t *testing.T) {
	appID := "lyhemnnmr48fmmifdf3f1ukl"
	appName := "m2605081521cctqpk"
	component := &model.ApplicationComponent{
		Name:          "m2605081521cctqpk-mysql-8",
		AppID:         appID,
		Namespace:     "paas-game-review",
		ComponentType: config.StoreJob,
		Image:         "mysql:8",
		Replicas:      1,
	}
	steps := model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "deploy-store",
			WorkflowType: config.JobDeploy,
			Properties:   []model.Policies{{Policies: []string{component.Name}}},
		}},
	}
	stepsJSON, err := model.NewJSONStructByStruct(steps)
	require.NoError(t, err)

	store := &fakeDataStore{
		workflow: &model.Workflow{ID: "wf-store", Steps: stepsJSON},
		application: &model.Applications{
			ID:              appID,
			Name:            appName,
			Version:         "8.0.41",
			TemplateEnabled: true,
		},
		components: []*model.ApplicationComponent{component},
	}
	task := &model.WorkflowQueue{
		WorkflowID:   "wf-store",
		AppID:        appID,
		ProjectID:    "proj-store",
		WorkflowName: "store-workflow",
		TaskID:       "task-store",
	}

	executions := mustGenerateJobTasks(t, context.Background(), task, store, int64(config.DefaultJobTaskTimeout))
	require.Len(t, executions, 1)
	jobs := executions[0].Jobs[config.JobPriorityNormal]
	require.Len(t, jobs, 1)
	require.Equal(t, appName, jobs[0].ResourceAppName)

	statefulSet, ok := jobs[0].JobInfo.(*appsv1.StatefulSet)
	require.True(t, ok)
	require.Equal(t, wfNaming.StoreServerName(component.Name, appName), statefulSet.Name)
	require.LessOrEqual(t, len(statefulSet.Name), 52)
}
