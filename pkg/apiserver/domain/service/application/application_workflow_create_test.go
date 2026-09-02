package application

import (
	"context"

	"errors"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"

	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestCreateApplications_UpdatePreservesWorkflowID(t *testing.T) {
	store := newInMemoryAppStore()
	app := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Project:   "proj-1",
	}
	store.apps[app.ID] = app

	previousSteps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "old",
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
		}},
	})
	require.NoError(t, err)

	workflow := &model.Workflow{
		ID:           "wf-1",
		Name:         "demo-old",
		Alias:        "demo-workflow",
		Namespace:    app.Namespace,
		AppID:        app.ID,
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps:        previousSteps,
	}
	store.workflows[workflow.ID] = workflow
	existingUpdateWorkflow := &model.Workflow{
		ID:           "wf-update",
		Name:         "demo-update-old",
		Alias:        "old-update",
		Namespace:    app.Namespace,
		AppID:        app.ID,
		WorkflowType: config.WorkflowTaskTypeUpdate,
		Steps:        previousSteps,
	}
	store.workflows[existingUpdateWorkflow.ID] = existingUpdateWorkflow

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		ID:        app.ID,
		Name:      "demo",
		Namespace: app.Namespace,
		Version:   "2.0.0",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "c1",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "step1",
			WorkflowType: config.JobDeploy,
			Components:   []string{"c1"},
			Mode:         string(config.WorkflowModeStepByStep),
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, workflow.ID, resp.WorkflowID)
	require.Len(t, store.workflows, 2)

	updated := store.workflows[workflow.ID]
	require.NotNil(t, updated)
	require.Equal(t, "demo-workflow", updated.Name)

	decoded := decodeWorkflowSteps(t, updated.Steps)
	require.Len(t, decoded.Steps, 1)
	require.Equal(t, "step1", decoded.Steps[0].Name)

	var updateWorkflow *model.Workflow
	for _, wf := range store.workflows {
		if wf != nil && wf.WorkflowType == config.WorkflowTaskTypeUpdate {
			updateWorkflow = wf
			break
		}
	}
	require.NotNil(t, updateWorkflow)
	require.Equal(t, existingUpdateWorkflow.ID, updateWorkflow.ID)
	require.Equal(t, "demo-update-workflow", updateWorkflow.Name)
	require.Equal(t, "app-1-update-workflow", updateWorkflow.Alias)
	require.Equal(t, config.WorkflowTaskTypeUpdate, updateWorkflow.WorkflowType)
}

func TestCreateApplicationsRejectsLogArchiveWorkflowForNonPodComponent(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Version:   "1.0.0",
		Project:   "proj-1",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "config",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"app.yaml": "debug: true"},
			},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "archive-config",
			WorkflowType: config.JobLogArchiveUpload,
			Components:   []string{"config"},
			Mode:         string(config.WorkflowModeStepByStep),
			Properties: apisv1.WorkflowProperties{
				Path: "/var/log/api",
			},
		}},
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.True(t, errors.Is(err, bcode.ErrWorkflowConfig))
	require.Contains(t, err.Error(), "does not use pods")
}

func TestCreateApplicationsDefaultWorkflowsUseAppCallback(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-callback",
		Namespace: config.DefaultNamespace,
		Callback: &apisv1.WorkflowCallback{
			Success: "https://example.com/app-success",
		},
		Component: []apisv1.CreateComponentRequest{{
			Name:          "web",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	storedApp := store.apps[resp.ID]
	require.NotNil(t, storedApp)
	requireWorkflowCallbackSuccess(t, storedApp.Callback, "https://example.com/app-success")
	require.Len(t, store.workflows, 2)
	for _, workflow := range store.workflows {
		requireWorkflowCallbackSuccess(t, workflow.Callback, "https://example.com/app-success")
	}
}

func TestCreateApplicationsCustomWorkflowCallbackOverridesAppCallback(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-workflow-callback",
		Namespace: config.DefaultNamespace,
		Callback: &apisv1.WorkflowCallback{
			Success: "ftp://ignored.example.com/app",
		},
		WorkflowCallback: &apisv1.WorkflowCallback{
			Success: "https://example.com/workflow-success",
		},
		Component: []apisv1.CreateComponentRequest{{
			Name:          "web",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
			Mode:         string(config.WorkflowModeStepByStep),
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	storedApp := store.apps[resp.ID]
	require.NotNil(t, storedApp)
	requireWorkflowCallbackSuccess(t, storedApp.Callback, "https://example.com/workflow-success")
	require.Len(t, store.workflows, 2)
	for _, workflow := range store.workflows {
		requireWorkflowCallbackSuccess(t, workflow.Callback, "https://example.com/workflow-success")
	}
}

func TestCreateApplicationsStoresAndEchoesWorkflowFailurePolicy(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name:                  "demo-failure-policy",
		Namespace:             config.DefaultNamespace,
		WorkflowFailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupAll,
		Component: []apisv1.CreateComponentRequest{{
			Name:          "web",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
			Mode:         string(config.WorkflowModeStepByStep),
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	defaultWorkflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, defaultWorkflow)
	steps := decodeWorkflowSteps(t, defaultWorkflow.Steps)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, steps.FailurePolicy)

	dto, err := assembler.ConvertWorkflowModelToDTO(defaultWorkflow)
	require.NoError(t, err)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, dto.FailurePolicy)
}

func TestCreateApplicationsDefaultsWorkflowFailurePolicyToCleanupAll(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo-default-failure-policy",
		Namespace: config.DefaultNamespace,
		Component: []apisv1.CreateComponentRequest{{
			Name:          "web",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
			Mode:         string(config.WorkflowModeStepByStep),
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	defaultWorkflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, defaultWorkflow)
	steps := decodeWorkflowSteps(t, defaultWorkflow.Steps)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, steps.FailurePolicy)

	dto, err := assembler.ConvertWorkflowModelToDTO(defaultWorkflow)
	require.NoError(t, err)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupAll, dto.FailurePolicy)
}

func TestCreateApplicationsUpdateAppCallbackOverwritesAllWorkflowCallbacks(t *testing.T) {
	store := newInMemoryAppStore()
	app := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Project:   "proj-1",
	}
	store.apps[app.ID] = app

	oldCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: "https://example.com/old"})
	require.NoError(t, err)
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "old",
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
		}},
	})
	require.NoError(t, err)
	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		Name:         "demo-workflow",
		Alias:        "demo-workflow",
		AppID:        app.ID,
		Namespace:    app.Namespace,
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps:        steps,
		Callback:     oldCallback,
	}
	store.workflows["wf-custom"] = &model.Workflow{
		ID:           "wf-custom",
		Name:         "custom-workflow",
		AppID:        app.ID,
		Namespace:    app.Namespace,
		WorkflowType: config.WorkflowTaskTypeTesting,
		Steps:        steps,
		Callback:     oldCallback,
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		ID:        app.ID,
		Name:      app.Name,
		Namespace: app.Namespace,
		Version:   "2.0.0",
		Callback: &apisv1.WorkflowCallback{
			Success: "https://example.com/new-success",
		},
		WorkflowCallback: &apisv1.WorkflowCallback{
			Success: "https://example.com/ignored-workflow",
		},
		Component: []apisv1.CreateComponentRequest{{
			Name:          "web",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
			Mode:         string(config.WorkflowModeStepByStep),
		}},
	}

	_, err = svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)

	requireWorkflowCallbackSuccess(t, store.apps[app.ID].Callback, "https://example.com/new-success")
	for _, workflow := range store.workflows {
		requireWorkflowCallbackSuccess(t, workflow.Callback, "https://example.com/new-success")
	}
}

func TestCreateApplicationsUpdateEmptyAppCallbackClearsSQLStyleCallbacks(t *testing.T) {
	store := newInMemoryAppStore()
	store.skipNilCallbackOnPut = true
	store.errExistingApplicationAdd = true
	app := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Project:   "proj-1",
	}
	oldCallback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Success: "https://example.com/old"})
	require.NoError(t, err)
	app.Callback = oldCallback
	store.apps[app.ID] = app

	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{
			Name:         "old",
			WorkflowType: config.JobDeploy,
			Mode:         config.WorkflowModeStepByStep,
		}},
	})
	require.NoError(t, err)
	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		Name:         "demo-workflow",
		Alias:        "demo-workflow",
		AppID:        app.ID,
		Namespace:    app.Namespace,
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps:        steps,
		Callback:     oldCallback,
	}
	store.workflows["wf-custom"] = &model.Workflow{
		ID:           "wf-custom",
		Name:         "custom-workflow",
		AppID:        app.ID,
		Namespace:    app.Namespace,
		WorkflowType: config.WorkflowTaskTypeTesting,
		Steps:        steps,
		Callback:     oldCallback,
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		ID:        app.ID,
		Name:      app.Name,
		Namespace: app.Namespace,
		Version:   "2.0.0",
		Callback:  &apisv1.WorkflowCallback{},
		Component: []apisv1.CreateComponentRequest{{
			Name:          "web",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{"web"},
			Mode:         string(config.WorkflowModeStepByStep),
		}},
	}

	_, err = svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)

	require.Nil(t, store.apps[app.ID].Callback)
	for _, workflow := range store.workflows {
		require.Nil(t, workflow.Callback)
	}
}

func TestCreateApplicationsUpdateBlockedWhenWorkflowTaskActive(t *testing.T) {
	store := newInMemoryAppStore()
	app := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Project:   "proj-1",
	}
	store.apps[app.ID] = app
	store.tasks["task-active"] = &model.WorkflowQueue{
		TaskID:          "task-active",
		AppID:           app.ID,
		Status:          config.StatusWaitingApprove,
		ApprovalPending: true,
	}
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		ID:        app.ID,
		Name:      app.Name,
		Namespace: app.Namespace,
		Version:   "2.0.0",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "c1",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
	}

	_, err := svc.CreateApplications(context.Background(), req)
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
}

func TestCreateApplicationsCreatesUpdateWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Version:   "1.0.0",
		Project:   "proj-1",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "c1",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.WorkflowID)
	require.Len(t, store.workflows, 2)

	var appID string
	for id := range store.apps {
		appID = id
		break
	}
	require.NotEmpty(t, appID)

	var updateWorkflow *model.Workflow
	for _, wf := range store.workflows {
		if wf != nil && wf.WorkflowType == config.WorkflowTaskTypeUpdate {
			updateWorkflow = wf
			break
		}
	}
	require.NotNil(t, updateWorkflow)
	require.Equal(t, "demo-update-workflow", updateWorkflow.Name)
	require.Equal(t, appID+"-update-workflow", updateWorkflow.Alias)
}

func TestCreateApplicationsRejectsInvalidApprovalWorkflowStep(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Version:   "1.0.0",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "backend",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
			Properties:    apisv1.Properties{},
			Traits:        apisv1.Traits{},
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:       "manual-check",
			StepType:   config.WorkflowStepTypeApproval,
			Mode:       string(config.WorkflowModeStepByStep),
			Components: []string{"backend"},
			Approval: &apisv1.WorkflowStepApproval{
				NotifyURL: "ftp://example.com/approval",
				Method:    "PATCH",
			},
		}},
	}

	_, err := svc.CreateApplications(context.Background(), req)
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
}

func TestCreateApplications_TemplateWithoutWorkflow_GeneratesPhasedDefaultWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-phase", Name: "tmpl-phase", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	store.components["tpl-config"] = &model.ApplicationComponent{
		Name:          "tpl-config",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ConfJob,
	}
	store.components["tpl-secret"] = &model.ApplicationComponent{
		Name:          "tpl-secret",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
	}
	store.components["tpl-store"] = &model.ApplicationComponent{
		Name:          "tpl-store",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:8.0",
		ComponentType: config.StoreJob,
	}
	store.components["tpl-job"] = &model.ApplicationComponent{
		Name:          "tpl-job",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "busybox:1.36",
		ComponentType: config.InstantJob,
	}
	store.components["tpl-web"] = &model.ApplicationComponent{
		Name:          "tpl-web",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:1.25",
		ComponentType: config.ServerJob,
	}
	store.components["tpl-cron"] = &model.ApplicationComponent{
		Name:          "tpl-cron",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "busybox:1.36",
		ComponentType: config.ScheduledJob,
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "phase-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "cfg", ComponentType: config.ConfJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tpl-config"}},
			{Name: "sec", ComponentType: config.SecretJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tpl-secret"}},
			{Name: "db", ComponentType: config.StoreJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tpl-store"}},
			{Name: "runner", ComponentType: config.InstantJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tpl-job"}},
			{Name: "init-sql", ComponentType: config.InstantJob, Image: "busybox:1.36"},
			{Name: "api", ComponentType: config.ServerJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tpl-web"}},
			{Name: "nightly", ComponentType: config.ScheduledJob, Template: &apisv1.TemplateRef{ID: templateApp.ID, Target: "tpl-cron"}},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	defaultWorkflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, defaultWorkflow)
	defaultSteps := decodeWorkflowSteps(t, defaultWorkflow.Steps)
	require.Len(t, defaultSteps.Steps, 4)
	require.Equal(t, "phase-2-config-secret", defaultSteps.Steps[0].Name)
	require.Equal(t, "phase-3-store", defaultSteps.Steps[1].Name)
	require.Equal(t, "phase-4-job", defaultSteps.Steps[2].Name)
	require.Equal(t, "phase-5-webservice", defaultSteps.Steps[3].Name)
	require.ElementsMatch(t, []string{"cfg", "sec"}, defaultSteps.Steps[0].ComponentNames())
	require.ElementsMatch(t, []string{"db"}, defaultSteps.Steps[1].ComponentNames())
	require.ElementsMatch(t, []string{"runner", "init-sql"}, defaultSteps.Steps[2].ComponentNames())
	require.ElementsMatch(t, []string{"api", "nightly"}, defaultSteps.Steps[3].ComponentNames())
	for _, step := range defaultSteps.Steps {
		require.Equal(t, config.WorkflowModeDAG, step.Mode)
	}

	var updateWorkflow *model.Workflow
	for _, wf := range store.workflows {
		if wf != nil && wf.WorkflowType == config.WorkflowTaskTypeUpdate {
			updateWorkflow = wf
			break
		}
	}
	require.NotNil(t, updateWorkflow)

	updateSteps := decodeWorkflowSteps(t, updateWorkflow.Steps)
	require.Len(t, updateSteps.Steps, 4)
	require.Equal(t, "phase-2-config-secret", updateSteps.Steps[0].Name)
	require.Equal(t, "phase-3-store", updateSteps.Steps[1].Name)
	require.Equal(t, "phase-4-job", updateSteps.Steps[2].Name)
	require.Equal(t, "phase-5-webservice", updateSteps.Steps[3].Name)
	require.ElementsMatch(t, []string{"cfg", "sec"}, updateSteps.Steps[0].ComponentNames())
	require.ElementsMatch(t, []string{"db"}, updateSteps.Steps[1].ComponentNames())
	require.ElementsMatch(t, []string{"runner", "init-sql"}, updateSteps.Steps[2].ComponentNames())
	require.ElementsMatch(t, []string{"api", "nightly"}, updateSteps.Steps[3].ComponentNames())
	for _, step := range updateSteps.Steps {
		require.Equal(t, config.WorkflowModeDAG, step.Mode)
	}
}

func TestCreateApplications_TemplateWithExplicitWorkflow_DoesNotRewrite(t *testing.T) {
	store := newInMemoryAppStore()
	templateApp := &model.Applications{ID: "tmpl-explicit", Name: "tmpl-explicit", TemplateEnabled: true}
	store.apps[templateApp.ID] = templateApp

	store.components["tpl-store"] = &model.ApplicationComponent{
		Name:          "tpl-store",
		AppID:         templateApp.ID,
		Namespace:     config.DefaultNamespace,
		Image:         "mysql:8.0",
		ComponentType: config.StoreJob,
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "explicit-app",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "tenant-store",
				ComponentType: config.StoreJob,
				Template:      &apisv1.TemplateRef{ID: templateApp.ID, Target: "tpl-store"},
			},
		},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{
			{
				Name:         "custom-step",
				WorkflowType: config.JobDeploy,
				Mode:         string(config.WorkflowModeStepByStep),
				Components:   []string{"tenant-store"},
			},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	defaultWorkflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, defaultWorkflow)
	steps := decodeWorkflowSteps(t, defaultWorkflow.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, "custom-step", steps.Steps[0].Name)
	require.Equal(t, config.WorkflowModeStepByStep, steps.Steps[0].Mode)
	require.ElementsMatch(t, []string{"tenant-store"}, steps.Steps[0].ComponentNames())
}

func TestCreateApplicationsPreservesWorkflowComponentReferenceCasing(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "case-app",
		Namespace: config.DefaultNamespace,
		Version:   "1.0.0",
		Project:   "proj-1",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "Web",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      1,
		}},
		WorkflowSteps: []apisv1.CreateWorkflowStepRequest{{
			Name:         "deploy-web",
			WorkflowType: config.JobDeploy,
			Components:   []string{" Web ", "WEB"},
			Properties: apisv1.WorkflowProperties{
				Policies: []string{" web "},
			},
			Mode: string(config.WorkflowModeStepByStep),
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	defaultWorkflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, defaultWorkflow)
	steps := decodeWorkflowSteps(t, defaultWorkflow.Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, []model.Policies{{Policies: []string{"Web"}}}, steps.Steps[0].Properties)
}

func TestCreateApplications_NonTemplateWithoutWorkflow_GeneratesPhasedDefaultWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name: "phased-app",
		Component: []apisv1.CreateComponentRequest{
			{Name: "config-a", ComponentType: config.ConfJob},
			{Name: "init-sql", ComponentType: config.InstantJob, Image: "busybox:1.36"},
			{Name: "store-a", ComponentType: config.StoreJob, Image: "mysql:8.0"},
			{Name: "api-a", ComponentType: config.ServerJob, Image: "nginx:1.25"},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	defaultWorkflow := store.workflows[resp.WorkflowID]
	require.NotNil(t, defaultWorkflow)
	steps := decodeWorkflowSteps(t, defaultWorkflow.Steps)
	require.Len(t, steps.Steps, 4)
	require.Equal(t, "phase-2-config-secret", steps.Steps[0].Name)
	require.Equal(t, "phase-3-store", steps.Steps[1].Name)
	require.Equal(t, "phase-4-job", steps.Steps[2].Name)
	require.Equal(t, "phase-5-webservice", steps.Steps[3].Name)
	require.ElementsMatch(t, []string{"config-a"}, steps.Steps[0].ComponentNames())
	require.ElementsMatch(t, []string{"store-a"}, steps.Steps[1].ComponentNames())
	require.ElementsMatch(t, []string{"init-sql"}, steps.Steps[2].ComponentNames())
	require.ElementsMatch(t, []string{"api-a"}, steps.Steps[3].ComponentNames())
	for _, step := range steps.Steps {
		require.Equal(t, config.WorkflowModeDAG, step.Mode)
		require.Equal(t, config.JobDeploy, step.WorkflowType)
	}
}
