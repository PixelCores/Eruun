package api

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestListWorkflowSchedulesEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{
		scheduleListResp: []apis.WorkflowSchedule{
			{
				ID:         "sch-1",
				AppID:      "app-1",
				WorkflowID: "wf-1",
				Cron:       "*/5 * * * *",
				Enabled:    true,
			},
		},
	}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.GET("/applications/:appID/workflow/schedules", appHandler.listWorkflowSchedules)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/workflow/schedules", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	var payload apis.ListWorkflowSchedulesResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload.Schedules) != 1 || payload.Schedules[0].WorkflowID != "wf-1" {
		t.Fatalf("unexpected schedule list payload: %+v", payload.Schedules)
	}
	if svc.scheduleListApp != "app-1" {
		t.Fatalf("expected list workflow schedules to be invoked")
	}
}

func TestGetApplicationComponentStatusEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appID := "app-1"
	components := []*model.ApplicationComponent{
		{
			AppID:         appID,
			Name:          "mysql",
			Namespace:     "default",
			ComponentType: config.StoreJob,
			Status:        string(config.ComponentStatusRunning),
			Replicas:      2,
			ReadyReplicas: 1,
		},
		{
			AppID:         appID,
			Name:          "redis",
			Namespace:     "default",
			ComponentType: config.StoreJob,
			Status:        "",
			Replicas:      1,
			ReadyReplicas: 0,
		},
	}
	svc := componentListApplicationService{components: components}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components/status", appHandler.getApplicationComponentStatus)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/status", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload apis.ApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.AppID != "app-1" {
		t.Fatalf("unexpected appId: %s", payload.AppID)
	}
	if len(payload.Components) != 2 {
		t.Fatalf("expected 2 components, got %d", len(payload.Components))
	}
	if payload.Components[0].Name != "mysql" || payload.Components[0].Status != string(config.ComponentStatusRunning) {
		t.Fatalf("unexpected first component: %+v", payload.Components[0])
	}
	if payload.Components[1].Name != "redis" || payload.Components[1].Status != string(config.ComponentStatusNotDeploy) {
		t.Fatalf("unexpected second component: %+v", payload.Components[1])
	}
}

func TestGetApplicationComponentStatusEndpointKeepsRecentFailedStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appID := "app-1"
	components := []*model.ApplicationComponent{
		{
			AppID:         appID,
			Name:          "mysql",
			Namespace:     "default",
			ComponentType: config.StoreJob,
			Status:        string(config.ComponentStatusFailed),
			Replicas:      1,
			ReadyReplicas: 0,
			LastAbnormal:  "container=init-db reason=CrashLoopBackOff",
			BaseModel:     model.BaseModel{UpdateTime: time.Now()},
		},
	}
	svc := componentListApplicationService{components: components}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/components/status", appHandler.getApplicationComponentStatus)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/status", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Components, 1)
	require.Equal(t, string(config.ComponentStatusFailed), payload.Components[0].Status)
	require.Equal(t, "container=init-db reason=CrashLoopBackOff", payload.Components[0].LastAbnormal)
}

func TestWorkflowComponentStatusRouteIsNotRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	appHandler.RegisterRoutes(r.Group(""))

	req := httptest.NewRequest(http.MethodGet, "/workflow/app-1/components/status", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusNotFound, resp.Code)
}

func TestListApplicationWorkflowsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	steps := &model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:       "deploy-nginx",
				Mode:       config.WorkflowModeStepByStep,
				Properties: []model.Policies{{Policies: []string{"nginx"}}},
			},
		},
	}
	stepStruct, err := model.NewJSONStructByStruct(steps)
	if err != nil {
		t.Fatalf("build steps json: %v", err)
	}
	appSvc := workflowListApplicationService{
		workflows: []*model.Workflow{
			{
				ID:          "wf-1",
				Name:        "deploy",
				Alias:       "Deploy",
				Namespace:   "default",
				ProjectID:   "proj",
				Description: "desc",
				Status:      config.StatusRunning,
				Steps:       stepStruct,
			},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/workflows", appHandler.listApplicationWorkflows)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/workflows", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	var payload apis.ListApplicationWorkflowsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload.Workflows) != 1 {
		t.Fatalf("expected one workflow, got %d", len(payload.Workflows))
	}
	if payload.Workflows[0].ID != "wf-1" {
		t.Fatalf("unexpected workflow ID %s", payload.Workflows[0].ID)
	}
	if len(payload.Workflows[0].Steps) != 1 {
		t.Fatalf("expected one workflow step")
	}
	if len(payload.Workflows[0].Steps[0].Components) != 1 || payload.Workflows[0].Steps[0].Components[0] != "nginx" {
		t.Fatalf("unexpected workflow step components: %+v", payload.Workflows[0].Steps[0].Components)
	}
}

func TestNormalizeWorkflowStepsAppliesNodeRulesToStepsAndSubSteps(t *testing.T) {
	steps := []apis.CreateWorkflowStepRequest{
		{
			Name:       "Deploy-API",
			StepType:   config.WorkflowStepType(" Component "),
			Components: []string{" API ", "Worker"},
			Properties: apis.WorkflowProperties{
				Policies: []string{" Policy-A "},
			},
			SubSteps: []apis.CreateWorkflowSubStepRequest{
				{
					Name:       "Migrate-DB",
					Components: []string{" DB "},
					Properties: apis.WorkflowProperties{
						Policies: []string{" Cache "},
					},
				},
			},
		},
		{
			Name:     "Manual-Gate",
			StepType: config.WorkflowStepType(" Approval "),
			Approval: &apis.WorkflowStepApproval{
				NotifyURL: " https://example.com/approve ",
				Message:   " please approve ",
				Method:    " post ",
			},
		},
	}

	normalizeWorkflowSteps(steps)

	require.Equal(t, "deploy-api", steps[0].Name)
	require.Equal(t, config.WorkflowStepTypeComponent, steps[0].StepType)
	require.Equal(t, []string{"API", "Worker"}, steps[0].Components)
	require.Equal(t, []string{"Policy-A"}, steps[0].Properties.Policies)
	require.Nil(t, steps[0].Approval)
	require.Len(t, steps[0].SubSteps, 1)
	require.Equal(t, "migrate-db", steps[0].SubSteps[0].Name)
	require.Equal(t, []string{"DB"}, steps[0].SubSteps[0].Components)
	require.Equal(t, []string{"Cache"}, steps[0].SubSteps[0].Properties.Policies)

	require.Equal(t, "manual-gate", steps[1].Name)
	require.Equal(t, config.WorkflowStepTypeApproval, steps[1].StepType)
	require.Equal(t, "https://example.com/approve", steps[1].Approval.NotifyURL)
	require.Equal(t, "please approve", steps[1].Approval.Message)
	require.Equal(t, "POST", steps[1].Approval.Method)
}

func TestTryWorkflowAcceptsStepsAlias(t *testing.T) {
	gin.SetMode(gin.TestMode)
	validationSvc := &recordingValidationService{}
	appHandler := &applications{
		ValidationService: validationSvc,
	}
	r := gin.New()
	r.POST("/applications/:appID/workflow/try", appHandler.tryWorkflow)

	body := `{
		"workflowId": "wf-1",
		"name": "archive-flow",
		"workflowType": "log_archive_upload",
		"callback": {"success": "https://example.com/archive/success"},
		"steps": [
			{
				"name": "Archive-API",
				"workflowType": "log_archive_upload",
				"components": ["API"],
				"properties": [
					{"policies": ["API"], "path": "/var/log/api", "container": "api"}
				]
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/workflow/try", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	var payload apis.TryWorkflowResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.True(t, payload.Valid)
	require.True(t, validationSvc.called)
	require.Equal(t, "app-1", validationSvc.appID)
	require.Equal(t, "wf-1", validationSvc.workflow.WorkflowID)
	require.Equal(t, "archive-flow", validationSvc.workflow.Name)
	require.Equal(t, config.WorkflowTaskTypeLogArchiveUpload, validationSvc.workflow.WorkflowType)
	require.NotNil(t, validationSvc.workflow.Callback)
	require.Equal(t, "https://example.com/archive/success", validationSvc.workflow.Callback.Success)
	require.Len(t, validationSvc.workflow.Workflow, 1)
	require.Equal(t, "archive-api", validationSvc.workflow.Workflow[0].Name)
	require.Equal(t, config.JobLogArchiveUpload, validationSvc.workflow.Workflow[0].WorkflowType)
	require.Equal(t, []string{"API"}, validationSvc.workflow.Workflow[0].Components)
	require.True(t, validationSvc.workflow.Workflow[0].WorkflowPropertiesFromArray())
	require.Equal(t, []string{"API"}, validationSvc.workflow.Workflow[0].WorkflowPropertiesList()[0].Policies)
}

func TestTryWorkflowRejectsWorkflowStepsAliasConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	validationSvc := &recordingValidationService{}
	appHandler := &applications{
		ValidationService: validationSvc,
	}
	r := gin.New()
	r.POST("/applications/:appID/workflow/try", appHandler.tryWorkflow)

	body := `{
		"workflow": [{"name": "deploy-api"}],
		"steps": [{"name": "deploy-worker"}]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/workflow/try", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrWorkflowConfig.BusinessCode, envelope.Code)
	require.False(t, validationSvc.called)
}

func TestCreateApplicationsValidationErrorUsesApplicationConfigCode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
	}
	r := gin.New()
	r.POST("/applications", appHandler.createApplications)

	body := `{"name":"x"}`
	req := httptest.NewRequest(http.MethodPost, "/applications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrApplicationConfig.BusinessCode {
		t.Fatalf("unexpected response code: %d", envelope.Code)
	}
	if envelope.Message != bcode.ErrApplicationConfig.Message {
		t.Fatalf("unexpected response message: %s", envelope.Message)
	}
	if string(envelope.Data) != "null" {
		t.Fatalf("expected null data, got: %s", string(envelope.Data))
	}
}

func TestCreateApplicationsAcceptsStorageSubPathExpr(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{}
	appHandler := &applications{
		ApplicationService: appSvc,
	}
	r := gin.New()
	r.POST("/applications", appHandler.createApplications)

	body := `{
		"name":"demo-app",
		"namespace":"default",
		"component":[
			{
				"name":"backend",
				"type":"webservice",
				"image":"nginx:latest",
				"traits":{
					"storage":[
						{
							"name":"logs",
							"type":"persistent",
							"claimName":"developer-pvc",
							"mountPath":"/app/log",
							"subPathExpr":"$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)"
						}
					]
				}
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, 1, appSvc.createCalls)
	require.Len(t, appSvc.lastCreate.Component, 1)
	require.Len(t, appSvc.lastCreate.Component[0].Traits.Storage, 1)
	storage := appSvc.lastCreate.Component[0].Traits.Storage[0]
	require.Equal(t, "/app/log", storage.MountPath)
	require.Empty(t, storage.SubPath)
	require.Equal(t, "$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)", storage.SubPathExpr)
}

func TestCreateApplicationsRejectsLegacySecretMetaField(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
	}
	r := gin.New()
	r.POST("/applications", appHandler.createApplications)

	body := `{
		"name":"demo",
		"component":[
			{
				"name":"app-secret",
				"type":"secret",
				"properties":{"secret":{"password":"value"}},
				"traits":{},
				"secretMeta":{"valuesBase64Encoded":true}
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
}

func TestCreateApplicationsRejectsComponentPropertiesImageField(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
	}
	r := gin.New()
	r.POST("/applications", appHandler.createApplications)

	body := `{
		"name":"demo",
		"component":[
			{
				"name":"api",
				"type":"webservice",
				"image":"nginx:1.25",
				"properties":{"image":"","ports":[{"port":80}]},
				"traits":{}
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
}

func TestListApplicationTasksEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	appSvc := taskListApplicationService{
		tasks: []*model.WorkflowQueue{
			{
				TaskID:              "task-1",
				AppID:               "app-1",
				WorkflowID:          "wf-1",
				WorkflowName:        "deploy",
				WorkflowDisplayName: "Deploy",
				Status:              config.StatusRunning,
				Type:                config.WorkflowTaskTypeWorkflow,
				TaskCreator:         "tester",
				BaseModel: model.BaseModel{
					CreateTime: now,
					UpdateTime: now.Add(2 * time.Minute),
				},
			},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/workflow/tasks", appHandler.listApplicationTasks)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/workflow/tasks", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	var payload apis.ListApplicationTasksResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload.Tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(payload.Tasks))
	}
	task := payload.Tasks[0]
	if task.TaskID != "task-1" || task.WorkflowID != "wf-1" || task.Status != string(config.StatusRunning) {
		t.Fatalf("unexpected task payload: %+v", task)
	}
	if !task.CreateTime.Equal(now) || !task.UpdateTime.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("unexpected task timestamps: %+v", task)
	}
}

func TestListTemplateApplicationsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &templateApplicationService{
		templates: []*apis.ApplicationBase{
			{ID: "app-tmpl-1", Name: "tmpl-1", TemplateEnabled: true, Resources: apis.ApplicationResources{
				CPUReq:   "160m",
				MemReq:   "260Mi",
				CPULimit: "300m",
				MemLimit: "600Mi",
				Replicas: 1,
			}},
			{ID: "app-tmpl-2", Name: "tmpl-2", TemplateEnabled: true},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/templates", appHandler.listTemplateApplications)

	req := httptest.NewRequest(http.MethodGet, "/applications/templates", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload apis.ListApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload.Applications) != 2 {
		t.Fatalf("expected 2 templates, got %d", len(payload.Applications))
	}
	if payload.Applications[0].ID != "app-tmpl-1" {
		t.Fatalf("unexpected first template: %+v", payload.Applications[0])
	}
	require.Equal(t, "160m", payload.Applications[0].Resources.CPUReq)
	require.Equal(t, "260Mi", payload.Applications[0].Resources.MemReq)
	require.Equal(t, "300m", payload.Applications[0].Resources.CPULimit)
	require.Equal(t, "600Mi", payload.Applications[0].Resources.MemLimit)
	require.EqualValues(t, 1, payload.Applications[0].Resources.Replicas)
	require.Contains(t, resp.Body.String(), `"resources":{"cpuReq":"160m","memReq":"260Mi","cpuLimit":"300m","memLimit":"600Mi","replicas":1}`)
	require.Contains(t, resp.Body.String(), `"cpuReq":"160m"`)
	require.Contains(t, resp.Body.String(), `"memReq":"260Mi"`)
	require.Contains(t, resp.Body.String(), `"cpuLimit":"300m"`)
	require.Contains(t, resp.Body.String(), `"memLimit":"600Mi"`)
	require.Contains(t, resp.Body.String(), `"replicas":1`)
	require.NotContains(t, resp.Body.String(), `"templateEnabled":true,"cpuReq"`)
}

func TestListApplicationsEndpointPassesPaginationOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &listApplicationService{
		apps: []*apis.ApplicationBase{{ID: "app-1", Name: "demo-1"}},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications", appHandler.listApplications)

	req := httptest.NewRequest(http.MethodGet, "/applications?page=0&pageSize=2", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, 0, appSvc.lastOpts.Page)
	require.Equal(t, 2, appSvc.lastOpts.PageSize)
}

func TestQueryApplicationsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &batchGetApplicationService{
		resp: &apis.BatchGetApplicationsResponse{
			Applications: []*apis.ApplicationWithComponents{
				{
					ApplicationBase: apis.ApplicationBase{ID: "app-1", Name: "demo-1"},
					Components: []*apis.BatchApplicationComponent{
						{
							AppID:         "app-1",
							Name:          "api",
							ComponentType: config.ServerJob,
							Properties: apis.BatchApplicationComponentProperties{
								Ports: []spec.Ports{{Port: 8080}},
							},
						},
					},
				},
			},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/query", appHandler.batchGetApplications)

	req := httptest.NewRequest(http.MethodPost, "/applications/query", strings.NewReader(`{"appIds":["app-1","app-2"]}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, []string{"app-1", "app-2"}, appSvc.lastAppIDs)

	var payload apis.BatchGetApplicationsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Applications, 1)
	require.Equal(t, "app-1", payload.Applications[0].ID)
	require.Len(t, payload.Applications[0].Components, 1)
	require.Equal(t, "api", payload.Applications[0].Components[0].Name)
	require.Equal(t, []spec.Ports{{Port: 8080}}, payload.Applications[0].Components[0].Properties.Ports)
	require.Contains(t, resp.Body.String(), `"ports":[{"port":8080}]`)
	require.NotContains(t, resp.Body.String(), `"env"`)
	require.NotContains(t, resp.Body.String(), `"conf"`)
	require.NotContains(t, resp.Body.String(), `"secret"`)
	require.NotContains(t, resp.Body.String(), `"command"`)
	require.NotContains(t, resp.Body.String(), `"labels"`)
	require.NotContains(t, resp.Body.String(), "traits")
}

func TestListTemplateApplicationsEndpointPassesPaginationOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &templateApplicationService{
		templates: []*apis.ApplicationBase{{ID: "app-tmpl-1", Name: "tmpl-1", TemplateEnabled: true}},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/templates", appHandler.listTemplateApplications)

	req := httptest.NewRequest(http.MethodGet, "/applications/templates?page=1&pageSize=5", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, 1, appSvc.lastOpts.Page)
	require.Equal(t, 5, appSvc.lastOpts.PageSize)
}

func TestUpdateVersionEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{
		updateResp: &apis.UpdateVersionResponse{
			AppID:             "app-123",
			Version:           "2.0.0",
			PreviousVersion:   "1.0.0",
			Strategy:          "rolling",
			ExecutionScope:    "changed_components",
			TaskID:            "task-456",
			WorkflowID:        "wf-456",
			UpdatedComponents: []string{"backend", "frontend"},
			AddedComponents:   []string{"cache"},
			RemovedComponents: []string{"old-worker"},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	body := `{
		"version": "2.0.0",
		"strategy": "rolling",
		"executionScope": "changed_components",
		"components": [
			{"action": "update", "name": "backend", "image": "backend:v2"},
			{"action": "update", "name": "frontend", "image": "frontend:v2"},
			{"action": "add", "name": "cache", "type": "store", "image": "redis:7"},
			{"action": "remove", "name": "old-worker"}
		],
		"executeAt": 1735689600,
		"autoExec": true,
		"imageReadyTimeoutSeconds": 300,
		"description": "Major version update"
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d, body: %s", resp.Code, resp.Body.String())
	}

	var payload apis.UpdateVersionResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)

	if payload.AppID != "app-123" {
		t.Fatalf("unexpected appId: %s", payload.AppID)
	}
	if payload.Version != "2.0.0" {
		t.Fatalf("unexpected version: %s", payload.Version)
	}
	if payload.Strategy != "rolling" {
		t.Fatalf("unexpected strategy: %s", payload.Strategy)
	}
	if payload.ExecutionScope != "changed_components" {
		t.Fatalf("unexpected executionScope: %s", payload.ExecutionScope)
	}
	require.Equal(t, "changed_components", appSvc.lastReq.ExecutionScope)
	if payload.TaskID != "task-456" {
		t.Fatalf("unexpected taskId: %s", payload.TaskID)
	}
	if payload.WorkflowID != "wf-456" {
		t.Fatalf("unexpected workflowId: %s", payload.WorkflowID)
	}
	require.NotContains(t, resp.Body.String(), "workflow_id")
	if len(payload.UpdatedComponents) != 2 {
		t.Fatalf("expected 2 updated components, got %d", len(payload.UpdatedComponents))
	}
	if len(payload.AddedComponents) != 1 {
		t.Fatalf("expected 1 added component, got %d", len(payload.AddedComponents))
	}
	if len(payload.RemovedComponents) != 1 {
		t.Fatalf("expected 1 removed component, got %d", len(payload.RemovedComponents))
	}

	// 验证请求参数
	if appSvc.lastAppID != "app-123" {
		t.Fatalf("expected appID app-123, got %s", appSvc.lastAppID)
	}
	if appSvc.lastReq.Version != "2.0.0" {
		t.Fatalf("expected version 2.0.0, got %s", appSvc.lastReq.Version)
	}
	if len(appSvc.lastReq.Components) != 4 {
		t.Fatalf("expected 4 components in request, got %d", len(appSvc.lastReq.Components))
	}
	if appSvc.lastReq.ExecuteAt != 1735689600 {
		t.Fatalf("expected executeAt 1735689600, got %d", appSvc.lastReq.ExecuteAt)
	}
	if appSvc.lastReq.ImageReadyTimeoutSeconds != 300 {
		t.Fatalf("expected imageReadyTimeoutSeconds 300, got %d", appSvc.lastReq.ImageReadyTimeoutSeconds)
	}
}

func TestUpdateVersionEndpointMinimalRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	// 最简请求 - 仅更新版本号
	body := `{"version": "1.1.0"}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d, body: %s", resp.Code, resp.Body.String())
	}

	if appSvc.lastReq.Version != "1.1.0" {
		t.Fatalf("expected version 1.1.0, got %s", appSvc.lastReq.Version)
	}
}

func TestUpdateVersionEndpointAcceptsCallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	body := `{
		"version": "1.1.0",
		"components": [
			{"name": "backend", "image": "backend:v1.1"}
		],
		"callback": {
			"success": "https://example.com/version/success",
			"failure": "https://example.com/version/failure",
			"methods": {"success": "POST"},
			"headers": {"X-Source": "eruun"},
			"timeoutSeconds": 30
		}
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.NotNil(t, appSvc.lastReq.Callback)
	require.Equal(t, "https://example.com/version/success", appSvc.lastReq.Callback.Success)
	require.Equal(t, "https://example.com/version/failure", appSvc.lastReq.Callback.Failure)
	require.Equal(t, "POST", appSvc.lastReq.Callback.Methods["success"])
	require.Equal(t, "eruun", appSvc.lastReq.Callback.Headers["X-Source"])
	require.Equal(t, int64(30), appSvc.lastReq.Callback.TimeoutSeconds)
}

func TestUpdateVersionEndpointReturnsInvalidComponentAction(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{updateErr: bcode.ErrInvalidComponentAction}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	body := `{
		"version": "1.1.0",
		"components": [
			{"action": "remvoe", "name": "api"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrInvalidComponentAction.BusinessCode, envelope.Code)
}

func TestUpdateVersionEndpointPreservesWorkflowConflictCodes(t *testing.T) {
	tests := []struct {
		name string
		err  *bcode.Bcode
	}{
		{name: "running", err: bcode.ErrWorkflowTaskRunning},
		{name: "cancelling", err: bcode.ErrWorkflowTaskCancelling},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			appHandler := &applications{
				ApplicationService: &fakeUpdateVersionService{updateErr: tt.err},
				WorkflowService:    &fakeWorkflowService{},
			}
			r := gin.New()
			r.POST("/applications/:appID/version", appHandler.updateVersion)
			req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(`{"version":"2.0.0"}`))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			r.ServeHTTP(resp, req)

			require.Equal(t, http.StatusConflict, resp.Code)
			envelope := decodeResponse(t, resp.Body.Bytes(), nil)
			require.Equal(t, tt.err.BusinessCode, envelope.Code)
			require.Equal(t, tt.err.Message, envelope.Message)
		})
	}
}

func TestUpdateVersionEndpointReturnsComponentAlreadyExists(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{updateErr: bcode.ErrComponentAlreadyExists}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	body := `{
		"version": "1.1.0",
		"components": [
			{"action": "add", "name": "api", "type": "webservice", "image": "api:v2"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrComponentAlreadyExists.BusinessCode, envelope.Code)
}

func TestUpdateVersionEndpointReturnsSafeStatefulSetMigrationGuidance(t *testing.T) {
	internalErr := fmt.Errorf("%w: component mysql changes StatefulSet immutable field serviceName from internal-old to internal-new", bcode.ErrApplicationConfig)
	publicMessage := "component mysql changes StatefulSet immutable field serviceName; explicit StatefulSet/PVC migration or recreation is required"
	appSvc := &fakeUpdateVersionService{
		updateErr: bcode.WithSafeClientMessage(internalErr, publicMessage),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(`{"version":"2.0.0"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
	require.Equal(t, publicMessage, envelope.Message)
	require.NotContains(t, resp.Body.String(), "internal-old")
	require.NotContains(t, resp.Body.String(), "internal-new")
}

func TestUpdateVersionEndpointRejectsLegacySecretMetaField(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	body := `{
		"version":"1.1.0",
		"components":[
			{
				"action":"add",
				"name":"app-secret",
				"type":"secret",
				"properties":{"secret":{"password":"value"}},
				"traits":{},
				"secretMeta":{"valuesBase64Encoded":true}
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
	require.Empty(t, appSvc.lastReq.Version)
}

func TestUpdateVersionEndpointMissingAppID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	body := `{"version": "1.1.0"}`
	// 空的 appID
	req := httptest.NewRequest(http.MethodPost, "/applications//version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationNotExist.BusinessCode, envelope.Code)
}

func TestUpdateVersionEndpointImageUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{
		updateResp: &apis.UpdateVersionResponse{
			AppID:             "app-1",
			Version:           "1.1.0",
			PreviousVersion:   "1.0.0",
			Strategy:          "rolling",
			UpdatedComponents: []string{"backend"},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version", appHandler.updateVersion)

	body := `{
		"version": "1.1.0",
		"components": [
			{"name": "backend", "image": "myapp/backend:v1.1.0"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	// 验证组件名称被规范化为小写
	if len(appSvc.lastReq.Components) != 1 {
		t.Fatalf("expected 1 component, got %d", len(appSvc.lastReq.Components))
	}
	if appSvc.lastReq.Components[0].Image != "myapp/backend:v1.1.0" {
		t.Fatalf("unexpected image: %s", appSvc.lastReq.Components[0].Image)
	}
}

func TestDiffUpdateVersionEndpointDryRun(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{
		diffResp: &apis.DiffUpdateVersionResponse{
			TargetAppID:           "target-app",
			SourceAppID:           "source-app",
			TargetPreviousVersion: "1.0.0",
			TargetVersion:         "1.0.1",
			SourceVersion:         "1.0.1",
			DryRun:                true,
			TargetOnlyStrategy:    apis.DiffUpdateTargetOnlyStrategyRemove,
			VersionChanged:        true,
			HasChanges:            true,
			Executable:            true,
			UpdatedComponents: []apis.VersionComponentDiff{{
				Action: "update",
				Name:   "backend",
				Type:   config.ServerJob,
				Fields: []apis.VersionComponentField{{
					Field:  "image",
					Before: "backend:v1.0.0",
					After:  "backend:v1.0.1",
				}},
			}},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version/diff-update", appHandler.diffUpdateVersion)

	body := `{
		"sourceAppId": "source-app",
		"dryRun": true,
		"targetOnlyStrategy": "remove",
		"strategy": "rolling",
		"executionScope": "changed_components",
		"autoExec": false,
		"description": "sync from source"
	}`
	req := httptest.NewRequest(http.MethodPost, "/applications/target-app/version/diff-update", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.DiffUpdateVersionResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "target-app", payload.TargetAppID)
	require.Equal(t, "source-app", payload.SourceAppID)
	require.True(t, payload.DryRun)
	require.Equal(t, apis.DiffUpdateTargetOnlyStrategyRemove, payload.TargetOnlyStrategy)
	require.Len(t, payload.UpdatedComponents, 1)
	require.Equal(t, "target-app", appSvc.lastDiffID)
	require.Equal(t, "source-app", appSvc.lastDiffReq.SourceAppID)
	require.True(t, appSvc.lastDiffReq.DryRun)
	require.Equal(t, apis.DiffUpdateTargetOnlyStrategyRemove, appSvc.lastDiffReq.TargetOnlyStrategy)
	require.Equal(t, "changed_components", appSvc.lastDiffReq.ExecutionScope)
	require.NotNil(t, appSvc.lastDiffReq.AutoExec)
	require.False(t, *appSvc.lastDiffReq.AutoExec)
}

func TestDiffUpdateVersionEndpointRejectsMissingSourceAppID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeUpdateVersionService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/version/diff-update", appHandler.diffUpdateVersion)

	req := httptest.NewRequest(http.MethodPost, "/applications/target-app/version/diff-update", strings.NewReader(`{"dryRun":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
	require.Empty(t, appSvc.lastDiffID)
}

func TestCancelDelayedVersionUpdateEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	wfSvc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: &fakeUpdateVersionService{},
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/:appID/version/cancel", appHandler.cancelDelayedVersionUpdate)

	body := `{"taskId":"task-delay-1","reason":"manual cancel"}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version/cancel", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.True(t, wfSvc.cancelDelayedCalled)
	require.Equal(t, "app-1", wfSvc.lastCancelAppID)
	require.Equal(t, "task-delay-1", wfSvc.lastCancelTaskID)
	require.Equal(t, "manual cancel", wfSvc.lastCancelReason)
	require.Equal(t, config.DefaultTaskRevoker, wfSvc.lastCancelUser)

	var payload apis.CancelWorkflowResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "task-delay-1", payload.TaskID)
	require.Equal(t, string(config.StatusCancelled), payload.Status)
}

func TestCancelDelayedVersionUpdateEndpointInvalidRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	wfSvc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: &fakeUpdateVersionService{},
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/:appID/version/cancel", appHandler.cancelDelayedVersionUpdate)

	body := `{"reason":"missing task id"}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version/cancel", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	require.False(t, wfSvc.cancelDelayedCalled)
}

func TestCancelDelayedVersionUpdateEndpointNotCancellable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	wfSvc := &fakeWorkflowService{cancelDelayedErr: bcode.ErrVersionUpdateTaskNotCancellable}
	appHandler := &applications{
		ApplicationService: &fakeUpdateVersionService{},
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/:appID/version/cancel", appHandler.cancelDelayedVersionUpdate)

	body := `{"taskId":"task-done","reason":"manual cancel"}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/version/cancel", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusConflict, resp.Code)
	require.True(t, wfSvc.cancelDelayedCalled)

	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, int32(bcode.ErrVersionUpdateTaskNotCancellable.BusinessCode), envelope.Code)
}
