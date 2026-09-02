package api

import (
	"context"

	"errors"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"

	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	containerutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/container"
)

func TestCancelApplicationWorkflowEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.POST("/applications/:appID/workflow/cancel", appHandler.cancelApplicationWorkflow)

	body := `{"taskId":"demo-task","user":"tester","reason":"manual stop"}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-2/workflow/cancel", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload apis.CancelWorkflowResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)

	if payload.TaskID != "demo-task" {
		t.Fatalf("expected taskId demo-task, got %s", payload.TaskID)
	}
	if payload.Status != string(config.StatusCancelled) {
		t.Fatalf("expected status cancelled, got %s", payload.Status)
	}
	if !svc.cancelForAppCalled || svc.lastCancelAppID != "app-2" || svc.lastCancelUser != "tester" || svc.lastCancelTaskID != "demo-task" {
		t.Fatalf("expected cancel for app to be invoked")
	}
	if svc.lastCancelReason != "manual stop" {
		t.Fatalf("unexpected cancel reason: %s", svc.lastCancelReason)
	}
}

func TestCancelApplicationWorkflowEndpointReturnsConflictCodes(t *testing.T) {
	tests := []struct {
		name string
		err  *bcode.Bcode
	}{
		{name: "terminal status", err: bcode.ErrWorkflowTaskNotCancellable},
		{name: "active state contention", err: bcode.ErrWorkflowTaskCancelConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			svc := &fakeWorkflowService{cancelForAppErr: tt.err}
			appHandler := &applications{
				ApplicationService: noopApplicationsService{},
				WorkflowService:    svc,
			}
			r := gin.New()
			r.POST("/applications/:appID/workflow/cancel", appHandler.cancelApplicationWorkflow)

			body := `{"taskId":"demo-task","user":"tester","reason":"manual stop"}`
			req := httptest.NewRequest(http.MethodPost, "/applications/app-2/workflow/cancel", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			r.ServeHTTP(resp, req)

			require.Equal(t, http.StatusConflict, resp.Code)
			envelope := decodeResponse(t, resp.Body.Bytes(), nil)
			require.Equal(t, tt.err.BusinessCode, envelope.Code)
		})
	}
}

func TestCancelAllApplicationWorkflowsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{cancelAllResp: []string{"task-1", "task-2"}}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.POST("/applications/:appID/workflow/tasks/cancel-all", appHandler.cancelAllApplicationWorkflows)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-2/workflow/tasks/cancel-all", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.CancelAllApplicationWorkflowsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "app-2", payload.AppID)
	require.Equal(t, []string{"task-1", "task-2"}, payload.CancelledTaskIDs)
	require.True(t, svc.cancelAllCalled)
	require.Equal(t, "app-2", svc.lastCancelAppID)
	require.Equal(t, config.DefaultTaskRevoker, svc.lastCancelUser)
	require.Empty(t, svc.lastCancelReason)
}

func TestCancelAllApplicationWorkflowsEndpointNoTasks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.POST("/applications/:appID/workflow/tasks/cancel-all", appHandler.cancelAllApplicationWorkflows)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-empty/workflow/tasks/cancel-all", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.CancelAllApplicationWorkflowsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "app-empty", payload.AppID)
	require.Empty(t, payload.CancelledTaskIDs)
	require.NotNil(t, payload.CancelledTaskIDs)
	require.True(t, svc.cancelAllCalled)
}

func TestApproveWorkflowTaskEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{
		approveResp: &apis.TaskApprovalResponse{
			TaskID: "task-approve-1",
			Action: "continue",
			Status: string(config.StatusWaiting),
		},
	}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.POST("/workflow/tasks/:taskID/approval", appHandler.approveWorkflowTask)

	body := `{"action":"continue","user":"approver","reason":"looks good"}`
	req := httptest.NewRequest(http.MethodPost, "/workflow/tasks/task-approve-1/approval", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.TaskApprovalResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "task-approve-1", payload.TaskID)
	require.Equal(t, "continue", payload.Action)
	require.Equal(t, string(config.StatusWaiting), payload.Status)
	require.True(t, svc.approveCalled)
	require.Equal(t, "task-approve-1", svc.lastApprovalTaskID)
	require.Equal(t, "continue", svc.lastApprovalAction)
	require.Equal(t, "approver", svc.lastCancelUser)
	require.Equal(t, "looks good", svc.lastCancelReason)
}

func TestApproveWorkflowTaskEndpointRejectsInvalidAction(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.POST("/workflow/tasks/:taskID/approval", appHandler.approveWorkflowTask)

	body := `{"action":"invalid"}`
	req := httptest.NewRequest(http.MethodPost, "/workflow/tasks/task-approve-2/approval", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.NotEqual(t, http.StatusOK, resp.Code)
	require.False(t, svc.approveCalled)
}

func TestGetWorkflowTaskStatusEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{
		taskStatusResp: &apis.TaskStatusResponse{
			TaskID:       "task-abc",
			Status:       string(config.StatusQueued),
			WorkflowID:   "wf-1",
			WorkflowName: "deploy",
			AppID:        "app-1",
			Components: []apis.ComponentTaskStatus{
				{Name: "web", Status: string(config.StatusRunning)},
				{Name: "db", Status: string(config.StatusWaiting)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.GET("/workflow/tasks/:taskID/status", appHandler.getWorkflowTaskStatus)

	req := httptest.NewRequest(http.MethodGet, "/workflow/tasks/task-abc/status", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload apis.TaskStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.TaskID != "task-abc" || payload.Status != string(config.StatusQueued) {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload.WorkflowID != "wf-1" || payload.WorkflowName != "deploy" || payload.AppID != "app-1" {
		t.Fatalf("unexpected workflow info: %+v", payload)
	}
	if len(payload.Components) != 2 {
		t.Fatalf("expected 2 components, got %d", len(payload.Components))
	}
	if payload.Components[0].Name != "web" || payload.Components[0].Status != string(config.StatusRunning) {
		t.Fatalf("unexpected first component: %+v", payload.Components[0])
	}
	if payload.Components[1].Name != "db" || payload.Components[1].Status != string(config.StatusWaiting) {
		t.Fatalf("unexpected second component: %+v", payload.Components[1])
	}
}

func TestGetWorkflowTaskStagesEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{
		taskStagesResp: &apis.TaskStagesResponse{
			TaskID:       "task-xyz",
			Status:       string(config.StatusRunning),
			WorkflowID:   "wf-2",
			WorkflowName: "release",
			AppID:        "app-2",
			Stages: []apis.TaskStageDetail{
				{
					ID:     1,
					Name:   "web",
					Type:   "deploy",
					Status: string(config.StatusRunning),
					Info: []apis.TaskStageMessage{
						{
							Type:    "deploy",
							Message: "apply deployment",
						},
					},
				},
				{
					ID:     2,
					Name:   "db",
					Type:   "pvc",
					Status: string(config.StatusFailed),
					Error: []apis.TaskStageMessage{
						{
							Component: "db",
							Message:   "pvc timeout",
						},
					},
				},
			},
		},
	}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.GET("/workflow/tasks/:taskID/stages", appHandler.getWorkflowTaskStages)

	req := httptest.NewRequest(http.MethodGet, "/workflow/tasks/task-xyz/stages", nil)
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload apis.TaskStagesResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.TaskID != "task-xyz" || payload.Status != string(config.StatusRunning) {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload.WorkflowID != "wf-2" || payload.WorkflowName != "release" || payload.AppID != "app-2" {
		t.Fatalf("unexpected workflow info: %+v", payload)
	}
	if len(payload.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(payload.Stages))
	}
	if payload.Stages[0].Name != "web" || len(payload.Stages[0].Info) != 1 {
		t.Fatalf("unexpected first stage: %+v", payload.Stages[0])
	}
	if payload.Stages[0].Info[0].Type != "deploy" || payload.Stages[0].Info[0].Message != "apply deployment" {
		t.Fatalf("unexpected first stage info: %+v", payload.Stages[0].Info)
	}
	if payload.Stages[1].Name != "db" || len(payload.Stages[1].Error) != 1 {
		t.Fatalf("unexpected second stage: %+v", payload.Stages[1])
	}
	if payload.Stages[1].Error[0].Component != "db" || payload.Stages[1].Error[0].Message != "pvc timeout" {
		t.Fatalf("unexpected second stage error: %+v", payload.Stages[1].Error)
	}
}

func TestGetApplicationStatusEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	components := []*model.ApplicationComponent{
		{Name: "mysql", Status: string(config.ComponentStatusRunning)},
		{Name: "redis", Status: string(config.ComponentStatusFailed)},
	}
	svc := componentListApplicationService{components: components}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/status", appHandler.getApplicationStatus)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/status", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ApplicationStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "app-1", payload.AppID)
	require.Equal(t, "failed", payload.Status)
}

func TestApplicationStatusRuntimeReaderWiring(t *testing.T) {
	t.Run("production service implements private reader", func(t *testing.T) {
		appService := service.NewApplicationService()
		runtimeReader, ok := appService.(applicationRuntimeComponentReader)
		require.True(t, ok)
		require.NotNil(t, runtimeReader)
	})

	t.Run("container injects one bean into both interfaces", func(t *testing.T) {
		appService := &statusReadApplicationService{}
		dependencies := &struct {
			ApplicationService     service.ApplicationsService       `inject:""`
			RuntimeComponentReader applicationRuntimeComponentReader `inject:""`
		}{}
		beanContainer := containerutil.NewContainer()
		require.NoError(t, beanContainer.Provides(appService, dependencies))
		require.NoError(t, beanContainer.Populate())
		require.Same(t, appService, dependencies.ApplicationService)
		require.Same(t, appService, dependencies.RuntimeComponentReader)
	})
}

func TestApplicationStatusHandlersUseRuntimeComponentRead(t *testing.T) {
	newService := func(cachedStatus, runtimeStatus config.ComponentStatus) *statusReadApplicationService {
		return &statusReadApplicationService{
			cachedComponents: []*model.ApplicationComponent{
				{Name: "cached-web", ComponentType: config.ServerJob, Status: string(cachedStatus)},
			},
			runtimeComponents: []*model.ApplicationComponent{
				{Name: "runtime-web", ComponentType: config.ServerJob, Status: string(runtimeStatus)},
			},
		}
	}

	t.Run("aggregate status", func(t *testing.T) {
		svc := newService(config.ComponentStatusRunning, config.ComponentStatusStopped)
		handler := &applications{
			ApplicationService:     svc,
			RuntimeComponentReader: svc,
			WorkflowService:        &fakeWorkflowService{},
		}

		response, err := handler.applicationStatus(context.Background(), "app-1")
		require.NoError(t, err)
		require.Equal(t, "stopped", response.Status)
		require.Equal(t, 0, svc.cachedCalls)
		require.Equal(t, 1, svc.runtimeCalls)
	})

	t.Run("component status", func(t *testing.T) {
		svc := newService(config.ComponentStatusPending, config.ComponentStatusRunning)
		handler := &applications{
			ApplicationService:     svc,
			RuntimeComponentReader: svc,
			WorkflowService:        &fakeWorkflowService{},
		}

		response, err := handler.applicationComponentStatus(context.Background(), "app-1")
		require.NoError(t, err)
		require.Len(t, response.Components, 1)
		require.Equal(t, "runtime-web", response.Components[0].Name)
		require.Equal(t, string(config.ComponentStatusRunning), response.Components[0].Status)
		require.Equal(t, 0, svc.cachedCalls)
		require.Equal(t, 1, svc.runtimeCalls)
	})

	t.Run("batch status", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		svc := newService(config.ComponentStatusPending, config.ComponentStatusRunning)
		handler := &applications{
			ApplicationService:     svc,
			RuntimeComponentReader: svc,
			WorkflowService:        &fakeWorkflowService{},
		}
		router := gin.New()
		router.POST("/applications/components/status", handler.listBatchApplicationComponentStatus)

		request := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(`{"appIds":["app-1"]}`))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)

		require.Equal(t, http.StatusOK, response.Code)
		var payload apis.BatchApplicationComponentStatusResponse
		requireSuccessResponse(t, response.Body.Bytes(), &payload)
		require.Len(t, payload.Results, 1)
		require.Equal(t, "running", payload.Results[0].Status)
		require.Equal(t, 0, svc.cachedCalls)
		require.Equal(t, 1, svc.runtimeCalls)
	})
}

func TestGetApplicationStatusEndpointReturnsStarting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	components := []*model.ApplicationComponent{
		{Name: "web", Status: string(config.ComponentStatusStarting)},
	}
	svc := componentListApplicationService{components: components}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/status", appHandler.getApplicationStatus)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/status", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ApplicationStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "app-1", payload.AppID)
	require.Equal(t, "starting", payload.Status)
}

func TestGetApplicationStatusEndpointReturnsUpdatingForActiveVersionUpdateTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := componentListApplicationService{
		components: []*model.ApplicationComponent{
			{Name: "web", Status: string(config.ComponentStatusRunning)},
		},
		activeVersionUpdate: true,
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/status", appHandler.getApplicationStatus)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/status", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ApplicationStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "app-1", payload.AppID)
	require.Equal(t, "updating", payload.Status)
}

func TestGetApplicationStatusEndpointReturnsDeployingForDeployingComponent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := componentListApplicationService{
		components: []*model.ApplicationComponent{
			{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusDeploying)},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.GET("/applications/:appID/status", appHandler.getApplicationStatus)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/status", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.ApplicationStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "app-1", payload.AppID)
	require.Equal(t, "deploying", payload.Status)
}

func TestApplicationStatusVersionUpdateTaskOverrideBoundaries(t *testing.T) {
	tests := []struct {
		name                string
		components          []*model.ApplicationComponent
		activeVersionUpdate bool
		want                string
	}{
		{
			name: "inactive version update keeps running",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusRunning)},
			},
			want: "running",
		},
		{
			name: "active version update lifts not deploy",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusNotDeploy)},
			},
			activeVersionUpdate: true,
			want:                "updating",
		},
		{
			name: "inactive version update keeps not deploy",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusNotDeploy)},
			},
			want: "not_deploy",
		},
		{
			name: "failed keeps precedence",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusFailed)},
			},
			activeVersionUpdate: true,
			want:                "failed",
		},
		{
			name: "deploying keeps precedence",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusDeploying)},
			},
			activeVersionUpdate: true,
			want:                "deploying",
		},
		{
			name: "restarting keeps precedence",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusRestarting)},
			},
			activeVersionUpdate: true,
			want:                "restarting",
		},
		{
			name: "starting keeps precedence",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusStarting)},
			},
			activeVersionUpdate: true,
			want:                "starting",
		},
		{
			name: "cleaning keeps precedence",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusCleaning)},
			},
			activeVersionUpdate: true,
			want:                "cleaning",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := componentListApplicationService{
				components:          tt.components,
				activeVersionUpdate: tt.activeVersionUpdate,
			}
			appHandler := &applications{
				ApplicationService:     svc,
				RuntimeComponentReader: svc,
				WorkflowService:        &fakeWorkflowService{},
			}

			got, err := appHandler.applicationStatus(context.Background(), "app-1")
			require.NoError(t, err)
			require.Equal(t, tt.want, got.Status)
		})
	}
}

func TestAggregateApplicationStatusStartingPriority(t *testing.T) {
	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "starting beats cleaning and pending",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusStarting)},
				{Name: "worker", Status: string(config.ComponentStatusCleaning)},
				{Name: "api", Status: string(config.ComponentStatusPending)},
			},
			want: "starting",
		},
		{
			name: "restarting beats starting",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusStarting)},
				{Name: "api", Status: string(config.ComponentStatusRestarting)},
			},
			want: "restarting",
		},
		{
			name: "failed beats starting",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusStarting)},
				{Name: "db", Status: string(config.ComponentStatusFailed)},
			},
			want: "failed",
		},
		{
			name: "deploying beats updating and starting",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusDeploying)},
				{Name: "api", Status: string(config.ComponentStatusUpdating)},
				{Name: "worker", Status: string(config.ComponentStatusStarting)},
			},
			want: "deploying",
		},
		{
			name: "failed beats deploying",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusDeploying)},
				{Name: "db", Status: string(config.ComponentStatusFailed)},
			},
			want: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, aggregateApplicationStatus(tt.components))
		})
	}
}

func TestAggregateApplicationStatusUsesServingComponentsForAvailability(t *testing.T) {
	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "stopped webservice beats running store for app availability",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "stopped",
		},
		{
			name: "running webservice keeps app running when store is stopped",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusStopped)},
			},
			want: "running",
		},
		{
			name: "starting webservice keeps start recovery visible with stopped store",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStarting)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusStopped)},
			},
			want: "starting",
		},
		{
			name: "starting webservice keeps start recovery visible with running store",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStarting)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "starting",
		},
		{
			name: "store restart stays globally visible with running webservice",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRestarting)},
			},
			want: "restarting",
		},
		{
			name: "store only app keeps existing aggregate behavior",
			components: []*model.ApplicationComponent{
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "running",
		},
		{
			name: "store failure still fails app with stopped webservice",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusFailed)},
			},
			want: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, aggregateApplicationStatus(tt.components))
		})
	}
}

func TestAggregateApplicationStatusPrefersManagedServingComponents(t *testing.T) {
	shareTraits := func(strategy domainspec.ShareStrategy) *model.JSONStruct {
		return &model.JSONStruct{
			"share": map[string]interface{}{
				"strategy": string(strategy),
			},
		}
	}
	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "stopped managed workloads are not hidden by running shared proxy and stores",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning), Traits: shareTraits(domainspec.ShareStrategyDefault)},
				{Name: "mysql", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
				{Name: "redis", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "stopped",
		},
		{
			name: "pending shared proxy does not hide ready managed workloads",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "frontend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyDefault)},
				{Name: "mysql", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
				{Name: "redis", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "running",
		},
		{
			name: "shared ignore pending does not hide managed running",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "ignored-proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyIgnore)},
			},
			want: "running",
		},
		{
			name: "unknown shared strategy pending does not hide managed running",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "future-proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategy("future-default"))},
			},
			want: "running",
		},
		{
			name: "shared failure remains globally visible",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "shared-socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusFailed), Traits: shareTraits(domainspec.ShareStrategyDefault)},
			},
			want: "failed",
		},
		{
			name: "shared only application falls back to shared availability",
			components: []*model.ApplicationComponent{
				{Name: "shared-socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyDefault)},
			},
			want: "pending",
		},
		{
			name: "share force remains managed",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "forced-socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyForce)},
			},
			want: "pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, aggregateApplicationStatus(tt.components))
		})
	}
}

func TestAggregateApplicationStatusSmoothsRecentPodBackedFailures(t *testing.T) {
	now := time.Unix(1000, 0)
	recentFailureTime := now.Add(-config.DefaultApplicationStatusTransientFailedWindow + time.Second)
	expiredFailureTime := now.Add(-config.DefaultApplicationStatusTransientFailedWindow - time.Second)

	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "running webservice keeps app running when store failure is recent",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning), Replicas: 1, ReadyReplicas: 1},
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      1,
					ReadyReplicas: 1,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "running",
		},
		{
			name: "store only recent failed component is treated as pending while recovering",
			components: []*model.ApplicationComponent{
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      1,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "pending",
		},
		{
			name: "zero replica recent failed component is treated as pending while recovering",
			components: []*model.ApplicationComponent{
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      0,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "pending",
		},
		{
			name: "updating shrink to zero recent failed component is treated as pending while recovering",
			components: []*model.ApplicationComponent{
				{
					Name:          "web",
					ComponentType: config.ServerJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      0,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "pending",
		},
		{
			name: "expired pod backed failure still fails app",
			components: []*model.ApplicationComponent{
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      1,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: expiredFailureTime},
				},
			},
			want: "failed",
		},
		{
			name: "non pod backed recent failure still fails app",
			components: []*model.ApplicationComponent{
				{
					Name:          "settings",
					ComponentType: config.ConfJob,
					Status:        string(config.ComponentStatusFailed),
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "failed",
		},
		{
			name: "missing update time still fails app",
			components: []*model.ApplicationComponent{
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusFailed), Replicas: 1},
			},
			want: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, aggregateApplicationStatusWithReferenceTime(tt.components, now))
		})
	}
}

func TestBatchApplicationComponentStatusEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusRunning)},
				{Name: "cache", Status: ""},
				{Name: "worker", Status: string(config.ComponentStatusRunning)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.NotContains(t, resp.Body.String(), "\"notFound\"")

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "running", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsUpdatingForActiveVersionUpdateTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusRunning)},
			},
		},
		activeTaskByAppID: map[string]bool{
			"app-1": true,
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "updating", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsDeployingForDeployingComponent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusDeploying)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "deploying", result.Status)
}

func TestBatchApplicationComponentStatusEndpointDoesNotReturnDeployingForGenericWorkflowTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusNotDeploy)},
			},
		},
		activeTaskByAppID: map[string]bool{
			"app-1": false,
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "not_deploy", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsFailed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusRunning)},
				{Name: "db", Status: string(config.ComponentStatusFailed)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "failed", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsStarting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusStarting)},
				{Name: "worker", Status: string(config.ComponentStatusPending)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "starting", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsStopped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusStopped)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "stopped", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsStoppedWhenServingStoppedAndStoreRunning(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "stopped", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsStartingWhenServingStartingAndStoreRunning(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStarting)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "starting", result.Status)
}

func TestBatchApplicationComponentStatusEndpointReturnsRestartingWhenStoreRestarting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRestarting)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 1)
	result := payload.Results[0]
	require.Equal(t, "app-1", result.AppID)
	require.Equal(t, "restarting", result.Status)
}

func TestBatchApplicationComponentStatusEndpointRejectsLegacyArrayBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{
		ApplicationService: batchComponentStatusApplicationService{
			componentsByAppID: map[string][]*model.ApplicationComponent{"app-1": {}},
		},
		WorkflowService: &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `[{"appId":"app-1"}]`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, int32(bcode.ErrApplicationConfig.BusinessCode), envelope.Code)
}

func TestBatchApplicationComponentStatusEndpointPartialNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusRunning)},
			},
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1","missing-app",""]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	require.NotContains(t, resp.Body.String(), "\"notFound\"")
	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 3)
	require.Equal(t, "running", payload.Results[0].Status)
	require.Equal(t, bcode.ErrApplicationNotExist.Message, payload.Results[1].Error)
	require.Equal(t, "appId is required", payload.Results[2].Error)
}

func TestBatchApplicationComponentStatusEndpointPartialGenericError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusRunning)},
			},
			"app-3": {
				{Name: "worker", Status: string(config.ComponentStatusPending)},
			},
		},
		errByAppID: map[string]error{
			"app-2": errors.New("store timeout"),
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1","app-2","app-3"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 3)

	require.Equal(t, "app-1", payload.Results[0].AppID)
	require.Equal(t, "running", payload.Results[0].Status)
	require.Empty(t, payload.Results[0].Error)

	require.Equal(t, "app-2", payload.Results[1].AppID)
	require.Empty(t, payload.Results[1].Status)
	require.Equal(t, "store timeout", payload.Results[1].Error)

	require.Equal(t, "app-3", payload.Results[2].AppID)
	require.Equal(t, "pending", payload.Results[2].Status)
	require.Empty(t, payload.Results[2].Error)
}

func TestBatchApplicationComponentStatusEndpointBcodeErrorMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := batchComponentStatusApplicationService{
		componentsByAppID: map[string][]*model.ApplicationComponent{
			"app-1": {
				{Name: "backend", Status: string(config.ComponentStatusRunning)},
			},
			"app-3": {
				{Name: "worker", Status: string(config.ComponentStatusRunning)},
			},
		},
		errByAppID: map[string]error{
			"app-2": bcode.ErrDistributedLockUnavailable,
		},
	}
	appHandler := &applications{
		ApplicationService:     svc,
		RuntimeComponentReader: svc,
		WorkflowService:        &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":["app-1","app-2","app-3"]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.BatchApplicationComponentStatusResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Len(t, payload.Results, 3)
	require.Equal(t, bcode.ErrDistributedLockUnavailable.Message, payload.Results[1].Error)
	require.NotContains(t, payload.Results[1].Error, "HTTPCode:")
	require.Equal(t, "running", payload.Results[2].Status)
}

func TestBatchApplicationComponentStatusEndpointRejectsEmptyItems(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/components/status", appHandler.listBatchApplicationComponentStatus)

	body := `{"appIds":[]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/components/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, int32(bcode.ErrApplicationConfig.BusinessCode), envelope.Code)
}
