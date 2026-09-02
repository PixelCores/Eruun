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

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestExecApplicationWorkflowEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    svc,
	}
	r := gin.New()
	r.POST("/applications/:appID/workflow/exec", appHandler.execApplicationWorkflow)

	body := `{"workflowId":"wf-123","executeAt":1735689600}`
	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/workflow/exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload apis.ExecWorkflowResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.TaskID != "test-task" {
		t.Fatalf("unexpected taskID %s", payload.TaskID)
	}
	if !svc.execForAppCalled || svc.lastExecAppID != "app-1" || svc.lastExecWorkflowID != "wf-123" || svc.lastExecExecuteAt != 1735689600 {
		t.Fatalf("expected exec workflow for app to be invoked")
	}
}

func TestCreateAndExecApplicationsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wf-default",
			Resources: apis.ApplicationResources{
				CPUReq:   "300m",
				MemReq:   "600Mi",
				CPULimit: "300m",
				MemLimit: "600Mi",
				Replicas: 1,
			},
		},
	}
	wfSvc := &fakeWorkflowService{execResp: &apis.ExecWorkflowResponse{TaskID: "task-1"}}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	futureExecuteAt := time.Now().Add(time.Hour).Unix()
	body := fmt.Sprintf(`{"name":"demoapp","component":[],"workflow":[],"workflowId":"wf123","executeAt":%d}`, futureExecuteAt)
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.Application == nil || payload.Application.ID != "app-1" {
		t.Fatalf("unexpected application payload: %+v", payload.Application)
	}
	require.Equal(t, "300m", payload.Application.Resources.CPUReq)
	require.Equal(t, "600Mi", payload.Application.Resources.MemReq)
	require.Equal(t, "300m", payload.Application.Resources.CPULimit)
	require.Equal(t, "600Mi", payload.Application.Resources.MemLimit)
	require.EqualValues(t, 1, payload.Application.Resources.Replicas)
	if payload.WorkflowID != "wf123" {
		t.Fatalf("unexpected workflowID: %s", payload.WorkflowID)
	}
	if payload.TaskID != "task-1" {
		t.Fatalf("unexpected taskID: %s", payload.TaskID)
	}
	if payload.ExecStatus != apis.CreateAndExecStatusQueued {
		t.Fatalf("unexpected exec status: %s", payload.ExecStatus)
	}
	if payload.ExecError != "" {
		t.Fatalf("unexpected exec error: %s", payload.ExecError)
	}
	if !wfSvc.execForAppCalled || wfSvc.lastExecAppID != "app-1" || wfSvc.lastExecWorkflowID != "wf123" || wfSvc.lastExecExecuteAt != futureExecuteAt {
		t.Fatalf("expected create-and-exec workflow call")
	}
	require.Zero(t, appSvc.markCalls)
	if appSvc.lastCreate.Name != "demoapp" {
		t.Fatalf("unexpected create request name: %s", appSvc.lastCreate.Name)
	}
}

func TestCreateAndExecApplicationsEndpointUsesDefaultWorkflow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wfdefault",
		},
	}
	wfSvc := &fakeWorkflowService{execResp: &apis.ExecWorkflowResponse{TaskID: "task-default"}}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	body := `{"name":"demoapp","component":[],"workflow":[]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.WorkflowID != "wfdefault" {
		t.Fatalf("unexpected workflowID: %s", payload.WorkflowID)
	}
	if payload.TaskID != "task-default" {
		t.Fatalf("unexpected taskID: %s", payload.TaskID)
	}
	if payload.ExecStatus != apis.CreateAndExecStatusQueued {
		t.Fatalf("unexpected exec status: %s", payload.ExecStatus)
	}
	if !wfSvc.execForAppCalled || wfSvc.lastExecWorkflowID != "wfdefault" {
		t.Fatalf("expected default workflow execution")
	}
	require.Equal(t, 1, appSvc.markCalls)
	require.Equal(t, "app-1", appSvc.lastMarkApp)
	require.Equal(t, "wfdefault", appSvc.lastMarkWorkflow)
}

func TestCreateAndExecApplicationsEndpointMarksDeployingForExplicitWorkflow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wfdefault",
		},
	}
	wfSvc := &fakeWorkflowService{execResp: &apis.ExecWorkflowResponse{TaskID: "task-explicit"}}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	body := `{"name":"demoapp","component":[],"workflow":[],"workflowId":"wf-custom"}`
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, apis.CreateAndExecStatusQueued, payload.ExecStatus)
	require.Equal(t, "wf-custom", payload.WorkflowID)
	require.Equal(t, "task-explicit", payload.TaskID)
	require.True(t, wfSvc.execForAppCalled)
	require.Equal(t, "app-1", wfSvc.lastExecAppID)
	require.Equal(t, "wf-custom", wfSvc.lastExecWorkflowID)
	require.Equal(t, 1, appSvc.markCalls)
	require.Equal(t, "app-1", appSvc.lastMarkApp)
	require.Equal(t, "wf-custom", appSvc.lastMarkWorkflow)
}

func TestCreateAndExecApplicationsEndpointDoesNotMarkDeployingForDelayedExec(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wfdefault",
		},
	}
	wfSvc := &fakeWorkflowService{execResp: &apis.ExecWorkflowResponse{TaskID: "task-default"}}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	futureExecuteAt := time.Now().Add(time.Hour).Unix()
	body := fmt.Sprintf(`{"name":"demoapp","component":[],"workflow":[],"executeAt":%d}`, futureExecuteAt)
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, apis.CreateAndExecStatusQueued, payload.ExecStatus)
	require.Equal(t, "task-default", payload.TaskID)
	require.Equal(t, futureExecuteAt, wfSvc.lastExecExecuteAt)
	require.Zero(t, appSvc.markCalls)
}

func TestCreateAndExecApplicationsEndpointMarksDeployingForPastExecuteAt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wfdefault",
		},
	}
	wfSvc := &fakeWorkflowService{execResp: &apis.ExecWorkflowResponse{TaskID: "task-default"}}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	pastExecuteAt := time.Now().Add(-time.Minute).Unix()
	body := fmt.Sprintf(`{"name":"demoapp","component":[],"workflow":[],"executeAt":%d}`, pastExecuteAt)
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, apis.CreateAndExecStatusQueued, payload.ExecStatus)
	require.Equal(t, "task-default", payload.TaskID)
	require.True(t, wfSvc.execForAppCalled)
	require.Equal(t, pastExecuteAt, wfSvc.lastExecExecuteAt)
	require.Equal(t, 1, appSvc.markCalls)
	require.Equal(t, "app-1", appSvc.lastMarkApp)
	require.Equal(t, "wfdefault", appSvc.lastMarkWorkflow)
}

func TestShouldMarkCreateAndExecDeploying(t *testing.T) {
	now := time.Unix(1000, 0)
	tests := []struct {
		name      string
		executeAt int64
		now       time.Time
		want      bool
	}{
		{
			name:      "negative",
			executeAt: -1,
			now:       now,
			want:      false,
		},
		{
			name:      "zero",
			executeAt: 0,
			now:       now,
			want:      true,
		},
		{
			name:      "past",
			executeAt: 999,
			now:       now,
			want:      true,
		},
		{
			name:      "current second",
			executeAt: 1000,
			now:       now,
			want:      true,
		},
		{
			name:      "near future became due",
			executeAt: 1001,
			now:       time.Unix(1001, 0),
			want:      true,
		},
		{
			name:      "future",
			executeAt: 1001,
			now:       now,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldMarkCreateAndExecDeploying(tt.executeAt, tt.now))
		})
	}
}

func TestCreateAndExecApplicationsEndpointInvalidExecuteAtDoesNotExecOrMark(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wfdefault",
		},
	}
	wfSvc := &fakeWorkflowService{execResp: &apis.ExecWorkflowResponse{TaskID: "task-default"}}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	body := `{"name":"demoapp","component":[],"workflow":[],"executeAt":-1}`
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, apis.CreateAndExecStatusFailed, payload.ExecStatus)
	require.Equal(t, bcode.ErrWorkflowConfig.Error(), payload.ExecError)
	require.Empty(t, payload.TaskID)
	require.False(t, wfSvc.execForAppCalled)
	require.Zero(t, appSvc.markCalls)
}

func TestCreateAndExecApplicationsEndpointAcceptsWorkflowObject(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wfdefault",
		},
	}
	wfSvc := &fakeWorkflowService{execResp: &apis.ExecWorkflowResponse{TaskID: "task-default"}}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	body := `{"name":"demoapp","component":[],"callback":{"success":"https://example.com/app"},"workflow":{"callback":{"success":"https://example.com/workflow"},"steps":[{"name":"deploy-web","components":["web"]}]}}`
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "wfdefault", payload.WorkflowID)
	require.NotNil(t, appSvc.lastCreate.Callback)
	require.Equal(t, "https://example.com/app", appSvc.lastCreate.Callback.Success)
	require.NotNil(t, appSvc.lastCreate.WorkflowCallback)
	require.Equal(t, "https://example.com/workflow", appSvc.lastCreate.WorkflowCallback.Success)
	require.Len(t, appSvc.lastCreate.WorkflowSteps, 1)
	require.Equal(t, "deploy-web", appSvc.lastCreate.WorkflowSteps[0].Name)
}

func TestCreateAndExecApplicationsEndpointExecFailureReturnsSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:         "app-1",
			Name:       "demoapp",
			WorkflowID: "wfdefault",
		},
	}
	wfSvc := &fakeWorkflowService{execErr: bcode.ErrWorkflowNotExist}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	body := `{"name":"demoapp","component":[],"workflow":[]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.Application == nil || payload.Application.ID != "app-1" {
		t.Fatalf("unexpected application payload: %+v", payload.Application)
	}
	if payload.ExecStatus != apis.CreateAndExecStatusFailed {
		t.Fatalf("unexpected exec status: %s", payload.ExecStatus)
	}
	if payload.TaskID != "" {
		t.Fatalf("expected empty taskID when execution fails, got %s", payload.TaskID)
	}
	if payload.ExecError == "" {
		t.Fatalf("expected execution error message")
	}
	require.Zero(t, appSvc.markCalls)
}

func TestCreateAndExecApplicationsEndpointPendingCleanupRefreshFailureDoesNotExec(t *testing.T) {
	gin.SetMode(gin.TestMode)
	publicMessage := "components mysql have an unfinished StatefulSet migration; resume it with the version update API before executing another workflow"
	appSvc := &fakeCreateAndExecApplicationService{createErr: bcode.WithSafeClientMessage(
		fmt.Errorf("%w: unfinished StatefulSet cleanup for component mysql", bcode.ErrApplicationConfig),
		publicMessage,
	)}
	wfSvc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	body := `{"name":"demoapp","component":[],"workflow":[]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrApplicationConfig.BusinessCode {
		t.Fatalf("unexpected response code: %d", envelope.Code)
	}
	require.Equal(t, publicMessage, envelope.Message)
	if wfSvc.execForAppCalled {
		t.Fatalf("workflow execution should not be called when creation fails")
	}
}

func TestCreateAndExecApplicationsEndpointFailsWhenWorkflowUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCreateAndExecApplicationService{
		createResp: &apis.ApplicationBase{
			ID:   "app-1",
			Name: "demoapp",
		},
	}
	wfSvc := &fakeWorkflowService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    wfSvc,
	}
	r := gin.New()
	r.POST("/applications/create-and-exec", appHandler.createAndExecApplications)

	body := `{"name":"demoapp","component":[],"workflow":[]}`
	req := httptest.NewRequest(http.MethodPost, "/applications/create-and-exec", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	var payload apis.CreateAndExecApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.ExecStatus != apis.CreateAndExecStatusFailed {
		t.Fatalf("unexpected exec status: %s", payload.ExecStatus)
	}
	if payload.ExecError == "" {
		t.Fatalf("expected workflow unavailable error")
	}
	if wfSvc.execForAppCalled {
		t.Fatalf("workflow execution should not be called when workflow id is unavailable")
	}
	require.Zero(t, appSvc.markCalls)
}
