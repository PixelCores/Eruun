package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeDatabaseResetApplicationService struct {
	noopApplicationsService
	lastAppID string
	lastReq   apis.DatabaseResetRequest
	resp      *apis.DatabaseResetResponse
	err       error
}

func (f *fakeDatabaseResetApplicationService) ResetApplicationDatabases(_ context.Context, appID string, req apis.DatabaseResetRequest) (*apis.DatabaseResetResponse, error) {
	f.lastAppID = appID
	f.lastReq = req
	if f.resp == nil && f.err == nil {
		f.resp = &apis.DatabaseResetResponse{AppID: appID}
	}
	return f.resp, f.err
}

func TestDatabaseResetEndpointSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDatabaseResetApplicationService{
		resp: &apis.DatabaseResetResponse{
			AppID:              "app-123",
			WorkflowID:         "workflow-123",
			TaskID:             "task-123",
			DatabaseComponents: []string{"mysql", "redis"},
			RestartComponents:  []string{},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/database-reset", appHandler.resetApplicationDatabases)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/database-reset", strings.NewReader(`{"components":["mysql","redis"],"initSqlUrl":"https://files.example/game-1.0.8.sql"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	if !strings.Contains(resp.Body.String(), `"restartComponents":[]`) {
		t.Fatalf("expected restartComponents to be an empty array: %s", resp.Body.String())
	}
	var payload apis.DatabaseResetResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.TaskID != "task-123" || payload.WorkflowID != "workflow-123" {
		t.Fatalf("unexpected workflow response: %+v", payload)
	}
	if appSvc.lastAppID != "app-123" {
		t.Fatalf("expected appID app-123, got %s", appSvc.lastAppID)
	}
	if len(appSvc.lastReq.Components) != 2 || appSvc.lastReq.Components[0] != "mysql" || appSvc.lastReq.Components[1] != "redis" {
		t.Fatalf("unexpected request: %+v", appSvc.lastReq)
	}
	if appSvc.lastReq.InitSQLURL != "https://files.example/game-1.0.8.sql" {
		t.Fatalf("unexpected initSqlUrl: %q", appSvc.lastReq.InitSQLURL)
	}
	if !appSvc.lastReq.InitSQLURLProvided() {
		t.Fatalf("expected initSqlUrl to be marked as provided")
	}
}

func TestDatabaseResetEndpointAllowsOmittedInitSQLURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDatabaseResetApplicationService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/database-reset", appHandler.resetApplicationDatabases)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/database-reset", strings.NewReader(`{"components":["mysql"]}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	if appSvc.lastAppID != "app-123" {
		t.Fatalf("expected appID app-123, got %s", appSvc.lastAppID)
	}
	if appSvc.lastReq.InitSQLURLProvided() {
		t.Fatalf("omitted initSqlUrl should not be marked as provided")
	}
}

func TestDatabaseResetEndpointRejectsInvalidInitSQLURLJSON(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "null",
			body: `{"components":["mysql"],"initSqlUrl":null}`,
		},
		{
			name: "non-string",
			body: `{"components":["mysql"],"initSqlUrl":42}`,
		},
		{
			name: "unknown field",
			body: `{"components":["mysql"],"initSqlUrl":"https://files.example/game.sql","unknown":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			appSvc := &fakeDatabaseResetApplicationService{}
			appHandler := &applications{
				ApplicationService: appSvc,
				WorkflowService:    &fakeWorkflowService{},
			}
			r := gin.New()
			r.POST("/applications/:appID/database-reset", appHandler.resetApplicationDatabases)

			req := httptest.NewRequest(http.MethodPost, "/applications/app-123/database-reset", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
			}
			result := decodeResponse(t, resp.Body.Bytes(), nil)
			if result.Code != bcode.ErrApplicationConfig.BusinessCode {
				t.Fatalf("expected application config error, got %+v", result)
			}
			if appSvc.lastAppID != "" {
				t.Fatalf("service should not be called on invalid request")
			}
		})
	}
}

func TestDatabaseResetEndpointMissingComponents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDatabaseResetApplicationService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/database-reset", appHandler.resetApplicationDatabases)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/database-reset", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	result := decodeResponse(t, resp.Body.Bytes(), nil)
	if result.Code != bcode.ErrApplicationConfig.BusinessCode {
		t.Fatalf("expected application config error, got %+v", result)
	}
	if appSvc.lastAppID != "" {
		t.Fatalf("service should not be called on invalid request")
	}
}

func TestDatabaseResetEndpointBusinessError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDatabaseResetApplicationService{
		err: fmt.Errorf("%w: component %q is not a store component", bcode.ErrApplicationConfig, "api"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/database-reset", appHandler.resetApplicationDatabases)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/database-reset", strings.NewReader(`{"components":["api"]}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	result := decodeResponse(t, resp.Body.Bytes(), nil)
	if result.Code != bcode.ErrApplicationConfig.BusinessCode {
		t.Fatalf("expected application config error, got %+v", result)
	}
	if appSvc.lastAppID != "app-123" {
		t.Fatalf("expected service call for valid JSON, got appID=%s", appSvc.lastAppID)
	}
}
