package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeDeleteApplicationService struct {
	noopApplicationsService
	lastAppID string
	lastReq   apis.DeleteApplicationRequest
	resp      *apis.DeleteApplicationResponse
	err       error
}

func (f *fakeDeleteApplicationService) DeleteApplicationCascade(_ context.Context, appID string, req apis.DeleteApplicationRequest) (*apis.DeleteApplicationResponse, error) {
	f.lastAppID = appID
	f.lastReq = req
	if f.resp == nil {
		f.resp = &apis.DeleteApplicationResponse{AppID: appID}
	}
	return f.resp, f.err
}

func TestDeleteApplicationEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDeleteApplicationService{
		resp: &apis.DeleteApplicationResponse{
			AppID:            "app-123",
			CancelledTaskIDs: []string{"task-1"},
			ActiveTaskIDs:    []string{"task-2"},
			Warnings:         []string{"timeout waiting task-2"},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.DELETE("/applications/:appID", appHandler.deleteApplication)

	req := httptest.NewRequest(http.MethodDelete, "/applications/app-123", strings.NewReader(`{"waitSeconds":30}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	var payload apis.DeleteApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.AppID != "app-123" {
		t.Fatalf("unexpected appID: %s", payload.AppID)
	}
	if appSvc.lastAppID != "app-123" {
		t.Fatalf("expected appID app-123, got %s", appSvc.lastAppID)
	}
	if appSvc.lastReq.WaitSeconds == nil || *appSvc.lastReq.WaitSeconds != 30 {
		t.Fatalf("expected waitSeconds=30, got %#v", appSvc.lastReq.WaitSeconds)
	}
}

func TestDeleteApplicationEndpointReturnsWarningDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDeleteApplicationService{
		resp: &apis.DeleteApplicationResponse{
			AppID:    "app-123",
			Warnings: []string{"timeout waiting task-2"},
		},
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.DELETE("/applications/:appID", appHandler.deleteApplication)

	req := httptest.NewRequest(http.MethodDelete, "/applications/app-123", strings.NewReader(`{"waitSeconds":30}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	var payload apis.DeleteApplicationResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.AppID != "app-123" {
		t.Fatalf("unexpected appID: %s", payload.AppID)
	}
	if len(payload.Warnings) != 1 || payload.Warnings[0] != "timeout waiting task-2" {
		t.Fatalf("expected warning details, got %+v", payload.Warnings)
	}
	if appSvc.lastReq.WaitSeconds == nil || *appSvc.lastReq.WaitSeconds != 30 {
		t.Fatalf("expected waitSeconds=30, got %#v", appSvc.lastReq.WaitSeconds)
	}
}

func TestDeleteApplicationEndpointInvalidWaitSeconds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDeleteApplicationService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.DELETE("/applications/:appID", appHandler.deleteApplication)

	req := httptest.NewRequest(http.MethodDelete, "/applications/app-123", strings.NewReader(`{"waitSeconds":-1}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	result := decodeResponse(t, resp.Body.Bytes(), nil)
	if result.Code == bcode.SuccessCode {
		t.Fatalf("expected error response, got success")
	}
	if appSvc.lastAppID != "" {
		t.Fatalf("service should not be called on invalid request")
	}
}

func TestDeleteApplicationEndpointChunkedBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDeleteApplicationService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.DELETE("/applications/:appID", appHandler.deleteApplication)

	req := httptest.NewRequest(http.MethodDelete, "/applications/app-123", strings.NewReader(`{"waitSeconds":12}`))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = -1
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	if appSvc.lastReq.WaitSeconds == nil || *appSvc.lastReq.WaitSeconds != 12 {
		t.Fatalf("expected waitSeconds=12 for chunked request, got %#v", appSvc.lastReq.WaitSeconds)
	}
}

func TestDeleteApplicationEndpointChunkedBodyInvalidWaitSeconds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeDeleteApplicationService{}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.DELETE("/applications/:appID", appHandler.deleteApplication)

	req := httptest.NewRequest(http.MethodDelete, "/applications/app-123", strings.NewReader(`{"waitSeconds":-1}`))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = -1
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	result := decodeResponse(t, resp.Body.Bytes(), nil)
	if result.Code == bcode.SuccessCode {
		t.Fatalf("expected error response, got success")
	}
	if appSvc.lastAppID != "" {
		t.Fatalf("service should not be called for invalid chunked request")
	}
}

func TestApplicationLifecycleRouteRegistration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	appHandler.RegisterRoutes(r.Group("/api/v1"))

	registered := map[string]bool{}
	for _, route := range r.Routes() {
		registered[route.Method+" "+route.Path] = true
	}

	expectRoute := func(method, path string) {
		t.Helper()
		key := method + " " + path
		if !registered[key] {
			t.Fatalf("expected route %s to be registered", key)
		}
	}
	rejectRoute := func(method, path string) {
		t.Helper()
		key := method + " " + path
		if registered[key] {
			t.Fatalf("expected route %s to be removed", key)
		}
	}

	expectRoute(http.MethodDelete, "/api/v1/applications/:appID")
	expectRoute(http.MethodPost, "/api/v1/applications/:appID/restart")
	expectRoute(http.MethodPost, "/api/v1/applications/:appID/stop")
	expectRoute(http.MethodPost, "/api/v1/applications/:appID/start")
	expectRoute(http.MethodPost, "/api/v1/applications/:appID/workflow/tasks/cancel-all")

	rejectRoute(http.MethodPost, "/api/v1/applications/:appID/delete")
	rejectRoute(http.MethodPost, "/api/v1/applications/:appID/actions/restart")
	rejectRoute(http.MethodPost, "/api/v1/applications/:appID/actions/stop")
	rejectRoute(http.MethodPost, "/api/v1/applications/:appID/actions/start")
	rejectRoute(http.MethodPost, "/api/v1/applications/:appID/cancelall")
}
