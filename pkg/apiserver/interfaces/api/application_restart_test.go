package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeRestartApplicationService struct {
	noopApplicationsService
	lastAppID string
	lastReq   apis.ApplicationLifecycleRequest
	resp      *apis.RestartApplicationWorkloadsResponse
	err       error
}

func (f *fakeRestartApplicationService) RestartApplicationWorkloads(_ context.Context, appID string, req apis.ApplicationLifecycleRequest) (*apis.RestartApplicationWorkloadsResponse, error) {
	f.lastAppID = appID
	f.lastReq = req
	if f.resp == nil && f.err == nil {
		f.resp = &apis.RestartApplicationWorkloadsResponse{AppID: appID}
	}
	return f.resp, f.err
}

func TestRestartApplicationEndpointWarningErrorReturnsSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeRestartApplicationService{
		resp: &apis.RestartApplicationWorkloadsResponse{
			AppID: "app-123",
			FailedResources: []string{
				"Deployment:default/web (timeout)",
			},
		},
		err: errors.New("partial restart failures"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/restart", appHandler.restartApplicationWorkloads)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/restart", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	var payload apis.RestartApplicationWorkloadsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.AppID != "app-123" {
		t.Fatalf("unexpected appID: %s", payload.AppID)
	}
	if len(payload.FailedResources) != 1 {
		t.Fatalf("expected failed resource details, got %+v", payload.FailedResources)
	}
}

func TestRestartApplicationEndpointMarkFailureReturnsFailedResource(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeRestartApplicationService{
		resp: &apis.RestartApplicationWorkloadsResponse{
			AppID:              "app-123",
			TaskID:             "task-123",
			RestartedResources: []string{"Deployment:default/web"},
			FailedResources:    []string{"ComponentStatus:default/app-123 (mark components restarting: status store unavailable)"},
		},
		err: errors.New("mark components restarting: status store unavailable"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/restart", appHandler.restartApplicationWorkloads)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/restart", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	var payload apis.RestartApplicationWorkloadsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.TaskID != "task-123" {
		t.Fatalf("expected task id to be preserved, got %s", payload.TaskID)
	}
	if len(payload.FailedResources) != 1 {
		t.Fatalf("expected mark failure details, got %+v", payload.FailedResources)
	}
	if payload.FailedResources[0] != "ComponentStatus:default/app-123 (mark components restarting: status store unavailable)" {
		t.Fatalf("unexpected failed resource: %s", payload.FailedResources[0])
	}
}

func TestRestartApplicationEndpointHardErrorReturnsFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeRestartApplicationService{
		resp: nil,
		err:  errors.New("patch deployment failed"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/restart", appHandler.restartApplicationWorkloads)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/restart", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code == http.StatusOK {
		t.Fatalf("expected non-OK status, body=%s", resp.Body.String())
	}
	result := decodeResponse(t, resp.Body.Bytes(), nil)
	if result.Code == bcode.SuccessCode {
		t.Fatalf("expected error response, got success")
	}
}
