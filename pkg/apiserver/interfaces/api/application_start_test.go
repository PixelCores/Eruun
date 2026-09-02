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

type fakeStartApplicationService struct {
	noopApplicationsService
	lastAppID string
	lastReq   apis.ApplicationLifecycleRequest
	resp      *apis.StartApplicationDeploymentsResponse
	err       error
}

func (f *fakeStartApplicationService) StartApplicationDeployments(_ context.Context, appID string, req apis.ApplicationLifecycleRequest) (*apis.StartApplicationDeploymentsResponse, error) {
	f.lastAppID = appID
	f.lastReq = req
	if f.resp == nil && f.err == nil {
		f.resp = &apis.StartApplicationDeploymentsResponse{AppID: appID}
	}
	return f.resp, f.err
}

func TestStartApplicationEndpointWarningErrorReturnsSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeStartApplicationService{
		resp: &apis.StartApplicationDeploymentsResponse{
			AppID: "app-123",
			FailedResources: []string{
				"Deployment:default/web (stored replicas must be greater than 0)",
			},
		},
		err: errors.New("partial start failures"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/start", appHandler.startApplicationDeployments)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/start", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	var payload apis.StartApplicationDeploymentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.AppID != "app-123" {
		t.Fatalf("unexpected appID: %s", payload.AppID)
	}
	if len(payload.FailedResources) != 1 {
		t.Fatalf("expected failed resource details, got %+v", payload.FailedResources)
	}
}

func TestStartApplicationEndpointHardErrorReturnsFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeStartApplicationService{
		resp: nil,
		err:  errors.New("patch deployment failed"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/start", appHandler.startApplicationDeployments)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/start", nil)
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
