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

type fakeStopApplicationService struct {
	noopApplicationsService
	lastAppID string
	lastReq   apis.ApplicationLifecycleRequest
	resp      *apis.StopApplicationDeploymentsResponse
	err       error
}

func (f *fakeStopApplicationService) StopApplicationDeployments(_ context.Context, appID string, req apis.ApplicationLifecycleRequest) (*apis.StopApplicationDeploymentsResponse, error) {
	f.lastAppID = appID
	f.lastReq = req
	if f.resp == nil && f.err == nil {
		f.resp = &apis.StopApplicationDeploymentsResponse{AppID: appID}
	}
	return f.resp, f.err
}

func TestStopApplicationEndpointWarningErrorReturnsSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeStopApplicationService{
		resp: &apis.StopApplicationDeploymentsResponse{
			AppID: "app-123",
			FailedResources: []string{
				"Deployment:default/web (timeout)",
			},
		},
		err: errors.New("partial stop failures"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/stop", appHandler.stopApplicationDeployments)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/stop", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}

	var payload apis.StopApplicationDeploymentsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.AppID != "app-123" {
		t.Fatalf("unexpected appID: %s", payload.AppID)
	}
	if len(payload.FailedResources) != 1 {
		t.Fatalf("expected failed resource details, got %+v", payload.FailedResources)
	}
}

func TestStopApplicationEndpointHardErrorReturnsFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeStopApplicationService{
		resp: nil,
		err:  errors.New("patch deployment failed"),
	}
	appHandler := &applications{
		ApplicationService: appSvc,
		WorkflowService:    &fakeWorkflowService{},
	}
	r := gin.New()
	r.POST("/applications/:appID/stop", appHandler.stopApplicationDeployments)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/stop", nil)
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
