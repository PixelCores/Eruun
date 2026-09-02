package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type lifecycleEndpointTest struct {
	name       string
	path       string
	newHandler func() (*applications, func() (string, *apis.WorkflowCallback))
	register   func(*gin.Engine, *applications)
}

func lifecycleEndpointTests() []lifecycleEndpointTest {
	return []lifecycleEndpointTest{
		{
			name: "restart",
			path: "/applications/app-123/restart",
			newHandler: func() (*applications, func() (string, *apis.WorkflowCallback)) {
				appSvc := &fakeRestartApplicationService{}
				return &applications{ApplicationService: appSvc, WorkflowService: &fakeWorkflowService{}}, func() (string, *apis.WorkflowCallback) {
					return appSvc.lastAppID, appSvc.lastReq.Callback
				}
			},
			register: func(r *gin.Engine, appHandler *applications) {
				r.POST("/applications/:appID/restart", appHandler.restartApplicationWorkloads)
			},
		},
		{
			name: "stop",
			path: "/applications/app-123/stop",
			newHandler: func() (*applications, func() (string, *apis.WorkflowCallback)) {
				appSvc := &fakeStopApplicationService{}
				return &applications{ApplicationService: appSvc, WorkflowService: &fakeWorkflowService{}}, func() (string, *apis.WorkflowCallback) {
					return appSvc.lastAppID, appSvc.lastReq.Callback
				}
			},
			register: func(r *gin.Engine, appHandler *applications) {
				r.POST("/applications/:appID/stop", appHandler.stopApplicationDeployments)
			},
		},
		{
			name: "start",
			path: "/applications/app-123/start",
			newHandler: func() (*applications, func() (string, *apis.WorkflowCallback)) {
				appSvc := &fakeStartApplicationService{}
				return &applications{ApplicationService: appSvc, WorkflowService: &fakeWorkflowService{}}, func() (string, *apis.WorkflowCallback) {
					return appSvc.lastAppID, appSvc.lastReq.Callback
				}
			},
			register: func(r *gin.Engine, appHandler *applications) {
				r.POST("/applications/:appID/start", appHandler.startApplicationDeployments)
			},
		},
	}
}

func TestApplicationLifecycleEndpointsAllowEmptyBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range lifecycleEndpointTests() {
		t.Run(tt.name, func(t *testing.T) {
			appHandler, observe := tt.newHandler()
			r := gin.New()
			tt.register(r, appHandler)

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			if resp.Code != http.StatusOK {
				t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
			}
			appID, callback := observe()
			if appID != "app-123" {
				t.Fatalf("unexpected appID: %s", appID)
			}
			if callback != nil {
				t.Fatalf("expected empty callback, got %+v", callback)
			}
		})
	}
}

func TestApplicationLifecycleEndpointsAcceptCallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{
		"callback": {
			"success": "https://example.com/lifecycle/success",
			"failure": "https://example.com/lifecycle/failure",
			"methods": {"success": "POST"},
			"headers": {"X-Source": "eruun"},
			"timeoutSeconds": 30
		}
	}`
	for _, tt := range lifecycleEndpointTests() {
		t.Run(tt.name, func(t *testing.T) {
			appHandler, observe := tt.newHandler()
			r := gin.New()
			tt.register(r, appHandler)

			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			if resp.Code != http.StatusOK {
				t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
			}
			appID, callback := observe()
			if appID != "app-123" {
				t.Fatalf("unexpected appID: %s", appID)
			}
			if callback == nil {
				t.Fatal("expected callback to be passed to service")
			}
			if callback.Success != "https://example.com/lifecycle/success" {
				t.Fatalf("unexpected success callback: %s", callback.Success)
			}
			if callback.Failure != "https://example.com/lifecycle/failure" {
				t.Fatalf("unexpected failure callback: %s", callback.Failure)
			}
			if callback.Methods["success"] != "POST" {
				t.Fatalf("unexpected success method: %s", callback.Methods["success"])
			}
			if callback.Headers["X-Source"] != "eruun" {
				t.Fatalf("unexpected header: %s", callback.Headers["X-Source"])
			}
			if callback.TimeoutSeconds != 30 {
				t.Fatalf("unexpected timeout: %d", callback.TimeoutSeconds)
			}
		})
	}
}

func TestApplicationLifecycleEndpointRejectsInvalidBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"callback":{"success":"https://example.com/success"},"unexpected":true}`},
		{name: "malformed json", body: `{"callback":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appSvc := &fakeStartApplicationService{}
			appHandler := &applications{
				ApplicationService: appSvc,
				WorkflowService:    &fakeWorkflowService{},
			}
			r := gin.New()
			r.POST("/applications/:appID/start", appHandler.startApplicationDeployments)

			req := httptest.NewRequest(http.MethodPost, "/applications/app-123/start", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			if resp.Code == http.StatusOK {
				t.Fatalf("expected non-OK status, body=%s", resp.Body.String())
			}
			result := decodeResponse(t, resp.Body.Bytes(), nil)
			if result.Code == bcode.SuccessCode {
				t.Fatalf("expected error response, got success")
			}
			if appSvc.lastAppID != "" {
				t.Fatalf("service should not be called, got appID=%s", appSvc.lastAppID)
			}
		})
	}
}
