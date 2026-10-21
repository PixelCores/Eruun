package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeLogArchiveDownloadApplicationService struct {
	noopApplicationsService
	lastAppID string
	lastReq   apis.LogArchiveDownloadRequest
	stream    *applicationservice.ComponentFileArchiveStream
	err       error
}

func (f *fakeLogArchiveDownloadApplicationService) DownloadLogArchive(_ context.Context, appID string, req apis.LogArchiveDownloadRequest) (*applicationservice.ComponentFileArchiveStream, error) {
	f.lastAppID = appID
	f.lastReq = req
	if f.stream == nil && f.err == nil {
		f.stream = &applicationservice.ComponentFileArchiveStream{
			Reader:        io.NopCloser(strings.NewReader("zip")),
			PodName:       "pod-default",
			ContainerName: "main",
			FileName:      "log-archive.zip",
			ContentType:   "application/zip",
		}
	}
	return f.stream, f.err
}

func TestLogArchiveDownloadEndpointWritesArchiveStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeLogArchiveDownloadApplicationService{
		stream: &applicationservice.ComponentFileArchiveStream{
			Reader:        io.NopCloser(strings.NewReader("zip-bytes")),
			Namespace:     "default",
			PodName:       "pod-worker",
			ContainerName: "worker",
			FileName:      "worker.zip",
			ContentType:   "application/zip",
		},
	}
	appHandler := &applications{ApplicationService: appSvc}
	r := gin.New()
	r.POST("/applications/:appID/log-archives", appHandler.downloadLogArchive)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/log-archives", strings.NewReader(`{
		"components":["worker"],
		"path":"/data/logs/archive",
		"container":"worker"
	}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Type"); got != "application/zip" {
		t.Fatalf("unexpected content type: %s", got)
	}
	if got := resp.Header().Get("Content-Disposition"); !strings.Contains(got, `filename="worker.zip"`) {
		t.Fatalf("unexpected content disposition: %s", got)
	}
	if got := resp.Header().Get("X-Eruun-Pod"); got != "pod-worker" {
		t.Fatalf("unexpected pod header: %s", got)
	}
	if got := resp.Header().Get("X-Eruun-Container"); got != "worker" {
		t.Fatalf("unexpected container header: %s", got)
	}
	if body := resp.Body.String(); body != "zip-bytes" {
		t.Fatalf("unexpected body: %q", body)
	}
	if appSvc.lastAppID != "app-123" {
		t.Fatalf("expected appID app-123, got %s", appSvc.lastAppID)
	}
	if len(appSvc.lastReq.Components) != 1 || appSvc.lastReq.Components[0] != "worker" {
		t.Fatalf("unexpected request: %+v", appSvc.lastReq)
	}
	if appSvc.lastReq.Path != "/data/logs/archive" || appSvc.lastReq.Container != "worker" {
		t.Fatalf("unexpected path/container: %+v", appSvc.lastReq)
	}
}

func TestLogArchiveDownloadEndpointRejectsInvalidRequest(t *testing.T) {
	tests := []struct {
		name string
		body string
		code int32
	}{
		{name: "malformed body", body: `{"components":`, code: bcode.ErrApplicationConfig.BusinessCode},
		{name: "removed jobType field", body: `{"jobType":"log_archive_upload","components":["api"],"path":"/data/logs"}`, code: bcode.ErrApplicationConfig.BusinessCode},
		{name: "removed name and mode fields", body: `{"name":"archive","mode":"StepByStep","components":["api"],"path":"/data/logs"}`, code: bcode.ErrApplicationConfig.BusinessCode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			appSvc := &fakeLogArchiveDownloadApplicationService{}
			appHandler := &applications{ApplicationService: appSvc}
			r := gin.New()
			r.POST("/applications/:appID/log-archives", appHandler.downloadLogArchive)

			req := httptest.NewRequest(http.MethodPost, "/applications/app-123/log-archives", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
			}
			result := decodeResponse(t, resp.Body.Bytes(), nil)
			if result.Code != tt.code {
				t.Fatalf("expected business code %d, got %+v", tt.code, result)
			}
			if appSvc.lastAppID != "" {
				t.Fatalf("service should not be called on invalid request")
			}
		})
	}
}

func TestLogArchiveDownloadEndpointBusinessError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeLogArchiveDownloadApplicationService{
		err: fmt.Errorf("%w: component %q does not use pods", bcode.ErrApplicationConfig, "config"),
	}
	appHandler := &applications{ApplicationService: appSvc}
	r := gin.New()
	r.POST("/applications/:appID/log-archives", appHandler.downloadLogArchive)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-123/log-archives", strings.NewReader(`{"components":["config"],"path":"/data/logs"}`))
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
