package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

type fakeComponentPodOperationsService struct {
	noopApplicationsService
	lastExportAppID         string
	lastExportComponentName string
	lastExportReq           apis.ExportComponentFilesRequest
	exportStream            *service.ComponentFileArchiveStream
	exportErr               error
	lastExecAppID           string
	lastExecComponentName   string
	lastExecReq             apis.ExecComponentShellScriptRequest
	execResp                *apis.ExecComponentShellScriptResponse
	execErr                 error
	lastStreamAppID         string
	lastStreamComponentName string
	lastStreamReq           apis.ExecComponentShellScriptRequest
	streamResp              *service.ComponentShellScriptStream
	streamErr               error
}

func (f *fakeComponentPodOperationsService) ExportComponentFilesZip(_ context.Context, appID, componentName string, req apis.ExportComponentFilesRequest) (*service.ComponentFileArchiveStream, error) {
	f.lastExportAppID = appID
	f.lastExportComponentName = componentName
	f.lastExportReq = req
	if f.exportStream == nil {
		f.exportStream = &service.ComponentFileArchiveStream{
			Reader:        io.NopCloser(strings.NewReader("zip")),
			PodName:       "pod-default",
			ContainerName: "main",
			FileName:      "component.zip",
			ContentType:   "application/zip",
		}
	}
	return f.exportStream, f.exportErr
}

func (f *fakeComponentPodOperationsService) ExecComponentShellScript(_ context.Context, appID, componentName string, req apis.ExecComponentShellScriptRequest) (*apis.ExecComponentShellScriptResponse, error) {
	f.lastExecAppID = appID
	f.lastExecComponentName = componentName
	f.lastExecReq = req
	if f.execResp == nil {
		f.execResp = &apis.ExecComponentShellScriptResponse{
			Namespace:     "default",
			PodName:       "pod-default",
			ContainerName: "main",
			ExitCode:      0,
			Succeeded:     true,
		}
	}
	return f.execResp, f.execErr
}

func (f *fakeComponentPodOperationsService) StreamComponentShellScript(_ context.Context, appID, componentName string, req apis.ExecComponentShellScriptRequest) (*service.ComponentShellScriptStream, error) {
	f.lastStreamAppID = appID
	f.lastStreamComponentName = componentName
	f.lastStreamReq = req
	if f.streamResp == nil {
		events := make(chan kube.PodShellStreamEvent, 1)
		events <- kube.PodShellStreamEvent{Type: kube.PodShellStreamEventExit, ExitCode: 0, Succeeded: true}
		close(events)
		f.streamResp = &service.ComponentShellScriptStream{
			Namespace:     "default",
			PodName:       "pod-default",
			ContainerName: "main",
			Events:        events,
		}
	}
	return f.streamResp, f.streamErr
}

func TestExportComponentFilesZipEndpointWritesArchiveHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentPodOperationsService{
		exportStream: &service.ComponentFileArchiveStream{
			Reader:        io.NopCloser(strings.NewReader("zip-bytes")),
			Namespace:     "default",
			PodName:       "pod-api",
			ContainerName: "sidecar",
			FileName:      "api.zip",
			ContentType:   "application/zip",
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.POST("/applications/:appID/components/:componentName/files/export", appHandler.exportComponentFilesZip)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/files/export", strings.NewReader(`{"path":"/tmp/out","container":"sidecar"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Type"); got != "application/zip" {
		t.Fatalf("unexpected content type: %s", got)
	}
	if got := resp.Header().Get("Content-Disposition"); !strings.Contains(got, `filename="api.zip"`) {
		t.Fatalf("unexpected content disposition: %s", got)
	}
	if got := resp.Header().Get("X-Eruun-Pod"); got != "pod-api" {
		t.Fatalf("unexpected pod header: %s", got)
	}
	if got := resp.Header().Get("X-Eruun-Container"); got != "sidecar" {
		t.Fatalf("unexpected container header: %s", got)
	}
	if body := resp.Body.String(); body != "zip-bytes" {
		t.Fatalf("unexpected body: %q", body)
	}
	if appSvc.lastExportAppID != "app-1" || appSvc.lastExportComponentName != "api" {
		t.Fatalf("unexpected app/component args: %s %s", appSvc.lastExportAppID, appSvc.lastExportComponentName)
	}
	if appSvc.lastExportReq.Path != "/tmp/out" || appSvc.lastExportReq.Container != "sidecar" {
		t.Fatalf("unexpected export request: %+v", appSvc.lastExportReq)
	}
}

func TestExportComponentFilesZipEndpointReturnsErrorBeforeHeadersOnEmptyStream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentPodOperationsService{
		exportStream: &service.ComponentFileArchiveStream{
			Reader:        failingReadCloser{err: errors.New("tar missing")},
			Namespace:     "default",
			PodName:       "pod-api",
			ContainerName: "api",
			FileName:      "api.zip",
			ContentType:   "application/zip",
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.POST("/applications/:appID/components/:componentName/files/export", appHandler.exportComponentFilesZip)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/files/export", strings.NewReader(`{"path":"/tmp/out"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrServer.BusinessCode {
		t.Fatalf("unexpected business code: %d message=%s", envelope.Code, envelope.Message)
	}
	if ct := resp.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("expected json error response, got %q", ct)
	}
}

func TestExportComponentFilesZipEndpointMapsLookupErrorsOnProbeToInvalidPath(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentPodOperationsService{
		exportStream: &service.ComponentFileArchiveStream{
			Reader:        failingReadCloser{err: errors.New("read tar stream: archive pod path: exit error: tar: out: cannot stat: no such file or directory")},
			Namespace:     "default",
			PodName:       "pod-api",
			ContainerName: "api",
			FileName:      "api.zip",
			ContentType:   "application/zip",
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.POST("/applications/:appID/components/:componentName/files/export", appHandler.exportComponentFilesZip)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/files/export", strings.NewReader(`{"path":"/tmp/missing"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrComponentFilePathInvalid.BusinessCode {
		t.Fatalf("unexpected business code: %d message=%s", envelope.Code, envelope.Message)
	}
	if ct := resp.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("expected json error response, got %q", ct)
	}
}

func TestExportComponentFilesZipEndpointSupportsMultipartFallbackHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	body := "--boundary\r\nContent-Type: application/octet-stream\r\n\r\nhello\r\n--boundary--\r\n"
	appSvc := &fakeComponentPodOperationsService{
		exportStream: &service.ComponentFileArchiveStream{
			Reader:        io.NopCloser(strings.NewReader(body)),
			Namespace:     "default",
			PodName:       "pod-api",
			ContainerName: "api",
			FileName:      "api.multipart",
			ContentType:   "multipart/mixed; boundary=boundary",
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.POST("/applications/:appID/components/:componentName/files/export", appHandler.exportComponentFilesZip)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/files/export", strings.NewReader(`{"path":"/tmp/out"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Type"); got != "multipart/mixed; boundary=boundary" {
		t.Fatalf("unexpected content type: %s", got)
	}
	if got := resp.Header().Get("Content-Disposition"); !strings.Contains(got, `filename="api.multipart"`) {
		t.Fatalf("unexpected content disposition: %s", got)
	}
	if got := resp.Body.String(); got != body {
		t.Fatalf("unexpected body: %q", got)
	}
}

func TestExecComponentShellScriptEndpointReturnsSyncResult(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentPodOperationsService{
		execResp: &apis.ExecComponentShellScriptResponse{
			Namespace:     "default",
			PodName:       "pod-api",
			ContainerName: "api",
			Stdout:        "done",
			Stderr:        "warning",
			ExitCode:      7,
			Succeeded:     false,
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.POST("/applications/:appID/components/:componentName/shell/exec", appHandler.execComponentShellScript)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/shell/exec", strings.NewReader(`{"script":"echo done","container":"api"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	var payload apis.ExecComponentShellScriptResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.ExitCode != 7 || payload.Succeeded {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload.PodName != "pod-api" || payload.ContainerName != "api" {
		t.Fatalf("unexpected pod/container: %+v", payload)
	}
	if appSvc.lastExecReq.Script != "echo done" || appSvc.lastExecReq.Container != "api" {
		t.Fatalf("unexpected exec request: %+v", appSvc.lastExecReq)
	}
}

func TestExecComponentShellScriptEndpointRejectsEmptyScript(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentPodOperationsService{}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.POST("/applications/:appID/components/:componentName/shell/exec", appHandler.execComponentShellScript)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/shell/exec", strings.NewReader(`{"script":"   "}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrComponentShellScriptInvalid.BusinessCode {
		t.Fatalf("unexpected business code: %d message=%s", envelope.Code, envelope.Message)
	}
	if appSvc.lastExecAppID != "" {
		t.Fatalf("service should not be called when script is empty")
	}
}

func TestStreamComponentShellScriptEndpointWritesSSEEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)

	events := make(chan kube.PodShellStreamEvent, 3)
	events <- kube.PodShellStreamEvent{Type: kube.PodShellStreamEventStdout, Chunk: "hello\n"}
	events <- kube.PodShellStreamEvent{Type: kube.PodShellStreamEventStderr, Chunk: "warn\n"}
	events <- kube.PodShellStreamEvent{Type: kube.PodShellStreamEventExit, ExitCode: 3, Succeeded: false}
	close(events)
	appSvc := &fakeComponentPodOperationsService{
		streamResp: &service.ComponentShellScriptStream{
			Namespace:     "default",
			PodName:       "pod-api",
			ContainerName: "api",
			Events:        events,
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.POST("/applications/:appID/components/:componentName/shell/stream", appHandler.streamComponentShellScript)

	req := httptest.NewRequest(http.MethodPost, "/applications/app-1/components/api/shell/stream", strings.NewReader(`{"script":"echo done","container":"api"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	if ct := resp.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("unexpected content type: %s", ct)
	}
	if pod := resp.Header().Get("X-Eruun-Pod"); pod != "pod-api" {
		t.Fatalf("unexpected pod header: %s", pod)
	}
	if container := resp.Header().Get("X-Eruun-Container"); container != "api" {
		t.Fatalf("unexpected container header: %s", container)
	}
	body := resp.Body.String()
	if !strings.Contains(body, "event: stdout") || !strings.Contains(body, "event: stderr") || !strings.Contains(body, "event: exit") {
		t.Fatalf("unexpected sse payload: %q", body)
	}
	if appSvc.lastStreamAppID != "app-1" || appSvc.lastStreamComponentName != "api" {
		t.Fatalf("unexpected stream app/component args: %s %s", appSvc.lastStreamAppID, appSvc.lastStreamComponentName)
	}
	if appSvc.lastStreamReq.Script != "echo done" || appSvc.lastStreamReq.Container != "api" {
		t.Fatalf("unexpected stream request: %+v", appSvc.lastStreamReq)
	}
}

type failingReadCloser struct {
	err error
}

func (r failingReadCloser) Read(_ []byte) (int, error) {
	return 0, r.err
}

func (r failingReadCloser) Close() error {
	return nil
}
