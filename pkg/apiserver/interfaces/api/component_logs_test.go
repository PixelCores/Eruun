package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeComponentLogsApplicationService struct {
	noopApplicationsService
	lastAppID              string
	lastComponentName      string
	lastRequestedContainer string
	stream                 *service.ComponentLogStream
	err                    error
}

func (f *fakeComponentLogsApplicationService) StreamComponentLogs(_ context.Context, appID, componentName, requestedContainer string) (*service.ComponentLogStream, error) {
	f.lastAppID = appID
	f.lastComponentName = componentName
	f.lastRequestedContainer = requestedContainer
	if f.stream == nil {
		f.stream = &service.ComponentLogStream{
			Reader:        io.NopCloser(strings.NewReader("")),
			PodName:       "pod-default",
			ContainerName: "main",
		}
	}
	return f.stream, f.err
}

func TestStreamComponentLogsEndpointPassesContainerQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentLogsApplicationService{err: bcode.ErrComponentLogContainerInvalid}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.GET("/applications/:appID/components/:componentName/logs", appHandler.streamComponentLogs)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/api/logs?container=sidecar", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrComponentLogContainerInvalid.BusinessCode {
		t.Fatalf("unexpected business code: %d message=%s", envelope.Code, envelope.Message)
	}
	if appSvc.lastAppID != "app-1" || appSvc.lastComponentName != "api" {
		t.Fatalf("unexpected app/component args: appID=%s component=%s", appSvc.lastAppID, appSvc.lastComponentName)
	}
	if appSvc.lastRequestedContainer != "sidecar" {
		t.Fatalf("expected requested container sidecar, got %s", appSvc.lastRequestedContainer)
	}
}

func TestStreamComponentLogsEndpointWritesContainerHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentLogsApplicationService{
		stream: &service.ComponentLogStream{
			Reader:        io.NopCloser(strings.NewReader("line one\n")),
			Namespace:     "default",
			PodName:       "pod-api",
			ContainerName: "api",
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.GET("/applications/:appID/components/:componentName/logs", appHandler.streamComponentLogs)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/api/logs", nil)
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
	if body := resp.Body.String(); !strings.Contains(body, "data: line one\n\n") {
		t.Fatalf("unexpected sse payload: %q", body)
	}
	if appSvc.lastRequestedContainer != "" {
		t.Fatalf("expected empty requested container, got %s", appSvc.lastRequestedContainer)
	}
}
