package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeComponentContainersApplicationService struct {
	noopApplicationsService
	lastAppID         string
	lastComponentName string
	resp              *apis.ComponentContainersResponse
	err               error
}

func (f *fakeComponentContainersApplicationService) ListComponentContainers(_ context.Context, appID, componentName string) (*apis.ComponentContainersResponse, error) {
	f.lastAppID = appID
	f.lastComponentName = componentName
	if f.resp == nil {
		f.resp = &apis.ComponentContainersResponse{
			AppID:         appID,
			ComponentName: componentName,
			Pods:          []apis.ComponentPodContainers{},
		}
	}
	return f.resp, f.err
}

func TestListComponentContainersEndpointPassesRouteParams(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentContainersApplicationService{err: bcode.ErrComponentNotFound}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.GET("/applications/:appID/components/:componentName/containers", appHandler.listComponentContainers)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/api/containers", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	if envelope.Code != bcode.ErrComponentNotFound.BusinessCode {
		t.Fatalf("unexpected business code: %d message=%s", envelope.Code, envelope.Message)
	}
	if appSvc.lastAppID != "app-1" || appSvc.lastComponentName != "api" {
		t.Fatalf("unexpected app/component args: appID=%s component=%s", appSvc.lastAppID, appSvc.lastComponentName)
	}
}

func TestListComponentContainersEndpointReturnsPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)

	appSvc := &fakeComponentContainersApplicationService{
		resp: &apis.ComponentContainersResponse{
			AppID:         "app-1",
			ComponentName: "api",
			ComponentType: "webservice",
			Pods: []apis.ComponentPodContainers{
				{
					PodName:   "pod-api-1",
					Namespace: "default",
					Phase:     "Running",
					Containers: []apis.ComponentContainerInfo{
						{
							Name:         "api",
							Image:        "nginx:1.25",
							Ready:        true,
							RestartCount: 0,
							State:        "running",
						},
					},
				},
			},
		},
	}
	appHandler := &applications{ApplicationService: appSvc}

	r := gin.New()
	r.GET("/applications/:appID/components/:componentName/containers", appHandler.listComponentContainers)

	req := httptest.NewRequest(http.MethodGet, "/applications/app-1/components/api/containers", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", resp.Code, resp.Body.String())
	}
	var payload apis.ComponentContainersResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if payload.AppID != "app-1" || payload.ComponentName != "api" {
		t.Fatalf("unexpected payload app/component: %+v", payload)
	}
	if len(payload.Pods) != 1 || payload.Pods[0].PodName != "pod-api-1" {
		t.Fatalf("unexpected pods payload: %+v", payload.Pods)
	}
	if len(payload.Pods[0].Containers) != 1 || payload.Pods[0].Containers[0].Name != "api" {
		t.Fatalf("unexpected container payload: %+v", payload.Pods[0].Containers)
	}
	if appSvc.lastAppID != "app-1" || appSvc.lastComponentName != "api" {
		t.Fatalf("unexpected app/component args: appID=%s component=%s", appSvc.lastAppID, appSvc.lastComponentName)
	}
}
