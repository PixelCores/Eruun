package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type fakeConversionService struct {
	resp    *apis.ConvertApplicationsResponse
	err     error
	lastReq apis.ConvertApplicationsRequest
}

func (f *fakeConversionService) ConvertKubeResources(_ context.Context, req apis.ConvertApplicationsRequest) (*apis.ConvertApplicationsResponse, error) {
	f.lastReq = req
	return f.resp, f.err
}

func TestConvertApplicationsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeSvc := &fakeConversionService{
		resp: &apis.ConvertApplicationsResponse{
			Components: []apis.CreateComponentRequest{
				{
					Name:          "mysql",
					ComponentType: config.StoreJob,
				},
			},
			Valid: true,
		},
	}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    &fakeWorkflowService{},
		ConversionService:  fakeSvc,
	}
	r := gin.New()
	r.POST("/applications/convert", appHandler.convertApplications)

	body := []byte(`{"yaml":"kind: ConfigMap\napiVersion: v1\nmetadata:\n  name: test","validate":true}`)
	req := httptest.NewRequest(http.MethodPost, "/applications/convert", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	var payload apis.ConvertApplicationsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload.Components) != 1 || payload.Components[0].Name != "mysql" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if fakeSvc.lastReq.FileURL != "" {
		t.Fatalf("unexpected fileUrl: %s", fakeSvc.lastReq.FileURL)
	}
}

func TestConvertApplicationsEndpointPreservesLegacyValidationErrorField(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeSvc := &fakeConversionService{resp: &apis.ConvertApplicationsResponse{
		Components: []apis.CreateComponentRequest{},
		Valid:      false,
		Errors: []apis.ValidationError{{
			Field:   "component[0].name",
			Code:    apis.ErrCodeInvalidComponentName,
			Message: "invalid component name",
		}},
	}}
	appHandler := &applications{ConversionService: fakeSvc}
	r := gin.New()
	r.POST("/applications/convert", appHandler.convertApplications)

	req := httptest.NewRequest(http.MethodPost, "/applications/convert", bytes.NewReader([]byte(`{"yaml":"kind: ConfigMap"}`)))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte(`"field":"component[0].name"`)) {
		t.Fatalf("legacy field missing from response: %s", resp.Body.String())
	}
	if bytes.Contains(resp.Body.Bytes(), []byte(`"path":`)) {
		t.Fatalf("Try-only path leaked into convert response: %s", resp.Body.String())
	}
}

func TestConvertApplicationsEndpoint_FileURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeSvc := &fakeConversionService{
		resp: &apis.ConvertApplicationsResponse{
			Components: []apis.CreateComponentRequest{
				{
					Name:          "from-url",
					ComponentType: config.ConfJob,
				},
			},
			Valid: true,
		},
	}
	appHandler := &applications{
		ApplicationService: noopApplicationsService{},
		WorkflowService:    &fakeWorkflowService{},
		ConversionService:  fakeSvc,
	}
	r := gin.New()
	r.POST("/applications/convert", appHandler.convertApplications)

	body := []byte(`{"fileUrl":"http://example.com/config.yaml","yaml":"kind: ConfigMap\napiVersion: v1\nmetadata:\n  name: local"}`)
	req := httptest.NewRequest(http.MethodPost, "/applications/convert", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}
	requireSuccessResponse(t, resp.Body.Bytes(), &apis.ConvertApplicationsResponse{})
	if fakeSvc.lastReq.FileURL != "http://example.com/config.yaml" {
		t.Fatalf("unexpected fileUrl: %s", fakeSvc.lastReq.FileURL)
	}
}
