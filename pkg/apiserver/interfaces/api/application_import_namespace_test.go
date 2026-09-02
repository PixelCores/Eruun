package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeNamespaceImportService struct {
	resp    *apis.ImportNamespaceApplicationsResponse
	err     error
	calls   int
	lastReq apis.ImportNamespaceApplicationsRequest

	tryResp    *apis.TryImportNamespaceApplicationsResponse
	tryErr     error
	tryCalls   int
	lastTryReq apis.TryImportNamespaceApplicationsRequest
}

func (f *fakeNamespaceImportService) ImportNamespaceResources(_ context.Context, req apis.ImportNamespaceApplicationsRequest) (*apis.ImportNamespaceApplicationsResponse, error) {
	f.calls++
	f.lastReq = req
	return f.resp, f.err
}

func (f *fakeNamespaceImportService) TryImportNamespaceResources(_ context.Context, req apis.TryImportNamespaceApplicationsRequest) (*apis.TryImportNamespaceApplicationsResponse, error) {
	f.tryCalls++
	f.lastTryReq = req
	return f.tryResp, f.tryErr
}

func TestImportNamespaceApplicationsEndpoint_RejectsDefaultNamespace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeSvc := &fakeNamespaceImportService{
		resp: &apis.ImportNamespaceApplicationsResponse{},
	}
	appHandler := &applications{
		ImportService: fakeSvc,
	}
	r := gin.New()
	r.POST("/applications/import/namespace", appHandler.importNamespaceApplications)

	body := []byte(`{"namespace":"default","mode":"apply","includeKinds":["deployments"]}`)
	req := httptest.NewRequest(http.MethodPost, "/applications/import/namespace", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "default namespace")
	require.Zero(t, fakeSvc.calls)
}

func TestImportNamespaceApplicationsEndpoint_NonDefaultNamespacePassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeSvc := &fakeNamespaceImportService{
		resp: &apis.ImportNamespaceApplicationsResponse{
			Namespace: "project-ns",
			Mode:      "dry-run",
			Summary: apis.ImportNamespaceSummary{
				ResourcesScanned: 1,
			},
		},
	}
	appHandler := &applications{
		ImportService: fakeSvc,
	}
	r := gin.New()
	r.POST("/applications/import/namespace", appHandler.importNamespaceApplications)

	body := []byte(`{"namespace":"project-ns","mode":"dry-run","includeKinds":["deployments"]}`)
	req := httptest.NewRequest(http.MethodPost, "/applications/import/namespace", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.ImportNamespaceApplicationsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "project-ns", payload.Namespace)
	require.Equal(t, "dry-run", payload.Mode)
	require.Equal(t, 1, fakeSvc.calls)
	require.Equal(t, "project-ns", fakeSvc.lastReq.Namespace)
	require.Equal(t, "dry-run", fakeSvc.lastReq.Mode)
	require.Equal(t, []string{"deployments"}, fakeSvc.lastReq.IncludeKinds)
}

func TestImportNamespaceApplicationsEndpoint_RejectsUnknownAndDuplicateFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{
		`{"namespace":"production","namesapce":"typo"}`,
		`{"namespace":"production","namespace":"other"}`,
		`{"namespace":"production","applications":[{"name":"api","components":[],"unexpected":true}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			fakeSvc := &fakeNamespaceImportService{}
			appHandler := &applications{ImportService: fakeSvc}
			r := gin.New()
			r.POST("/applications/import/namespace", appHandler.importNamespaceApplications)

			req := httptest.NewRequest(http.MethodPost, "/applications/import/namespace", bytes.NewReader([]byte(body)))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			require.Equal(t, http.StatusBadRequest, resp.Code)
			require.Zero(t, fakeSvc.calls)
		})
	}
}

func TestTryImportNamespaceApplicationsEndpoint_RejectsDefaultNamespace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeSvc := &fakeNamespaceImportService{
		tryResp: &apis.TryImportNamespaceApplicationsResponse{},
	}
	appHandler := &applications{
		ImportService: fakeSvc,
	}
	r := gin.New()
	r.POST("/applications/import/namespace/try", appHandler.tryImportNamespaceApplications)

	body := []byte(`{"namespace":"default","includeKinds":["deployments"]}`)
	req := httptest.NewRequest(http.MethodPost, "/applications/import/namespace/try", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	envelope := decodeResponse(t, resp.Body.Bytes(), nil)
	require.Equal(t, bcode.ErrApplicationConfig.BusinessCode, envelope.Code)
	require.Contains(t, envelope.Message, "default namespace")
	require.Zero(t, fakeSvc.tryCalls)
}

func TestTryImportNamespaceApplicationsEndpoint_NonDefaultNamespacePassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fakeSvc := &fakeNamespaceImportService{
		tryResp: &apis.TryImportNamespaceApplicationsResponse{
			Namespace: "project-ns",
			Summary: apis.TryImportNamespaceSummary{
				ResourcesScanned: 1,
			},
		},
	}
	appHandler := &applications{
		ImportService: fakeSvc,
	}
	r := gin.New()
	r.POST("/applications/import/namespace/try", appHandler.tryImportNamespaceApplications)

	body := []byte(`{"namespace":"project-ns","includeKinds":["deployments"]}`)
	req := httptest.NewRequest(http.MethodPost, "/applications/import/namespace/try", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var payload apis.TryImportNamespaceApplicationsResponse
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	require.Equal(t, "project-ns", payload.Namespace)
	require.Equal(t, 1, payload.Summary.ResourcesScanned)
	require.Equal(t, 1, fakeSvc.tryCalls)
	require.Equal(t, "project-ns", fakeSvc.lastTryReq.Namespace)
	require.Equal(t, []string{"deployments"}, fakeSvc.lastTryReq.IncludeKinds)
}
