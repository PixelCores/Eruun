package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type fakeResourceImportService struct {
	scanRequest   apisv1.ResourceImportScanJobRequest
	manageRequest apisv1.ResourceImportManageJobRequest
	scanCalls     int
	manageCalls   int
}

func (f *fakeResourceImportService) ImportNamespaceResources(context.Context, apisv1.ImportNamespaceApplicationsRequest) (*apisv1.ImportNamespaceApplicationsResponse, error) {
	return nil, nil
}

func (f *fakeResourceImportService) TryImportNamespaceResources(context.Context, apisv1.TryImportNamespaceApplicationsRequest) (*apisv1.TryImportNamespaceApplicationsResponse, error) {
	return nil, nil
}

func (f *fakeResourceImportService) SubmitScanJob(_ context.Context, request apisv1.ResourceImportScanJobRequest) (*apisv1.ResourceImportJobAcceptedResponse, error) {
	f.scanCalls++
	f.scanRequest = request
	return &apisv1.ResourceImportJobAcceptedResponse{
		TaskID: "scan-task-1",
		Type:   config.WorkflowTaskTypeResourceImportScan,
		Status: string(config.StatusWaiting),
	}, nil
}

func (f *fakeResourceImportService) SubmitManageJob(_ context.Context, request apisv1.ResourceImportManageJobRequest) (*apisv1.ResourceImportJobAcceptedResponse, error) {
	f.manageCalls++
	f.manageRequest = request
	return &apisv1.ResourceImportJobAcceptedResponse{
		TaskID: "manage-task-1",
		Type:   config.WorkflowTaskTypeResourceImportManage,
		Status: string(config.StatusWaiting),
	}, nil
}

func (f *fakeResourceImportService) GetJob(context.Context, string) (*apisv1.ResourceImportJobResponse, error) {
	return &apisv1.ResourceImportJobResponse{
		TaskID: "scan-task-1",
		Type:   config.WorkflowTaskTypeResourceImportScan,
		Status: string(config.StatusCompleted),
	}, nil
}

func TestResourceImportScanEndpointReturnsAcceptedTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service := &fakeResourceImportService{}
	handler := &resourceImports{Service: service}
	router := gin.New()
	handler.RegisterRoutes(router.Group("/api/v1"))
	body := []byte(`{"namespace":"team-production","rules":[{"kinds":["Deployment"],"nameRegex":"^payments-"}]}`)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/resource-import/jobs/scan", bytes.NewReader(body)))

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, "team-production", service.scanRequest.Namespace)
	require.Len(t, service.scanRequest.Rules, 1)
	assert.JSONEq(t, `{"code":0,"message":"","data":{"taskId":"scan-task-1","type":"resource_import_scan","status":"waiting"}}`, recorder.Body.String())
}

func TestResourceImportEndpointsRejectUnknownFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name string
		path string
		body string
	}{
		{
			name: "scan",
			path: "/api/v1/resource-import/jobs/scan",
			body: `{"namespace":"team-production","rules":[],"unexpected":true}`,
		},
		{
			name: "manage",
			path: "/api/v1/resource-import/jobs/manage",
			body: `{"scanTaskId":"scan-task-1","applications":[],"unexpected":true}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &fakeResourceImportService{}
			handler := &resourceImports{Service: service}
			router := gin.New()
			handler.RegisterRoutes(router.Group("/api/v1"))
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader([]byte(tc.body))))

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			assert.Zero(t, service.scanCalls)
			assert.Zero(t, service.manageCalls)
		})
	}
}
