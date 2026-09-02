package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type fakeCleanupApplicationService struct {
	noopApplicationsService
	resp *apis.CleanupApplicationResourcesResponse
	err  error
}

func (f *fakeCleanupApplicationService) ApplyApplicationResourceCleanup(
	context.Context,
	string,
	apis.CleanupApplicationResourcesRequest,
) (*apis.CleanupApplicationResourcesResponse, error) {
	return f.resp, f.err
}

func TestDeleteApplicationResourcesEndpointPartialErrorReturnsDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appSvc := &fakeCleanupApplicationService{
		resp: &apis.CleanupApplicationResourcesResponse{
			AppID:             "app-123",
			DeletedResources:  []string{"Deployment:default/web"},
			FailedResources:   []string{"Secret:default/config"},
			RetainedResources: []string{"PersistentVolumeClaim:default/data"},
		},
		err: errors.New("partial cleanup failures"),
	}
	appHandler := &applications{ApplicationService: appSvc}
	router := gin.New()
	router.DELETE("/applications/:appID/resources", appHandler.deleteApplicationResources)

	request := httptest.NewRequest(http.MethodDelete, "/applications/app-123/resources", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var payload apis.CleanupApplicationResourcesResponse
	requireSuccessResponse(t, response.Body.Bytes(), &payload)
	require.Equal(t, appSvc.resp, &payload)
}

func TestDeleteApplicationResourcesEndpointHardErrorReturnsFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	appHandler := &applications{ApplicationService: &fakeCleanupApplicationService{
		err: errors.New("cleanup unavailable"),
	}}
	router := gin.New()
	router.DELETE("/applications/:appID/resources", appHandler.deleteApplicationResources)

	request := httptest.NewRequest(http.MethodDelete, "/applications/app-123/resources", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.NotEqual(t, http.StatusOK, response.Code)
	result := decodeResponse(t, response.Body.Bytes(), nil)
	require.NotEqual(t, bcode.SuccessCode, result.Code)
}
