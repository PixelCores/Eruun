package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type canonicalSpecApplicationService struct {
	noopApplicationsService
	spec *apis.CreateApplicationsRequest
}

func (s canonicalSpecApplicationService) GetApplicationSpec(context.Context, string) (*apis.CreateApplicationsRequest, error) {
	return s.spec, nil
}

func TestGetApplicationSpecReturnsResubmittableCanonicalPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &applications{ApplicationService: canonicalSpecApplicationService{spec: &apis.CreateApplicationsRequest{
		ID:         "app-1",
		Name:       "demo",
		Components: []apis.CreateComponentRequest{{Name: "web"}},
		Workflow:   []apis.CreateWorkflowStepRequest{{Name: "deploy", Components: []string{"web"}}},
	}}}
	router := gin.New()
	router.GET("/applications/:appID/spec", handler.getApplicationSpec)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/applications/app-1/spec", nil))

	require.Equal(t, http.StatusOK, response.Code)
	var spec apis.CreateApplicationsRequest
	requireSuccessResponse(t, response.Body.Bytes(), &spec)
	require.Equal(t, "app-1", spec.ID)
	require.Len(t, spec.Components, 1)
	require.Len(t, spec.Workflow, 1)
	require.NotContains(t, response.Body.String(), `"component":`)
}
