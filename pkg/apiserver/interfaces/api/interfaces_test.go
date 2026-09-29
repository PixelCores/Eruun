package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	containerutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/container"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestApplicationWorkflowHandlerInjection(t *testing.T) {
	workflowService := &fakeWorkflowService{}
	handler := &applicationWorkflows{}
	beans := containerutil.NewContainer()
	require.NoError(t, beans.Provides(workflowService, handler))
	require.NoError(t, beans.Populate())
	require.Same(t, workflowService, handler.WorkflowService)
}

func TestHandlersKeepServerDependenciesIsolated(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newRouter := func(runtime RuntimeReadiness) *gin.Engine {
		router := gin.New()
		for _, handler := range NewHandlers() {
			if h, ok := handler.(*health); ok {
				h.Runtime = runtime
			}
			handler.RegisterRoutes(router.Group("/api/v1"))
		}
		return router
	}
	first := newRouter(mockRuntimeReadiness{ready: true})
	second := newRouter(mockRuntimeReadiness{reason: "second server initializing"})

	// Registering and injecting the second server must not change the first.
	for _, tc := range []struct {
		name   string
		router *gin.Engine
		status int
	}{
		{"first", first, http.StatusOK},
		{"second", second, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 20 {
				resp := httptest.NewRecorder()
				tc.router.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/v1/ready", nil))
				require.Equal(t, tc.status, resp.Code)
			}
		})
	}
}
