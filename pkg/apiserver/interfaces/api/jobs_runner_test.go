package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
)

func TestRunnerEventRequestIsStrictAndBounded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.RequestBodyLimit(config.DefaultRequestBodyLimitBytes))
	handler := &workspaceJobs{}
	router.POST("/api/v1/job-runners/:taskID/events", handler.runnerEvent)

	unknown := httptest.NewRequest(http.MethodPost, "/api/v1/job-runners/task/events", strings.NewReader(`{"protocolVersion":"v1","sequence":1,"kind":"claim","unknown":true}`))
	unknown.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, unknown)
	require.Equal(t, http.StatusBadRequest, response.Code)

	oversized := httptest.NewRequest(http.MethodPost, "/api/v1/job-runners/task/events", strings.NewReader(strings.Repeat("x", (64<<10)+1)))
	oversized.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, oversized)
	require.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
}
