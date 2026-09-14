package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
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

func TestRunnerEventStopConflictReturnsAuthoritativeOutcome(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, outcome := range []string{"cancelled", "timed_out"} {
		t.Run(outcome, func(t *testing.T) {
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			runnerEventResponse(c, nil, &jobs.RunnerStopConflictError{Outcome: outcome})

			require.Equal(t, http.StatusConflict, response.Code)
			var body struct {
				Code int `json:"code"`
				Data struct {
					StopOutcome string `json:"stopOutcome"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, 34004, body.Code)
			require.Equal(t, outcome, body.Data.StopOutcome)
		})
	}

	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	runnerEventResponse(c, nil, jobs.ErrRunnerConflict)
	var generic map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &generic))
	require.Nil(t, generic["data"])
}
