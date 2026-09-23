package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/middleware"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRunnerSandboxRequestsAreStrictAndBounded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.Auth(middleware.AuthOptions{}))
	handler := &workspaceJobs{Service: &jobs.Service{}}
	router.POST("/api/v1/job-runners/:taskID/sandboxes", handler.runnerSandboxCreate)
	router.POST("/api/v1/job-runners/:taskID/sandboxes/:trialID/release", handler.runnerSandboxRelease)
	for _, test := range []struct {
		path, body string
		status     int
	}{
		{"", `{"trialId":"trial","image":"image:v1","storageMiB":1,"podSpec":{}}`, 400},
		{"", strings.Repeat(" ", (64<<10)+1) + `{}`, 400},
		{"/trial/release", `{"collectionComplete":false,"extra":true}`, 400},
		{"/trial/release", `{"sandboxUID":"uid"}`, 400},
		{"/trial/release", strings.Repeat(" ", (64<<10)+1) + `{}`, 400},
	} {
		t.Run(test.path+test.body[:8], func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/job-runners/task/sandboxes"+test.path, strings.NewReader(test.body))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, test.status, response.Code)
			require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
		})
	}
}
