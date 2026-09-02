package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

type scheduledJobListService struct {
	noopApplicationsService
	jobs []*apis.ScheduledJobInfo
	err  error
}

func (s scheduledJobListService) ListScheduledJobs(_ context.Context) ([]*apis.ScheduledJobInfo, error) {
	return s.jobs, s.err
}

func TestListScheduledJobsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Date(2025, 10, 21, 3, 21, 28, 0, time.UTC)

	appHandler := &applications{
		ApplicationService: scheduledJobListService{
			jobs: []*apis.ScheduledJobInfo{
				{
					AppID:              "app-1",
					AppName:            "demo-app",
					AppNamespace:       "default",
					ComponentName:      "cron-task",
					ComponentNamespace: "default",
					Image:              "busybox:1.36",
					Schedule:           "0 */5 * * * *",
					RunPolicy:          "skip_if_completed",
					CreateTime:         now,
					UpdateTime:         now.Add(2 * time.Minute),
				},
			},
		},
	}

	r := gin.New()
	r.GET("/scheduledjobs", appHandler.listScheduledJobs)

	req := httptest.NewRequest(http.MethodGet, "/scheduledjobs", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload []*apis.ScheduledJobInfo
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload) != 1 {
		t.Fatalf("expected 1 scheduled job, got %d", len(payload))
	}
	if payload[0].AppID != "app-1" || payload[0].ComponentName != "cron-task" {
		t.Fatalf("unexpected scheduled job payload: %+v", payload[0])
	}
}
