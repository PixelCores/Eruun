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

type cronJobListService struct {
	noopApplicationsService
	cronJobs []*apis.CronJobInfo
	err      error
}

func (s cronJobListService) ListCronJobs(_ context.Context) ([]*apis.CronJobInfo, error) {
	return s.cronJobs, s.err
}

func TestListCronJobsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Date(2025, 10, 21, 3, 21, 28, 0, time.UTC)
	lastSchedule := now.Add(5 * time.Minute)
	lastSuccess := now.Add(5*time.Minute + 3*time.Second)

	appHandler := &applications{
		ApplicationService: cronJobListService{
			cronJobs: []*apis.CronJobInfo{
				{
					Name:                       "game-instance-rollback-cron",
					Namespace:                  "pass",
					Schedule:                   "0 */5 * * * *",
					Suspend:                    false,
					ConcurrencyPolicy:          "Allow",
					SuccessfulJobsHistoryLimit: int32Ptr(3),
					FailedJobsHistoryLimit:     int32Ptr(1),
					LastScheduleTime:           &lastSchedule,
					LastSuccessfulTime:         &lastSuccess,
					CreateTime:                 now,
				},
			},
		},
	}

	r := gin.New()
	r.GET("/cronjobs", appHandler.listCronJobs)

	req := httptest.NewRequest(http.MethodGet, "/cronjobs", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.Code)
	}

	var payload []*apis.CronJobInfo
	requireSuccessResponse(t, resp.Body.Bytes(), &payload)
	if len(payload) != 1 {
		t.Fatalf("expected 1 cronjob, got %d", len(payload))
	}
	if payload[0].Name != "game-instance-rollback-cron" || payload[0].Namespace != "pass" {
		t.Fatalf("unexpected cronjob payload: %+v", payload[0])
	}
	if payload[0].Schedule != "0 */5 * * * *" || payload[0].ConcurrencyPolicy != "Allow" {
		t.Fatalf("unexpected cronjob scheduling: %+v", payload[0])
	}
	if payload[0].SuccessfulJobsHistoryLimit == nil || *payload[0].SuccessfulJobsHistoryLimit != 3 {
		t.Fatalf("unexpected success history limit: %+v", payload[0].SuccessfulJobsHistoryLimit)
	}
	if payload[0].FailedJobsHistoryLimit == nil || *payload[0].FailedJobsHistoryLimit != 1 {
		t.Fatalf("unexpected failure history limit: %+v", payload[0].FailedJobsHistoryLimit)
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}
