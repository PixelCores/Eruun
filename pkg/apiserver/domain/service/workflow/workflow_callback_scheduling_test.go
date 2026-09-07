package workflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

func TestTerminalCallbackUsesGlobalAdmissionWithoutWorker(t *testing.T) {
	for _, outcome := range []string{"admitted", "duplicate while queued", "duplicate while admitted", "cancelled while queued", "parent status changed", "parent generation changed"} {
		t.Run(outcome, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "callbacks.db")), &gorm.Config{
				NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent),
			})
			require.NoError(t, err)
			connection, err := db.DB()
			require.NoError(t, err)
			connection.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			require.NoError(t, db.AutoMigrate(&model.Applications{}, &model.ApplicationComponent{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.SystemSetting{}))
			raw := &sqlstore.Driver{Client: *db}
			store := account.NewStore(raw)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
			require.NoError(t, raw.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler,
				Value: json.RawMessage(`{"strategy":"priority","maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1,"agingSeconds":60}`)}))
			app := &model.Applications{ID: "app", Name: "app", WorkspaceID: "workspace", Namespace: "workspace-ns"}
			require.NoError(t, raw.Add(ctx, app))
			ctx = account.WithScope(ctx, account.ForWorkspace(&model.Workspace{ID: app.WorkspaceID, Namespace: app.Namespace}))
			var sends atomic.Int32
			idempotencyKey := make(chan string, 1)
			allowResponse := make(chan struct{})
			var releaseOnce sync.Once
			releaseResponse := func() { releaseOnce.Do(func() { close(allowResponse) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sends.Add(1)
				select {
				case idempotencyKey <- r.Header.Get("Idempotency-Key"):
				default:
				}
				if outcome == "duplicate while admitted" {
					<-allowResponse
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			defer releaseResponse()
			callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{Cancelled: server.URL})
			require.NoError(t, err)
			// A task cancelled before its first Worker has no run token or
			// generation. Historical queues may also lack WorkspaceID.
			task := &model.WorkflowQueue{TaskID: "task", AppID: app.ID, Status: config.StatusCancelled, Type: config.WorkflowTaskTypeWorkflow, Callback: callback}
			require.NoError(t, raw.Add(ctx, task))
			service := withAllowPrivateURLPolicy(t, &workflowServiceImpl{Store: store, Cfg: &config.Config{}})
			done := make(chan struct{})
			go func() {
				defer close(done)
				service.triggerWorkflowTerminalCallbackOnApprovalAction(ctx, task, config.StatusCancelled, "cancel before execution")
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("terminal callback did not stop")
				}
			})
			require.Eventually(t, func() bool {
				var count int64
				return db.Model(&model.JobInfo{}).Where("scheduling_state = ?", "queued").Count(&count).Error == nil && count == 1
			}, time.Second, 10*time.Millisecond)
			require.Zero(t, sends.Load(), "terminal HTTP callback must wait for global admission")
			duplicate := func() {
				duplicateDone := make(chan struct{})
				go func() {
					defer close(duplicateDone)
					// Both calls inherit the same earlier parent deadline. A
					// deadline alone cannot distinguish concurrent callbacks.
					service.triggerWorkflowTerminalCallbackOnApprovalAction(ctx, task, config.StatusCancelled, "concurrent duplicate")
				}()
				t.Cleanup(func() {
					cancel()
					select {
					case <-duplicateDone:
					case <-time.After(5 * time.Second):
						t.Error("duplicate callback did not stop")
					}
				})
				select {
				case <-duplicateDone:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("concurrent callback was not rejected while the first remains active")
				}
			}
			var sentKey string
			switch outcome {
			case "admitted", "duplicate while queued", "duplicate while admitted":
				if outcome == "duplicate while queued" {
					duplicate()
					require.Zero(t, sends.Load())
				}
				admitted, err := repository.AdmitQueuedJobs(context.Background(), store)
				require.NoError(t, err)
				require.Equal(t, 1, admitted)
				if outcome == "duplicate while admitted" {
					select {
					case sentKey = <-idempotencyKey:
					case <-time.After(time.Second):
						t.Fatal("admitted callback did not start HTTP request")
					}
					duplicate()
					require.Equal(t, int32(1), sends.Load(), "duplicate callback cannot share the first admission")
					releaseResponse()
				}
			case "cancelled while queued":
				cancel()
			case "parent status changed":
				require.NoError(t, db.Model(&model.WorkflowQueue{TaskID: task.TaskID}).Update("status", config.StatusWaiting).Error)
			case "parent generation changed":
				require.NoError(t, db.Model(&model.WorkflowQueue{TaskID: task.TaskID}).Update("run_generation", 1).Error)
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("callback did not finish after admission or invalidation")
			}
			_, err = repository.AdmitQueuedJobs(context.Background(), store)
			require.NoError(t, err)
			var record model.JobInfo
			require.NoError(t, db.First(&record).Error)
			require.Equal(t, app.WorkspaceID, record.WorkspaceID)
			require.Equal(t, "released", record.SchedulingState)
			if outcome == "admitted" || outcome == "duplicate while queued" || outcome == "duplicate while admitted" {
				require.Equal(t, int32(1), sends.Load())
				require.Equal(t, string(config.StatusCompleted), record.Status)
				if sentKey == "" {
					sentKey = <-idempotencyKey
				}
				require.Equal(t, workflowjob.TerminalCallbackExecutionKey(task.TaskID, 0, "cancelled"), sentKey)
				service.triggerWorkflowTerminalCallbackOnApprovalAction(ctx, task, config.StatusCancelled, "duplicate trigger")
				require.Equal(t, int32(1), sends.Load(), "committed callback must not execute again")
				var records int64
				require.NoError(t, db.Model(&model.JobInfo{}).Count(&records).Error)
				require.EqualValues(t, 1, records)
			} else {
				require.Zero(t, sends.Load(), "invalidated callback must not send HTTP")
			}
		})
	}
}
