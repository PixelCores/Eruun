//go:build integration

package workflow

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestWorkflowSubmissionMySQLSerializesAfterApplicationLockLoss(t *testing.T) {
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is required for MySQL application scheduling tests")
	}
	parsed, err := mysqldsn.ParseDSN(dsn)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(parsed.DBName, "eruun_scheduler_test"), "use an isolated eruun_scheduler_test schema")
	db, err := gorm.Open(mysqlgorm.Open(dsn), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	connection, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	models := []any{&model.Applications{}, &model.Workflow{}, &model.WorkflowQueue{}, &model.JobInfo{}}
	for _, entity := range models {
		require.False(t, db.Migrator().HasTable(entity), "integration schema must be empty")
	}
	require.NoError(t, db.AutoMigrate(models...))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(models...)) })
	raw := &sqlstore.Driver{Client: *db}

	t.Run("missing application", func(t *testing.T) {
		ctx := context.Background()
		steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "approval", StepType: config.WorkflowStepTypeApproval}}})
		require.NoError(t, err)
		require.NoError(t, raw.Add(ctx, &model.Workflow{ID: "orphan-workflow", AppID: "missing-app", Steps: steps}))
		service := &workflowServiceImpl{Store: raw, ScheduleLocker: locker.NewNoopLocker("missing-app")}
		_, err = service.ExecWorkflowTaskForApp(ctx, "missing-app", "orphan-workflow", 0, "")
		require.ErrorIs(t, err, datastore.ErrRecordNotExist)
		var count int64
		require.NoError(t, db.Model(&model.WorkflowQueue{}).Where("app_id = ?", "missing-app").Count(&count).Error)
		require.Zero(t, count)
	})
	t.Run("independent future tasks remain allowed", func(t *testing.T) {
		ctx := context.Background()
		steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "approval", StepType: config.WorkflowStepTypeApproval}}})
		require.NoError(t, err)
		require.NoError(t, raw.Add(ctx, &model.Applications{ID: "future-app", Name: "future"}))
		require.NoError(t, raw.Add(ctx, &model.Workflow{ID: "future-workflow", AppID: "future-app", Steps: steps}))
		service := &workflowServiceImpl{Store: raw, ScheduleLocker: locker.NewNoopLocker("future")}
		first, err := service.ExecWorkflowTaskForApp(ctx, "future-app", "future-workflow", time.Now().Add(time.Hour).Unix(), "future-first")
		require.NoError(t, err)
		second, err := service.ExecWorkflowTaskForApp(ctx, "future-app", "future-workflow", time.Now().Add(2*time.Hour).Unix(), "future-second")
		require.NoError(t, err)
		require.NotEqual(t, first.TaskID, second.TaskID)
	})

	for _, key := range []string{"", "different-key", "same-key"} {
		t.Run("second key="+key, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			appID, workflowID := "app-"+key, "workflow-"+key
			steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "approval", StepType: config.WorkflowStepTypeApproval}}})
			require.NoError(t, err)
			require.NoError(t, raw.Add(ctx, &model.Applications{ID: appID, Name: appID}))
			require.NoError(t, raw.Add(ctx, &model.Workflow{ID: workflowID, AppID: appID, Steps: steps}))
			paused, resume := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-resume:
				default:
					close(resume)
				}
			}()
			firstStore := &schedulingPauseStore{DataStore: raw, beforeTaskInsert: func(ctx context.Context) error {
				close(paused)
				select {
				case <-resume:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			// Model Redis lease loss: both processes believe they may enter the
			// app operation. The database must still serialize their submissions.
			first := &workflowServiceImpl{Store: firstStore, ScheduleLocker: locker.NewNoopLocker("lost-lease")}
			second := &workflowServiceImpl{Store: raw, ScheduleLocker: locker.NewNoopLocker("new-owner")}
			type result struct {
				response *apis.ExecWorkflowResponse
				err      error
			}
			a, b := make(chan result, 1), make(chan result, 1)
			go func() {
				response, err := first.ExecWorkflowTaskForApp(ctx, appID, workflowID, 0, "same-key")
				a <- result{response, err}
			}()
			select {
			case <-paused:
			case <-ctx.Done():
				t.Fatal("first request did not reach task insert")
			}
			go func() {
				response, err := second.ExecWorkflowTaskForApp(ctx, appID, workflowID, 0, key)
				b <- result{response, err}
			}()
			var early *result
			select {
			case result := <-b:
				early = &result
			case <-time.After(150 * time.Millisecond):
			}
			close(resume)
			firstResult := <-a
			var secondResult result
			if early != nil {
				secondResult = *early
			} else {
				secondResult = <-b
			}
			require.Nil(t, early, "second request must wait for the first app transaction, even after Redis lease loss")
			require.NoError(t, firstResult.err)
			if key == "same-key" {
				require.NoError(t, secondResult.err)
				require.Equal(t, firstResult.response.TaskID, secondResult.response.TaskID)
			} else {
				require.ErrorIs(t, secondResult.err, bcode.ErrWorkflowTaskRunning)
			}
			var count int64
			require.NoError(t, db.Model(&model.WorkflowQueue{}).Where("app_id = ?", appID).Count(&count).Error)
			require.EqualValues(t, 1, count)
		})
	}
}

type schedulingPauseStore struct {
	datastore.DataStore
	beforeTaskInsert func(context.Context) error
}

func (s *schedulingPauseStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		return fn(&schedulingPauseStore{DataStore: tx, beforeTaskInsert: s.beforeTaskInsert})
	})
}
func (s *schedulingPauseStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.ReadCommittedTransactional).WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		return fn(&schedulingPauseStore{DataStore: tx, beforeTaskInsert: s.beforeTaskInsert})
	})
}
func (s *schedulingPauseStore) GetForUpdate(ctx context.Context, entity datastore.Entity) error {
	return s.DataStore.(datastore.RowLocker).GetForUpdate(ctx, entity)
}
func (s *schedulingPauseStore) Add(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		if err := s.beforeTaskInsert(ctx); err != nil {
			return err
		}
	}
	return s.DataStore.Add(ctx, entity)
}
