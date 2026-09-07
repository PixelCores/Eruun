package job

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	batchv1 "k8s.io/api/batch/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
)

func delayedWorkspaceSQLFixture(t *testing.T, storedWorkspace interface{}) (*gorm.DB, *sqlstore.Driver, *DelayDispatcher, *DelayJobPayload, *int) {
	t.Helper()
	fixture, manager, payloads := delayedWorkspaceFixture(t)
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "delay.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	connection, err := db.DB()
	require.NoError(t, err)
	connection.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	require.NoError(t, db.AutoMigrate(&model.Applications{}, &model.Workspace{}, &model.WorkflowQueue{}, &model.JobInfo{}, &model.JobResultOutbox{}, &model.SystemSetting{}))
	store := &sqlstore.Driver{Client: *db}
	ctx := context.Background()
	require.NoError(t, store.Add(ctx, fixture.apps["app-1"]))
	require.NoError(t, store.Add(ctx, fixture.spaces["space-1"]))
	require.NoError(t, store.Add(ctx, fixture.jobInfos[1]))
	require.NoError(t, db.Model(&model.JobInfo{ID: 1}).Update("workspace_id", storedWorkspace).Error)
	require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
	creates := 0
	var created *batchv1.Job
	manager.RESTConfig.Transport = workspaceRoundTripper(func(r *http.Request) (*http.Response, error) {
		scope, ok := access.FromContext(r.Context())
		require.True(t, ok)
		require.Equal(t, "space-1", scope.WorkspaceID)
		if r.Method == http.MethodGet {
			if created == nil {
				return workspaceResponse(404, []byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)), nil
			}
			raw, err := json.Marshal(created)
			return workspaceResponse(200, raw), err
		}
		require.Equal(t, http.MethodPost, r.Method)
		creates++
		created = &batchv1.Job{}
		require.NoError(t, json.NewDecoder(r.Body).Decode(created))
		created.UID = "delayed-job-uid"
		require.Equal(t, "namespace-1", created.Namespace)
		raw, err := json.Marshal(created)
		return workspaceResponse(201, raw), err
	})
	return db, store, NewDelayDispatcher(nil, manager, access.NewStore(store), "", ""), payloads[0], &creates
}

func TestDelayedAdmissionRecoversLegacyWorkspaceIdentity(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value interface{}
	}{{"empty", ""}, {"null", nil}, {"already populated", "space-1"}} {
		t.Run(tt.name, func(t *testing.T) {
			db, store, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, tt.value)
			ctx := context.Background()
			require.NoError(t, dispatcher.recoverDueCheckpoints(ctx))
			item, wait := dispatcher.nextItem()
			require.NotNil(t, item)
			require.Zero(t, wait)
			require.ErrorIs(t, dispatcher.dispatch(ctx, item), errDelayWaitingAdmission)
			require.Zero(t, *creates, "backfill must not bypass global admission")
			var record model.JobInfo
			require.NoError(t, db.First(&record, 1).Error)
			require.Equal(t, "space-1", record.WorkspaceID)
			require.Equal(t, "queued", record.SchedulingState)
			admitted, err := repository.AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, admitted)
			require.NoError(t, dispatcher.dispatch(ctx, item))
			dispatcher.finish(ctx, item)
			require.NoError(t, dispatcher.dispatch(ctx, &delayItem{payload: payload}), "duplicate delivery uses the committed result outbox")
			require.Equal(t, 1, *creates)
			require.NoError(t, db.First(&record, 1).Error)
			require.Equal(t, config.JobDelayStateDispatched, record.DelayState)
			require.Equal(t, "released", record.SchedulingState)
		})
	}
}

func TestDelayedWorkspaceBackfillFailureRemainsRetryable(t *testing.T) {
	db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, "")
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("fail-delayed-workspace-backfill", func(tx *gorm.DB) {
		if updates, ok := tx.Statement.Dest.(map[string]interface{}); ok && updates["workspace_id"] == "space-1" {
			tx.AddError(errors.New("workspace write unavailable"))
		}
	}))
	err := dispatcher.dispatch(context.Background(), &delayItem{payload: payload})
	require.ErrorContains(t, err, "workspace write unavailable")
	require.NotErrorIs(t, err, errDelayDispatchNoRetry)
	require.Zero(t, *creates)
	var record model.JobInfo
	require.NoError(t, db.First(&record, 1).Error)
	require.Empty(t, record.WorkspaceID)
	require.Equal(t, config.JobDelayStatePending, record.DelayState)
	require.NoError(t, db.Callback().Update().Remove("fail-delayed-workspace-backfill"))
	require.ErrorIs(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}), errDelayWaitingAdmission)
}

func TestDelayedWorkspaceBackfillPreservesConcurrentTransition(t *testing.T) {
	for _, transition := range []string{"workspace", "generation", "completed", "dispatched"} {
		t.Run(transition, func(t *testing.T) {
			db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, "")
			changed := false
			require.NoError(t, db.Callback().Update().Before("gorm:update").Register("change-delayed-checkpoint", func(tx *gorm.DB) {
				updates, ok := tx.Statement.Dest.(map[string]interface{})
				if !ok || updates["workspace_id"] != "space-1" || changed {
					return
				}
				changed = true
				column, value := "workspace_id", interface{}("space-2")
				switch transition {
				case "generation":
					column, value = "run_generation", payload.RunGeneration+1
				case "completed":
					column, value = "status", string(config.StatusCompleted)
				case "dispatched":
					column, value = "delay_state", string(config.JobDelayStateDispatched)
				}
				tx.AddError(tx.Exec("UPDATE "+(&model.JobInfo{}).TableName()+" SET "+column+" = ? WHERE id = ?", value, 1).Error)
			}))
			require.ErrorIs(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}), repository.ErrWorkflowOwnershipLost)
			require.True(t, changed)
			require.Zero(t, *creates)
			var record model.JobInfo
			require.NoError(t, db.First(&record, 1).Error)
			if transition == "workspace" {
				require.Equal(t, "space-2", record.WorkspaceID)
			} else {
				require.Empty(t, record.WorkspaceID)
			}
		})
	}
}

func TestDelayedWorkspaceBackfillRejectsInvalidOwnership(t *testing.T) {
	for _, storedWorkspace := range []string{"", "space-2"} {
		t.Run(storedWorkspace, func(t *testing.T) {
			db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, storedWorkspace)
			if storedWorkspace == "" {
				changeDelayedNamespaceOwner(t, dispatcher.workspaceManager)
			}
			require.ErrorIs(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}), errDelayDispatchNoRetry)
			require.Zero(t, *creates)
			var record model.JobInfo
			require.NoError(t, db.First(&record, 1).Error)
			require.Equal(t, storedWorkspace, record.WorkspaceID)
			require.Equal(t, string(config.StatusFailed), record.Status)
		})
	}
}
