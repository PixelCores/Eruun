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
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
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

func TestDelayedNotificationDispatchesCommittedWorkload(t *testing.T) {
	db, store, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, "space-1")
	ctx := context.Background()
	producer := &enqueueCaptureQueue{}
	_, err := EnqueueDelayJob(ctx, producer, payload)
	require.NoError(t, err)
	raw := producer.enqueued[0]
	committed, err := json.Marshal(payload)
	require.NoError(t, err)
	require.Less(t, len(raw), len(committed))
	queue := &dispatcherAckQueue{}
	dispatcher.queue = queue
	dispatcher.handleMessage(ctx, msg.Message{ID: "delivery", Payload: raw})
	select {
	case <-dispatcher.wake:
	default:
		t.Fatal("queue delivery did not wake the dispatcher")
	}
	item, wait := dispatcher.nextItem()
	require.NotNil(t, item)
	require.Zero(t, wait)
	require.ErrorIs(t, dispatcher.dispatch(ctx, item), errDelayWaitingAdmission)
	require.Zero(t, *creates, "global admission must still gate Job creation")
	admitted, err := repository.AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, admitted)
	require.NoError(t, dispatcher.dispatch(ctx, item))
	dispatcher.finish(ctx, item)
	require.Equal(t, 1, *creates)
	require.Len(t, queue.ackCalls, 1)
	require.Equal(t, []string{"delivery"}, queue.ackCalls[0].ids)
	var record model.JobInfo
	require.NoError(t, db.First(&record, 1).Error)
	require.Equal(t, config.JobDelayStateDispatched, record.DelayState)
}

func TestDelayedIdentityNotificationRejectsChangedCheckpointIdentity(t *testing.T) {
	tests := []struct {
		name             string
		change           func(*delayJobNotification)
		expectNoRetryErr bool
	}{
		{"execute time", func(n *delayJobNotification) { n.ExecuteAt++ }, true},
		{"run token", func(n *delayJobNotification) { n.RunToken = "other-run" }, true},
		{"execution key", func(n *delayJobNotification) { n.ExecutionKey = "other-execution" }, false},
		{"generation", func(n *delayJobNotification) { n.RunGeneration++ }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, "space-1")
			notification := notificationForDelayJob(payload)
			tt.change(&notification)
			raw, err := json.Marshal(notification)
			require.NoError(t, err)
			queue := &dispatcherAckQueue{}
			dispatcher.queue = queue
			dispatcher.handleMessage(context.Background(), msg.Message{ID: "changed", Payload: raw})
			item, _ := dispatcher.nextItem()
			require.NotNil(t, item)
			err = dispatcher.dispatch(context.Background(), item)
			if tt.expectNoRetryErr {
				require.ErrorIs(t, err, errDelayDispatchNoRetry)
				dispatcher.acknowledge(context.Background(), item)
			} else {
				require.NoError(t, err, "unknown or stale identity must leave the committed checkpoint for recovery")
				dispatcher.finish(context.Background(), item)
			}
			require.Len(t, queue.ackCalls, 1)
			require.Equal(t, []string{"changed"}, queue.ackCalls[0].ids)
			require.Zero(t, *creates)
			var record model.JobInfo
			require.NoError(t, db.First(&record, 1).Error)
			require.Equal(t, config.JobDelayStatePending, record.DelayState)
			require.Equal(t, string(config.StatusDistributed), record.Status)
		})
	}
}

func TestDelayedNotificationRejectsObsoleteFormats(t *testing.T) {
	for _, format := range []string{"missing version", "old version", "unknown version", "embedded workload", "null workload"} {
		t.Run(format, func(t *testing.T) {
			db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, "space-1")
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			var envelope map[string]interface{}
			require.NoError(t, json.Unmarshal(raw, &envelope))
			switch format {
			case "old version":
				envelope["version"] = 1
			case "unknown version":
				envelope["version"] = 3
			case "embedded workload":
				envelope["version"] = 2
			case "null workload":
				envelope["version"], envelope["job"] = 2, nil
			}
			raw, err = json.Marshal(envelope)
			require.NoError(t, err)
			queue := &dispatcherAckQueue{}
			dispatcher.queue = queue
			dispatcher.handleMessage(context.Background(), msg.Message{ID: "obsolete", Payload: raw})
			item, _ := dispatcher.nextItem()
			require.Nil(t, item, "unsupported wire formats must not reach scheduling")
			require.Len(t, queue.ackCalls, 1)
			require.Equal(t, []string{"obsolete"}, queue.ackCalls[0].ids)
			require.Zero(t, *creates)
			var record model.JobInfo
			require.NoError(t, db.First(&record, 1).Error)
			require.Equal(t, config.JobDelayStatePending, record.DelayState)
			require.Equal(t, string(config.StatusDistributed), record.Status)
		})
	}
}

func TestDelayedRecoveryRequiresCommittedWorkspaceIdentity(t *testing.T) {
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
			if tt.value != "space-1" {
				require.ErrorIs(t, dispatcher.dispatch(ctx, item), errDelayDispatchNoRetry)
				require.Zero(t, *creates)
				var record model.JobInfo
				require.NoError(t, db.First(&record, 1).Error)
				require.Empty(t, record.WorkspaceID, "missing identity must not be inferred")
				require.Equal(t, string(config.StatusFailed), record.Status)
				require.NotEqual(t, "queued", record.SchedulingState)
				return
			}
			require.ErrorIs(t, dispatcher.dispatch(ctx, item), errDelayWaitingAdmission)
			require.Zero(t, *creates, "recovery must not bypass global admission")
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

func TestDelayedWorkspaceRejectionFailureRemainsRetryable(t *testing.T) {
	db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, "")
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("fail-delayed-workspace-rejection", func(tx *gorm.DB) {
		if updates, ok := tx.Statement.Dest.(map[string]interface{}); ok && updates["status"] == string(config.StatusFailed) {
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
	require.NoError(t, db.Callback().Update().Remove("fail-delayed-workspace-rejection"))
	require.ErrorIs(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}), errDelayDispatchNoRetry)
}

func TestDelayedWorkspaceRejectionPreservesConcurrentTransition(t *testing.T) {
	for _, transition := range []string{"generation", "completed", "dispatched"} {
		t.Run(transition, func(t *testing.T) {
			db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, "")
			changed := false
			require.NoError(t, db.Callback().Update().Before("gorm:update").Register("change-delayed-checkpoint", func(tx *gorm.DB) {
				updates, ok := tx.Statement.Dest.(map[string]interface{})
				if !ok || updates["status"] != string(config.StatusFailed) || changed {
					return
				}
				changed = true
				column, value := "run_generation", interface{}(payload.RunGeneration+1)
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
			require.ErrorIs(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}), errDelayDispatchNoRetry)
			require.True(t, changed)
			require.Zero(t, *creates)
			var record model.JobInfo
			require.NoError(t, db.First(&record, 1).Error)
			require.Empty(t, record.WorkspaceID)
			switch transition {
			case "generation":
				require.Equal(t, payload.RunGeneration+1, record.RunGeneration)
				require.Equal(t, string(config.StatusDistributed), record.Status)
			case "completed":
				require.Equal(t, string(config.StatusCompleted), record.Status)
			case "dispatched":
				require.Equal(t, config.JobDelayStateDispatched, record.DelayState)
				require.Equal(t, string(config.StatusDistributed), record.Status)
			}
		})
	}
}

func TestDelayedWorkspaceRejectsInvalidOwnership(t *testing.T) {
	for _, storedWorkspace := range []string{"", "space-1", "space-2"} {
		t.Run(storedWorkspace, func(t *testing.T) {
			db, _, dispatcher, payload, creates := delayedWorkspaceSQLFixture(t, storedWorkspace)
			if storedWorkspace == "space-1" {
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
