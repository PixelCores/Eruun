//go:build integration

package job

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mysqldsn "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	mysqlgorm "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
)

func resultRecoveryMySQLStore(t *testing.T) *sqlstore.Driver {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is required for isolated result recovery integration tests")
	}
	parsed, err := mysqldsn.ParseDSN(dsn)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(parsed.DBName, "eruun_scheduler_test"), "requires an isolated test schema")
	parsed.ClientFoundRows = true
	db, err := gorm.Open(mysqlgorm.Open(parsed.FormatDSN()), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	entities := []interface{}{&model.JobInfo{}, &model.JobResultOutbox{}}
	for _, entity := range entities {
		require.False(t, db.Migrator().HasTable(entity), "test schema must be empty")
	}
	require.NoError(t, db.AutoMigrate(entities...))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(entities...)); require.NoError(t, sqlDB.Close()) })
	return &sqlstore.Driver{Client: *db}
}

func TestResultRecoveryMySQLTransactionFencesOldOwner(t *testing.T) {
	store := resultRecoveryMySQLStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	payload, _, _, _ := completedResultFixture(t)
	record := testResultJobInfo(1, payload)
	record.Status = string(config.StatusDistributed)
	require.NoError(t, store.Add(ctx, record))
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "first"
	require.NoError(t, store.Add(ctx, outbox))
	claimed, err := claimResultOutbox(ctx, store, outbox, "first")
	require.NoError(t, err)
	require.True(t, claimed)
	now, err := resultOutboxDatabaseTime(ctx, store)
	require.NoError(t, err)
	lease := now.Add(500 * time.Millisecond)
	require.NoError(t, store.Client.Model(outbox).UpdateColumn("lease_expires_at", lease).Error)
	entered := make(chan struct{})
	release := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- withResultOutboxOwnership(ctx, store, outbox, func(tx datastore.DataStore, _ *model.JobResultOutbox) error {
			close(entered)
			<-release
			return updateJobInfoStatus(ctx, tx, payload, config.StatusCompleted, "", 1, 2, "durable result logs")
		})
	}()
	<-entered
	time.Sleep(600 * time.Millisecond)
	recovery := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, store)
	recoveryDone := make(chan error, 1)
	go func() {
		recoveryDone <- recovery.recoverResultOutboxes(ctx, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingQueue})
	}()
	select {
	case err := <-recoveryDone:
		t.Fatalf("recovery crossed result transaction: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-writerDone)
	require.NoError(t, <-recoveryDone)
	committed := &model.JobInfo{ID: 1}
	require.NoError(t, store.Get(ctx, committed))
	require.Equal(t, string(config.StatusCompleted), committed.Status)
	require.Equal(t, "durable result logs", committed.Info)
	recovered, err := getJobResultOutboxByID(ctx, store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, recovered.State)
	require.ErrorIs(t, renewResultOutboxLease(ctx, store, outbox), errResultOutboxOwnershipLost)
	called := false
	require.ErrorIs(t, withResultOutboxOwnership(ctx, store, outbox, func(datastore.DataStore, *model.JobResultOutbox) error { called = true; return nil }), errResultOutboxOwnershipLost)
	require.False(t, called)
}

func TestResultRecoveryMySQLRenewalAndRollback(t *testing.T) {
	store := resultRecoveryMySQLStore(t)
	ctx := context.Background()
	payload, _, _, _ := completedResultFixture(t)
	record := testResultJobInfo(1, payload)
	record.Status = string(config.StatusDistributed)
	require.NoError(t, store.Add(ctx, record))
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "delivery"
	require.NoError(t, store.Add(ctx, outbox))
	claimed, err := claimResultOutbox(ctx, store, outbox, "delivery")
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, renewResultOutboxLease(ctx, store, outbox))
	recovery := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, store)
	require.NoError(t, recovery.processOnce(ctx))
	active, err := getJobResultOutboxByID(ctx, store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, outbox.MessageID, active.MessageID)
	injected := errors.New("rollback result transaction")
	require.ErrorIs(t, withResultOutboxOwnership(ctx, store, outbox, func(tx datastore.DataStore, _ *model.JobResultOutbox) error {
		if err := updateJobInfoStatus(ctx, tx, payload, config.StatusCompleted, "", 1, 2, "uncommitted logs"); err != nil {
			return err
		}
		return injected
	}), injected)
	preserved := &model.JobInfo{ID: 1}
	require.NoError(t, store.Get(ctx, preserved))
	require.Equal(t, string(config.StatusDistributed), preserved.Status)
	require.Empty(t, preserved.Info)
}

func TestResultRecoveryMySQLRejectsActiveNullLeaseWithoutBlockingExpiredRows(t *testing.T) {
	store := resultRecoveryMySQLStore(t)
	ctx := context.Background()
	payload, _, _, _ := completedResultFixture(t)
	rows := make([]*model.JobResultOutbox, 0, 3)
	for _, state := range []config.JobResultOutboxState{config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultQueued, config.JobResultOutboxStateResultProcessingQueue} {
		next := *payload
		next.TaskID = string(state)
		outbox := buildJobResultOutbox(&next, state)
		outbox.MessageID = "invalid-message"
		require.NoError(t, store.Add(ctx, outbox))
		rows = append(rows, outbox)
	}
	expired := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
	deadline := expired.LeaseExpiresAt.Add(-2 * resultOutboxDispatchGrace)
	expired.LeaseExpiresAt = &deadline
	expired.MessageID = "lost-message"
	require.NoError(t, store.Add(ctx, expired))
	queue := &enqueueCaptureQueue{enqueueID: "recovered"}
	recovery := NewResultOutboxDispatcher(queue, store)
	recovery.batchSize = len(rows)
	require.NoError(t, recovery.processOnce(ctx))
	require.Empty(t, queue.enqueued)
	for _, outbox := range rows {
		current, err := getJobResultOutboxByID(ctx, store, outbox.ID)
		require.NoError(t, err)
		require.Nil(t, current.LeaseExpiresAt)
		require.Equal(t, config.JobResultOutboxStateFailed, current.State, "NULL lease rejection CAS must use IS NULL")
		require.Equal(t, "invalid-message", current.MessageID)
		require.Contains(t, current.LastError, "active result outbox has no lease")
	}
	require.NoError(t, recovery.processOnce(ctx))
	require.Len(t, queue.enqueued, 1, "rejected invalid rows cannot starve normal expired recovery")
	current, err := getJobResultOutboxByID(ctx, store, expired.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultQueued, current.State)
	require.Equal(t, "recovered", current.MessageID)
}

func TestResultRecoveryMySQLConcurrentCancellationWinsBeforeResultCAS(t *testing.T) {
	store := resultRecoveryMySQLStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	payload, live, pod, _ := completedResultFixture(t)
	record := testResultJobInfo(1, payload)
	record.Status = string(config.StatusDistributed)
	record.Attempt = 1
	require.NoError(t, store.Add(ctx, record))
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "result-message"
	require.NoError(t, store.Add(ctx, outbox))
	beforeWrite := make(chan struct{})
	releaseWrite := make(chan struct{})
	var once sync.Once
	require.NoError(t, store.Client.Callback().Update().Before("gorm:update").Register("pause-result-before-cas", func(tx *gorm.DB) {
		if _, ok := tx.Statement.Model.(*model.JobInfo); !ok {
			return
		}
		updates, ok := tx.Statement.Dest.(map[string]interface{})
		if !ok || updates["status"] != string(config.StatusCompleted) {
			return
		}
		once.Do(func() {
			close(beforeWrite)
			select {
			case <-releaseWrite:
			case <-ctx.Done():
			}
		})
	}))
	t.Cleanup(func() { require.NoError(t, store.Client.Callback().Update().Remove("pause-result-before-cas")) })
	client := fake.NewSimpleClientset(live, pod)
	queue := &dispatcherAckQueue{}
	dispatcher := NewResultDispatcher(queue, client, store, "result", "consumer")
	raw, err := json.Marshal(jobResultPayloadFromOutbox(outbox))
	require.NoError(t, err)
	resultDone := make(chan bool, 1)
	go func() { resultDone <- dispatcher.handleMessage(ctx, msg.Message{ID: outbox.MessageID, Payload: raw}) }()
	select {
	case <-beforeWrite:
	case <-ctx.Done():
		t.Fatal("result did not reach the SQL write barrier")
	}
	require.NoError(t, repository.TerminalizeCancelledWorkflowJobs(ctx, store, payload.TaskID, "user cancelled", ""))
	close(releaseWrite)
	select {
	case result := <-resultDone:
		require.True(t, result)
	case <-ctx.Done():
		t.Fatal("result did not finish")
	}
	saved := &model.JobInfo{ID: 1}
	require.NoError(t, store.Get(ctx, saved))
	require.Equal(t, string(config.StatusCancelled), saved.Status)
	require.Equal(t, "user cancelled", saved.Error)
	require.Equal(t, "parent workflow cancelled", saved.SchedulingReason)
	require.Empty(t, saved.Info)
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb())
	}
	require.Len(t, queue.ackCalls, 1)
	_, err = getJobResultOutboxByID(ctx, store, outbox.ID)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}
