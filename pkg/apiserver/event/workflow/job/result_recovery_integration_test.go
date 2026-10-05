//go:build integration

package job

import (
	"context"
	"errors"
	"os"
	"strings"
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
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
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
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
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
	recovery := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
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
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultQueued)
	outbox.MessageID = "delivery"
	require.NoError(t, store.Add(ctx, outbox))
	claimed, err := claimResultOutbox(ctx, store, outbox, "delivery")
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, renewResultOutboxLease(ctx, store, outbox))
	recovery := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
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

func TestResultRecoveryMySQLLegacyNullLeaseGetsGraceAndRecovers(t *testing.T) {
	store := resultRecoveryMySQLStore(t)
	ctx := context.Background()
	payload, _, _, _ := completedResultFixture(t)
	rows := make([]*model.JobResultOutbox, 0, 2)
	for _, state := range []config.JobResultOutboxState{config.JobResultOutboxStateResultQueued, config.JobResultOutboxStateResultProcessingQueue} {
		next := *payload
		next.TaskID = string(state)
		outbox := buildJobResultOutbox(&next, state)
		outbox.MessageID = "legacy-message"
		require.NoError(t, store.Add(ctx, outbox))
		rows = append(rows, outbox)
	}
	queue := &enqueueCaptureQueue{}
	recovery := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)
	require.NoError(t, recovery.processOnce(ctx))
	require.Empty(t, queue.enqueued, "old active consumers first receive a bounded grace")
	now, err := resultOutboxDatabaseTime(ctx, store)
	require.NoError(t, err)
	for _, outbox := range rows {
		current, err := getJobResultOutboxByID(ctx, store, outbox.ID)
		require.NoError(t, err)
		require.NotNil(t, current.LeaseExpiresAt, "NULL lease CAS must use IS NULL")
		require.True(t, current.LeaseExpiresAt.After(now))
		require.Equal(t, outbox.State, current.State)
		require.NoError(t, store.Client.Model(outbox).UpdateColumn("lease_expires_at", now.Add(-time.Second)).Error)
	}
	require.NoError(t, recovery.processOnce(ctx))
	require.Len(t, queue.enqueued, 2, "lost queued and processing notifications recover without broker state")
	for _, outbox := range rows {
		current, err := getJobResultOutboxByID(ctx, store, outbox.ID)
		require.NoError(t, err)
		require.Equal(t, config.JobResultOutboxStateResultQueued, current.State)
	}
}
