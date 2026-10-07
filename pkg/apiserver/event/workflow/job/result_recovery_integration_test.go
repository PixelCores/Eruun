//go:build integration

package job

import (
	"context"
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
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultPending)

	require.NoError(t, store.Add(ctx, outbox))
	claimed, err := claimResultOutbox(ctx, store, outbox)
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
	recovery := NewResultDispatcher(nil, store)
	recoveryDone := make(chan error, 1)
	go func() {
		recoveryDone <- recovery.recoverResultOutboxes(ctx)
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
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultPending)

	require.NoError(t, store.Add(ctx, outbox))
	claimed, err := claimResultOutbox(ctx, store, outbox)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, renewResultOutboxLease(ctx, store, outbox))
	recovery := NewResultDispatcher(nil, store)
	require.NoError(t, recovery.recoverResultOutboxes(ctx))
	active, err := getJobResultOutboxByID(ctx, store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, outbox.ClaimToken, active.ClaimToken)
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
	invalid := buildJobResultOutbox(payload, config.JobResultOutboxStateResultProcessing)
	invalid.ClaimToken = "invalid-owner"
	require.NoError(t, store.Add(ctx, invalid))
	next := *payload
	next.TaskID = "expired"
	expired := buildLeasedTestResultOutbox(t, store, &next, config.JobResultOutboxStateResultProcessing)
	deadline := expired.LeaseExpiresAt.Add(-2 * resultOutboxProcessGrace)
	expired.LeaseExpiresAt = &deadline
	require.NoError(t, store.Add(ctx, expired))
	recovery := NewResultDispatcher(nil, store)
	recovery.recoveryBatchSize = 1
	require.NoError(t, recovery.recoverResultOutboxes(ctx))
	current, err := getJobResultOutboxByID(ctx, store, invalid.ID)
	require.NoError(t, err)
	require.Nil(t, current.LeaseExpiresAt)
	require.Equal(t, config.JobResultOutboxStateFailed, current.State, "NULL lease rejection CAS must use IS NULL")
	require.Equal(t, "invalid-owner", current.ClaimToken)
	require.Contains(t, current.LastError, "active result outbox has no lease")
	require.NoError(t, recovery.recoverResultOutboxes(ctx))
	current, err = getJobResultOutboxByID(ctx, store, expired.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, current.State)
	require.Empty(t, current.ClaimToken)
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
	outbox := buildLeasedTestResultOutbox(t, store, payload, config.JobResultOutboxStateResultPending)

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
	dispatcher := NewResultDispatcher(client, store)
	claimed, err := claimResultOutbox(ctx, store, outbox)
	require.NoError(t, err)
	require.True(t, claimed)
	resultDone := make(chan error, 1)
	go func() { resultDone <- dispatcher.processResult(ctx, outbox) }()
	select {
	case <-beforeWrite:
	case <-ctx.Done():
		t.Fatal("result did not reach the SQL write barrier")
	}
	require.NoError(t, repository.TerminalizeCancelledWorkflowJobs(ctx, store, payload.TaskID, "user cancelled", ""))
	close(releaseWrite)
	select {
	case result := <-resultDone:
		require.NoError(t, result)
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
	_, err = getJobResultOutboxByID(ctx, store, outbox.ID)
	require.ErrorIs(t, err, datastore.ErrRecordNotExist)
}

func TestResultRecoveryMySQLConcurrentClaimsAndPendingCursor(t *testing.T) {
	store := resultRecoveryMySQLStore(t)
	ctx := context.Background()
	payload, _, _, _ := completedResultFixture(t)
	pending := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	pending.ID = "b"
	require.NoError(t, store.Add(ctx, pending))
	winners := make(chan *model.JobResultOutbox, 8)
	errs := make(chan error, 8)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		snapshot := *pending
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed, err := claimResultOutbox(ctx, store, &snapshot)
			errs <- err
			if claimed {
				winners <- &snapshot
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, winners, 1)
	owner := <-winners
	oldToken := owner.ClaimToken
	require.NoError(t, retryResultOutbox(ctx, store, owner, "temporary failure"))
	claimed, err := claimResultOutbox(ctx, store, pending)
	require.NoError(t, err)
	require.False(t, claimed, "attempt CAS must reject snapshots from before the prior claim")
	current, err := getJobResultOutboxByID(ctx, store, pending.ID)
	require.NoError(t, err)
	claimed, err = claimResultOutbox(ctx, store, current)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotEqual(t, oldToken, current.ClaimToken)
	for _, id := range []string{"a", "c"} {
		next := *pending
		next.ID = id
		require.NoError(t, store.Add(ctx, &next))
	}
	rows, err := listPendingResultOutboxes(ctx, store, "", 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "c", rows[0].ID)
	rows, err = listPendingResultOutboxes(ctx, store, "c", 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "a", rows[0].ID, "SQL keyset must skip processing claims and advance below the last attempted id")
	rows, err = listPendingResultOutboxes(ctx, store, "a", 1)
	require.NoError(t, err)
	require.Empty(t, rows)
}
