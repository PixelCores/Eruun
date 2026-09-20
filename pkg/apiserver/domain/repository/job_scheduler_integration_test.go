//go:build integration

package repository

import (
	"context"
	"fmt"
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

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	wfc "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func newMySQLJobSchedulerTestStore(t *testing.T) *sqlstore.Driver {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN is required for MySQL admission integration tests")
	}
	parsed, err := mysqldsn.ParseDSN(dsn)
	require.NoError(t, err)
	// This test creates and removes only its own schema's scheduler tables.
	// Never point it at an application or production database.
	require.True(t, strings.HasPrefix(parsed.DBName, "eruun_scheduler_test"), "integration database must start with eruun_scheduler_test")
	// Production MySQL connections enforce matched-row CAS semantics too.
	parsed.ClientFoundRows = true
	db, err := gorm.Open(mysqlgorm.Open(parsed.FormatDSN()), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(20)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	models := []interface{}{&model.JobInfo{}, &model.WorkflowQueue{}, &model.SystemSetting{}, &model.ResourceCreationBudget{}, &model.JobSandbox{}}
	for _, entity := range models {
		require.False(t, db.Migrator().HasTable(entity), "integration schema must be empty")
	}
	require.NoError(t, db.AutoMigrate(models...))
	t.Cleanup(func() { require.NoError(t, db.Migrator().DropTable(models...)) })
	store := &sqlstore.Driver{Client: *db}
	require.NoError(t, EnsureJobSchedulerPolicy(context.Background(), store))
	return store
}

func TestResourceCreationMySQLConcurrentBudget(t *testing.T) {
	testResourceCreationConcurrentBudget(t, newMySQLJobSchedulerTestStore(t))
}

func TestJobSchedulerMySQLTerminalCallbacksWithoutWorker(t *testing.T) {
	testTerminalCallbackScheduling(t, newMySQLJobSchedulerTestStore(t))
}

func TestJobSchedulerMySQLConcurrentAdmission(t *testing.T) {
	store := newMySQLJobSchedulerTestStore(t)
	db := &store.Client
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	t.Run("upgrade existing Job table and admit legacy checkpoint", func(t *testing.T) {
		lease := time.Now().UTC().Add(time.Hour)
		owner := &model.WorkflowQueue{TaskID: "legacy-job", WorkspaceID: "legacy-space", RunGeneration: 1, RunToken: "legacy-token", WorkerID: "worker", Status: config.StatusRunning, LeaseExpiresAt: &lease}
		require.NoError(t, store.Add(ctx, owner))
		key := "legacy-execution"
		job := &model.JobInfo{TaskID: owner.TaskID, WorkspaceID: owner.WorkspaceID, RunGeneration: 1, ExecutionKey: &key, Status: string(config.StatusWaiting), Info: "preserved workload"}
		require.NoError(t, store.Add(ctx, job))
		// Reproduce the pre-scheduler table while retaining an existing row.
		// Production schema migration uses the same GORM AutoMigrate operation.
		require.NoError(t, db.Migrator().DropIndex(job, "idx_job_scheduling"))
		columns := []string{"scheduling_state", "scheduling_class", "scheduling_priority", "scheduling_queued_at", "scheduling_generation", "scheduling_owner_status", "scheduling_expires_at", "scheduling_reason"}
		for _, column := range columns {
			require.NoError(t, db.Migrator().DropColumn(job, column))
			require.False(t, db.Migrator().HasColumn(job, column))
		}
		require.NoError(t, db.AutoMigrate(job))
		for _, column := range columns {
			require.True(t, db.Migrator().HasColumn(job, column))
		}
		require.True(t, db.Migrator().HasIndex(job, "idx_job_scheduling"))
		require.NoError(t, store.Get(ctx, job))
		require.Equal(t, "preserved workload", job.Info)
		require.Empty(t, job.SchedulingState, "legacy rows start with nullable admission state")
		require.Zero(t, job.SchedulingGeneration)
		require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
		n, err := AdmitQueuedJobs(ctx, store)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		admitted, err := IsJobAdmitted(ctx, store, owner, key)
		require.NoError(t, err)
		require.True(t, admitted)
		require.NoError(t, store.Delete(ctx, job))
		require.NoError(t, store.Delete(ctx, owner))
	})
	frozenUpdateTime := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("pin-scheduler-cas-time", func(tx *gorm.DB) {
		if _, ok := tx.Statement.Model.(*model.SystemSetting); !ok {
			return
		}
		if updates, ok := tx.Statement.Dest.(map[string]interface{}); ok {
			updates["update_time"] = frozenUpdateTime
		}
	}))
	for i := 0; i < 20; i++ {
		matched, err := store.CompareAndSwap(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}, "type", model.SystemSettingTypeWorkflowScheduler, nil)
		require.NoError(t, err)
		require.True(t, matched, "same-millisecond lock touches must return matched rows")
	}
	require.NoError(t, db.Callback().Update().Remove("pin-scheduler-cas-time"))
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":4,"maxConcurrentJobsPerWorkspace":2}`)
	var jobs []*model.JobInfo
	for i := 0; i < 24; i++ {
		_, job := schedulerTestJob(t, store, fmt.Sprint(i), fmt.Sprint(i%4), "normal")
		jobs = append(jobs, job)
	}
	start := make(chan struct{})
	results := make(chan error, 12)
	runtimeStore := &schedulerReadBarrierStore{DataStore: store}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _, err := AdmitQueuedJobs(ctx, runtimeStore); results <- err }()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	active := 0
	spaces := map[string]int{}
	for _, job := range jobs {
		require.NoError(t, store.Get(ctx, job))
		if job.SchedulingState == wfc.JobSchedulingAdmitted {
			active++
			spaces[job.WorkspaceID]++
		}
	}
	require.Equal(t, 4, active)
	for _, count := range spaces {
		require.LessOrEqual(t, count, 2)
	}
	t.Logf("12 concurrent schedulers admitted %d jobs across %d workspaces; per-workspace maximum 2", active, len(spaces))
	t.Run("concurrent delayed notifications retain first queue time", func(t *testing.T) {
		key := "detached-concurrent"
		checkpoint := &model.JobInfo{WorkspaceID: "delayed-space", ExecutionKey: &key, RunGeneration: 1, Status: string(config.StatusDistributed), DelayState: config.JobDelayStatePending, DelayExecuteAt: time.Now().Add(-time.Minute).Unix(), DelayPayload: `{"committed":true}`}
		require.NoError(t, store.Add(ctx, checkpoint))
		deadline := time.Now().UTC().Add(time.Minute)
		start := make(chan struct{})
		errors := make(chan error, 8)
		queuedTimes := make(chan time.Time, 8)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				copy := *checkpoint
				<-start
				err := EnqueueJobForScheduling(ctx, store, nil, &copy, &deadline)
				errors <- err
				if err == nil {
					queuedTimes <- *copy.SchedulingQueuedAt
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errors)
		close(queuedTimes)
		for err := range errors {
			require.NoError(t, err)
		}
		require.NoError(t, store.Get(ctx, checkpoint))
		for queued := range queuedTimes {
			require.True(t, queued.Equal(*checkpoint.SchedulingQueuedAt))
		}
	})

	t.Run("representative queue scan", func(t *testing.T) {
		require.NoError(t, db.Exec("DELETE FROM "+(&model.JobInfo{}).TableName()).Error)
		require.NoError(t, db.Exec("DELETE FROM "+(&model.WorkflowQueue{}).TableName()).Error)
		setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":100,"maxConcurrentJobsPerWorkspace":10}`)
		now, err := currentWorkflowDatabaseTime(ctx, store)
		require.NoError(t, err)
		lease := now.Add(time.Hour)
		var entities []datastore.Entity
		for i := 0; i < 100; i++ {
			entities = append(entities, &model.WorkflowQueue{TaskID: fmt.Sprint("perf-", i), WorkspaceID: fmt.Sprint(i % 20), Status: config.StatusRunning, RunGeneration: 1, RunToken: "token", WorkerID: "worker", LeaseExpiresAt: &lease})
		}
		require.NoError(t, store.BatchAdd(ctx, entities))
		entities = nil
		for i := 0; i < 1000; i++ {
			key := fmt.Sprint("perf-", i)
			entities = append(entities, &model.JobInfo{TaskID: fmt.Sprint("perf-", i%100), WorkspaceID: fmt.Sprint(i % 20), ExecutionKey: &key, RunGeneration: 1, Status: string(config.StatusWaiting), SchedulingState: wfc.JobSchedulingQueued, SchedulingOwnerStatus: config.StatusRunning, SchedulingGeneration: 1, SchedulingQueuedAt: &now, SchedulingPriority: 50})
		}
		require.NoError(t, store.BatchAdd(ctx, entities))
		counts := &schedulerCallCounts{}
		counted := countedSchedulerStore{DataStore: store, counts: counts}
		started := time.Now()
		n, err := AdmitQueuedJobs(ctx, counted)
		require.NoError(t, err)
		require.Equal(t, 100, n)
		require.Equal(t, 12, counts.lists, "11 Job pages including the final empty page, plus one batch of 100 unique parents")
		require.Equal(t, 1, counts.gets, "policy read only; parent ownership is fetched in a batch")
		t.Logf("1000 queued Jobs / 100 Workflows: admitted %d in %s; datastore List=%d Get=%d", n, time.Since(started), counts.lists, counts.gets)
	})
	t.Run("concurrent cleanup keeps release idempotent", func(t *testing.T) {
		owner, job := schedulerTestJob(t, store, "release-race", "race-space", "normal")
		require.NoError(t, db.Model(job).Update("scheduling_state", wfc.JobSchedulingAdmitted).Error)
		read, continueRead := make(chan struct{}), make(chan struct{})
		barrier := &schedulerReadBarrierStore{DataStore: store, jobID: job.ID, read: read, resume: continueRead}
		result := make(chan error, 1)
		go func() { result <- ReleaseJobAdmission(ctx, barrier, owner, *job.ExecutionKey, "worker returned") }()
		select {
		case <-read:
		case <-ctx.Done():
			t.Fatal("release did not reach read barrier")
		}
		// The scheduler can clear a completed Job without locking its parent.
		require.NoError(t, db.Model(job).Update("scheduling_state", wfc.JobSchedulingReleased).Error)
		close(continueRead)
		require.NoError(t, <-result, "same-owner scheduler cleanup is an idempotent release")
	})
	t.Run("expired queued snapshot cannot clear refreshed deadline", func(t *testing.T) {
		key := "refresh-deadline-race"
		now := time.Now().UTC()
		expired := now.Add(-time.Minute)
		checkpoint := &model.JobInfo{WorkspaceID: "delay-refresh", ExecutionKey: &key, RunGeneration: 1, Status: string(config.StatusDistributed), DelayState: config.JobDelayStatePending, DelayExecuteAt: expired.Unix(), DelayPayload: `{"committed":true}`, SchedulingState: wfc.JobSchedulingQueued, SchedulingQueuedAt: &expired, SchedulingGeneration: 1, SchedulingExpiresAt: &expired}
		require.NoError(t, store.Add(ctx, checkpoint))
		read, continueRead := make(chan struct{}), make(chan struct{})
		barrier := &schedulerReadBarrierStore{DataStore: store, jobID: checkpoint.ID, read: read, resume: continueRead}
		result := make(chan error, 1)
		go func() { _, err := AdmitQueuedJobs(ctx, barrier); result <- err }()
		select {
		case <-read:
		case <-ctx.Done():
			t.Fatal("scheduler did not reach read barrier")
		}
		deadline := now.Add(time.Minute)
		require.NoError(t, EnqueueJobForScheduling(ctx, store, nil, checkpoint, &deadline))
		refreshedDeadline := *checkpoint.SchedulingExpiresAt
		close(continueRead)
		require.NoError(t, <-result, "a refreshed candidate is reconsidered next tick")
		require.NoError(t, store.Get(ctx, checkpoint))
		require.NotEqual(t, wfc.JobSchedulingReleased, checkpoint.SchedulingState)
		require.True(t, checkpoint.SchedulingExpiresAt.Equal(refreshedDeadline))
	})
	t.Run("policy updates serialize with admission", func(t *testing.T) {
		require.NoError(t, db.Exec("DELETE FROM "+(&model.JobInfo{}).TableName()).Error)
		require.NoError(t, db.Exec("DELETE FROM "+(&model.WorkflowQueue{}).TableName()).Error)
		setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`)
		_, first := schedulerTestJob(t, store, "policy-first", "a", "normal")
		_, _ = schedulerTestJob(t, store, "policy-second", "b", "normal")
		read, resume := make(chan struct{}), make(chan struct{})
		barrier := &schedulerReadBarrierStore{DataStore: store, jobID: first.ID, read: read, resume: resume}
		admission := make(chan error, 1)
		go func() { _, err := AdmitQueuedJobs(ctx, barrier); admission <- err }()
		select {
		case <-read:
		case <-ctx.Done():
			t.Fatal("scheduler did not hold the policy lock")
		}
		policy, err := wfc.NormalizeJobSchedulerPolicyValue([]byte(`{"maxConcurrentJobs":2,"maxConcurrentJobsPerWorkspace":1}`))
		require.NoError(t, err)
		updated := make(chan error, 1)
		go func() {
			updated <- store.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: policy})
		}()
		select {
		case err := <-updated:
			t.Fatalf("policy update overtook the admission lock: %v", err)
		case <-time.After(30 * time.Millisecond):
		}
		close(resume)
		require.NoError(t, <-admission)
		require.NoError(t, <-updated)
		active, err := store.Count(ctx, &model.JobInfo{}, &datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "scheduling_state", Values: []string{wfc.JobSchedulingAdmitted}}}})
		require.NoError(t, err)
		require.Equal(t, int64(1), active)
		n, err := AdmitQueuedJobs(ctx, store)
		require.NoError(t, err)
		require.Equal(t, 1, n, "the next tick must observe the committed larger policy")
	})
}

type schedulerReadBarrierStore struct {
	datastore.DataStore
	jobID  int
	read   chan struct{}
	resume chan struct{}
	once   sync.Once
}

func (s *schedulerReadBarrierStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error { return fn(&schedulerReadBarrierTx{DataStore: tx, barrier: s}) })
}
func (s *schedulerReadBarrierStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.ReadCommittedTransactional).WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error { return fn(&schedulerReadBarrierTx{DataStore: tx, barrier: s}) })
}

type schedulerReadBarrierTx struct {
	datastore.DataStore
	barrier *schedulerReadBarrierStore
}

func (s *schedulerReadBarrierTx) List(ctx context.Context, e datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	rows, err := s.DataStore.List(ctx, e, opts)
	for _, row := range rows {
		if job, ok := row.(*model.JobInfo); ok && job.ID == s.barrier.jobID {
			s.barrier.once.Do(func() { close(s.barrier.read); <-s.barrier.resume })
		}
	}
	return rows, err
}
func (s *schedulerReadBarrierTx) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	return s.DataStore.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
}
func (s *schedulerReadBarrierTx) CompareAndSwap(ctx context.Context, e datastore.Entity, field string, value interface{}, updates map[string]interface{}) (bool, error) {
	// The account Store checks an existing row before its CAS. This read
	// deliberately establishes a snapshot before a contended policy lock.
	if setting, ok := e.(*model.SystemSetting); ok {
		copy := *setting
		if err := s.DataStore.Get(ctx, &copy); err != nil {
			return false, err
		}
	}
	return s.DataStore.CompareAndSwap(ctx, e, field, value, updates)
}
func (s *schedulerReadBarrierTx) CompareAndSwapWithConditions(ctx context.Context, e datastore.Entity, c, u map[string]interface{}) (bool, error) {
	return s.DataStore.(datastore.ConditionalCompareAndSwap).CompareAndSwapWithConditions(ctx, e, c, u)
}

type schedulerCallCounts struct{ lists, gets int }
type countedSchedulerStore struct {
	datastore.DataStore
	counts *schedulerCallCounts
}

func (s countedSchedulerStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.ReadCommittedTransactional).WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error { return fn(countedSchedulerStore{tx, s.counts}) })
}
func (s countedSchedulerStore) Get(ctx context.Context, e datastore.Entity) error {
	s.counts.gets++
	return s.DataStore.Get(ctx, e)
}
func (s countedSchedulerStore) List(ctx context.Context, e datastore.Entity, o *datastore.ListOptions) ([]datastore.Entity, error) {
	s.counts.lists++
	return s.DataStore.List(ctx, e, o)
}
func (s countedSchedulerStore) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	return s.DataStore.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
}
func (s countedSchedulerStore) CompareAndSwapWithConditions(ctx context.Context, e datastore.Entity, c, u map[string]interface{}) (bool, error) {
	return s.DataStore.(datastore.ConditionalCompareAndSwap).CompareAndSwapWithConditions(ctx, e, c, u)
}

func TestJobSchedulerMySQLConcurrentResourceBudget(t *testing.T) {
	testJobSchedulerConcurrentResourceBudget(t, newMySQLJobSchedulerTestStore(t))
}
func TestJobSchedulerMySQLRetainedResources(t *testing.T) {
	testJobSchedulerRetainedResources(t, newMySQLJobSchedulerTestStore(t))
}
func TestJobSchedulerMySQLLowerResourceQuota(t *testing.T) {
	testJobSchedulerLowerResourceQuota(t, newMySQLJobSchedulerTestStore(t))
}

func TestJobSchedulerMySQLRecoveryPreservesCreatedReservation(t *testing.T) {
	testJobSchedulerRecoveryPreservesCreatedReservation(t, newMySQLJobSchedulerTestStore(t))
}
