package repository

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sqlnamer"
	wfc "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func newJobSchedulerTestStore(t *testing.T) *sqlstore.Driver {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "scheduler.db")), &gorm.Config{NamingStrategy: sqlnamer.SQLNamer{}, TranslateError: true, Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&model.JobInfo{}, &model.WorkflowQueue{}, &model.SystemSetting{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	store := &sqlstore.Driver{Client: *db}
	require.NoError(t, EnsureJobSchedulerPolicy(context.Background(), store))
	return store
}

func schedulerTestJob(t *testing.T, store datastore.DataStore, name, workspace, class string) (*model.WorkflowQueue, *model.JobInfo) {
	t.Helper()
	ctx := context.Background()
	now, err := currentWorkflowDatabaseTime(ctx, store)
	require.NoError(t, err)
	lease := now.Add(time.Hour)
	owner := &model.WorkflowQueue{TaskID: name, WorkspaceID: workspace, RunGeneration: 1, RunToken: "token-" + name, WorkerID: "worker", Status: config.StatusRunning, LeaseExpiresAt: &lease}
	require.NoError(t, store.Add(ctx, owner))
	key := "execution-" + name
	job := &model.JobInfo{TaskID: name, WorkspaceID: workspace, RunGeneration: 1, ExecutionKey: &key, Status: string(config.StatusWaiting), SchedulingClass: class, Type: string(config.JobDeployService)}
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
	return owner, job
}

func setSchedulerTestPolicy(t *testing.T, store datastore.DataStore, policy string) {
	t.Helper()
	value, err := wfc.NormalizeJobSchedulerPolicyValue([]byte(policy))
	require.NoError(t, err)
	require.NoError(t, store.Put(context.Background(), &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: value}))
}

func TestJobSchedulerGlobalAndWorkspaceLimits(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":3,"maxConcurrentJobsPerWorkspace":1}`)
	var owners []*model.WorkflowQueue
	var jobs []*model.JobInfo
	for i, space := range []string{"a", "a", "b", "b", "c", "c", "d"} {
		owner, job := schedulerTestJob(t, store, fmt.Sprint(i), space, "normal")
		owners, jobs = append(owners, owner), append(jobs, job)
	}
	n, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Zero(t, n)
	counts := map[string]int{}
	for i, job := range jobs {
		admitted, err := IsJobAdmitted(ctx, store, owners[i], *job.ExecutionKey)
		require.NoError(t, err)
		if admitted {
			counts[job.WorkspaceID]++
		}
	}
	require.Equal(t, map[string]int{"a": 1, "b": 1, "c": 1}, counts)
	require.NoError(t, ReleaseJobAdmission(ctx, store, owners[0], *jobs[0].ExecutionKey, "finished"))
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestJobSchedulerPriorityFairnessAgingAndFIFO(t *testing.T) {
	for _, test := range []struct {
		name, strategy string
		aging          bool
		want           string
	}{
		{"priority", "priority", false, "high"}, {"aging", "priority", true, "old"}, {"fifo", "fifo", false, "old"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newJobSchedulerTestStore(t)
			ctx := context.Background()
			setSchedulerTestPolicy(t, store, fmt.Sprintf(`{"strategy":%q,"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1,"agingSeconds":1}`, test.strategy))
			ownerOld, old := schedulerTestJob(t, store, "old", "a", "background")
			ownerHigh, high := schedulerTestJob(t, store, "high", "b", "high")
			if test.aging {
				past := time.Now().UTC().Add(-101 * time.Second)
				_, err := store.CompareAndSwap(ctx, old, "id", old.ID, map[string]interface{}{"scheduling_queued_at": past})
				require.NoError(t, err)
			}
			n, err := AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			oldAdmitted, err := IsJobAdmitted(ctx, store, ownerOld, *old.ExecutionKey)
			require.NoError(t, err)
			highAdmitted, err := IsJobAdmitted(ctx, store, ownerHigh, *high.ExecutionKey)
			require.NoError(t, err)
			require.Equal(t, test.want == "old", oldAdmitted)
			require.Equal(t, test.want == "high", highAdmitted)
		})
	}
	t.Run("equal priority prefers less active workspace", func(t *testing.T) {
		store := newJobSchedulerTestStore(t)
		ctx := context.Background()
		setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":2,"maxConcurrentJobsPerWorkspace":2,"agingSeconds":86400}`)
		_, active := schedulerTestJob(t, store, "active", "a", "normal")
		_, err := AdmitQueuedJobs(ctx, store)
		require.NoError(t, err)
		_, older := schedulerTestJob(t, store, "older", "a", "normal")
		owner, newer := schedulerTestJob(t, store, "newer", "b", "normal")
		n, err := AdmitQueuedJobs(ctx, store)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		ok, err := IsJobAdmitted(ctx, store, owner, *newer.ExecutionKey)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, store.Get(ctx, active))
		require.NoError(t, store.Get(ctx, older))
		require.Equal(t, wfc.JobSchedulingAdmitted, active.SchedulingState)
		require.Equal(t, wfc.JobSchedulingQueued, older.SchedulingState)
	})
}

func TestJobSchedulerOwnershipRecoveryAndIdempotency(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	owner, job := schedulerTestJob(t, store, "work", "a", "high")
	queuedAt := *job.SchedulingQueuedAt
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
	require.True(t, queuedAt.Equal(*job.SchedulingQueuedAt))
	n, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
	require.Equal(t, wfc.JobSchedulingAdmitted, job.SchedulingState)
	oldOwner := *owner
	owner.RunGeneration++
	owner.RunToken = "replacement-token"
	require.NoError(t, store.Put(ctx, owner))
	_, err = IsJobAdmitted(ctx, store, &oldOwner, *job.ExecutionKey)
	require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
	require.ErrorIs(t, ReleaseJobAdmission(ctx, store, &oldOwner, *job.ExecutionKey, "stale"), ErrWorkflowOwnershipLost)
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, store.Get(ctx, job))
	require.Equal(t, wfc.JobSchedulingReleased, job.SchedulingState)
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
	require.Equal(t, owner.RunGeneration, job.SchedulingGeneration)
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, ReleaseJobAdmission(ctx, store, owner, *job.ExecutionKey, "finished"))
	require.NoError(t, ReleaseJobAdmission(ctx, store, owner, *job.ExecutionKey, "finished"))
	_, err = IsJobAdmitted(ctx, store, owner, *job.ExecutionKey)
	require.ErrorIs(t, err, ErrJobAdmissionReleased)
}

func TestJobSchedulerCallbackAndDetachedBoundaries(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	owner, job := schedulerTestJob(t, store, "callback", "a", "normal")
	owner.Status = config.StatusCompleted
	owner.LeaseExpiresAt = nil
	require.NoError(t, store.Put(ctx, owner))
	deadline := time.Now().Add(time.Minute)
	require.Error(t, EnqueueJobForScheduling(ctx, store, owner, job, &deadline))
	job.Type = string(config.JobDeployCallback)
	require.NoError(t, store.Put(ctx, job))
	require.Error(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, &deadline))
	n, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	admitted, err := IsJobAdmitted(ctx, store, owner, *job.ExecutionKey, job.SchedulingExpiresAt)
	require.NoError(t, err)
	require.True(t, admitted)
	expired := time.Now().Add(-time.Second)
	_, err = store.CompareAndSwap(ctx, job, "id", job.ID, map[string]interface{}{"scheduling_expires_at": expired})
	require.NoError(t, err)
	_, err = IsJobAdmitted(ctx, store, owner, *job.ExecutionKey, &expired)
	require.ErrorIs(t, err, ErrJobAdmissionExpired)
	_, delayed := schedulerTestJob(t, store, "delayed", "b", "background")
	require.Error(t, EnqueueJobForScheduling(ctx, store, nil, delayed, &deadline))
	delayed.DelayState = config.JobDelayStatePending
	delayed.DelayExecuteAt = time.Now().Add(-time.Second).Unix()
	delayed.DelayPayload = `{"checkpoint":true}`
	delayed.Status = string(config.StatusDistributed)
	require.NoError(t, store.Put(ctx, delayed))
	require.NoError(t, EnqueueJobForScheduling(ctx, store, nil, delayed, &deadline))
	first := *delayed.SchedulingQueuedAt
	require.NoError(t, EnqueueJobForScheduling(ctx, store, nil, delayed, &deadline))
	require.True(t, first.Equal(*delayed.SchedulingQueuedAt))
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	admitted, err = IsJobAdmitted(ctx, store, nil, *delayed.ExecutionKey, delayed.SchedulingExpiresAt)
	require.NoError(t, err)
	require.True(t, admitted)
	previousDeadline := *delayed.SchedulingExpiresAt
	_, err = store.CompareAndSwap(ctx, delayed, "id", delayed.ID, map[string]interface{}{"scheduling_expires_at": expired})
	require.NoError(t, err)
	newDeadline := deadline.Add(time.Minute)
	require.NoError(t, EnqueueJobForScheduling(ctx, store, nil, delayed, &newDeadline))
	_, err = IsJobAdmitted(ctx, store, nil, *delayed.ExecutionKey, &previousDeadline)
	require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
	require.ErrorIs(t, ReleaseJobAdmission(ctx, store, nil, *delayed.ExecutionKey, "stale", &previousDeadline), ErrWorkflowOwnershipLost)
	_, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	admitted, err = IsJobAdmitted(ctx, store, nil, *delayed.ExecutionKey, delayed.SchedulingExpiresAt)
	require.NoError(t, err)
	require.True(t, admitted)
	require.NoError(t, ReleaseJobAdmission(ctx, store, nil, *delayed.ExecutionKey, "dispatched", delayed.SchedulingExpiresAt))
	missing := *delayed
	key := "missing-checkpoint"
	missing.ExecutionKey = &key
	require.ErrorIs(t, EnqueueJobForScheduling(ctx, store, nil, &missing, &deadline), datastore.ErrRecordNotExist)
}

func TestJobSchedulerCancelledOwnerCanReleaseOnlyItsRunningAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*model.WorkflowQueue)
		wantOK bool
	}{
		{name: "same cancelled owner", wantOK: true},
		{name: "new generation", change: func(owner *model.WorkflowQueue) { owner.RunGeneration++ }},
		{name: "new token", change: func(owner *model.WorkflowQueue) { owner.RunToken = "replacement" }},
		{name: "new worker", change: func(owner *model.WorkflowQueue) { owner.WorkerID = "replacement" }},
		{name: "completed owner", change: func(owner *model.WorkflowQueue) { owner.Status = config.StatusCompleted }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newJobSchedulerTestStore(t)
			ctx := context.Background()
			owner, job := schedulerTestJob(t, store, "work", "space", "normal")
			_, err := AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			cancelled := *owner
			cancelled.Status = config.StatusCancelled
			if tc.change != nil {
				tc.change(&cancelled)
			}
			require.NoError(t, store.Put(ctx, &cancelled))
			_, err = IsJobAdmitted(ctx, store, owner, *job.ExecutionKey)
			require.ErrorIs(t, err, ErrWorkflowOwnershipLost, "cancellation must not restore execution permission")
			err = ReleaseJobAdmission(ctx, store, owner, *job.ExecutionKey, "cancelled job cleanup finished")
			if tc.wantOK {
				require.NoError(t, err)
				require.NoError(t, ReleaseJobAdmission(ctx, store, owner, *job.ExecutionKey, "cancelled job cleanup finished"))
			} else {
				require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
			}
			require.NoError(t, store.Get(ctx, job))
			if tc.wantOK {
				require.Equal(t, wfc.JobSchedulingReleased, job.SchedulingState)
			} else {
				require.Equal(t, wfc.JobSchedulingAdmitted, job.SchedulingState)
			}
		})
	}
}

func TestJobSchedulerCancelledOwnerRetainsCapacityUntilCleanupRelease(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`)
	owner, running := schedulerTestJob(t, store, "running", "a", "normal")
	_, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	_, waiting := schedulerTestJob(t, store, "waiting", "b", "normal")

	cancelled := *owner
	cancelled.Status = config.StatusCancelled
	require.NoError(t, store.Put(ctx, &cancelled))
	n, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, store.Get(ctx, running))
	require.Equal(t, wfc.JobSchedulingAdmitted, running.SchedulingState)
	require.NoError(t, store.Get(ctx, waiting))
	require.Equal(t, wfc.JobSchedulingQueued, waiting.SchedulingState)

	require.NoError(t, ReleaseJobAdmission(ctx, store, owner, *running.ExecutionKey, "cancelled job cleanup finished"))
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, store.Get(ctx, waiting))
	require.Equal(t, wfc.JobSchedulingAdmitted, waiting.SchedulingState)
}

func TestJobSchedulerRetainsCancelledKubernetesAdmissionAfterCleanupLeaseExpires(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`)
	owner, running := schedulerTestJob(t, store, "running-expired", "a", "normal")
	running.Type = string(config.JobDeployInstant)
	require.NoError(t, store.Put(ctx, running))
	_, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	_, waiting := schedulerTestJob(t, store, "waiting-after-expiry", "b", "normal")

	expired := time.Now().UTC().Add(-time.Second)
	owner.Status = config.StatusCancelled
	owner.LeaseExpiresAt = &expired
	require.NoError(t, store.Put(ctx, owner))
	n, err := AdmitQueuedJobs(ctx, store)

	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, store.Get(ctx, running))
	require.Equal(t, wfc.JobSchedulingAdmitted, running.SchedulingState)
	require.NoError(t, store.Get(ctx, waiting))
	require.Equal(t, wfc.JobSchedulingQueued, waiting.SchedulingState)
}

type failedAdmissionStore struct{ datastore.DataStore }

func (s failedAdmissionStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.ReadCommittedTransactional).WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error { return fn(failedAdmissionTx{DataStore: tx}) })
}

type failedAdmissionTx struct{ datastore.DataStore }

func (s failedAdmissionTx) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	return s.DataStore.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
}
func (s failedAdmissionTx) CompareAndSwapWithConditions(ctx context.Context, e datastore.Entity, c, u map[string]interface{}) (bool, error) {
	if u["scheduling_state"] == wfc.JobSchedulingAdmitted {
		return false, errors.New("admission write unavailable")
	}
	return s.DataStore.(datastore.ConditionalCompareAndSwap).CompareAndSwapWithConditions(ctx, e, c, u)
}
func TestJobSchedulerTransactionFailureRollsBackCleanup(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	owner, stale := schedulerTestJob(t, store, "stale", "a", "normal")
	owner.Status = config.StatusCancelled
	require.NoError(t, store.Put(ctx, owner))
	_, ready := schedulerTestJob(t, store, "ready", "b", "normal")
	n, err := AdmitQueuedJobs(ctx, failedAdmissionStore{store})
	require.ErrorContains(t, err, "admission write unavailable")
	require.Zero(t, n)
	require.NoError(t, store.Get(ctx, stale))
	require.NoError(t, store.Get(ctx, ready))
	require.Equal(t, wfc.JobSchedulingQueued, stale.SchedulingState)
	require.Equal(t, wfc.JobSchedulingQueued, ready.SchedulingState)
}

func TestJobSchedulerExpiredLeaseRetainsAdmissionUntilOwnershipIsRevoked(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`)
	owner, running := schedulerTestJob(t, store, "running", "a", "normal")
	_, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	_, waiting := schedulerTestJob(t, store, "waiting", "b", "normal")
	expired := time.Now().UTC().Add(-time.Second)
	owner.LeaseExpiresAt = &expired
	require.NoError(t, store.Put(ctx, owner))
	n, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Zero(t, n, "lease expiry alone does not revoke a worker that can still renew this generation")
	renewed, err := RenewWorkflowTaskLease(ctx, store, owner.TaskID, owner.RunGeneration, owner.RunToken, owner.WorkerID, time.Minute)
	require.NoError(t, err)
	require.True(t, renewed)
	require.NoError(t, store.Get(ctx, running))
	require.Equal(t, wfc.JobSchedulingAdmitted, running.SchedulingState)
	require.NoError(t, store.Get(ctx, waiting))
	require.Equal(t, wfc.JobSchedulingQueued, waiting.SchedulingState)
	require.NoError(t, store.Get(ctx, owner))
	owner.LeaseExpiresAt = &expired
	require.NoError(t, store.Put(ctx, owner))
	recovered, err := RecoverExpiredWorkflowTasks(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, recovered)
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the existing reaper revokes ownership before its admission can be reassigned")
	require.NoError(t, store.Get(ctx, running))
	require.Equal(t, wfc.JobSchedulingReleased, running.SchedulingState)
}

func TestJobSchedulerTerminalCallbacksWithoutWorker(t *testing.T) {
	testTerminalCallbackScheduling(t, newJobSchedulerTestStore(t))
}

func testTerminalCallbackScheduling(t *testing.T, store *sqlstore.Driver) {
	t.Helper()
	ctx := context.Background()
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`)
	for _, status := range []config.Status{config.StatusCancelled, config.StatusReject, config.StatusCompleted} {
		t.Run(string(status), func(t *testing.T) {
			owner := &model.WorkflowQueue{TaskID: "terminal-" + string(status), WorkspaceID: "terminal-space", Status: status}
			require.NoError(t, store.Add(ctx, owner))
			key := owner.TaskID + "-callback"
			job := &model.JobInfo{TaskID: owner.TaskID, WorkspaceID: owner.WorkspaceID, ExecutionKey: &key, Type: string(config.JobDeployCallback), Status: string(config.StatusWaiting)}
			deadline := time.Now().UTC().Add(time.Minute)
			require.ErrorContains(t, EnqueueJobForScheduling(ctx, store, owner, job, nil), "bounded callback")
			require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, &deadline))
			admitted, err := IsJobAdmitted(ctx, store, owner, key, job.SchedulingExpiresAt)
			require.NoError(t, err)
			require.False(t, admitted, "terminal API callbacks still wait for scheduler admission")
			n, err := AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			admitted, err = IsJobAdmitted(ctx, store, owner, key, job.SchedulingExpiresAt)
			require.NoError(t, err)
			require.True(t, admitted)
			require.NoError(t, ReleaseJobAdmission(ctx, store, owner, key, "callback finished", job.SchedulingExpiresAt))
		})
	}
	t.Run("ordinary Job cannot use terminal identity", func(t *testing.T) {
		owner := &model.WorkflowQueue{TaskID: "terminal-ordinary", WorkspaceID: "terminal-space", Status: config.StatusCancelled}
		require.NoError(t, store.Add(ctx, owner))
		key := "ordinary-job"
		job := &model.JobInfo{TaskID: owner.TaskID, WorkspaceID: owner.WorkspaceID, ExecutionKey: &key, Type: string(config.JobDeployService), Status: string(config.StatusWaiting)}
		deadline := time.Now().UTC().Add(time.Minute)
		require.ErrorContains(t, EnqueueJobForScheduling(ctx, store, owner, job, &deadline), "bounded callback")
		_, err := scheduledJobByExecutionKey(ctx, store, owner, key)
		require.ErrorIs(t, err, datastore.ErrRecordNotExist, "rejected registration rolls back its initial Job row")
	})
	for _, status := range []config.Status{config.StatusRunning, config.StatusWaiting, config.StatusWaitingApprove} {
		t.Run("zero identity rejected for "+string(status), func(t *testing.T) {
			owner := &model.WorkflowQueue{TaskID: "zero-identity-" + string(status), WorkspaceID: "terminal-space", Status: status}
			require.NoError(t, store.Add(ctx, owner))
			key := owner.TaskID + "-callback"
			job := &model.JobInfo{TaskID: owner.TaskID, WorkspaceID: owner.WorkspaceID, ExecutionKey: &key, Type: string(config.JobDeployCallback), Status: string(config.StatusWaiting)}
			deadline := time.Now().UTC().Add(time.Minute)
			require.ErrorIs(t, EnqueueJobForScheduling(ctx, store, owner, job, &deadline), ErrWorkflowOwnershipRequired)
		})
	}
	for _, change := range []struct {
		name   string
		update func(*model.WorkflowQueue)
	}{
		{"generation", func(owner *model.WorkflowQueue) { owner.RunGeneration++ }},
		{"status", func(owner *model.WorkflowQueue) { owner.Status = config.StatusCompleted }},
		{"token", func(owner *model.WorkflowQueue) { owner.RunToken = "changed-token" }},
		{"worker", func(owner *model.WorkflowQueue) { owner.WorkerID = "changed-worker" }},
	} {
		t.Run("terminal owner change fences "+change.name, func(t *testing.T) {
			owner := &model.WorkflowQueue{TaskID: "terminal-fence-" + change.name, WorkspaceID: "terminal-space", Status: config.StatusCancelled}
			require.NoError(t, store.Add(ctx, owner))
			key := owner.TaskID + "-callback"
			job := &model.JobInfo{TaskID: owner.TaskID, WorkspaceID: owner.WorkspaceID, ExecutionKey: &key, Type: string(config.JobDeployCallback), Status: string(config.StatusWaiting)}
			deadline := time.Now().UTC().Add(time.Minute)
			require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, &deadline))
			n, err := AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			latest := *owner
			change.update(&latest)
			require.NoError(t, store.Put(ctx, &latest))
			require.ErrorIs(t, EnqueueJobForScheduling(ctx, store, owner, job, &deadline), ErrWorkflowOwnershipLost)
			_, err = IsJobAdmitted(ctx, store, owner, key, job.SchedulingExpiresAt)
			require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
			require.ErrorIs(t, ReleaseJobAdmission(ctx, store, owner, key, "stale callback", job.SchedulingExpiresAt), ErrWorkflowOwnershipLost)
			require.NoError(t, store.Delete(ctx, job))
		})
	}
}
