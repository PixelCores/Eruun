package job

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinstant"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

func TestExpiredRecoveredJobSettlesWithoutAdmission(t *testing.T) {
	for _, kind := range []config.JobType{config.JobCommand, config.JobEval} {
		for _, tc := range []struct {
			name           string
			noUID          bool
			missing        bool
			replacement    bool
			cancelled      bool
			expiredLease   bool
			revokedOwner   bool
			infrastructure bool
		}{
			{name: "confirmed UID"},
			{name: "unconfirmed create", noUID: true},
			{name: "never created", noUID: true, missing: true},
			{name: "missing recorded execution", missing: true},
			{name: "replacement object", replacement: true},
			{name: "cancellation takes precedence", cancelled: true},
			{name: "expired lease before reaper", expiredLease: true},
			{name: "revoked lease", expiredLease: true, revokedOwner: true},
			{name: "infrastructure stop", infrastructure: true},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				db, scopedStore, _ := jobtest.EvaluationScopeStore(t)
				store := &sqlstore.Driver{Client: *db}
				task := evaluationScopeTask(t, scopedStore, "")
				task.JobType = string(kind)
				task.Status, task.Attempt = config.StatusRunning, 1
				live := task.JobInfo.(*batchv1.Job)
				live.UID, live.ResourceVersion = "original-execution", "1"
				jobresult.StampJobExecutionIdentity(task, live)
				cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: live.DeepCopy(),
					Attempt: 1, CurrentUID: live.UID, Deadline: time.Now().Add(-time.Second).UnixNano()}
				if tc.noUID {
					cp.CurrentUID = ""
				}
				raw, err := json.Marshal(cp)
				require.NoError(t, err)
				task.InternalInfo = string(raw)
				record := jobinfo.BuildJobInfoRecord(task)
				ctx := context.Background()
				require.NoError(t, store.Add(ctx, &record))
				owner := &model.WorkflowQueue{TaskID: task.TaskID}
				require.NoError(t, store.Get(ctx, owner))
				owner.RunGeneration, owner.RunToken, owner.WorkerID = 2, "replacement-token", "replacement-worker"
				task.OwnerRunGeneration, task.RunToken, task.WorkerID = owner.RunGeneration, owner.RunToken, owner.WorkerID
				if tc.expiredLease {
					expired := time.Now().Add(-time.Second)
					owner.LeaseExpiresAt = &expired
				}
				if tc.revokedOwner {
					owner.RunGeneration++
					owner.RunToken, owner.WorkerID = "newer-token", "newer-worker"
				}
				if tc.cancelled {
					owner.Status = config.StatusCancelled
				}
				require.NoError(t, store.Put(ctx, owner))
				client := fake.NewSimpleClientset()
				if !tc.missing {
					observed := live.DeepCopy()
					if tc.replacement {
						observed.UID = "replacement-object"
					}
					require.NoError(t, client.Tracker().Add(observed))
				}
				if tc.infrastructure {
					stopped, cancel := context.WithCancelCause(ctx)
					cancel(signal.ErrInfrastructureStop)
					ctx = stopped
				}
				err = runJob(ctx, task, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}})
				saved := &model.JobInfo{ID: record.ID}
				require.NoError(t, store.Get(context.Background(), saved))
				if tc.revokedOwner || tc.infrastructure {
					if tc.revokedOwner {
						require.ErrorIs(t, err, signal.ErrInfrastructureStop)
					}
					require.Equal(t, string(config.StatusRunning), saved.Status)
					require.Zero(t, saved.EndTime)
					require.Empty(t, client.Actions())
					return
				}
				require.NoError(t, err)
				wantStatus := config.StatusTimeout
				if tc.cancelled {
					wantStatus = config.StatusCancelled
				}
				require.Equal(t, wantStatus, task.Status)
				require.Equal(t, string(wantStatus), saved.Status)
				require.Positive(t, saved.EndTime)
				require.Zero(t, jobtest.CountClientActions(client, "create", "jobs"))
				if tc.replacement || tc.missing {
					require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
				} else {
					require.Equal(t, 1, jobtest.CountClientActions(client, "delete", "jobs"))
					for _, action := range client.Actions() {
						if action.GetVerb() == "delete" && action.GetResource().Resource == "jobs" {
							options := action.(k8stesting.DeleteAction).GetDeleteOptions()
							require.Equal(t, live.UID, *options.Preconditions.UID)
						}
					}
				}
				// Terminal persistence prevents another takeover from repeating work.
				before := len(client.Actions())
				require.NoError(t, runJob(ctx, task, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}}))
				require.Len(t, client.Actions(), before)
			})
		}
	}
}

type admissionRecoveryClockStore struct {
	*sqlstore.Driver
	now        time.Time
	clockErr   error
	clockCalls int
}

func (s *admissionRecoveryClockStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	s.clockCalls++
	return s.now, s.clockErr
}

func (s *admissionRecoveryClockStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.Driver.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		copy := *s
		copy.Driver = tx.(*sqlstore.Driver)
		return fn(&copy)
	})
}

func (s *admissionRecoveryClockStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.WithReadCommittedTransaction(ctx, fn)
}

func TestRecoveredJobAdmissionUsesDatabaseDeadline(t *testing.T) {
	for _, kind := range []config.JobType{config.JobCommand, config.JobEval} {
		for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
			for _, remaining := range []time.Duration{-time.Nanosecond, 0, time.Hour} {
				t.Run(string(kind)+"/"+skew.String()+"/"+remaining.String(), func(t *testing.T) {
					db, scopedStore, _ := jobtest.EvaluationScopeStore(t)
					store := &admissionRecoveryClockStore{Driver: &sqlstore.Driver{Client: *db}, now: time.Now().Add(skew)}
					task := evaluationScopeTask(t, scopedStore, "")
					task.JobType, task.Status, task.Attempt = string(kind), config.StatusRunning, 1
					live := task.JobInfo.(*batchv1.Job)
					live.UID, live.ResourceVersion = "existing-attempt", "1"
					live.Labels = map[string]string{spec.LabelManagedBy: config.ManagedByEruun}
					ttl := int32(7200)
					live.Spec.TTLSecondsAfterFinished = &ttl
					jobresult.StampJobExecutionIdentity(task, live)
					cp := jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Attempt: 1, CurrentUID: live.UID, Job: live.DeepCopy(), Deadline: store.now.Add(remaining).UnixNano()}
					raw, err := json.Marshal(cp)
					require.NoError(t, err)
					task.InternalInfo = string(raw)
					ctx := context.Background()
					owner := &model.WorkflowQueue{TaskID: task.TaskID}
					require.NoError(t, store.Get(ctx, owner))
					lease := store.now.Add(2 * time.Hour)
					owner.LeaseExpiresAt = &lease
					require.NoError(t, store.Put(ctx, owner))
					record := jobinfo.BuildJobInfoRecord(task)
					require.NoError(t, repository.EnqueueJobForScheduling(ctx, store, owner, &record, nil))
					n, err := repository.AdmitQueuedJobs(ctx, store)
					require.NoError(t, err)
					require.Equal(t, 1, n)
					require.NoError(t, store.Get(ctx, &record))
					record.Status, record.InternalInfo = string(config.StatusRunning), string(raw)
					require.NoError(t, store.Put(ctx, &record))
					client := fake.NewSimpleClientset(live)
					if remaining <= 0 {
						release, err := jobinstant.WaitForJobAdmission(ctx, store, task, client)
						require.ErrorIs(t, err, context.DeadlineExceeded)
						require.NoError(t, release())
						require.Empty(t, client.Actions(), "expired recovery must not query or mutate Kubernetes before settling")
						require.NoError(t, store.Get(ctx, &record))
						require.Equal(t, workflowconfig.JobSchedulingAdmitted, record.SchedulingState, "expiry preflight must not refresh admission")
						return
					}
					// Reach the real retry controller, then stop without a terminal result.
					// A fast node must not take runJob's Timeout cleanup/persistence branch.
					observed := false
					stop := errors.New("stop after valid recovery admission")
					observer := jobtest.RetryDeadlineObserver{Observe: func(context.Context) error { observed = true; return stop }}
					err = runJob(ctx, task, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}, ResourceWaiter: observer})
					require.ErrorIs(t, err, stop)
					require.True(t, observed)
					require.NoError(t, store.Get(ctx, &record))
					require.Equal(t, string(config.StatusRunning), record.Status)
					require.Zero(t, record.EndTime)
					require.Equal(t, string(raw), record.InternalInfo, "retain the original deadline and execution identity")
					require.Zero(t, jobtest.CountClientActions(client, "create", "jobs"))
					require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
				})
			}
		}
	}
}

func TestRecoveredJobAdmissionClockFailureKeepsExecution(t *testing.T) {
	for _, kind := range []config.JobType{config.JobCommand, config.JobEval} {
		for _, mode := range []string{"query failure", "zero", "unsupported"} {
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				db, scopedStore, _ := jobtest.EvaluationScopeStore(t)
				rawStore := &sqlstore.Driver{Client: *db}
				task := evaluationScopeTask(t, scopedStore, "")
				task.JobType, task.Status, task.Attempt = string(kind), config.StatusRunning, 1
				live := task.JobInfo.(*batchv1.Job)
				live.UID = "existing-attempt"
				jobresult.StampJobExecutionIdentity(task, live)
				// Node time would already declare this execution expired.
				raw, err := json.Marshal(jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Attempt: 1, CurrentUID: live.UID, Job: live.DeepCopy(), Deadline: time.Now().Add(-time.Hour).UnixNano()})
				require.NoError(t, err)
				task.InternalInfo = string(raw)
				record := jobinfo.BuildJobInfoRecord(task)
				require.NoError(t, rawStore.Add(context.Background(), &record))
				clock := &admissionRecoveryClockStore{Driver: rawStore}
				if mode == "query failure" {
					clock.clockErr = errors.New("database unavailable")
				}
				var store datastore.DataStore = clock
				if mode == "unsupported" {
					store = struct{ datastore.DataStore }{rawStore}
				}
				client := fake.NewSimpleClientset(live)
				err = runJob(context.Background(), task, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}})
				require.ErrorIs(t, err, signal.ErrInfrastructureStop)
				require.ErrorContains(t, err, "database clock")
				require.Empty(t, client.Actions())
				require.NoError(t, rawStore.Get(context.Background(), &record))
				require.Equal(t, string(config.StatusRunning), record.Status)
				require.Zero(t, record.EndTime)
				require.Empty(t, record.SchedulingState)
				require.Equal(t, string(raw), record.InternalInfo)
			})
		}
	}
}
