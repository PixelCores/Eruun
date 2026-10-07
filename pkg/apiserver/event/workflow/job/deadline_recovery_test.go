package job

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRecoveredEvaluationRetryPreservesAbsoluteDeadline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline time.Duration
		valid    bool
	}{
		{"remaining budget", 5 * time.Second, true},
		{"expired", -time.Second, false},
		{"cannot extend budget", time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			task.JobType = string(config.JobEval)
			workload := task.JobInfo.(*batchv1.Job)
			deadline := time.Now().Add(tc.deadline).UnixNano()
			workload.Annotations[EvaluationDeadlineAnnotation] = strconv.FormatInt(deadline, 10)
			ctl := NewInstantJobCtl(task, &Runtime{Client: fake.NewSimpleClientset(), Store: withJobTestOwner(&retryCheckpointStore{}, task), Ack: func() {}})
			checkpoint, err := ctl.newRetryCheckpoint(context.Background(), workload)
			if !tc.valid {
				require.ErrorContains(t, err, "invalid evaluation recovery deadline")
				return
			}
			require.NoError(t, err)
			require.Equal(t, deadline, checkpoint.Deadline)
			restored, err := decodeInstantJobRetryCheckpoint(task)
			require.NoError(t, err)
			require.Equal(t, deadline, restored.Deadline)
		})
	}
}

type retryDeadlineClockStore struct {
	*retryCheckpointStore
	now   time.Time
	err   error
	calls int
}

func (s *retryDeadlineClockStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	s.calls++
	return s.now, s.err
}

func TestRetryCheckpointUsesDatabaseDeadlineAcrossNodeSkew(t *testing.T) {
	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		for _, budget := range []time.Duration{-time.Nanosecond, 0, 5 * time.Second, 10 * time.Second, 11*time.Second + time.Nanosecond} {
			t.Run(skew.String()+"/"+budget.String(), func(t *testing.T) {
				task := retryTestTask(t, retryTestPolicy())
				task.JobType = string(config.JobEval)
				workload := task.JobInfo.(*batchv1.Job)
				now := time.Now().Add(skew)
				deadline := now.Add(budget).UnixNano()
				workload.Annotations[EvaluationDeadlineAnnotation] = strconv.FormatInt(deadline, 10)
				store := &retryDeadlineClockStore{retryCheckpointStore: &retryCheckpointStore{}, now: now}
				ctl := NewInstantJobCtl(task, &Runtime{Client: fake.NewSimpleClientset(), Store: withJobTestOwner(store, task), Ack: func() {}})
				cp, err := ctl.newRetryCheckpoint(context.Background(), workload)
				if budget <= 0 || budget > 11*time.Second {
					require.ErrorContains(t, err, "invalid evaluation recovery deadline")
					require.Nil(t, store.record)
				} else {
					require.NoError(t, err)
					require.Equal(t, deadline, cp.Deadline)
					require.EqualValues(t, int64(config.DefaultJobTTLSeconds)+int64(budget/time.Second)+1, *cp.Job.Spec.TTLSecondsAfterFinished)
				}
				require.Equal(t, 1, store.calls)
			})
		}
		t.Run(skew.String()+"/initial deadline", func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			store := &retryDeadlineClockStore{retryCheckpointStore: &retryCheckpointStore{}, now: time.Now().Add(skew)}
			ctl := NewInstantJobCtl(task, &Runtime{Client: fake.NewSimpleClientset(), Store: withJobTestOwner(store, task), Ack: func() {}})
			cp, err := ctl.newRetryCheckpoint(context.Background(), task.JobInfo.(*batchv1.Job))
			require.NoError(t, err)
			require.Equal(t, store.now.Add(time.Duration(task.Timeout)*time.Second).UnixNano(), cp.Deadline)
			require.Equal(t, 1, store.calls)
		})
	}
}

type retryDeadlineObserver struct {
	informer.ComponentReadyObserver
	observe func(context.Context) error
}

func (o retryDeadlineObserver) WaitForJob(ctx context.Context, _, _ string, _ func(*batchv1.Job) (bool, error)) error {
	return o.observe(ctx)
}

func TestRetryRecoveryUsesDatabaseRemainingBudget(t *testing.T) {
	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		for _, budget := range []time.Duration{-time.Nanosecond, 0, 5 * time.Second} {
			t.Run(skew.String()+"/"+budget.String(), func(t *testing.T) {
				task := retryTestTask(t, retryTestPolicy())
				task.JobType = string(config.JobEval)
				live := task.JobInfo.(*batchv1.Job)
				live.UID = "owned"
				live.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
				live.Labels[config.LabelManagedBy] = config.ManagedByEruun
				ttl := int32(config.DefaultJobTTLSeconds + 10)
				live.Spec.TTLSecondsAfterFinished = &ttl
				now := time.Now().Add(skew)
				deadline := now.Add(budget).UnixNano()
				cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Attempt: 1, CurrentUID: live.UID, Job: live, Deadline: deadline}
				raw, err := json.Marshal(cp)
				require.NoError(t, err)
				task.InternalInfo = string(raw)
				store := &retryDeadlineClockStore{retryCheckpointStore: &retryCheckpointStore{}, now: now}
				client := fake.NewSimpleClientset(live)
				ctl := NewInstantJobCtl(task, &Runtime{Client: client, Store: withJobTestOwner(store, task), Ack: func() {}})
				observed := false
				stop := errors.New("stop after observing remaining budget")
				ctl.resourceWaiter = retryDeadlineObserver{observe: func(ctx context.Context) error {
					observed = true
					localDeadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.InDelta(t, budget.Seconds(), time.Until(localDeadline).Seconds(), 0.5)
					return stop
				}}
				err = ctl.runWithRetryPolicy(context.Background(), live, retryTestPolicy())
				if budget <= 0 {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.False(t, observed)
					require.Empty(t, client.Actions())
				} else {
					require.ErrorIs(t, err, stop)
					require.True(t, observed)
				}
				require.Equal(t, string(raw), task.InternalInfo, "recovery must not grant a new deadline")
			})
		}
	}
}

func TestRetryDeadlineClockFailureStopsBeforeKubernetes(t *testing.T) {
	for _, restored := range []bool{false, true} {
		for _, mode := range []string{"error", "zero", "unsupported"} {
			t.Run(strconv.FormatBool(restored)+"/"+mode, func(t *testing.T) {
				task := retryTestTask(t, retryTestPolicy())
				workload := task.JobInfo.(*batchv1.Job)
				workload.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
				if restored {
					raw, err := json.Marshal(&instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Attempt: 1, Job: workload, Deadline: time.Now().Add(time.Minute).UnixNano()})
					require.NoError(t, err)
					task.InternalInfo = string(raw)
				}
				clock := &retryDeadlineClockStore{retryCheckpointStore: &retryCheckpointStore{}}
				if mode == "error" {
					clock.err = errors.New("database unavailable")
				}
				var store datastore.DataStore = withJobTestOwner(clock, task)
				if mode == "unsupported" {
					store = struct{ datastore.DataStore }{store}
				}
				client := fake.NewSimpleClientset()
				ctl := NewInstantJobCtl(task, &Runtime{Client: client, Store: store, Ack: func() {}})
				err := ctl.runWithRetryPolicy(context.Background(), workload, retryTestPolicy())
				require.ErrorIs(t, err, signal.ErrInfrastructureStop)
				require.ErrorContains(t, err, "database clock")
				require.Empty(t, client.Actions())
				require.Nil(t, clock.record)
			})
		}
	}
}

func TestRetryBackoffUsesDatabaseClockAcrossNodeSkew(t *testing.T) {
	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		for _, remaining := range []time.Duration{0, 2 * time.Second} {
			t.Run(skew.String()+"/"+remaining.String(), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					task := retryTestTask(t, retryTestPolicy())
					now := time.Now().Add(skew)
					store := &retryDeadlineClockStore{retryCheckpointStore: &retryCheckpointStore{}, now: now}
					client := fake.NewSimpleClientset()
					ctl := NewInstantJobCtl(task, &Runtime{Client: client, Store: withJobTestOwner(store, task), Ack: func() {}})
					cp := &instantJobRetryCheckpoint{Job: task.JobInfo.(*batchv1.Job), CurrentUID: "missing-owned-job", RetryAt: now.Add(remaining).UnixNano()}
					start := time.Now()
					err := ctl.ensureRetryAttempt(context.Background(), cp)
					require.ErrorContains(t, err, "disappeared")
					require.Equal(t, remaining, time.Since(start))
					require.Zero(t, countClientActions(client, "create", "jobs"))
				})
			})
		}
	}
}

func TestRetryBackoffTimestampPreservesDatabaseDeadline(t *testing.T) {
	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		t.Run(skew.String(), func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			stop := errors.New("stop after saving next attempt")
			store := &retryDeadlineClockStore{retryCheckpointStore: &retryCheckpointStore{}, now: time.Now().Add(skew)}
			store.afterSave = func(record *model.JobInfo) error {
				if record.Attempt == 2 {
					return stop
				}
				return nil
			}
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			installRetryJobReactor(t, client, 1, "OOMKilled", &created)
			ctl := NewInstantJobCtl(task, &Runtime{Client: client, Store: withJobTestOwner(store, task), Ack: func() {}})
			ctl.resourceWaiter = fixedJobSnapshotObserver{}
			err := ctl.runWithRetryPolicy(context.Background(), task.JobInfo.(*batchv1.Job), retryTestPolicy())
			require.ErrorIs(t, err, stop)
			cp, err := decodeInstantJobRetryCheckpoint(task)
			require.NoError(t, err)
			require.Equal(t, store.now.Add(time.Second).UnixNano(), cp.RetryAt)
			require.Equal(t, store.now.Add(time.Duration(task.Timeout)*time.Second).UnixNano(), cp.Deadline)
			require.Len(t, created, 1)
		})
	}
}
