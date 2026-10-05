package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
)

// Keep the authoritative clock attached to transaction handles as in SQL.
type recoveryClockStore struct {
	artifacts.Backend
	now time.Time
	err error
}

func (s recoveryClockStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	return s.now, s.err
}

func (s recoveryClockStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.Backend.WithTransaction(ctx, func(tx datastore.DataStore) error {
		backend, err := artifacts.RequireBackend(tx)
		if err != nil {
			return err
		}
		return fn(recoveryClockStore{Backend: backend, now: s.now, err: s.err})
	})
}

func TestEvaluationRecoveryUsesDatabaseDeadlineAcrossNodeSkew(t *testing.T) {
	grace := time.Duration(spec.EvaluationCollectionGraceSeconds) * time.Second
	for _, offset := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		for _, remaining := range []time.Duration{grace + time.Minute, grace, grace - time.Nanosecond} {
			t.Run(offset.String()+"/"+remaining.String(), func(t *testing.T) {
				f, task, point := newRecoverableEvaluation(t)
				ctx := context.Background()
				now := time.Now().Add(offset).Truncate(time.Second)
				deadline := now.Add(remaining)
				var checkpoint map[string]any
				require.NoError(t, json.Unmarshal([]byte(f.record.InternalInfo), &checkpoint))
				checkpoint["deadline"] = deadline.UnixNano()
				raw, err := json.Marshal(checkpoint)
				require.NoError(t, err)
				f.record.InternalInfo, task.InternalInfo = string(raw), string(raw)
				require.NoError(t, f.raw.Put(ctx, f.record))
				point.SourceDeadline, point.ExpiresAt = deadline, now.Add(time.Hour)
				require.NoError(t, f.raw.Put(ctx, point))
				f.service.Store = recoveryClockStore{Backend: f.service.Store, now: now}
				originalKey := task.ExecutionKey

				again, err := f.service.RecoverEvaluation(ctx, task)
				require.NoError(t, err)
				require.Equal(t, remaining > grace, again)
				count, err := f.raw.Count(ctx, &model.JobInfo{TaskID: task.TaskID}, nil)
				require.NoError(t, err)
				if !again {
					require.Equal(t, originalKey, task.ExecutionKey)
					require.EqualValues(t, 1, count)
					return
				}
				require.EqualValues(t, 2, count)
				info, err := decodeEvaluationInfo(task.EvaluationInfo)
				require.NoError(t, err)
				require.Equal(t, deadline.UnixNano(), info.ExecutionDeadline, "recovery cannot extend the original deadline")
				require.EqualValues(t, remaining/time.Second, task.Timeout)
				require.Equal(t, task.Timeout, *task.JobInfo.(*batchv1.Job).Spec.ActiveDeadlineSeconds)
			})
		}
	}
}

func TestEvaluationRecoveryDatabaseClockFailurePreservesExecution(t *testing.T) {
	clockErr := errors.New("database clock unavailable")
	for _, clockFailure := range []error{clockErr, nil} {
		name := "zero timestamp"
		if clockFailure != nil {
			name = "query failure"
		}
		t.Run(name, func(t *testing.T) {
			f, task, point := newRecoverableEvaluation(t)
			ctx := context.Background()
			point.ReferencedByExecutionKey = task.ExecutionKey
			require.NoError(t, f.raw.Put(ctx, point))
			f.service.Store = recoveryClockStore{Backend: f.service.Store, err: clockFailure}
			originalKey, originalInfo := task.ExecutionKey, task.EvaluationInfo
			again, err := f.service.RecoverEvaluation(ctx, task)
			if clockFailure != nil {
				require.ErrorIs(t, err, clockFailure)
			} else {
				require.ErrorContains(t, err, "zero timestamp")
			}
			require.False(t, again)
			require.Equal(t, originalKey, task.ExecutionKey)
			require.NoError(t, f.raw.Get(ctx, f.record))
			require.Equal(t, originalInfo, f.record.EvaluationInfo)
			require.NoError(t, f.raw.Get(ctx, point))
			require.Equal(t, originalKey, point.ReferencedByExecutionKey)
			count, err := f.raw.Count(ctx, &model.JobInfo{TaskID: task.TaskID}, nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
		})
	}
}

func TestRecoveryRenderingRejectsUnavailableDatabaseClock(t *testing.T) {
	clockErr := errors.New("database clock unavailable")
	for _, condition := range []string{"unsupported", "query failure", "zero timestamp"} {
		t.Run(condition, func(t *testing.T) {
			f, task, point := newRecoverableEvaluation(t)
			info, err := decodeEvaluationInfo(task.EvaluationInfo)
			require.NoError(t, err)
			info.ResumeCheckpointID = point.ID
			info.ExecutionDeadline = point.SourceDeadline.UnixNano()
			raw, err := json.Marshal(info)
			require.NoError(t, err)
			task.EvaluationInfo = string(raw)
			var store datastore.DataStore
			switch condition {
			case "unsupported":
				store = struct{ datastore.DataStore }{f.service.Store}
			case "query failure":
				store = recoveryClockStore{Backend: f.service.Store, err: clockErr}
			case "zero timestamp":
				store = recoveryClockStore{Backend: f.service.Store}
			}
			err = BuildEvaluationTask(context.Background(), store, f.service.Config, task, spec.JobTraits{})
			if condition == "query failure" {
				require.ErrorIs(t, err, clockErr)
			} else if condition == "zero timestamp" {
				require.ErrorContains(t, err, "zero timestamp")
			} else {
				require.ErrorContains(t, err, "requires a database clock")
			}
		})
	}
}
