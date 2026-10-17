package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
)

func addScheduledDefinition(t *testing.T, raw *sqlstore.Driver, ctx context.Context, id, policy string, now time.Time) *model.JobDefinition {
	t.Helper()
	declaration := commandRequest().JobSpec
	declaration.Name = id
	require.NoError(t, declaration.Normalize())
	snapshot, err := json.Marshal(declaration)
	require.NoError(t, err)
	record := &model.JobDefinition{
		ID: id, WorkspaceID: "space", Name: id, Revision: 7, Spec: string(snapshot), Creator: "definition-owner",
		Cron: "* * * * *", ConcurrencyPolicy: policy, ScheduleEnabled: true,
		NextRun: now.Add(-5 * time.Minute).Unix(), LastSkipReason: "previous failure",
	}
	require.NoError(t, raw.Add(ctx, record))
	return record
}

func TestDispatchScheduleQueuesImmutableDefinitionSnapshot(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Date(2026, time.October, 17, 2, 15, 30, 0, time.UTC)
	record := addScheduledDefinition(t, raw, ctx, "scheduled-definition", "Forbid", now)
	claimedNextRun := record.NextRun

	queued, err := service.dispatchSchedule(context.Background(), record, now)
	require.NoError(t, err)
	require.True(t, queued)
	stored := &model.JobDefinition{ID: record.ID}
	require.NoError(t, raw.Get(ctx, stored))
	require.Equal(t, now.Truncate(time.Minute).Add(time.Minute).Unix(), stored.NextRun, "missed occurrences are not replayed")
	require.NotEmpty(t, stored.LastRunTaskID)
	require.Empty(t, stored.LastSkipReason)
	run := &model.WorkflowQueue{TaskID: stored.LastRunTaskID}
	require.NoError(t, raw.Get(ctx, run))
	require.Equal(t, record.Spec, run.JobSpec)
	require.Equal(t, record.ID, run.JobDefinitionID)
	require.Equal(t, record.Revision, run.JobDefinitionRevision)
	require.Equal(t, record.Creator, run.TaskCreator)
	require.Equal(t, record.WorkspaceID, run.WorkspaceID)
	require.Equal(t, config.WorkflowTaskTypeJob, run.Type)
	require.Equal(t, config.StatusWaiting, run.Status)
	require.NotNil(t, run.IdempotencyKey)
	require.Equal(t, fmt.Sprintf("jobdef:%s:%d", record.ID, claimedNextRun), *run.IdempotencyKey)

	queued, err = service.dispatchSchedule(context.Background(), record, now)
	require.NoError(t, err)
	require.False(t, queued, "a repeated stale claim must not enqueue another run")
	count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

type scheduleClaimRaceStore struct {
	artifacts.Backend
	change func() error
}

func (s scheduleClaimRaceStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	if err := s.change(); err != nil {
		return err
	}
	return s.Backend.WithTransaction(ctx, fn)
}

func TestDispatchScheduleCASRejectsChangedDefinition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*model.JobDefinition)
	}{
		{"next run", func(record *model.JobDefinition) { record.NextRun++ }},
		{"revision", func(record *model.JobDefinition) { record.Revision++ }},
		{"disabled", func(record *model.JobDefinition) { record.ScheduleEnabled = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, raw, ctx := testJobService(t)
			now := time.Now().UTC()
			record := addScheduledDefinition(t, raw, ctx, "stale-definition", "Allow", now)
			changed := *record
			tc.change(&changed)
			claimAttempted := false
			service.Store = scheduleClaimRaceStore{Backend: service.Store, change: func() error {
				claimAttempted = true
				return raw.Client.Model(&model.JobDefinition{}).Where("id = ?", record.ID).Updates(map[string]interface{}{
					"next_run": changed.NextRun, "revision": changed.Revision, "schedule_enabled": changed.ScheduleEnabled,
				}).Error
			}}

			queued, err := service.dispatchSchedule(context.Background(), record, now)
			require.NoError(t, err)
			require.False(t, queued)
			require.True(t, claimAttempted, "the database change must race the transactional claim")
			stored := &model.JobDefinition{ID: record.ID}
			require.NoError(t, raw.Get(ctx, stored))
			require.Equal(t, changed.NextRun, stored.NextRun)
			require.Equal(t, changed.Revision, stored.Revision)
			require.Equal(t, changed.ScheduleEnabled, stored.ScheduleEnabled)
			require.Equal(t, changed.LastSkipReason, stored.LastSkipReason)
			require.Empty(t, stored.LastRunTaskID)
			count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}

func TestDispatchScheduleExistingIdempotencyKeyDoesNotDuplicateRun(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Now().UTC()
	record := addScheduledDefinition(t, raw, ctx, "idempotent-definition", "Allow", now)
	key := fmt.Sprintf("jobdef:%s:%d", record.ID, record.NextRun)
	existing := &model.WorkflowQueue{
		TaskID: "already-queued", WorkspaceID: record.WorkspaceID, Type: config.WorkflowTaskTypeJob,
		IdempotencyKey: &key, Status: config.StatusWaiting, JobSpec: record.Spec,
		JobDefinitionID: record.ID, JobDefinitionRevision: record.Revision, TaskCreator: record.Creator,
	}
	require.NoError(t, raw.Add(ctx, existing))

	_, err := service.dispatchSchedule(context.Background(), record, now)
	require.NoError(t, err)
	stored := &model.JobDefinition{ID: record.ID}
	require.NoError(t, raw.Get(ctx, stored))
	require.Greater(t, stored.NextRun, now.Unix())
	require.Equal(t, existing.TaskID, stored.LastRunTaskID)
	require.Empty(t, stored.LastSkipReason)
	count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestDispatchScheduleConcurrencyPolicies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy string
		status config.Status
		queue  bool
	}{
		{"Forbid running", "Forbid", config.StatusRunning, false},
		{"Forbid waiting", "Forbid", config.StatusWaiting, false},
		{"Forbid blocked", "Forbid", config.StatusBlocked, false},
		{"Forbid waiting approval", "Forbid", config.StatusWaitingApprove, false},
		{"Allow running", "Allow", config.StatusRunning, true},
		{"Forbid completed", "Forbid", config.StatusPassed, true},
		{"Replace completed", "Replace", config.StatusPassed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, raw, ctx := testJobService(t)
			now := time.Now().UTC()
			record := addScheduledDefinition(t, raw, ctx, "policy-definition", tc.policy, now)
			previous := &model.WorkflowQueue{TaskID: "previous-run", WorkspaceID: record.WorkspaceID, Type: config.WorkflowTaskTypeJob, Status: tc.status, JobSpec: record.Spec, JobDefinitionID: record.ID}
			require.NoError(t, raw.Add(ctx, previous))
			record.LastRunTaskID = previous.TaskID
			require.NoError(t, raw.Put(ctx, record))

			queued, err := service.dispatchSchedule(context.Background(), record, now)
			require.NoError(t, err)
			require.Equal(t, tc.queue, queued)
			stored := &model.JobDefinition{ID: record.ID}
			require.NoError(t, raw.Get(ctx, stored))
			require.Greater(t, stored.NextRun, now.Unix())
			count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
			require.NoError(t, err)
			if tc.queue {
				require.EqualValues(t, 2, count)
				require.NotEqual(t, previous.TaskID, stored.LastRunTaskID)
				require.Empty(t, stored.LastSkipReason)
			} else {
				require.EqualValues(t, 1, count)
				require.Equal(t, previous.TaskID, stored.LastRunTaskID)
				require.NotEmpty(t, stored.LastSkipReason)
				require.NotEqual(t, record.LastSkipReason, stored.LastSkipReason)
			}
		})
	}
}

type scheduleWorkflowService struct {
	domainservice.WorkflowService
	cancel func(context.Context, string, string, string) error
}

func (s scheduleWorkflowService) CancelWorkflowTask(ctx context.Context, userName, taskID, reason string) error {
	return s.cancel(ctx, userName, taskID, reason)
}

func TestDispatchScheduleReplaceCancelsBeforeEnqueueAndRetriesFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel fails=%t", failed), func(t *testing.T) {
			service, raw, ctx := testJobService(t)
			now := time.Now().UTC()
			record := addScheduledDefinition(t, raw, ctx, "replace-definition", "Replace", now)
			previous := &model.WorkflowQueue{TaskID: "previous-run", WorkspaceID: record.WorkspaceID, Type: config.WorkflowTaskTypeJob, Status: config.StatusRunning, JobSpec: record.Spec, JobDefinitionID: record.ID}
			require.NoError(t, raw.Add(ctx, previous))
			record.LastRunTaskID = previous.TaskID
			require.NoError(t, raw.Put(ctx, record))
			cancelFailure := errors.New("cancellation unavailable")
			calls := 0
			service.Workflow = scheduleWorkflowService{cancel: func(cancelCtx context.Context, userName, taskID, reason string) error {
				calls++
				require.Equal(t, record.Creator, userName)
				require.Equal(t, previous.TaskID, taskID)
				require.Equal(t, "replaced by schedule", reason)
				scope, ok := account.FromContext(cancelCtx)
				require.True(t, ok)
				require.Equal(t, "space", scope.WorkspaceID)
				require.Equal(t, "space-ns", scope.Namespace)
				stored := &model.JobDefinition{ID: record.ID}
				require.NoError(t, raw.Get(ctx, stored))
				require.Equal(t, record.NextRun, stored.NextRun, "claim must follow cancellation")
				count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
				require.NoError(t, err)
				require.EqualValues(t, 1, count, "replacement must not exist while cancellation is pending")
				if failed && calls == 1 {
					return cancelFailure
				}
				previous.Status = config.StatusCancelled
				return raw.Put(ctx, previous)
			}}

			queued, err := service.dispatchSchedule(context.Background(), record, now)
			require.Equal(t, 1, calls)
			stored := &model.JobDefinition{ID: record.ID}
			require.NoError(t, raw.Get(ctx, stored))
			if failed {
				require.ErrorIs(t, err, cancelFailure)
				require.False(t, queued)
				require.Equal(t, record.NextRun, stored.NextRun)
				require.Equal(t, previous.TaskID, stored.LastRunTaskID)
				count, countErr := raw.Count(ctx, &model.WorkflowQueue{}, nil)
				require.NoError(t, countErr)
				require.EqualValues(t, 1, count)
				queued, err = service.dispatchSchedule(context.Background(), record, now)
				require.Equal(t, 2, calls, "the unchanged due occurrence is retried")
				require.NoError(t, raw.Get(ctx, stored))
			}
			require.NoError(t, err)
			require.True(t, queued)
			require.Greater(t, stored.NextRun, now.Unix())
			require.NotEqual(t, previous.TaskID, stored.LastRunTaskID)
			require.Empty(t, stored.LastSkipReason)
			count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
			require.NoError(t, err)
			require.EqualValues(t, 2, count)
		})
	}
}

func TestDispatchScheduleValidationFailureAdvancesWithoutQueueing(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Now().UTC()
	record := addScheduledDefinition(t, raw, ctx, "invalid-definition", "Allow", now)
	var declaration spec.JobSpec
	require.NoError(t, json.Unmarshal([]byte(record.Spec), &declaration))
	declaration.Stages[0].Steps[0].Name = strings.Repeat("错误", 80)
	snapshot, err := json.Marshal(declaration)
	require.NoError(t, err)
	record.Spec = string(snapshot)
	require.NoError(t, raw.Put(ctx, record))

	queued, err := service.dispatchSchedule(context.Background(), record, now)
	require.NoError(t, err)
	require.False(t, queued)
	stored := &model.JobDefinition{ID: record.ID}
	require.NoError(t, raw.Get(ctx, stored))
	require.Greater(t, stored.NextRun, now.Unix())
	require.Contains(t, stored.LastSkipReason, "step")
	require.LessOrEqual(t, len(stored.LastSkipReason), 255)
	require.Greater(t, len(stored.LastSkipReason), 240, "exercise truncation, not a short validation error")
	require.True(t, utf8.ValidString(stored.LastSkipReason))
	require.Empty(t, stored.LastRunTaskID)
	count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestDispatchSchedulesRotatesPastFullFailedBatch(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Now().UTC()
	var last *model.JobDefinition
	for i := range 101 {
		record := addScheduledDefinition(t, raw, ctx, fmt.Sprintf("scheduled-%03d", i), "Allow", now)
		if i < 100 {
			record.Cron = "invalid"
			require.NoError(t, raw.Put(ctx, record))
		}
		last = record
	}
	queued, err := service.DispatchSchedules(context.Background())
	require.Error(t, err)
	require.Zero(t, queued)
	require.Equal(t, 2, service.scheduleDispatchPage)
	queued, err = service.DispatchSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, queued)
	require.Equal(t, 1, service.scheduleDispatchPage)
	stored := &model.JobDefinition{ID: last.ID}
	require.NoError(t, raw.Get(ctx, stored))
	require.NotEmpty(t, stored.LastRunTaskID)
	count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestDispatchScheduleEnqueueFailureRollsBackClaim(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Now().UTC()
	record := addScheduledDefinition(t, raw, ctx, "rollback-definition", "Allow", now)
	// A real SQL insert failure verifies that the preceding claim and metadata
	// update share the queue insertion's transaction.
	require.NoError(t, raw.Client.Exec(fmt.Sprintf("CREATE TRIGGER reject_scheduled_run BEFORE INSERT ON %s BEGIN SELECT RAISE(ABORT, 'scheduled queue unavailable'); END", (&model.WorkflowQueue{}).TableName())).Error)

	queued, err := service.dispatchSchedule(context.Background(), record, now)
	require.ErrorContains(t, err, "scheduled queue unavailable")
	require.False(t, queued)
	stored := &model.JobDefinition{ID: record.ID}
	require.NoError(t, raw.Get(ctx, stored))
	require.Equal(t, record.NextRun, stored.NextRun)
	require.Equal(t, record.LastSkipReason, stored.LastSkipReason)
	require.Empty(t, stored.LastRunTaskID)
	count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestDispatchSchedulesContinuesAfterOneDefinitionFails(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Now().UTC()
	invalid := addScheduledDefinition(t, raw, ctx, "a-invalid", "Allow", now)
	invalid.Cron = "invalid"
	require.NoError(t, raw.Put(ctx, invalid))
	valid := addScheduledDefinition(t, raw, ctx, "b-valid", "Allow", now)

	queued, err := service.DispatchSchedules(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), invalid.ID)
	require.Equal(t, 1, queued)
	stored := &model.JobDefinition{ID: valid.ID}
	require.NoError(t, raw.Get(ctx, stored))
	require.NotEmpty(t, stored.LastRunTaskID)
}

func TestDispatchSchedulesIgnoresInactiveDefinitions(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		change func(*model.JobDefinition)
	}{
		{"disabled", func(record *model.JobDefinition) { record.ScheduleEnabled = false }},
		{"empty-cron", func(record *model.JobDefinition) { record.Cron = "" }},
		{"not-due", func(record *model.JobDefinition) { record.NextRun = now.Add(time.Hour).Unix() }},
	} {
		record := addScheduledDefinition(t, raw, ctx, tc.name, "Allow", now)
		tc.change(record)
		require.NoError(t, raw.Client.Model(&model.JobDefinition{}).Where("id = ?", record.ID).Updates(map[string]interface{}{
			"next_run": record.NextRun, "cron": record.Cron, "schedule_enabled": record.ScheduleEnabled,
		}).Error)
		queued, err := service.dispatchSchedule(context.Background(), record, now)
		require.NoError(t, err)
		require.False(t, queued, tc.name)
		stored := &model.JobDefinition{ID: record.ID}
		require.NoError(t, raw.Get(ctx, stored))
		require.Equal(t, record.NextRun, stored.NextRun)
		require.Empty(t, stored.LastRunTaskID)
	}
	queued, err := service.DispatchSchedules(context.Background())
	require.NoError(t, err)
	require.Zero(t, queued)
	count, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestDispatchSchedulesDoesNotTriggerDeletedDefinition(t *testing.T) {
	service, raw, ctx := testJobService(t)
	now := time.Now().UTC()
	record := addScheduledDefinition(t, raw, ctx, "deleted-definition", "Allow", now)
	require.NoError(t, service.DeleteDefinition(ctx, record.ID))
	queued, err := service.dispatchSchedule(context.Background(), record, now)
	require.NoError(t, err)
	require.False(t, queued, "a definition deleted after listing cannot be claimed")
	count, err := service.DispatchSchedules(context.Background())
	require.NoError(t, err)
	require.Zero(t, count)
	count64, err := raw.Count(ctx, &model.WorkflowQueue{}, nil)
	require.NoError(t, err)
	require.Zero(t, count64)
}
