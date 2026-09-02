package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestExecWorkflowTaskForAppBlocksPendingStatefulSetCleanup(t *testing.T) {
	for _, cleanupVersion := range []int{
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
	} {
		t.Run(fmt.Sprintf("v%d", cleanupVersion), func(t *testing.T) {
			store, workflow := newStatefulSetCleanupFenceStore(t, cleanupVersion, config.StatusFailed)
			svc := &workflowServiceImpl{
				Store:          store,
				ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
			}

			resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0)

			require.Nil(t, resp)
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Empty(t, store.queues)
		})
	}
}

func TestExecWorkflowTaskBlocksPendingStatefulSetCleanup(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, config.StatusFailed)
	svc := &workflowServiceImpl{
		Store:          store,
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
	}

	resp, err := svc.ExecWorkflowTask(context.Background(), workflow.ID, 0)

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Empty(t, store.queues)
}

func TestStatefulSetCleanupFenceRequiresAV3ResumeForPendingPVCTemplates(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, config.StatusFailed)
	v2Store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusCompleted)
	prependStatefulSetCleanupFenceAttempt(store, v2Store, "migration-2")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestStatefulSetCleanupFenceClearsAfterSuccessfulV3Resume(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, config.StatusFailed)
	resumeStore, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, config.StatusCompleted)
	setStatefulSetCleanupFenceResolution(t, resumeStore.tasks[0], "migration-1")
	prependStatefulSetCleanupFenceAttempt(store, resumeStore, "migration-2")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.NoError(t, err)
}

func TestStatefulSetCleanupFenceTracksMixedV2V3Components(t *testing.T) {
	store, workflow := newMixedStatefulSetCleanupFenceStore(t, config.StatusFailed, config.StatusCompleted)
	svc := &workflowServiceImpl{
		Store:          store,
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
	}

	resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0)

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Empty(t, store.queues)

	resumeStore, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusCompleted)
	prependStatefulSetCleanupFenceAttempt(store, resumeStore, "migration-2")
	err = EnsureNoPendingStatefulSetCleanup(context.Background(), store, workflow.AppID)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "cache", "a v2-only resume must not clear the v3 component from the failed workflow")

	completedMixedStore, _ := newMixedStatefulSetCleanupFenceStore(t, config.StatusCompleted, config.StatusCompleted)
	setStatefulSetCleanupFenceResolution(t, completedMixedStore.tasks[0], "migration-1")
	prependStatefulSetCleanupFenceAttempt(store, completedMixedStore, "migration-3")
	require.NoError(t, EnsureNoPendingStatefulSetCleanup(context.Background(), store, workflow.AppID))
}

func TestStatefulSetCleanupFenceUsesExplicitCausalityInsteadOfCreateTime(t *testing.T) {
	testCases := []struct {
		name  string
		times []time.Time
	}{
		{
			name: "wall clocks run backwards",
			times: []time.Time{
				time.Unix(300, 0),
				time.Unix(200, 0),
				time.Unix(100, 0),
				time.Unix(50, 0),
			},
		},
		{
			name:  "timestamps tie",
			times: []time.Time{time.Unix(100, 0), time.Unix(100, 0), time.Unix(100, 0), time.Unix(100, 0)},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
			store.tasks[0].CreateTime = tt.times[0]

			failedRetry, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
			failedRetry.tasks[0].CreateTime = tt.times[1]
			setStatefulSetCleanupFenceResolution(t, failedRetry.tasks[0], "migration-1")
			prependStatefulSetCleanupFenceAttempt(store, failedRetry, "migration-2")

			successfulRetry, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusCompleted)
			successfulRetry.tasks[0].CreateTime = tt.times[2]
			setStatefulSetCleanupFenceResolution(t, successfulRetry.tasks[0], "migration-1", "migration-2")
			prependStatefulSetCleanupFenceAttempt(store, successfulRetry, "migration-3")
			orderStatefulSetCleanupFenceTasks(t, store, "migration-1", "migration-2", "migration-3")

			require.NoError(t, EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1"))

			independentFailure, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
			independentFailure.tasks[0].CreateTime = tt.times[3]
			prependStatefulSetCleanupFenceAttempt(store, independentFailure, "migration-4")
			orderStatefulSetCleanupFenceTasks(t, store, "migration-1", "migration-2", "migration-3", "migration-4")

			require.ErrorIs(t, EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1"), bcode.ErrApplicationConfig)
		})
	}
}

func TestStatefulSetCleanupFenceDoesNotGuessLegacyRecoveryOrder(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
	store.tasks[0].CreateTime = time.Unix(200, 0)
	legacySuccess, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusCompleted)
	legacySuccess.tasks[0].CreateTime = time.Unix(100, 0)
	prependStatefulSetCleanupFenceAttempt(store, legacySuccess, "migration-2")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestStatefulSetCleanupFenceRejectsUnknownResolutionTask(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusCompleted)
	setStatefulSetCleanupFenceResolution(t, store.tasks[0], "missing")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "resolves unknown StatefulSet cleanup task missing")
}

func TestStatefulSetCleanupFenceRejectsResolutionCycle(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
	setStatefulSetCleanupFenceResolution(t, store.tasks[0], "migration-2")
	second, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
	setStatefulSetCleanupFenceResolution(t, second.tasks[0], "migration-1")
	prependStatefulSetCleanupFenceAttempt(store, second, "migration-2")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "resolution graph contains a cycle")
}

func TestStatefulSetCleanupFenceAllowsCompletedMixedV2V3Task(t *testing.T) {
	store, _ := newMixedStatefulSetCleanupFenceStore(t, config.StatusCompleted, config.StatusCompleted)

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.NoError(t, err)
}

func TestStatefulSetCleanupFenceDoesNotTrustPassedOrSkippedCleanup(t *testing.T) {
	tests := []struct {
		name       string
		taskStatus config.Status
		jobStatus  config.Status
	}{
		{name: "cleanup passed", taskStatus: config.StatusCompleted, jobStatus: config.StatusPassed},
		{name: "cleanup skipped", taskStatus: config.StatusCompleted, jobStatus: config.StatusSkipped},
		{name: "workflow passed", taskStatus: config.StatusPassed, jobStatus: config.StatusCompleted},
		{name: "workflow skipped", taskStatus: config.StatusSkipped, jobStatus: config.StatusCompleted},
		{name: "workflow failed after cleanup completed", taskStatus: config.StatusFailed, jobStatus: config.StatusCompleted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, tt.jobStatus)
			store.tasks[0].Status = tt.taskStatus

			err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
		})
	}
}

func TestStatefulSetCleanupFenceBlocksTerminalTaskWithActiveCleanupJob(t *testing.T) {
	tests := []struct {
		name       string
		taskStatus config.Status
		jobStatus  config.Status
		want       error
	}{
		{name: "failed with running cleanup", taskStatus: config.StatusFailed, jobStatus: config.StatusRunning, want: bcode.ErrWorkflowTaskRunning},
		{name: "cancelled with running cleanup", taskStatus: config.StatusCancelled, jobStatus: config.StatusRunning, want: bcode.ErrWorkflowTaskCancelling},
		{name: "timed out with preparing cleanup", taskStatus: config.StatusTimeout, jobStatus: config.StatusPrepare, want: bcode.ErrWorkflowTaskRunning},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, tt.jobStatus)
			store.tasks[0].Status = tt.taskStatus

			err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestStatefulSetCleanupFenceTreatsPreStartJobOnTerminalTaskAsPending(t *testing.T) {
	tests := []struct {
		name       string
		taskStatus config.Status
		jobStatus  config.Status
	}{
		{name: "failed with empty cleanup status", taskStatus: config.StatusFailed, jobStatus: ""},
		{name: "failed with queued cleanup", taskStatus: config.StatusFailed, jobStatus: config.StatusQueued},
		{name: "timed out with created cleanup", taskStatus: config.StatusTimeout, jobStatus: config.StatusCreated},
		{name: "cancelled with waiting cleanup", taskStatus: config.StatusCancelled, jobStatus: config.StatusWaiting},
		{name: "rejected with pending cleanup", taskStatus: config.StatusReject, jobStatus: config.QueueItemPending},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, tt.jobStatus)
			store.tasks[0].Status = tt.taskStatus

			err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.NotErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
			require.NotErrorIs(t, err, bcode.ErrWorkflowTaskCancelling)
		})
	}
}

func TestStatefulSetCleanupFenceKeepsUnknownJobStatusInternal(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusCompleted)
	store.jobs[0].Status = "draining"

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.Error(t, err)
	require.NotErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.NotErrorIs(t, err, bcode.ErrWorkflowTaskCancelling)
	require.Contains(t, err.Error(), `unsupported status "draining"`)
}

func TestStatefulSetCleanupFenceRetainsTerminalTaskBeforeJobCreation(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
	store.jobs = nil

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestStatefulSetCleanupFenceIgnoresOrdinaryCleanupJobsForSameComponent(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusCompleted)
	store.jobs = append([]*model.JobInfo{
		{
			TaskID: "migration-1", AppID: "app-1", Type: string(config.JobCleanupResources),
			ServiceName: "db", Status: string(config.StatusCompleted),
		},
		{
			TaskID: "migration-1", AppID: "app-1", Type: string(config.JobCleanupResources),
			ServiceName: "db", Status: string(config.StatusCompleted), InternalInfo: `{"source":"ordinary_cleanup"}`,
		},
		{
			TaskID: "migration-1", AppID: "app-1", Type: string(config.JobCleanupResources),
			ServiceName: "db", Status: string(config.StatusCompleted), InternalInfo: "not-json",
		},
	}, store.jobs...)

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.NoError(t, err)
}

func TestStatefulSetCleanupFenceStrictlyValidatesExplicitMigrationMarker(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, config.StatusCompleted)
	store.jobs[0].InternalInfo = `{"source":"version_update_remove","version":"invalid","requireStatefulSetDeletion":true}`

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "decode cleanup job marker")
}

func TestMarkTaskStatusBlocksDelayedOrdinaryTaskAfterFailedStatefulSetMigration(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, 0, "")
	ordinary := &model.WorkflowQueue{
		TaskID: "ordinary-future", AppID: workflow.AppID, WorkflowID: workflow.ID,
		Status: config.StatusWaiting, ExecuteAt: time.Now().Add(time.Hour).Unix(),
		BaseModel: model.BaseModel{CreateTime: time.Now().Add(-time.Hour)},
	}
	store.tasks = append(store.tasks, ordinary)
	failedStore, _ := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
	failedStore.tasks[0].CreateTime = time.Now()
	store.tasks = append(failedStore.tasks, store.tasks...)
	store.jobs = append(store.jobs, failedStore.jobs...)
	svc := &workflowServiceImpl{Store: store, ScheduleLocker: locker.NewMemoryLocker("test-app-schedule")}
	ordinary.ExecuteAt = time.Now().Add(-time.Second).Unix()
	waiting, err := svc.WaitingTasks(context.Background())
	require.NoError(t, err)
	require.Contains(t, waiting, ordinary)

	claimed, err := svc.MarkTaskStatus(context.Background(), ordinary.TaskID, config.StatusWaiting, config.StatusQueued)

	require.False(t, claimed)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Equal(t, config.StatusWaiting, ordinary.Status)
}

func TestMarkTaskStatusAllowsStatefulSetMigrationResumeTask(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, config.StatusFailed)
	resume := &model.WorkflowQueue{
		TaskID: "migration-resume", AppID: workflow.AppID, WorkflowID: workflow.ID,
		Status: config.StatusWaiting, ExecuteAt: time.Now().Add(-time.Second).Unix(), CleanupInfo: store.tasks[0].CleanupInfo,
		BaseModel: model.BaseModel{CreateTime: time.Now()},
	}
	store.tasks = append([]*model.WorkflowQueue{resume}, store.tasks...)
	svc := &workflowServiceImpl{Store: store, ScheduleLocker: locker.NewMemoryLocker("test-app-schedule")}

	claimed, err := svc.MarkTaskStatus(context.Background(), resume.TaskID, config.StatusWaiting, config.StatusQueued)

	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, config.StatusQueued, resume.Status)
}

func TestMarkTaskStatusUsesApplicationScheduleLock(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, 0, "")
	task := &model.WorkflowQueue{
		TaskID: "ordinary-due", AppID: workflow.AppID, WorkflowID: workflow.ID,
		Status: config.StatusWaiting, ExecuteAt: time.Now().Add(-time.Second).Unix(),
	}
	store.tasks = append(store.tasks, task)
	lockProvider := locker.NewMemoryLocker("test-app-schedule")
	held := lockProvider.NewMutex("app-schedule:" + workflow.AppID)
	require.NoError(t, held.TryLock(context.Background()))
	t.Cleanup(func() { _ = held.Unlock(context.Background()) })
	svc := &workflowServiceImpl{Store: store, ScheduleLocker: lockProvider}

	claimed, err := svc.MarkTaskStatus(context.Background(), task.TaskID, config.StatusWaiting, config.StatusQueued)

	require.False(t, claimed)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Equal(t, config.StatusWaiting, task.Status)
}

func TestMarkTaskStatusKeepsImmediateTaskDispatchIndependentOfScheduleLocker(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, 0, "")
	task := &model.WorkflowQueue{
		TaskID: "ordinary-immediate", AppID: workflow.AppID, WorkflowID: workflow.ID,
		Status: config.StatusWaiting,
	}
	store.tasks = append(store.tasks, task)
	svc := &workflowServiceImpl{Store: store}

	claimed, err := svc.MarkTaskStatus(context.Background(), task.TaskID, config.StatusWaiting, config.StatusQueued)

	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, config.StatusQueued, task.Status)
}

func TestExecWorkflowTaskForAppAllowsCompletedStatefulSetCleanup(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, config.StatusCompleted)
	svc := &workflowServiceImpl{
		Store:          store,
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
	}

	resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.queues, 1)
}

func TestExecWorkflowTaskForAppUsesApplicationScheduleLock(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, 0, "")
	lockProvider := locker.NewMemoryLocker("test-app-schedule")
	held := lockProvider.NewMutex("app-schedule:" + workflow.AppID)
	require.NoError(t, held.TryLock(context.Background()))
	t.Cleanup(func() { _ = held.Unlock(context.Background()) })
	svc := &workflowServiceImpl{Store: store, ScheduleLocker: lockProvider}

	resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0)

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Empty(t, store.queues)
}

func TestDispatchWorkflowSchedulesSkipsPendingStatefulSetCleanup(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusFailed)
	initialNextRun := time.Now().Add(-time.Minute).Unix()
	store.schedules = []*model.WorkflowSchedule{{
		ID:         "schedule-1",
		AppID:      workflow.AppID,
		WorkflowID: workflow.ID,
		Cron:       "*/5 * * * *",
		Enabled:    true,
		NextRun:    initialNextRun,
	}}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: &store.scheduleDataStore}
	svc := &workflowServiceImpl{
		Store:          txStore,
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
	}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())

	require.NoError(t, err)
	require.Zero(t, processed)
	require.Empty(t, store.queues)
	require.Greater(t, store.schedules[0].NextRun, initialNextRun)
}

func TestDispatchWorkflowSchedulesSkipsMixedV2V3PendingCleanup(t *testing.T) {
	store, workflow := newMixedStatefulSetCleanupFenceStore(t, config.StatusFailed, config.StatusCompleted)
	initialNextRun := time.Now().Add(-time.Minute).Unix()
	store.schedules = []*model.WorkflowSchedule{{
		ID: "schedule-1", AppID: workflow.AppID, WorkflowID: workflow.ID,
		Cron: "*/5 * * * *", Enabled: true, NextRun: initialNextRun,
	}}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: &store.scheduleDataStore}
	svc := &workflowServiceImpl{Store: txStore, ScheduleLocker: locker.NewMemoryLocker("test-app-schedule")}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())

	require.NoError(t, err)
	require.Zero(t, processed)
	require.Empty(t, store.queues)
	require.Greater(t, store.schedules[0].NextRun, initialNextRun)
}

func TestDispatchWorkflowSchedulesSkipsCancellingStatefulSetCleanup(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, config.StatusRunning)
	store.tasks[0].Status = config.StatusCancelled
	initialNextRun := time.Now().Add(-time.Minute).Unix()
	store.schedules = []*model.WorkflowSchedule{{
		ID: "schedule-1", AppID: workflow.AppID, WorkflowID: workflow.ID,
		Cron: "*/5 * * * *", Enabled: true, NextRun: initialNextRun,
	}}
	txStore := &transactionalScheduleDataStore{scheduleDataStore: &store.scheduleDataStore}
	svc := &workflowServiceImpl{Store: txStore, ScheduleLocker: locker.NewMemoryLocker("test-app-schedule")}

	processed, err := svc.DispatchWorkflowSchedules(context.Background())

	require.NoError(t, err)
	require.Zero(t, processed)
	require.Empty(t, store.queues)
	require.Greater(t, store.schedules[0].NextRun, initialNextRun)
}

func newStatefulSetCleanupFenceStore(t *testing.T, cleanupVersion int, cleanupStatus config.Status) (*enqueueWorkflowDataStore, *model.Workflow) {
	t.Helper()
	steps, err := model.NewJSONStructByStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{
		Name:         "deploy-db",
		WorkflowType: config.JobDeploy,
		Properties:   []model.Policies{{Policies: []string{"db"}}},
	}}})
	require.NoError(t, err)
	workflow := &model.Workflow{ID: "workflow-1", AppID: "app-1", Name: "deploy", Steps: steps}
	store := &enqueueWorkflowDataStore{
		scheduleDataStore: scheduleDataStore{workflows: []*model.Workflow{workflow}},
		components:        []*model.ApplicationComponent{{ID: 1, AppID: workflow.AppID, Name: "db"}},
	}
	if cleanupVersion == 0 {
		return store, workflow
	}
	cleanupComponent := model.VersionUpdateCleanupComponent{
		Component:                  &model.ApplicationComponent{ID: 1, AppID: workflow.AppID, Name: "db", Namespace: "default"},
		ResourceAppName:            "shop",
		RequireStatefulSetDeletion: true,
	}
	marker := statefulSetCleanupJobMarker{
		Source:                     config.JobInfoSourceVersionUpdateRemove,
		RequireStatefulSetDeletion: true,
	}
	if cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
		cleanupComponent.StatefulSetPVCTemplatesToDelete = []string{"data"}
		marker.Version = cleanupVersion
		marker.StatefulSetPVCTemplatesToDelete = []string{"data"}
	}
	cleanupInfoJSON, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:     config.JobInfoSourceVersionUpdateRemove,
		Version:    cleanupVersion,
		Components: []model.VersionUpdateCleanupComponent{cleanupComponent},
	})
	require.NoError(t, err)
	markerJSON, err := json.Marshal(marker)
	require.NoError(t, err)
	store.tasks = []*model.WorkflowQueue{{
		TaskID:      "migration-1",
		AppID:       workflow.AppID,
		WorkflowID:  workflow.ID,
		Status:      cleanupStatus,
		CleanupInfo: string(cleanupInfoJSON),
	}}
	store.jobs = []*model.JobInfo{{
		TaskID:       "migration-1",
		AppID:        workflow.AppID,
		Type:         string(config.JobCleanupResources),
		ServiceName:  "db",
		Status:       string(cleanupStatus),
		InternalInfo: string(markerJSON),
	}}
	return store, workflow
}

func newMixedStatefulSetCleanupFenceStore(
	t *testing.T,
	v2Status config.Status,
	v3Status config.Status,
) (*enqueueWorkflowDataStore, *model.Workflow) {
	t.Helper()
	store, workflow := newStatefulSetCleanupFenceStore(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, v2Status)
	var cleanupInfo model.VersionUpdateCleanupInfo
	require.NoError(t, json.Unmarshal([]byte(store.tasks[0].CleanupInfo), &cleanupInfo))
	cleanupInfo.Version = model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion
	cleanupInfo.Components = append(cleanupInfo.Components, model.VersionUpdateCleanupComponent{
		Component: &model.ApplicationComponent{
			ID: 2, AppID: workflow.AppID, Name: "cache", Namespace: "default", ResourceAppName: "shop",
		},
		ResourceAppName:                 "shop",
		RequireStatefulSetDeletion:      true,
		StatefulSetPVCTemplatesToDelete: []string{"data"},
	})
	cleanupInfoJSON, err := json.Marshal(cleanupInfo)
	require.NoError(t, err)
	store.tasks[0].CleanupInfo = string(cleanupInfoJSON)
	if v2Status == config.StatusCompleted && v3Status == config.StatusCompleted {
		store.tasks[0].Status = config.StatusCompleted
	} else {
		store.tasks[0].Status = config.StatusFailed
	}
	v3MarkerJSON, err := json.Marshal(statefulSetCleanupJobMarker{
		Source: config.JobInfoSourceVersionUpdateRemove, Version: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		RequireStatefulSetDeletion: true, StatefulSetPVCTemplatesToDelete: []string{"data"},
	})
	require.NoError(t, err)
	store.jobs = append(store.jobs, &model.JobInfo{
		TaskID: "migration-1", AppID: workflow.AppID, Type: string(config.JobCleanupResources),
		ServiceName: "cache", Status: string(v3Status), InternalInfo: string(v3MarkerJSON),
	})
	return store, workflow
}

func prependStatefulSetCleanupFenceAttempt(target, attempt *enqueueWorkflowDataStore, taskID string) {
	task := *attempt.tasks[0]
	task.TaskID = taskID
	target.tasks = append([]*model.WorkflowQueue{&task}, target.tasks...)
	for _, source := range attempt.jobs {
		job := *source
		job.TaskID = taskID
		target.jobs = append(target.jobs, &job)
	}
}

func setStatefulSetCleanupFenceResolution(t *testing.T, task *model.WorkflowQueue, taskIDs ...string) {
	t.Helper()
	require.NotNil(t, task)
	var cleanupInfo model.VersionUpdateCleanupInfo
	require.NoError(t, json.Unmarshal([]byte(task.CleanupInfo), &cleanupInfo))
	cleanupInfo.ResolvesTaskIDs = append([]string(nil), taskIDs...)
	payload, err := json.Marshal(cleanupInfo)
	require.NoError(t, err)
	task.CleanupInfo = string(payload)
}

func orderStatefulSetCleanupFenceTasks(t *testing.T, store *enqueueWorkflowDataStore, taskIDs ...string) {
	t.Helper()
	require.NotNil(t, store)
	tasksByID := make(map[string]*model.WorkflowQueue, len(store.tasks))
	for _, task := range store.tasks {
		if task != nil {
			tasksByID[task.TaskID] = task
		}
	}
	ordered := make([]*model.WorkflowQueue, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		task := tasksByID[taskID]
		require.NotNil(t, task, "missing workflow task %s", taskID)
		ordered = append(ordered, task)
	}
	store.tasks = ordered
}
