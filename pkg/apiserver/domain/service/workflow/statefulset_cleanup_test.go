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

// statefulSetCleanupKind selects a cleanup fixture by its content: plain
// removal, StatefulSet recreation, or recreation plus VCT PVC deletion.
type statefulSetCleanupKind int

const (
	noStatefulSetCleanup statefulSetCleanupKind = iota
	statefulSetRecreation
	statefulSetPVCDeletion
)

func TestExecWorkflowTaskForAppBlocksPendingStatefulSetCleanup(t *testing.T) {
	for _, cleanupVersion := range []statefulSetCleanupKind{
		statefulSetRecreation,
		statefulSetPVCDeletion,
	} {
		t.Run(fmt.Sprintf("v%d", cleanupVersion), func(t *testing.T) {
			store, workflow := newStatefulSetCleanupFenceStore(t, cleanupVersion, config.StatusFailed)
			svc := &workflowServiceImpl{
				Store:          store,
				ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
			}

			resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0, "")

			require.Nil(t, resp)
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Empty(t, store.queues)
		})
	}
}

func TestLoadPendingStatefulSetCleanupsRequiresStoreAndAppID(t *testing.T) {
	_, err := LoadPendingStatefulSetCleanups(context.Background(), nil, "app-1")
	require.ErrorContains(t, err, "datastore is required")

	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetPVCDeletion, config.StatusFailed)
	_, err = LoadPendingStatefulSetCleanups(context.Background(), store, " ")
	require.ErrorContains(t, err, "application id is required")
}

func TestStatefulSetCleanupFenceRequiresAV3ResumeForPendingPVCTemplates(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetPVCDeletion, config.StatusFailed)
	v2Store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusCompleted)
	prependStatefulSetCleanupFenceAttempt(store, v2Store, "migration-2")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestStatefulSetCleanupFenceClearsAfterSuccessfulV3Resume(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetPVCDeletion, config.StatusFailed)
	resumeStore, _ := newStatefulSetCleanupFenceStore(t, statefulSetPVCDeletion, config.StatusCompleted)
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

	resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0, "")

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Empty(t, store.queues)

	resumeStore, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusCompleted)
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
			store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
			store.tasks[0].CreateTime = tt.times[0]

			failedRetry, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
			failedRetry.tasks[0].CreateTime = tt.times[1]
			setStatefulSetCleanupFenceResolution(t, failedRetry.tasks[0], "migration-1")
			prependStatefulSetCleanupFenceAttempt(store, failedRetry, "migration-2")

			successfulRetry, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusCompleted)
			successfulRetry.tasks[0].CreateTime = tt.times[2]
			setStatefulSetCleanupFenceResolution(t, successfulRetry.tasks[0], "migration-1", "migration-2")
			prependStatefulSetCleanupFenceAttempt(store, successfulRetry, "migration-3")
			orderStatefulSetCleanupFenceTasks(t, store, "migration-1", "migration-2", "migration-3")

			require.NoError(t, EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1"))

			independentFailure, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
			independentFailure.tasks[0].CreateTime = tt.times[3]
			prependStatefulSetCleanupFenceAttempt(store, independentFailure, "migration-4")
			orderStatefulSetCleanupFenceTasks(t, store, "migration-1", "migration-2", "migration-3", "migration-4")

			require.ErrorIs(t, EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1"), bcode.ErrApplicationConfig)
		})
	}
}

func TestStatefulSetCleanupFenceDoesNotGuessLegacyRecoveryOrder(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
	store.tasks[0].CreateTime = time.Unix(200, 0)
	legacySuccess, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusCompleted)
	legacySuccess.tasks[0].CreateTime = time.Unix(100, 0)
	prependStatefulSetCleanupFenceAttempt(store, legacySuccess, "migration-2")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestStatefulSetCleanupFenceRejectsUnknownResolutionTask(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusCompleted)
	setStatefulSetCleanupFenceResolution(t, store.tasks[0], "missing")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "resolves unknown StatefulSet cleanup task missing")
}

func TestStatefulSetCleanupFenceRejectsResolutionCycle(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
	setStatefulSetCleanupFenceResolution(t, store.tasks[0], "migration-2")
	second, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
	setStatefulSetCleanupFenceResolution(t, second.tasks[0], "migration-1")
	prependStatefulSetCleanupFenceAttempt(store, second, "migration-2")

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "resolution graph contains a cycle")
}

func TestStatefulSetCleanupFencePreservesResolutionCoverage(t *testing.T) {
	for _, tt := range []struct {
		name            string
		completedTarget bool
		secondResolver  bool
		namespace       string
		pvcTemplate     string
		resolverVersion statefulSetCleanupKind
		err             string
	}{
		{name: "normalized reference"},
		{name: "completed target", completedTarget: true, err: "has no pending cleanup"},
		{name: "multiple successful resolvers", secondResolver: true, err: "resolved by both tasks"},
		{name: "different resource identity", namespace: "other", err: "does not cover every referenced"},
		{name: "different PVC template", pvcTemplate: "other", err: "does not cover every referenced"},
		{name: "V2 cannot resolve V3", resolverVersion: statefulSetRecreation, err: "does not cover every referenced"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			targetStatus := config.StatusFailed
			if tt.completedTarget {
				targetStatus = config.StatusCompleted
			}
			store, _ := newStatefulSetCleanupFenceStore(t, statefulSetPVCDeletion, targetStatus)
			version := tt.resolverVersion
			if version == 0 {
				version = statefulSetPVCDeletion
			}
			resolver, _ := newStatefulSetCleanupFenceStore(t, version, config.StatusCompleted)
			var info model.VersionUpdateCleanupInfo
			require.NoError(t, json.Unmarshal([]byte(resolver.tasks[0].CleanupInfo), &info))
			info.ResolvesTaskIDs = []string{" migration-1\t"}
			if tt.namespace != "" {
				info.Components[0].Component.Namespace = tt.namespace
			}
			if tt.pvcTemplate != "" {
				info.Components[0].StatefulSetPVCTemplatesToDelete = []string{tt.pvcTemplate}
				var marker model.VersionUpdateCleanupJobMarker
				require.NoError(t, json.Unmarshal([]byte(resolver.jobs[0].InternalInfo), &marker))
				marker.StatefulSetPVCTemplatesToDelete = []string{tt.pvcTemplate}
				payload, err := json.Marshal(marker)
				require.NoError(t, err)
				resolver.jobs[0].InternalInfo = string(payload)
			}
			payload, err := json.Marshal(info)
			require.NoError(t, err)
			resolver.tasks[0].CleanupInfo = string(payload)
			prependStatefulSetCleanupFenceAttempt(store, resolver, "migration-2")
			if tt.secondResolver {
				prependStatefulSetCleanupFenceAttempt(store, resolver, "migration-3")
			}

			err = EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
		})
	}
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
			store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, tt.jobStatus)
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
			store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, tt.jobStatus)
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
			store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, tt.jobStatus)
			store.tasks[0].Status = tt.taskStatus

			err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.NotErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
			require.NotErrorIs(t, err, bcode.ErrWorkflowTaskCancelling)
		})
	}
}

func TestStatefulSetCleanupFenceKeepsUnknownJobStatusInternal(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusCompleted)
	store.jobs[0].Status = "draining"

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.Error(t, err)
	require.NotErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.NotErrorIs(t, err, bcode.ErrWorkflowTaskCancelling)
	require.Contains(t, err.Error(), `unsupported status "draining"`)
}

func TestStatefulSetCleanupFenceRetainsTerminalTaskBeforeJobCreation(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
	store.jobs = nil

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestStatefulSetCleanupFenceIgnoresOrdinaryCleanupJobsForSameComponent(t *testing.T) {
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusCompleted)
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
	store, _ := newStatefulSetCleanupFenceStore(t, statefulSetPVCDeletion, config.StatusCompleted)
	store.jobs[0].InternalInfo = `{"source":"version_update_remove","requireStatefulSetDeletion":"invalid"}`

	err := EnsureNoPendingStatefulSetCleanup(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "decode cleanup job marker")
}

func TestExecWorkflowTaskForAppAllowsCompletedStatefulSetCleanup(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, statefulSetPVCDeletion, config.StatusCompleted)
	svc := &workflowServiceImpl{
		Store:          store,
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
	}

	resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0, "")

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

	resp, err := svc.ExecWorkflowTaskForApp(context.Background(), workflow.AppID, workflow.ID, 0, "")

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Empty(t, store.queues)
}

func TestDispatchWorkflowSchedulesSkipsPendingStatefulSetCleanup(t *testing.T) {
	store, workflow := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusFailed)
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
	store, workflow := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, config.StatusRunning)
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

func newStatefulSetCleanupFenceStore(t *testing.T, cleanupVersion statefulSetCleanupKind, cleanupStatus config.Status) (*enqueueWorkflowDataStore, *model.Workflow) {
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
		Component:                  &model.ApplicationComponent{ID: 1, AppID: workflow.AppID, Name: "db", Namespace: "default", ComponentType: config.StoreJob},
		ResourceAppName:            "shop",
		RequireStatefulSetDeletion: true,
	}
	marker := model.VersionUpdateCleanupJobMarker{
		Source:                     config.JobInfoSourceVersionUpdateRemove,
		RequireStatefulSetDeletion: true,
	}
	if cleanupVersion == statefulSetPVCDeletion {
		cleanupComponent.StatefulSetPVCTemplatesToDelete = []string{"data"}
		marker.StatefulSetPVCTemplatesToDelete = []string{"data"}
	}
	cleanupInfoJSON, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:     config.JobInfoSourceVersionUpdateRemove,
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
	store, workflow := newStatefulSetCleanupFenceStore(t, statefulSetRecreation, v2Status)
	var cleanupInfo model.VersionUpdateCleanupInfo
	require.NoError(t, json.Unmarshal([]byte(store.tasks[0].CleanupInfo), &cleanupInfo))
	cleanupInfo.Components = append(cleanupInfo.Components, model.VersionUpdateCleanupComponent{
		Component: &model.ApplicationComponent{
			ID: 2, AppID: workflow.AppID, Name: "cache", Namespace: "default", ResourceAppName: "shop",
			ComponentType: config.StoreJob,
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
	v3MarkerJSON, err := json.Marshal(model.VersionUpdateCleanupJobMarker{
		Source:                     config.JobInfoSourceVersionUpdateRemove,
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
