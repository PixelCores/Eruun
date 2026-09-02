package application

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestUpdateVersionFullRebuildRestoresPendingStatefulSetDeletionV2(t *testing.T) {
	store, svc, req := newStatefulSetDeletionV2RetryFixture(t)

	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	firstCleanup := requireVersionUpdateCleanupInfoVersion(
		t,
		store.tasks[first.TaskID],
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
	)
	firstComponent := requireVersionUpdateCleanupComponent(t, firstCleanup, "mysql")
	require.True(t, firstComponent.RequireStatefulSetDeletion)
	require.Empty(t, firstComponent.StatefulSetPVCTemplatesToDelete)
	store.tasks[first.TaskID].CreateTime = time.Unix(300, 0)
	store.tasks[first.TaskID].Status = config.StatusFailed
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusFailed)

	req.Version = "2.1.0"
	second, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEqual(t, first.TaskID, second.TaskID)
	secondCleanup := requireVersionUpdateCleanupInfoVersion(
		t,
		store.tasks[second.TaskID],
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
	)
	require.Equal(t, []string{first.TaskID}, secondCleanup.ResolvesTaskIDs)
	secondComponent := requireVersionUpdateCleanupComponent(t, secondCleanup, "mysql")
	require.True(t, secondComponent.RequireStatefulSetDeletion)
	require.Empty(t, secondComponent.StatefulSetPVCTemplatesToDelete)
	var restoredTraits apisv1.Traits
	require.NoError(t, decodeJSONStruct(secondComponent.Component.Traits, &restoredTraits))
	require.Equal(t, "mysql-headless", restoredTraits.Service[0].Name)

	secondJob := requireVersionUpdateCleanupJobForTask(t, store, second.TaskID, "mysql")
	var marker versionUpdateCleanupJobMarker
	require.NoError(t, json.Unmarshal([]byte(secondJob.InternalInfo), &marker))
	require.Zero(t, marker.Version)
	require.True(t, marker.RequireStatefulSetDeletion)
	require.Empty(t, marker.StatefulSetPVCTemplatesToDelete)

	store.tasks[second.TaskID].CreateTime = time.Unix(200, 0)
	store.tasks[second.TaskID].Status = config.StatusFailed
	secondJob.Status = string(config.StatusFailed)
	req.Version = "2.2.0"
	third, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	thirdCleanup := requireVersionUpdateCleanupInfoVersion(
		t,
		store.tasks[third.TaskID],
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
	)
	require.ElementsMatch(t, []string{first.TaskID, second.TaskID}, thirdCleanup.ResolvesTaskIDs)
	store.tasks[third.TaskID].CreateTime = time.Unix(100, 0)
	store.tasks[third.TaskID].Status = config.StatusCompleted
	thirdJob := requireVersionUpdateCleanupJobForTask(t, store, third.TaskID, "mysql")
	thirdJob.Status = string(config.StatusCompleted)

	orderedStore := &workflowTaskListOrderStore{
		inMemoryAppStore: store,
		taskIDs:          []string{first.TaskID, second.TaskID, third.TaskID},
	}
	pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), orderedStore, "app-1")
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestUpdateVersionPendingStatefulSetDeletionV2RequiresImmutableReplay(t *testing.T) {
	store, svc, req := newStatefulSetDeletionV2RetryFixture(t)

	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusFailed
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusFailed)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:   "2.1.0",
		ExecuteAt: req.ExecuteAt,
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
			{Action: "add", Name: "all"},
			{Action: "update", Name: "mysql", Image: "mysql:8.1"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Contains(t, err.Error(), "does not reproduce a StatefulSet immutable field change")
	require.Len(t, store.tasks, 1)
}

func TestUpdateVersionPendingStatefulSetDeletionV2RequiresFullRebuild(t *testing.T) {
	store, svc, req := newStatefulSetDeletionV2RetryFixture(t)

	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusFailed
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusFailed)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:    "2.1.0",
		ExecuteAt:  req.ExecuteAt,
		Components: []apisv1.ComponentUpdateSpec{req.Components[2]},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Contains(t, bcode.SafeClientMessage(err), "unfinished StatefulSet migration")
	require.Len(t, store.tasks, 1)
}

func TestPendingStatefulSetDeletionV2TracksAndClearsSuccessfulRetry(t *testing.T) {
	component := statefulSetDeletionV2Component("mysql-headless")
	cleanupComponent := model.VersionUpdateCleanupComponent{
		Component:                  component,
		ResourceAppName:            "shop",
		RequireStatefulSetDeletion: true,
	}
	marker, err := versionUpdateCleanupJobInfoMarker(true, nil)
	require.NoError(t, err)
	job := func(taskID string, status config.Status) []*model.JobInfo {
		return []*model.JobInfo{{
			TaskID: taskID, Type: string(config.JobCleanupResources), ServiceName: component.Name,
			Status: string(status), InternalInfo: marker,
		}}
	}
	pending := make(map[string]map[string]*pendingStatefulSetPVCDeletion)

	valid, err := updatePendingStatefulSetDeletion(
		pending,
		&model.WorkflowQueue{TaskID: "failed-v2", Status: config.StatusFailed},
		job("failed-v2", config.StatusFailed),
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		cleanupComponent,
	)
	require.NoError(t, err)
	require.True(t, valid)
	require.Len(t, pending["mysql"], 1)
	for _, plan := range pending["mysql"] {
		require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, plan.cleanupVersion)
		require.Empty(t, plan.templates)
	}

	valid, err = updatePendingStatefulSetDeletion(
		pending,
		&model.WorkflowQueue{TaskID: "completed-v2", Status: config.StatusCompleted},
		job("completed-v2", config.StatusCompleted),
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		cleanupComponent,
	)
	require.NoError(t, err)
	require.True(t, valid)
	require.Empty(t, pending)
}

func TestPendingStatefulSetDeletionRequiresCompletedCleanupAndWorkflow(t *testing.T) {
	tests := []struct {
		name       string
		taskStatus config.Status
		jobStatus  config.Status
	}{
		{name: "cleanup completed but workflow failed", taskStatus: config.StatusFailed, jobStatus: config.StatusCompleted},
		{name: "cleanup passed", taskStatus: config.StatusCompleted, jobStatus: config.StatusPassed},
		{name: "cleanup skipped", taskStatus: config.StatusCompleted, jobStatus: config.StatusSkipped},
		{name: "workflow passed", taskStatus: config.StatusPassed, jobStatus: config.StatusCompleted},
		{name: "workflow skipped", taskStatus: config.StatusSkipped, jobStatus: config.StatusCompleted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component := statefulSetDeletionV2Component("mysql-headless")
			cleanupComponent := model.VersionUpdateCleanupComponent{
				Component: component, ResourceAppName: "shop", RequireStatefulSetDeletion: true,
			}
			marker, err := versionUpdateCleanupJobInfoMarker(true, nil)
			require.NoError(t, err)
			pending := make(map[string]map[string]*pendingStatefulSetPVCDeletion)
			job := func(taskID string, status config.Status) []*model.JobInfo {
				return []*model.JobInfo{{
					TaskID: taskID, Type: string(config.JobCleanupResources), ServiceName: component.Name,
					Status: string(status), InternalInfo: marker,
				}}
			}

			valid, err := updatePendingStatefulSetDeletion(
				pending,
				&model.WorkflowQueue{TaskID: "initial-failure", Status: config.StatusFailed},
				job("initial-failure", config.StatusFailed),
				model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
				cleanupComponent,
			)
			require.NoError(t, err)
			require.True(t, valid)

			valid, err = updatePendingStatefulSetDeletion(
				pending,
				&model.WorkflowQueue{TaskID: "unproven-attempt", Status: tt.taskStatus},
				job("unproven-attempt", tt.jobStatus),
				model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
				cleanupComponent,
			)

			require.NoError(t, err)
			require.True(t, valid)
			require.Len(t, pending["mysql"], 1)
		})
	}
}

func TestLoadPendingStatefulSetDeletionReturnsWorkflowConflictBcodes(t *testing.T) {
	tests := []struct {
		name       string
		taskStatus config.Status
		jobStatus  config.Status
		want       error
	}{
		{name: "created", taskStatus: config.StatusCreated, jobStatus: config.StatusCreated, want: bcode.ErrWorkflowTaskRunning},
		{name: "queued", taskStatus: config.StatusQueued, jobStatus: config.StatusQueued, want: bcode.ErrWorkflowTaskRunning},
		{name: "waiting", taskStatus: config.StatusWaiting, jobStatus: config.StatusWaiting, want: bcode.ErrWorkflowTaskRunning},
		{name: "pending", taskStatus: config.QueueItemPending, jobStatus: config.QueueItemPending, want: bcode.ErrWorkflowTaskRunning},
		{name: "prepare", taskStatus: config.StatusPrepare, jobStatus: config.StatusPrepare, want: bcode.ErrWorkflowTaskRunning},
		{name: "running", taskStatus: config.StatusRunning, jobStatus: config.StatusRunning, want: bcode.ErrWorkflowTaskRunning},
		{name: "failed task with running cleanup", taskStatus: config.StatusFailed, jobStatus: config.StatusRunning, want: bcode.ErrWorkflowTaskRunning},
		{name: "timed out task with preparing cleanup", taskStatus: config.StatusTimeout, jobStatus: config.StatusPrepare, want: bcode.ErrWorkflowTaskRunning},
		{name: "cancelling", taskStatus: config.StatusCancelled, jobStatus: config.StatusRunning, want: bcode.ErrWorkflowTaskCancelling},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			addStatefulSetDeletionV2History(t, store, tt.taskStatus, tt.jobStatus)

			_, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestLoadPendingStatefulSetDeletionTreatsPreStartJobOnTerminalTaskAsPending(t *testing.T) {
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
			store := newInMemoryAppStore()
			addStatefulSetDeletionV2History(t, store, tt.taskStatus, tt.jobStatus)

			pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

			require.NoError(t, err)
			require.Len(t, pending["mysql"], 1)
			require.Equal(t, []string{"cleanup-v2"}, pendingStatefulSetCleanupTaskIDs(pending))
		})
	}
}

func TestLoadPendingStatefulSetDeletionCompletedResolverClearsPreStartTerminalAttempt(t *testing.T) {
	store := newInMemoryAppStore()
	addStatefulSetDeletionV2Attempt(
		t,
		store,
		"failed-before-cleanup",
		config.StatusFailed,
		config.StatusQueued,
		nil,
		time.Time{},
	)

	pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")
	require.NoError(t, err)
	require.Equal(t, []string{"failed-before-cleanup"}, pendingStatefulSetCleanupTaskIDs(pending))

	addStatefulSetDeletionV2Attempt(
		t,
		store,
		"completed-resolver",
		config.StatusCompleted,
		config.StatusCompleted,
		[]string{"failed-before-cleanup"},
		time.Time{},
	)

	pending, err = loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestLoadPendingStatefulSetDeletionKeepsUnknownJobStatusInternal(t *testing.T) {
	store := newInMemoryAppStore()
	addStatefulSetDeletionV2History(t, store, config.StatusCompleted, config.Status("draining"))

	_, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

	require.Error(t, err)
	require.NotErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.NotErrorIs(t, err, bcode.ErrWorkflowTaskCancelling)
	require.Contains(t, err.Error(), `cleanup job is "draining"`)
}

func TestLoadPendingStatefulSetDeletionReturnsWorkflowConflictBeforeJobCreation(t *testing.T) {
	store := newInMemoryAppStore()
	addStatefulSetDeletionV2History(t, store, config.StatusWaiting, config.StatusWaiting)
	store.jobs = nil

	_, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
}

func TestLoadPendingStatefulSetDeletionRetainsTerminalTaskBeforeJobCreation(t *testing.T) {
	store := newInMemoryAppStore()
	addStatefulSetDeletionV2History(t, store, config.StatusFailed, config.StatusFailed)
	store.jobs = nil

	pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

	require.NoError(t, err)
	require.Len(t, pending["mysql"], 1)
}

func TestLoadPendingStatefulSetDeletionUsesExplicitCausalityInsteadOfCreateTime(t *testing.T) {
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
			store := newInMemoryAppStore()
			addStatefulSetDeletionV2Attempt(t, store, "failed-1", config.StatusFailed, config.StatusFailed, nil, tt.times[0])
			addStatefulSetDeletionV2Attempt(t, store, "failed-2", config.StatusFailed, config.StatusFailed, []string{"failed-1"}, tt.times[1])
			addStatefulSetDeletionV2Attempt(t, store, "recovered", config.StatusCompleted, config.StatusCompleted, []string{"failed-1", "failed-2"}, tt.times[2])

			orderedStore := &workflowTaskListOrderStore{
				inMemoryAppStore: store,
				taskIDs:          []string{"failed-1", "failed-2", "recovered"},
			}
			pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), orderedStore, "app-1")

			require.NoError(t, err)
			require.Empty(t, pending, "a successful resolver must close both failed attempts regardless of wall-clock order")

			addStatefulSetDeletionV2Attempt(t, store, "independent-failure", config.StatusFailed, config.StatusFailed, nil, tt.times[3])
			orderedStore.taskIDs = append(orderedStore.taskIDs, "independent-failure")
			pending, err = loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), orderedStore, "app-1")

			require.NoError(t, err)
			require.Len(t, pending["mysql"], 1)
			require.Equal(t, []string{"independent-failure"}, pendingStatefulSetCleanupTaskIDs(pending))
		})
	}
}

func TestLoadPendingStatefulSetDeletionDoesNotGuessLegacyRecoveryOrder(t *testing.T) {
	store := newInMemoryAppStore()
	addStatefulSetDeletionV2Attempt(t, store, "legacy-failure", config.StatusFailed, config.StatusFailed, nil, time.Unix(200, 0))
	addStatefulSetDeletionV2Attempt(t, store, "legacy-success", config.StatusCompleted, config.StatusCompleted, nil, time.Unix(100, 0))

	pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

	require.NoError(t, err)
	require.Equal(t, []string{"legacy-failure"}, pendingStatefulSetCleanupTaskIDs(pending))
}

func TestLoadPendingStatefulSetDeletionRejectsUnknownResolutionTask(t *testing.T) {
	store := newInMemoryAppStore()
	addStatefulSetDeletionV2Attempt(t, store, "recovered", config.StatusCompleted, config.StatusCompleted, []string{"missing"}, time.Time{})

	_, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "resolves unknown StatefulSet cleanup task missing")
}

func TestLoadPendingStatefulSetDeletionRejectsResolutionCycle(t *testing.T) {
	store := newInMemoryAppStore()
	addStatefulSetDeletionV2Attempt(t, store, "failed-1", config.StatusFailed, config.StatusFailed, []string{"failed-2"}, time.Time{})
	addStatefulSetDeletionV2Attempt(t, store, "failed-2", config.StatusFailed, config.StatusFailed, []string{"failed-1"}, time.Time{})

	_, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

	require.ErrorContains(t, err, "resolution graph contains a cycle")
}

func TestPendingStatefulSetDeletionV2RejectsPVCTemplatePlan(t *testing.T) {
	_, err := updatePendingStatefulSetDeletion(
		make(map[string]map[string]*pendingStatefulSetPVCDeletion),
		&model.WorkflowQueue{TaskID: "invalid-v2"},
		nil,
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		model.VersionUpdateCleanupComponent{
			Component:                       statefulSetDeletionV2Component("mysql-headless"),
			RequireStatefulSetDeletion:      true,
			StatefulSetPVCTemplatesToDelete: []string{"data"},
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "v2 cannot contain a StatefulSet PVC deletion plan")
}

func TestLoadPendingStatefulSetDeletionTracksMixedV2V3Components(t *testing.T) {
	tests := []struct {
		name     string
		v2Status config.Status
		v3Status config.Status
	}{
		{
			name: "v2 failed while v3 completed", v2Status: config.StatusFailed, v3Status: config.StatusCompleted,
		},
		{
			name: "v3 failed while v2 completed", v2Status: config.StatusCompleted, v3Status: config.StatusFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			v2Component := statefulSetDeletionV2Component("mysql-headless")
			v3Component := statefulSetDeletionV2Component("redis-headless")
			v3Component.ID = 202
			v3Component.Name = "redis"
			cleanupInfo := model.VersionUpdateCleanupInfo{
				Source:  config.JobInfoSourceVersionUpdateRemove,
				Version: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
				Components: []model.VersionUpdateCleanupComponent{
					{Component: v2Component, ResourceAppName: "shop", RequireStatefulSetDeletion: true},
					{
						Component: v3Component, ResourceAppName: "shop", RequireStatefulSetDeletion: true,
						StatefulSetPVCTemplatesToDelete: []string{"data"},
					},
				},
			}
			payload, err := json.Marshal(cleanupInfo)
			require.NoError(t, err)
			v2Marker, err := versionUpdateCleanupJobInfoMarker(true, nil)
			require.NoError(t, err)
			v3Marker, err := versionUpdateCleanupJobInfoMarker(true, []string{"data"})
			require.NoError(t, err)
			store.tasks["mixed-cleanup"] = &model.WorkflowQueue{
				TaskID: "mixed-cleanup", AppID: "app-1", Status: config.StatusFailed, CleanupInfo: string(payload),
			}
			store.jobs = append(store.jobs,
				&model.JobInfo{
					TaskID: "mixed-cleanup", Type: string(config.JobCleanupResources), ServiceName: "mysql",
					Status: string(tt.v2Status), InternalInfo: v2Marker,
				},
				&model.JobInfo{
					TaskID: "mixed-cleanup", Type: string(config.JobCleanupResources), ServiceName: "redis",
					Status: string(tt.v3Status), InternalInfo: v3Marker,
				},
			)

			pending, err := loadPendingVersionUpdateStatefulSetPVCDeletions(context.Background(), store, "app-1")

			require.NoError(t, err)
			require.Len(t, pending, 2, "a terminal workflow failure keeps every destructive component pending")
			require.Len(t, pending["mysql"], 1)
			for _, plan := range pending["mysql"] {
				require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, plan.cleanupVersion)
				require.Empty(t, pendingStatefulSetPVCTemplateNames(plan))
			}
			require.Len(t, pending["redis"], 1)
			for _, plan := range pending["redis"] {
				require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, plan.cleanupVersion)
				require.Equal(t, []string{"data"}, pendingStatefulSetPVCTemplateNames(plan))
			}
		})
	}
}

func addStatefulSetDeletionV2History(
	t *testing.T,
	store *inMemoryAppStore,
	taskStatus config.Status,
	jobStatus config.Status,
) {
	t.Helper()
	addStatefulSetDeletionV2Attempt(t, store, "cleanup-v2", taskStatus, jobStatus, nil, time.Time{})
}

func addStatefulSetDeletionV2Attempt(
	t *testing.T,
	store *inMemoryAppStore,
	taskID string,
	taskStatus config.Status,
	jobStatus config.Status,
	resolvesTaskIDs []string,
	createTime time.Time,
) {
	t.Helper()
	component := statefulSetDeletionV2Component("mysql-headless")
	cleanupInfo := model.VersionUpdateCleanupInfo{
		Source:          config.JobInfoSourceVersionUpdateRemove,
		Version:         model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		ResolvesTaskIDs: append([]string(nil), resolvesTaskIDs...),
		Components: []model.VersionUpdateCleanupComponent{{
			Component: component, ResourceAppName: "shop", RequireStatefulSetDeletion: true,
		}},
	}
	payload, err := json.Marshal(cleanupInfo)
	require.NoError(t, err)
	marker, err := versionUpdateCleanupJobInfoMarker(true, nil)
	require.NoError(t, err)
	store.tasks[taskID] = &model.WorkflowQueue{
		TaskID: taskID, AppID: "app-1", Status: taskStatus, CleanupInfo: string(payload),
		BaseModel: model.BaseModel{CreateTime: createTime},
	}
	store.jobs = append(store.jobs, &model.JobInfo{
		TaskID: taskID, Type: string(config.JobCleanupResources), ServiceName: component.Name,
		Status: string(jobStatus), InternalInfo: marker,
	})
}

type workflowTaskListOrderStore struct {
	*inMemoryAppStore
	taskIDs []string
}

func (s *workflowTaskListOrderStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	workflowQueue, ok := query.(*model.WorkflowQueue)
	if !ok {
		return s.inMemoryAppStore.List(ctx, query, opts)
	}
	result := make([]datastore.Entity, 0, len(s.taskIDs))
	for _, taskID := range s.taskIDs {
		task := s.tasks[taskID]
		if task == nil || workflowQueue.AppID != "" && task.AppID != workflowQueue.AppID {
			continue
		}
		result = append(result, task)
	}
	return result, nil
}

func newStatefulSetDeletionV2RetryFixture(t *testing.T) (*inMemoryAppStore, *applicationsServiceImpl, apisv1.UpdateVersionRequest) {
	t.Helper()
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "shop", Version: "1.0.0", Namespace: config.DefaultNamespace,
	}
	store.components["mysql"] = statefulSetDeletionV2Component("mysql-headless")
	store.workflows["wf-1"] = &model.Workflow{
		ID: "wf-1", AppID: "app-1", WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{
			{Name: "mysql", WorkflowType: config.JobDeploy},
		}}),
	}
	desiredTraits := statefulSetDeletionV2Traits("mysql-headless-v2")
	return store, newMockServiceWithStore(store), apisv1.UpdateVersionRequest{
		Version:   "2.0.0",
		ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
			{Action: "add", Name: "all"},
			{Action: "update", Name: "mysql", Traits: &desiredTraits},
		},
	}
}

func statefulSetDeletionV2Component(serviceName string) *model.ApplicationComponent {
	traits := statefulSetDeletionV2Traits(serviceName)
	return &model.ApplicationComponent{
		ID: 101, AppID: "app-1", Name: "mysql", Namespace: config.DefaultNamespace,
		ResourceAppName: "shop", ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1,
		Status: string(config.ComponentStatusRunning), Traits: mustJSONStruct(&traits),
	}
}

func statefulSetDeletionV2Traits(serviceName string) apisv1.Traits {
	return apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
		}},
		Service: []spec.ServiceTraitSpec{{
			Name: serviceName, Type: string(spec.ServiceAccessInternal), Headless: true,
			Selector: map[string]string{config.LabelComponentName: "mysql"},
			Ports:    []spec.ServicePortTraitSpec{{Name: "mysql", Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	}
}
