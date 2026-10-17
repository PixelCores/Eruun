package workflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	urlpolicy "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/systemsetting"
	workflowservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/workflow"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	containerutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/container"
)

type dispatchCleanupFenceStore struct {
	workflowAckTestStore
	pendingTask *model.WorkflowQueue
	claimHook   func()
}

func (s *dispatchCleanupFenceStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	if task, ok := query.(*model.WorkflowQueue); ok && task.Status == config.StatusWaiting {
		return []datastore.Entity{s.taskSnapshot()}, nil
	}
	if _, ok := query.(*model.WorkflowQueue); ok && s.pendingTask != nil {
		return []datastore.Entity{s.pendingTask}, nil
	}
	return s.workflowAckTestStore.List(ctx, query, opts)
}

func (s *dispatchCleanupFenceStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if s.claimHook != nil {
		s.claimHook()
	}
	return s.workflowAckTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

// Exercise the dispatch entry point with the production domain service wired
// through the same injection container used by the server.
func TestDispatchClaimPreservesStatefulSetCleanupFence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		pending          bool
		migration        int
		immediate        bool
		cron             bool
		job              bool
		future           bool
		lockHeld         bool
		staleGeneration  bool
		concurrentClaim  bool
		concurrentStatus bool
		wantDispatch     bool
		wantFailed       bool
	}{
		{name: "delayed ordinary task fails for unfinished cleanup", pending: true, wantFailed: true},
		{name: "delayed task without cleanup dispatches", wantDispatch: true},
		{name: "v2 migration resolves failed cleanup", pending: true, migration: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, wantDispatch: true},
		{name: "v3 migration resolves failed cleanup", pending: true, migration: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion, wantDispatch: true},
		{name: "delayed claim uses application lock", pending: true, lockHeld: true},
		{name: "immediate task needs no application lock", immediate: true, pending: true, lockHeld: true, wantDispatch: true},
		{name: "cron task needs no application lock", immediate: true, cron: true, pending: true, lockHeld: true, wantDispatch: true},
		{name: "immediate Job has no application", job: true, immediate: true, lockHeld: true, wantDispatch: true},
		{name: "delayed Job has no application lock or cleanup fence", job: true, pending: true, lockHeld: true, wantDispatch: true},
		{name: "future Job stays waiting until due", job: true, future: true},
		{name: "delayed Job preserves the generation fence", job: true, staleGeneration: true},
		{name: "stale dispatch cannot claim a newer generation", staleGeneration: true},
		{name: "CAS preserves generation after safety check", concurrentClaim: true},
		{name: "cleanup rejection preserves a newer generation", pending: true, concurrentClaim: true},
		{name: "cleanup rejection preserves a changed status", pending: true, concurrentStatus: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			task := &model.WorkflowQueue{TaskID: "delayed", AppID: "app-1", WorkflowID: "workflow-1", Status: config.StatusWaiting, ExecuteAt: time.Now().Add(-time.Second).Unix(), RunGeneration: 2, RunToken: "previous-token"}
			callback, err := model.NewJSONStructByStruct(&model.WorkflowCallback{})
			require.NoError(t, err)
			task.Callback = callback
			if tc.immediate {
				task.ExecuteAt = 0
			}
			if tc.job {
				task.Type = config.WorkflowTaskTypeJob
				task.AppID = ""
				task.WorkflowID = ""
			}
			if tc.future {
				task.ExecuteAt = time.Now().Add(time.Hour).Unix()
			}
			if tc.cron {
				key := "cron:schedule-1:123"
				task.IdempotencyKey = &key
			}
			if tc.migration != 0 {
				task.CleanupInfo = dispatchCleanupInfo(t, tc.migration)
			}
			store := &dispatchCleanupFenceStore{workflowAckTestStore: workflowAckTestStore{task: task}}
			if tc.pending {
				// A terminal migration with no cleanup job is still unfinished; a
				// failed start must not let ordinary deployment pass the fence.
				store.pendingTask = &model.WorkflowQueue{TaskID: "failed-migration", AppID: task.AppID, Status: config.StatusFailed, CleanupInfo: dispatchCleanupInfo(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion)}
			}
			lockProvider := locker.NewMemoryLocker("dispatch-fence-test")
			t.Cleanup(func() { require.NoError(t, lockProvider.Close()) })
			probe := lockProvider.NewMutex("app-schedule:" + task.AppID)
			if tc.lockHeld {
				require.NoError(t, probe.TryLock(ctx))
				t.Cleanup(func() { require.NoError(t, probe.Unlock(ctx)) })
			}
			candidate := *task
			if tc.staleGeneration {
				candidate.RunGeneration--
			}
			store.claimHook = func() {
				if !tc.immediate && !tc.job {
					// The lock must cover the ownership CAS, not only the check.
					err := probe.TryLock(ctx)
					if err == nil {
						require.NoError(t, probe.Unlock(ctx))
					}
					require.ErrorIs(t, err, locker.ErrLockAcquireFailed)
				}
				if tc.concurrentClaim {
					store.mutateTask(func(current *model.WorkflowQueue) { current.RunGeneration++ })
				}
				if tc.concurrentStatus {
					store.mutateTask(func(current *model.WorkflowQueue) { current.Status = config.StatusRunning })
				}
			}
			cfg := config.NewConfig()
			svc := workflowservice.NewWorkflowService()
			beans := containerutil.NewContainer()
			require.NoError(t, beans.Provides(svc, cfg, urlpolicy.NewProvider(store, time.Minute)))
			for name, dependency := range map[string]interface{}{
				"datastore": store, "appScheduleLocker": lockProvider,
				"kubeClient": fake.NewSimpleClientset(), "kubeConfig": &rest.Config{},
				"cache": cache.NewMemCache(), "redisClient": &redis.Client{},
			} {
				require.NoError(t, beans.ProvideWithName(name, dependency))
			}
			require.NoError(t, beans.Populate())
			w := &Workflow{Store: store, WorkflowService: svc, Cfg: cfg}
			if tc.job {
				// The dispatcher filters ExecuteAt using the database clock before
				// the ownership CAS; direct claims do not make timing decisions.
				waiting, err := w.waitingTasks(ctx)
				require.NoError(t, err)
				if tc.future {
					require.Empty(t, waiting)
					require.Equal(t, config.StatusWaiting, store.taskSnapshot().Status)
					require.EqualValues(t, 2, store.taskSnapshot().RunGeneration)
					return
				}
				require.Len(t, waiting, 1)
			}
			var dispatched *model.WorkflowQueue
			w.claimAndProcessTask(ctx, &candidate, func(_ context.Context, claimed *model.WorkflowQueue) error {
				dispatched = claimed
				return nil
			})
			if !tc.wantDispatch {
				if tc.pending || tc.lockHeld {
					// Rejected or deferred tasks remain unclaimed without an error.
					_, claimed, err := svc.ClaimTaskForDispatch(ctx, &candidate, time.Minute)
					require.NoError(t, err)
					require.False(t, claimed)
				}
				require.Nil(t, dispatched)
				current := store.taskSnapshot()
				wantStatus := config.StatusWaiting
				if tc.wantFailed {
					wantStatus = config.StatusFailed
					require.Contains(t, current.SchedulingReason, "delayed workflow dispatch rejected")
					require.Contains(t, current.SchedulingReason, "unfinished StatefulSet cleanup")
				} else {
					require.Empty(t, current.SchedulingReason)
				}
				if tc.concurrentStatus {
					wantStatus = config.StatusRunning
				}
				require.Equal(t, wantStatus, current.Status)
				wantGeneration := uint64(2)
				if tc.concurrentClaim {
					wantGeneration++
				}
				require.Equal(t, wantGeneration, current.RunGeneration)
				require.Equal(t, "previous-token", current.RunToken)
				return
			}
			require.NotNil(t, dispatched)
			require.Equal(t, config.StatusQueued, dispatched.Status)
			require.EqualValues(t, 3, dispatched.RunGeneration)
			require.NotEmpty(t, dispatched.RunToken)
			require.NotEqual(t, "previous-token", dispatched.RunToken)
			require.EqualValues(t, 1, dispatched.DispatchAttempts)
			require.NotNil(t, dispatched.LeaseExpiresAt)
			require.Equal(t, dispatched.RunToken, store.taskSnapshot().RunToken)
		})
	}
}

func dispatchCleanupInfo(t *testing.T, version int) string {
	t.Helper()
	component := model.VersionUpdateCleanupComponent{
		Component:       &model.ApplicationComponent{ID: 1, AppID: "app-1", Name: "db", Namespace: "default", ComponentType: config.StoreJob},
		ResourceAppName: "app", RequireStatefulSetDeletion: true,
	}
	if version == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
		component.StatefulSetPVCTemplatesToDelete = []string{"data"}
	}
	data, err := json.Marshal(model.VersionUpdateCleanupInfo{Source: config.JobInfoSourceVersionUpdateRemove, Version: version, Components: []model.VersionUpdateCleanupComponent{component}})
	require.NoError(t, err)
	return string(data)
}
