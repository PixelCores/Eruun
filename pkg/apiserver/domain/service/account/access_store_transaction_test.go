package account

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type accessReadCommittedTestStore struct {
	datastore.DataStore
	called bool
}

func (s *accessReadCommittedTestStore) WithReadCommittedTransaction(_ context.Context, fn func(datastore.DataStore) error) error {
	s.called = true
	return fn(s)
}

func TestScopedJobAdmissionLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name     string
		workflow config.WorkflowTaskType
		job      config.JobType
		app      bool
		terminal bool
		detached bool
	}{
		{name: "command", workflow: config.WorkflowTaskTypeJob, job: config.JobCommand},
		{name: "eval", workflow: config.WorkflowTaskTypeJob, job: config.JobEval},
		{name: "import scan", workflow: config.WorkflowTaskTypeResourceImportScan, job: config.JobResourceImportScan},
		{name: "import manage", workflow: config.WorkflowTaskTypeResourceImportManage, job: config.JobResourceImportManage},
		{name: "application", job: config.JobDeployService, app: true},
		{name: "terminal callback", job: config.JobDeployCallback, app: true, terminal: true},
		{name: "detached job", job: config.JobDeployInstant, app: true, detached: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, _, _ := testAccounts(t)
			raw, ctx := service.Repo.Store, context.Background()
			store := NewStore(raw)
			scopedCtx := WithScope(ctx, Scope{WorkspaceID: "allowed", Namespace: "allowed-ns"})
			require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
			lease := time.Now().Add(time.Hour)
			owner := &model.WorkflowQueue{TaskID: "task", WorkspaceID: "allowed", Type: tc.workflow,
				Status: config.StatusRunning, RunGeneration: 1, RunToken: "token", WorkerID: "worker", LeaseExpiresAt: &lease}
			if tc.workflow == config.WorkflowTaskTypeJob {
				owner.JobSpec = `{"type":"` + string(tc.job) + `"}`
			}
			if tc.app {
				owner.AppID = "app"
				require.NoError(t, raw.Add(ctx, &model.Applications{ID: owner.AppID, WorkspaceID: "allowed", Namespace: "allowed-ns"}))
			}
			var deadline *time.Time
			if tc.terminal || tc.detached {
				owner.Status = config.StatusCompleted
				deadline = &lease
			}
			require.NoError(t, raw.Add(ctx, owner))
			key := "execution"
			job := &model.JobInfo{TaskID: owner.TaskID, AppID: owner.AppID, WorkspaceID: owner.WorkspaceID,
				Type: string(tc.job), Status: string(config.StatusPrepare), ExecutionKey: &key, RunGeneration: 1}
			if tc.detached {
				job.Status, job.DelayState = string(config.StatusDistributed), config.JobDelayStatePending
				job.DelayPayload, job.DelayExecuteAt = `{"committed":true}`, time.Now().Add(-time.Minute).Unix()
				require.NoError(t, raw.Add(ctx, job))
				owner = nil
			}
			require.NoError(t, repository.EnqueueJobForScheduling(scopedCtx, store, owner, job, deadline))
			admitted, err := repository.IsJobAdmitted(scopedCtx, store, owner, key, job.SchedulingExpiresAt)
			require.NoError(t, err)
			require.False(t, admitted)
			if !tc.terminal {
				require.NoError(t, repository.EnqueueJobForScheduling(scopedCtx, store, owner, job, deadline), "repeated registration must find the same Job")
			}
			n, err := repository.AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			admitted, err = repository.IsJobAdmitted(scopedCtx, store, owner, key, job.SchedulingExpiresAt)
			require.NoError(t, err)
			require.True(t, admitted)
			require.NoError(t, repository.ReleaseJobAdmission(scopedCtx, store, owner, key, "finished", job.SchedulingExpiresAt))
			require.NoError(t, repository.ReleaseJobAdmission(scopedCtx, store, owner, key, "finished", job.SchedulingExpiresAt))
			require.NoError(t, raw.Get(ctx, job))
			require.Equal(t, "released", job.SchedulingState)
		})
	}
}

func TestScopedImportAdmissionRejectsForeignAndForgedOwners(t *testing.T) {
	service, _, _ := testAccounts(t)
	raw, ctx := service.Repo.Store, context.Background()
	store := NewStore(raw)
	require.NoError(t, repository.EnsureJobSchedulerPolicy(ctx, store))
	lease := time.Now().Add(time.Hour)
	owner := &model.WorkflowQueue{TaskID: "import", WorkspaceID: "allowed", Type: config.WorkflowTaskTypeResourceImportScan,
		Status: config.StatusRunning, RunGeneration: 1, RunToken: "token", WorkerID: "worker", LeaseExpiresAt: &lease}
	require.NoError(t, raw.Add(ctx, owner))
	other := *owner
	other.TaskID = "other-import"
	require.NoError(t, raw.Add(ctx, &other))
	key := "import-execution"
	job := &model.JobInfo{TaskID: owner.TaskID, WorkspaceID: owner.WorkspaceID, Type: string(config.JobResourceImportScan),
		Status: string(config.StatusPrepare), ExecutionKey: &key, RunGeneration: 1}
	require.NoError(t, repository.EnqueueJobForScheduling(ctx, store, owner, job, nil))
	_, err := repository.AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	forged := *owner
	forged.WorkspaceID = "foreign"
	for _, tc := range []struct {
		name, workspace string
		owner           model.WorkflowQueue
	}{
		{name: "foreign workspace", workspace: "foreign", owner: *owner},
		{name: "different task with same execution identity", workspace: "allowed", owner: other},
		{name: "forged workspace", workspace: "foreign", owner: forged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scopedCtx := WithScope(ctx, Scope{WorkspaceID: tc.workspace, Namespace: tc.workspace + "-ns"})
			copy := *job
			require.Error(t, repository.EnqueueJobForScheduling(scopedCtx, store, &tc.owner, &copy, nil))
			admitted, err := repository.IsJobAdmitted(scopedCtx, store, &tc.owner, key)
			require.Error(t, err)
			require.False(t, admitted)
			require.Error(t, repository.ReleaseJobAdmission(scopedCtx, store, &tc.owner, key, "forged release"))
			require.NoError(t, raw.Get(ctx, job))
			require.Equal(t, "admitted", job.SchedulingState)
		})
	}
}

func TestScopedStoreReadCommittedTransactionPreservesScope(t *testing.T) {
	raw := &accessReadCommittedTestStore{}
	store := NewStore(raw)
	ctx := WithScope(context.Background(), ForWorkspace(&model.Workspace{ID: "allowed", Namespace: "allowed-ns"}))
	err := store.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
		scoped, ok := tx.(*Store)
		require.True(t, ok, "transaction must retain the access wrapper")
		return scoped.Check(ctx, &model.Applications{WorkspaceID: "different", Namespace: "other-ns"})
	})
	require.True(t, raw.called)
	require.ErrorIs(t, err, bcode.ErrForbidden)
	called := false
	require.ErrorContains(t, NewStore(nil).WithReadCommittedTransaction(ctx, func(datastore.DataStore) error { called = true; return nil }), "read-committed transactions")
	require.False(t, called)
}

func TestScopedJobAdmissionTransactionUsesCanonicalApplicationWorkspace(t *testing.T) {
	service, _, _ := testAccounts(t)
	raw := service.Repo.Store
	ctx := context.Background()
	for _, app := range []*model.Applications{
		{ID: "allowed-app", Name: "allowed", WorkspaceID: "allowed", Namespace: "allowed-ns"},
		{ID: "foreign-app", Name: "foreign", WorkspaceID: "foreign", Namespace: "foreign-ns"},
	} {
		require.NoError(t, raw.Add(ctx, app))
	}
	store := NewStore(raw)
	ctx = WithScope(ctx, ForWorkspace(&model.Workspace{ID: "allowed", Namespace: "allowed-ns"}))
	for _, tc := range []struct {
		name      string
		appID     string
		workspace string
		allowed   bool
	}{
		{name: "current job", appID: "allowed-app", workspace: "allowed", allowed: true},
		{name: "historical job without workspace", appID: "allowed-app", allowed: true},
		{name: "foreign job with spoofed workspace", appID: "foreign-app", workspace: "allowed"},
		{name: "foreign historical job", appID: "foreign-app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &model.JobInfo{AppID: tc.appID, WorkspaceID: tc.workspace, TaskID: tc.name, SchedulingState: "queued"}
			require.NoError(t, raw.Add(context.Background(), job))
			err := store.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
				loaded := &model.JobInfo{ID: job.ID}
				if err := tx.Get(ctx, loaded); err != nil {
					return err
				}
				updated, err := tx.CompareAndSwap(ctx, loaded, "scheduling_state", "queued", map[string]interface{}{"scheduling_state": "admitted"})
				if err == nil {
					require.True(t, updated)
				}
				return err
			})
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, bcode.ErrForbidden)
				// A direct CAS must enforce the same scope as a read then CAS.
				err = store.WithReadCommittedTransaction(ctx, func(tx datastore.DataStore) error {
					updated, err := tx.CompareAndSwap(ctx, job, "scheduling_state", "queued", map[string]interface{}{"scheduling_state": "admitted"})
					require.False(t, updated)
					return err
				})
				require.ErrorIs(t, err, bcode.ErrForbidden)
			}
			stored := &model.JobInfo{ID: job.ID}
			require.NoError(t, raw.Get(context.Background(), stored))
			if tc.allowed {
				require.Equal(t, "admitted", stored.SchedulingState)
			} else {
				require.Equal(t, "queued", stored.SchedulingState)
			}
		})
	}
}

func TestWorkspaceJobScopeRequiresMatchingPersistedParent(t *testing.T) {
	legacy := &model.WorkflowQueue{Type: config.WorkflowTaskTypeJob, JobSpec: `{"type":"agent_evaluation"}`}
	require.Empty(t, workspaceTaskJobType(legacy))
	service, _, _ := testAccounts(t)
	ctx := WithScope(context.Background(), Scope{WorkspaceID: "allowed", Namespace: "allowed-ns"})
	raw := service.Repo.Store
	store := NewStore(raw)
	parent := &model.WorkflowQueue{TaskID: "command-parent", WorkspaceID: "allowed", Type: config.WorkflowTaskTypeJob, JobSpec: `{"type":"command"}`}
	require.NoError(t, raw.Add(context.Background(), parent))
	for _, tc := range []struct {
		name      string
		workspace string
		jobType   string
		wantError bool
	}{
		{"matching command", "allowed", "command", false},
		{"scoped query", "allowed", "", false},
		{"wrong workspace", "other", "command", true},
		{"wrong type", "allowed", "eval", true},
		{"internal operation", "allowed", "cleanup_resources", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := store.Check(ctx, &model.JobInfo{TaskID: parent.TaskID, WorkspaceID: tc.workspace, Type: tc.jobType})
			if tc.wantError {
				require.ErrorIs(t, err, bcode.ErrForbidden)
			} else {
				require.NoError(t, err)
			}
		})
	}
	for _, snapshot := range []string{"", "{", `{"type":"cleanup_resources"}`, `{"type":""}`} {
		candidate := *parent
		candidate.JobSpec = snapshot
		require.ErrorIs(t, store.Check(ctx, &candidate), bcode.ErrForbidden)
	}
}

func TestJobArtifactsStayInTheirWorkspaceAcrossStoreOperations(t *testing.T) {
	for _, newEntity := range []struct {
		name  string
		build func(string, string) datastore.Entity
	}{
		{"artifact", func(id, workspaceID string) datastore.Entity {
			return &model.JobArtifact{ID: id, WorkspaceID: workspaceID, Kind: "dataset"}
		}},
		{"chunk", func(id, workspaceID string) datastore.Entity {
			return &model.ArtifactChunk{ID: id, ArtifactID: id, WorkspaceID: workspaceID}
		}},
		{"delivery", func(id, workspaceID string) datastore.Entity {
			return &model.JobDelivery{ID: id, WorkspaceID: workspaceID, State: "pending"}
		}},
	} {
		t.Run(newEntity.name, func(t *testing.T) {
			service, _, _ := testAccounts(t)
			raw, ctx := service.Repo.Store, context.Background()
			store := NewStore(raw)
			scoped := WithScope(ctx, Scope{WorkspaceID: "allowed", Namespace: "allowed-ns"})
			require.NoError(t, raw.Add(ctx, newEntity.build("foreign-record", "foreign")))
			require.NoError(t, store.Add(scoped, newEntity.build("allowed-record", "allowed")))
			require.ErrorIs(t, store.Add(scoped, newEntity.build("forged-record", "foreign")), bcode.ErrForbidden)
			rows, err := store.List(scoped, newEntity.build("", ""), &datastore.ListOptions{Page: 1, PageSize: 1})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "allowed-record", rows[0].PrimaryKey())
			count, err := store.Count(scoped, newEntity.build("", ""), nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
			require.ErrorIs(t, store.Get(scoped, newEntity.build("foreign-record", "")), bcode.ErrForbidden)
			require.ErrorIs(t, store.Put(scoped, newEntity.build("allowed-record", "foreign")), bcode.ErrForbidden)
			require.ErrorIs(t, store.Put(scoped, newEntity.build("foreign-record", "allowed")), bcode.ErrForbidden)
			require.ErrorIs(t, store.Delete(scoped, newEntity.build("foreign-record", "")), bcode.ErrForbidden)
			_, err = store.CompareAndSwap(scoped, newEntity.build("allowed-record", ""), "workspace_id", "allowed", map[string]interface{}{"workspace_id": "foreign"})
			require.ErrorIs(t, err, bcode.ErrForbidden)
			require.NoError(t, store.DeleteByFilter(scoped, newEntity.build("", ""), nil))
			require.NoError(t, raw.Get(ctx, newEntity.build("foreign-record", "")))
		})
	}
}
