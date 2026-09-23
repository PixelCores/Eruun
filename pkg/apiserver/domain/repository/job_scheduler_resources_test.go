package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	wfc "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func schedulerEvaluation(t *testing.T, store datastore.DataStore, name, workspace string, concurrency int) (*model.WorkflowQueue, *model.JobInfo) {
	t.Helper()
	owner, job := schedulerTestJob(t, store, name, workspace, "normal")
	job.Type = string(config.JobEval)
	job.EvaluationInfo = fmt.Sprintf(`{"traits":{"resources":{"cpu":"1","memory":"2Gi","cpuLimit":"2","memoryLimit":"4Gi"},"eval":{"concurrency":%d,"sandboxResources":{"cpu":"1","memory":"2Gi","cpuLimit":"2","memoryLimit":"4Gi"}}}}`, concurrency)
	job.SchedulingResources = `{"pods":"1","requests.cpu":"1","requests.memory":"2Gi","limits.cpu":"2","limits.memory":"4Gi","requests.ephemeral-storage":"20Gi","limits.ephemeral-storage":"20Gi"}`
	require.NoError(t, store.Put(context.Background(), job))
	return owner, job
}

func schedulerResourceQuota(cpu string) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceRequestsCPU: resource.MustParse(cpu), corev1.ResourceRequestsMemory: resource.MustParse("16Gi"),
		corev1.ResourceLimitsCPU: resource.MustParse("16"), corev1.ResourceLimitsMemory: resource.MustParse("32Gi"), corev1.ResourcePods: resource.MustParse("20")}
}

func schedulerSandbox(t *testing.T, store datastore.DataStore, id string, job *model.JobInfo) *model.JobSandbox {
	t.Helper()
	row := &model.JobSandbox{ID: id, WorkspaceID: job.WorkspaceID, JobID: job.ID, ExecutionKey: *job.ExecutionKey,
		TaskID: job.TaskID, SlotReserved: true, State: "retained", StorageMiB: 1024, Deadline: time.Now().Add(time.Hour), ReconcileAt: time.Now()}
	require.NoError(t, store.Add(context.Background(), row))
	return row
}

func TestJobSchedulerResourceBundleAndSaturatedWorkspace(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	_, tooLarge := schedulerEvaluation(t, store, "parallel", "a", 2)
	owner, fits := schedulerEvaluation(t, store, "fits", "b", 1)
	n, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("2"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, store.Get(ctx, tooLarge))
	require.Equal(t, wfc.JobSchedulingQueued, tooLarge.SchedulingState)
	require.Contains(t, tooLarge.SchedulingReason, "requests.cpu")
	admitted, err := IsJobAdmitted(ctx, store, owner, *fits.ExecutionKey)
	require.NoError(t, err)
	require.True(t, admitted)
}

// Shared with the actual MySQL test: residual slots remain charged after the
// Runner admission ends, while live bundles and reattachment count each slot once.
func testJobSchedulerRetainedResources(t *testing.T, store datastore.DataStore) {
	t.Helper()
	ctx := context.Background()
	_, first := schedulerEvaluation(t, store, "retained-first", "a", 1)
	n, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("2"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	row := schedulerSandbox(t, store, "retained-sandbox", first)
	_, next := schedulerEvaluation(t, store, "retained-next", "a", 1)
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("4"))
	require.NoError(t, err)
	require.Equal(t, 1, n, "active bundle already includes its Sandbox")
	first.Status = string(config.StatusCompleted)
	require.NoError(t, store.Put(ctx, first))
	_, waiting := schedulerEvaluation(t, store, "retained-waiting", "a", 1)
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("4"))
	require.NoError(t, err)
	require.Zero(t, n, "ended Runner leaves one trial plus the second bundle reserved")
	require.NoError(t, store.Get(ctx, waiting))
	require.Contains(t, waiting.SchedulingReason, "budget exhausted")
	changed, err := store.CompareAndSwap(ctx, row, "id", row.ID, map[string]interface{}{"slot_reserved": false})
	require.NoError(t, err)
	require.True(t, changed)
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("4"))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Requeueing the same execution must reuse its existing trial allocation.
	require.NoError(t, store.Get(ctx, next))
	schedulerSandbox(t, store, "recover-sandbox", next)
	recoverOwner := &model.WorkflowQueue{TaskID: next.TaskID}
	require.NoError(t, store.Get(ctx, recoverOwner))
	require.NoError(t, ReleaseJobAdmission(ctx, store, recoverOwner, *next.ExecutionKey, "worker recovery"))
	require.NoError(t, EnqueueJobForScheduling(ctx, store, recoverOwner, next, nil))
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("4"))
	require.NoError(t, err)
	require.Equal(t, 1, n, "recovered execution must not double-charge its retained trial")
}

func TestJobSchedulerRetainedSandboxResources(t *testing.T) {
	testJobSchedulerRetainedResources(t, newJobSchedulerTestStore(t))
}

func TestSandboxReservationScanIndexUpgrade(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	row := &model.JobSandbox{ID: "retained-before-index", SlotReserved: true, State: "retained", Deadline: time.Now().Add(time.Hour), ReconcileAt: time.Now()}
	require.NoError(t, store.Add(ctx, row))

	const indexName = "idx_sandbox_reservation_scan"
	db := &store.Client
	require.NoError(t, db.Migrator().DropIndex(&model.JobSandbox{}, indexName))
	require.False(t, db.Migrator().HasIndex(&model.JobSandbox{}, indexName))
	require.NoError(t, db.AutoMigrate(&model.JobSandbox{}))
	require.True(t, db.Migrator().HasIndex(&model.JobSandbox{}, indexName))

	var columns []struct {
		Seqno int
		Name  string
	}
	require.NoError(t, db.Raw("PRAGMA index_info('idx_sandbox_reservation_scan')").Scan(&columns).Error)
	require.Len(t, columns, 2)
	require.Equal(t, "slot_reserved", columns[0].Name)
	require.Equal(t, "id", columns[1].Name)
	restored := &model.JobSandbox{ID: row.ID}
	require.NoError(t, store.Get(ctx, restored))
	require.True(t, restored.SlotReserved)
}

func testJobSchedulerLowerResourceQuota(t *testing.T, store datastore.DataStore) {
	t.Helper()
	ctx := context.Background()
	owner, active := schedulerEvaluation(t, store, "quota-active", "a", 1)
	n, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("4"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, queued := schedulerEvaluation(t, store, "quota-waiting", "a", 1)
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("1"))
	require.NoError(t, err)
	require.Zero(t, n)
	stillActive, err := IsJobAdmitted(ctx, store, owner, *active.ExecutionKey)
	require.NoError(t, err)
	require.True(t, stillActive, "quota decrease cannot evict a healthy execution")
	require.NoError(t, store.Get(ctx, queued))
	require.Equal(t, wfc.JobSchedulingQueued, queued.SchedulingState)
}

func TestJobSchedulerLowerResourceQuota(t *testing.T) {
	testJobSchedulerLowerResourceQuota(t, newJobSchedulerTestStore(t))
}

func TestJobSchedulerUnknownResourcesAndLegacyEvaluation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		raw       string
		kind      config.JobType
		ephemeral bool
		want      int
	}{
		{"legacy eval reconstructs declaration", "", config.JobEval, false, 1},
		{"legacy command cannot assume zero", "", config.JobCommand, false, 0},
		{"empty command resource snapshot", `{}`, config.JobCommand, false, 0},
		{"new command declared resources", `{"pods":"1","requests.cpu":"1","requests.memory":"2Gi","limits.cpu":"2","limits.memory":"4Gi"}`, config.JobCommand, false, 1},
		{"trial ephemeral storage unknown", "", config.JobEval, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newJobSchedulerTestStore(t)
			_, job := schedulerEvaluation(t, store, "unknown", "a", 1)
			job.Type, job.SchedulingResources = string(tc.kind), tc.raw
			require.NoError(t, store.Put(context.Background(), job))
			changed, err := store.CompareAndSwap(context.Background(), job, "id", job.ID, map[string]interface{}{"scheduling_resources": tc.raw})
			require.NoError(t, err)
			require.True(t, changed)
			quota := schedulerResourceQuota("2")
			if tc.ephemeral {
				quota[corev1.ResourceRequestsEphemeralStorage] = resource.MustParse("100Gi")
			}
			n, err := AdmitQueuedJobs(context.Background(), store, quota)
			require.NoError(t, err)
			require.Equal(t, tc.want, n)
		})
	}
	t.Run("unknown active Pod does not block configuration or another workspace", func(t *testing.T) {
		store := newJobSchedulerTestStore(t)
		ctx := context.Background()
		_, active := schedulerTestJob(t, store, "unknown-active", "a", "normal")
		active.Type = string(config.JobDeployInstant)
		require.NoError(t, store.Put(ctx, active))
		_, err := AdmitQueuedJobs(ctx, store)
		require.NoError(t, err)
		_, eval := schedulerEvaluation(t, store, "unknown-eval", "a", 1)
		_, configuration := schedulerTestJob(t, store, "configuration", "a", "normal")
		_, other := schedulerEvaluation(t, store, "other-eval", "b", 1)
		n, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("4"))
		require.NoError(t, err)
		require.Equal(t, 2, n)
		for _, row := range []*model.JobInfo{configuration, other} {
			require.NoError(t, store.Get(ctx, row))
			require.Equal(t, wfc.JobSchedulingAdmitted, row.SchedulingState)
		}
		require.NoError(t, store.Get(ctx, eval))
		require.Contains(t, eval.SchedulingReason, "unknown size")
	})
}

func TestJobSchedulerResourceBudgetPreservesLongLivedWorkflowAdmission(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	owner, evaluation := schedulerEvaluation(t, store, "mixed-evaluation", "a", 1)
	n, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("2"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, deployment := schedulerTestJob(t, store, "mixed-deployment", "a", "normal")
	deployment.Type = string(config.JobDeploy)
	require.NoError(t, store.Put(ctx, deployment))
	_, waiting := schedulerEvaluation(t, store, "mixed-next-evaluation", "a", 1)
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("4"))
	require.NoError(t, err)
	require.Equal(t, 1, n, "long-lived dependency admission must not deadlock behind its evaluation")
	require.NoError(t, store.Get(ctx, deployment))
	require.Equal(t, wfc.JobSchedulingAdmitted, deployment.SchedulingState)
	require.NoError(t, store.Get(ctx, waiting))
	require.Equal(t, wfc.JobSchedulingQueued, waiting.SchedulingState)
	require.Contains(t, waiting.SchedulingReason, "unknown size")
	active, err := IsJobAdmitted(ctx, store, owner, *evaluation.ExecutionKey)
	require.NoError(t, err)
	require.True(t, active)
}

func testJobSchedulerConcurrentResourceBudget(t *testing.T, store *sqlstore.Driver) {
	t.Helper()
	for i := range 8 {
		schedulerEvaluation(t, store, fmt.Sprintf("concurrent-resource-%d", i), "a", 1)
	}
	results := make(chan int, 8)
	errors := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			n, err := AdmitQueuedJobs(context.Background(), store, schedulerResourceQuota("2"))
			results <- n
			errors <- err
		})
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	total := 0
	for n := range results {
		total += n
	}
	require.Equal(t, 1, total, "concurrent schedulers share the policy row resource fence")
}

func TestJobSchedulerResourceSnapshotSurvivesRepeatedEnqueue(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	owner, job := schedulerTestJob(t, store, "resource-snapshot", "a", "normal")
	job.SchedulingResources = `{"pods":"1","requests.cpu":"1"}`
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
	var resources corev1.ResourceList
	require.NoError(t, json.Unmarshal([]byte(job.SchedulingResources), &resources))
	quantity := resources[corev1.ResourceRequestsCPU]
	require.Equal(t, "1", quantity.String())
	job.SchedulingResources = `{"pods":"99","requests.cpu":"99"}`
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil))
	require.NotContains(t, job.SchedulingResources, "99", "repeated enqueue preserves the committed resource envelope")
}
