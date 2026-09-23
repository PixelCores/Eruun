package repository

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	wfc "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func schedulerCreatedCheckpoint(t *testing.T, job *model.JobInfo, uid, policy string, deadline time.Time) string {
	t.Helper()
	raw, err := json.Marshal(map[string]interface{}{
		"kind": "instant_job_retry", "version": 1, "attempt": 1, "currentUID": uid, "deadline": deadline.UnixNano(),
		"job": map[string]interface{}{"metadata": map[string]interface{}{"name": "runner", "namespace": "ns", "annotations": map[string]string{
			config.AnnotationJobTaskID: job.TaskID, config.AnnotationJobExecutionKey: *job.ExecutionKey,
			config.AnnotationJobRunGeneration: "1", wfc.AnnotationJobAttempt: "1", wfc.AnnotationJobRetryPolicy: policy,
		}}},
	})
	require.NoError(t, err)
	return string(raw)
}

func testJobSchedulerRecoveryPreservesCreatedReservation(t *testing.T, store datastore.DataStore) {
	t.Helper()
	ctx := context.Background()
	owner, job := schedulerEvaluation(t, store, "recover-created", "a", 1)
	_, other := schedulerEvaluation(t, store, "other-created", "b", 1)
	n, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("2"))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.NoError(t, store.Get(ctx, job))
	job.Attempt, job.Status = 1, string(config.StatusRunning)
	job.InternalInfo = schedulerCreatedCheckpoint(t, job, "existing-runner", `{"onOOM":"stop"}`, time.Now().Add(time.Hour))
	require.NoError(t, store.Put(ctx, job))
	schedulerSandbox(t, store, "recover-existing-trial", job)
	// A lower global/workspace count and resource limit must not prevent
	// associating a healthy execution that already consumed its reservation.
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`)
	owner.RunGeneration++
	owner.RunToken, owner.WorkerID, owner.Status = "new-token", "new-worker", config.StatusWaiting
	require.NoError(t, store.Put(ctx, owner))
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("1"))
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, store.Get(ctx, job))
	require.Equal(t, wfc.JobSchedulingAdmitted, job.SchedulingState, "the lease gap still owns existing resources")
	owner.Status = config.StatusRunning
	require.NoError(t, store.Put(ctx, owner))
	require.NoError(t, EnqueueJobForScheduling(ctx, store, owner, job, nil, "existing-runner"))
	admitted, err := IsJobAdmitted(ctx, store, owner, *job.ExecutionKey)
	require.NoError(t, err)
	require.True(t, admitted)
	require.Equal(t, owner.RunGeneration, job.SchedulingGeneration)
	require.Equal(t, uint64(1), job.RunGeneration, "execution identity remains immutable")
	require.NoError(t, store.Get(ctx, other))
	require.Equal(t, wfc.JobSchedulingAdmitted, other.SchedulingState)
	_, queued := schedulerEvaluation(t, store, "new-after-recovery", "a", 1)
	n, err = AdmitQueuedJobs(ctx, store, schedulerResourceQuota("1"))
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, store.Get(ctx, queued))
	require.Equal(t, wfc.JobSchedulingQueued, queued.SchedulingState, "recovery is no permit for new execution")
}

func TestJobSchedulerRecoveryPreservesCreatedReservation(t *testing.T) {
	testJobSchedulerRecoveryPreservesCreatedReservation(t, newJobSchedulerTestStore(t))
}

func TestJobSchedulerExecutionReturnKeepsLiveCheckpointAdmission(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	owner, job := schedulerEvaluation(t, store, "live-return", "a", 1)
	n, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("2"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, store.Get(ctx, job))
	job.Attempt, job.Status = 1, string(config.StatusRunning)
	job.InternalInfo = schedulerCreatedCheckpoint(t, job, "live-runner", `{"onOOM":"stop"}`, time.Now().Add(time.Hour))
	require.NoError(t, store.Put(ctx, job))

	require.NoError(t, ReleaseJobAdmission(ctx, store, owner, *job.ExecutionKey, "job execution returned"))
	require.NoError(t, store.Get(ctx, job))
	require.Equal(t, wfc.JobSchedulingAdmitted, job.SchedulingState)

	job.Status = string(config.StatusCompleted)
	require.NoError(t, store.Put(ctx, job))
	require.NoError(t, ReleaseJobAdmission(ctx, store, owner, *job.ExecutionKey, "cleanup confirmed execution ended"))
	require.NoError(t, store.Get(ctx, job))
	require.Equal(t, wfc.JobSchedulingReleased, job.SchedulingState)
}

func TestReleaseLostJobAdmissionFencesIdentityAndReturnsBudget(t *testing.T) {
	store := newJobSchedulerTestStore(t)
	ctx := context.Background()
	setSchedulerTestPolicy(t, store, `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`)
	owner, job := schedulerEvaluation(t, store, "lost-recovery", "a", 1)
	n, err := AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, store.Get(ctx, job))
	job.Attempt, job.Status = 1, string(config.StatusRunning)
	checkpointDeadline := time.Now().Add(time.Hour)
	job.InternalInfo = schedulerCreatedCheckpoint(t, job, "lost-runner", `{"onOOM":"stop"}`, checkpointDeadline)
	require.NoError(t, store.Put(ctx, job))
	supplied := *job
	oldOwner := *owner
	owner.RunGeneration++
	owner.RunToken, owner.WorkerID = "new-token", "new-worker"
	require.NoError(t, store.Put(ctx, owner))

	require.ErrorIs(t, ReleaseLostJobAdmission(ctx, store, &oldOwner, &supplied, "lost"), ErrWorkflowOwnershipLost)
	wrong := supplied
	wrong.InternalInfo = schedulerCreatedCheckpoint(t, &wrong, "replacement", `{"onOOM":"stop"}`, checkpointDeadline)
	require.ErrorIs(t, ReleaseLostJobAdmission(ctx, store, owner, &wrong, "lost"), ErrWorkflowOwnershipLost)
	require.NoError(t, store.Get(ctx, job))
	require.Equal(t, wfc.JobSchedulingAdmitted, job.SchedulingState)

	_, waiting := schedulerEvaluation(t, store, "waiting-after-lost", "b", 1)
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Zero(t, n, "a mismatched recovery proof cannot free another execution's capacity")
	require.NoError(t, store.Get(ctx, waiting))
	require.Equal(t, wfc.JobSchedulingQueued, waiting.SchedulingState)

	require.NoError(t, ReleaseLostJobAdmission(ctx, store, owner, &supplied, "authoritative Kubernetes Job loss"))
	require.NoError(t, ReleaseLostJobAdmission(ctx, store, owner, &supplied, "authoritative Kubernetes Job loss"))
	require.NoError(t, store.Get(ctx, job))
	require.Equal(t, wfc.JobSchedulingReleased, job.SchedulingState)
	n, err = AdmitQueuedJobs(ctx, store)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, store.Get(ctx, waiting))
	require.Equal(t, wfc.JobSchedulingAdmitted, waiting.SchedulingState)
}

func TestJobSchedulerRecoveryProofCannotGrantNewAdmission(t *testing.T) {
	for _, tc := range []struct {
		name      string
		change    func(*model.JobInfo)
		proof     string
		wantState string
		wantError bool
	}{
		{"released after ownership transition", func(job *model.JobInfo) {
			job.SchedulingState = wfc.JobSchedulingReleased
			job.SchedulingReason = "execution completed or scheduling ownership expired"
		}, "existing", wfc.JobSchedulingQueued, false},
		{"queued never admitted", func(job *model.JobInfo) { job.SchedulingState = wfc.JobSchedulingQueued }, "existing", wfc.JobSchedulingQueued, false},
		{"wrong UID", func(job *model.JobInfo) {}, "replacement", "", true},
		{"no persisted UID", func(job *model.JobInfo) {
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "", `{"onOOM":"stop"}`, time.Now().Add(time.Hour))
		}, "existing", "", true},
		{"expired execution", func(job *model.JobInfo) {
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "existing", `{"onOOM":"stop"}`, time.Now().Add(-time.Second))
		}, "existing", "", true},
		{"resize policy", func(job *model.JobInfo) {
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "existing", `{"onOOM":"resize","maxRetries":1,"backoffSeconds":1,"memoryGrowthFactor":2,"maxResources":{"memory":"8Gi"}}`, time.Now().Add(time.Hour))
		}, "existing", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newJobSchedulerTestStore(t)
			ctx := context.Background()
			owner, job := schedulerEvaluation(t, store, "proof", "a", 1)
			_, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("2"))
			require.NoError(t, err)
			require.NoError(t, store.Get(ctx, job))
			job.Attempt, job.Status = 1, string(config.StatusRunning)
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "existing", `{"onOOM":"stop"}`, time.Now().Add(time.Hour))
			tc.change(job)
			require.NoError(t, store.Put(ctx, job))
			owner.RunGeneration++
			owner.RunToken = "new-owner"
			require.NoError(t, store.Put(ctx, owner))
			err = EnqueueJobForScheduling(ctx, store, owner, job, nil, tc.proof)
			if tc.wantError {
				require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantState, job.SchedulingState, "a confirmed resource without an active reservation gets no exemption")
			}
		})
	}
}

func TestJobSchedulerReleasedRecoveryReentersAdmissionBudgets(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     string
		workspaceB string
		quota      corev1.ResourceList
	}{
		{"global concurrency", `{"maxConcurrentJobs":1,"maxConcurrentJobsPerWorkspace":1}`, "b", nil},
		{"workspace concurrency", `{"maxConcurrentJobs":2,"maxConcurrentJobsPerWorkspace":1}`, "a", nil},
		{"workspace resources", `{"maxConcurrentJobs":2,"maxConcurrentJobsPerWorkspace":2}`, "a", schedulerResourceQuota("2")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newJobSchedulerTestStore(t)
			ctx := context.Background()
			setSchedulerTestPolicy(t, store, tc.policy)
			quotas := []corev1.ResourceList(nil)
			if tc.quota != nil {
				quotas = append(quotas, tc.quota)
			}

			ownerA, jobA := schedulerEvaluation(t, store, "released-a", "a", 1)
			n, err := AdmitQueuedJobs(ctx, store, quotas...)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.NoError(t, store.Get(ctx, jobA))
			jobA.Attempt, jobA.Status = 1, string(config.StatusRunning)
			jobA.InternalInfo = schedulerCreatedCheckpoint(t, jobA, "runner-a", `{"onOOM":"stop"}`, time.Now().Add(time.Hour))
			jobA.SchedulingState = wfc.JobSchedulingReleased
			jobA.SchedulingReason = "job execution returned"
			require.NoError(t, store.Put(ctx, jobA))

			ownerB, jobB := schedulerEvaluation(t, store, "admitted-b", tc.workspaceB, 1)
			n, err = AdmitQueuedJobs(ctx, store, quotas...)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			ownerA.RunGeneration++
			ownerA.RunToken, ownerA.WorkerID = "new-token-a", "new-worker-a"
			require.NoError(t, store.Put(ctx, ownerA))
			require.NoError(t, EnqueueJobForScheduling(ctx, store, ownerA, jobA, nil, "runner-a"))
			require.Equal(t, wfc.JobSchedulingQueued, jobA.SchedulingState)
			n, err = AdmitQueuedJobs(ctx, store, quotas...)
			require.NoError(t, err)
			require.Zero(t, n, "released execution must wait behind the current budget owner")
			require.NoError(t, store.Get(ctx, jobA))
			require.Equal(t, wfc.JobSchedulingQueued, jobA.SchedulingState)
			require.NoError(t, store.Get(ctx, jobB))
			require.Equal(t, wfc.JobSchedulingAdmitted, jobB.SchedulingState)

			require.NoError(t, ReleaseJobAdmission(ctx, store, ownerB, *jobB.ExecutionKey, "finished"))
			n, err = AdmitQueuedJobs(ctx, store, quotas...)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.NoError(t, store.Get(ctx, jobA))
			require.Equal(t, wfc.JobSchedulingAdmitted, jobA.SchedulingState)
		})
	}
}

func TestJobSchedulerRecoveryAllowsConcurrentProgressButFencesIdentityChanges(t *testing.T) {
	for _, tc := range []struct {
		name         string
		change       func(map[string]interface{})
		wantRecovery bool
	}{
		{"heartbeat and progress", func(checkpoint map[string]interface{}) {
			checkpoint["runner"] = map[string]interface{}{"lastSequence": 42, "heartbeat": "new", "progress": 0.5}
		}, true},
		{"changed UID", func(checkpoint map[string]interface{}) { checkpoint["currentUID"] = "replacement" }, false},
		{"changed deadline", func(checkpoint map[string]interface{}) {
			checkpoint["deadline"] = time.Now().Add(2 * time.Hour).UnixNano()
		}, false},
		{"changed namespace", func(checkpoint map[string]interface{}) {
			checkpoint["job"].(map[string]interface{})["metadata"].(map[string]interface{})["namespace"] = "another"
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newJobSchedulerTestStore(t)
			ctx := context.Background()
			owner, job := schedulerEvaluation(t, store, "concurrent-proof", "a", 1)
			_, err := AdmitQueuedJobs(ctx, store, schedulerResourceQuota("2"))
			require.NoError(t, err)
			require.NoError(t, store.Get(ctx, job))
			job.Attempt, job.Status = 1, string(config.StatusRunning)
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "existing", `{"onOOM":"stop"}`, time.Now().Add(time.Hour))
			require.NoError(t, store.Put(ctx, job))
			supplied := *job // Runtime read before the concurrent Runner update.
			var checkpoint map[string]interface{}
			decoder := json.NewDecoder(strings.NewReader(job.InternalInfo))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&checkpoint))
			tc.change(checkpoint)
			encoded, err := json.Marshal(checkpoint)
			require.NoError(t, err)
			job.InternalInfo = string(encoded)
			require.NoError(t, store.Put(ctx, job))
			owner.RunGeneration++
			owner.RunToken = "new-owner"
			require.NoError(t, store.Put(ctx, owner))
			err = EnqueueJobForScheduling(ctx, store, owner, &supplied, nil, "existing")
			if tc.wantRecovery {
				require.NoError(t, err)
				require.Equal(t, wfc.JobSchedulingAdmitted, supplied.SchedulingState)
				require.Contains(t, supplied.InternalInfo, "lastSequence", "retain the concurrently persisted Runner state")
			} else {
				require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
			}
		})
	}
}
