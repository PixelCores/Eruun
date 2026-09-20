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

func TestJobSchedulerRecoveryProofCannotGrantNewAdmission(t *testing.T) {
	for _, tc := range []struct {
		name         string
		change       func(*model.JobInfo)
		proof        string
		wantRecovery bool
	}{
		{"released after ownership transition", func(job *model.JobInfo) {
			job.SchedulingState = wfc.JobSchedulingReleased
			job.SchedulingReason = "execution completed or scheduling ownership expired"
		}, "existing", true},
		{"queued never admitted", func(job *model.JobInfo) { job.SchedulingState = wfc.JobSchedulingQueued }, "existing", false},
		{"wrong UID", func(job *model.JobInfo) {}, "replacement", false},
		{"no persisted UID", func(job *model.JobInfo) {
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "", `{"onOOM":"stop"}`, time.Now().Add(time.Hour))
		}, "existing", false},
		{"expired execution", func(job *model.JobInfo) {
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "existing", `{"onOOM":"stop"}`, time.Now().Add(-time.Second))
		}, "existing", false},
		{"resize policy", func(job *model.JobInfo) {
			job.InternalInfo = schedulerCreatedCheckpoint(t, job, "existing", `{"onOOM":"resize","maxRetries":1,"backoffSeconds":1,"memoryGrowthFactor":2,"maxResources":{"memory":"8Gi"}}`, time.Now().Add(time.Hour))
		}, "existing", false},
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
			if tc.wantRecovery {
				require.NoError(t, err)
				require.Equal(t, wfc.JobSchedulingAdmitted, job.SchedulingState)
			} else if tc.name == "queued never admitted" {
				require.NoError(t, err)
				require.Equal(t, wfc.JobSchedulingQueued, job.SchedulingState, "a confirmed resource without previous admission gets no exemption")
			} else {
				require.ErrorIs(t, err, ErrWorkflowOwnershipLost)
			}
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
