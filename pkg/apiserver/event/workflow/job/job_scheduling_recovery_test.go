package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreatedJobReattachesAfterOwnerAndQuotaChangeWithoutCreate(t *testing.T) {
	db, scopedStore, _ := evaluationScopeStore(t)
	require.NoError(t, db.AutoMigrate(&model.JobSandbox{}))
	store := &sqlstore.Driver{Client: *db}
	task := evaluationScopeTask(t, scopedStore, "")
	task.Status, task.Attempt = config.StatusRunning, 1
	task.EvaluationInfo = `{"traits":{"resources":{"cpu":"1","memory":"2Gi","cpuLimit":"2","memoryLimit":"4Gi"},"eval":{"concurrency":1,"sandboxResources":{"cpu":"1","memory":"2Gi","cpuLimit":"2","memoryLimit":"4Gi"}}}}`
	live := task.JobInfo.(*batchv1.Job)
	live.UID, live.ResourceVersion = "existing-runner", "1"
	stampJobExecutionIdentity(task, live)
	live.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}}
	owner := &model.WorkflowQueue{TaskID: task.TaskID}
	ctx := context.Background()
	require.NoError(t, store.Get(ctx, owner))
	record := buildJobInfoRecord(task)
	record.SchedulingResources, _ = jobSchedulingResourceSnapshot(task)
	require.NoError(t, repository.EnqueueJobForScheduling(ctx, store, owner, &record, nil))
	n, err := repository.AdmitQueuedJobs(ctx, store, corev1.ResourceList{corev1.ResourceRequestsCPU: resource.MustParse("2")})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: live.DeepCopy(), Attempt: 1, CurrentUID: live.UID, Deadline: time.Now().Add(time.Hour).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo = string(raw)
	require.NoError(t, store.Get(ctx, &record))
	record.InternalInfo = task.InternalInfo
	require.NoError(t, store.Put(ctx, &record))
	require.NoError(t, store.Add(ctx, &model.JobSandbox{ID: "allocated-trial", JobID: record.ID, WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, ExecutionKey: task.ExecutionKey, SlotReserved: true, StorageMiB: 1024, Deadline: time.Now().Add(time.Hour), ReconcileAt: time.Now()}))
	owner.RunGeneration, owner.RunToken, owner.WorkerID = 2, "replacement-token", "replacement-worker"
	require.NoError(t, store.Put(ctx, owner))
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = owner.RunGeneration, owner.RunToken, owner.WorkerID
	_, err = repository.AdmitQueuedJobs(ctx, store, corev1.ResourceList{corev1.ResourceRequestsCPU: resource.MustParse("1")})
	require.NoError(t, err)
	client := fake.NewSimpleClientset(live)
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	release, err := waitForJobAdmission(waitCtx, store, task, client)
	require.NoError(t, err, "existing Runner must attach even though its old bundle exceeds the new quota")
	ctl := NewInstantJobCtl(task, client, store, func() {})
	require.NoError(t, ctl.ensureRetryAttempt(ctx, cp))
	require.Zero(t, countClientActions(client, "create", "jobs"))
	// Losing the resource after confirmation still cannot turn that permission
	// into a replacement Create, even though admission has already transferred.
	require.NoError(t, client.BatchV1().Jobs(live.Namespace).Delete(ctx, live.Name, metav1.DeleteOptions{}))
	require.ErrorIs(t, ctl.ensureRetryAttempt(ctx, cp), signal.ErrInfrastructureStop)
	require.Zero(t, countClientActions(client, "create", "jobs"))
	require.NoError(t, release())
}

func TestJobAdmissionRecoveryRequiresConfirmedStopPolicyUID(t *testing.T) {
	for _, tc := range []struct {
		name        string
		missing     bool
		replacement bool
		noUID       bool
		resize      bool
		expired     bool
		wantError   bool
		wantProof   bool
	}{
		{name: "healthy", wantProof: true},
		{name: "missing", missing: true, wantError: true},
		{name: "replacement", replacement: true, wantError: true},
		{name: "no recorded UID", noUID: true},
		{name: "retry resize", resize: true},
		{name: "expired", expired: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := &workflowconfig.JobRetryPolicy{OnOOM: "stop"}
			if tc.resize {
				policy = retryTestPolicy()
			}
			task := retryTestTask(t, policy)
			task.JobType = string(config.JobEval)
			live := task.JobInfo.(*batchv1.Job)
			live.UID = "existing"
			live.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			cp := instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: live.DeepCopy(), CurrentUID: live.UID, Attempt: 1, Deadline: time.Now().Add(time.Hour).UnixNano()}
			if tc.noUID {
				cp.CurrentUID = ""
			}
			if tc.expired {
				cp.Deadline = time.Now().Add(-time.Second).UnixNano()
			}
			raw, err := json.Marshal(cp)
			require.NoError(t, err)
			task.InternalInfo = string(raw)
			if tc.replacement {
				live.UID = "replacement"
			}
			client := fake.NewSimpleClientset()
			if !tc.missing {
				require.NoError(t, client.Tracker().Add(live))
			}
			uid, err := confirmJobAdmissionRecovery(context.Background(), client, task)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantProof, uid != "")
			require.Zero(t, countClientActions(client, "create", "jobs"))
		})
	}
}
