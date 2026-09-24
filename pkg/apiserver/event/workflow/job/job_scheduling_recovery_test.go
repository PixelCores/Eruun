package job

import (
	"context"
	"encoding/json"
	"errors"
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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
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
	policy, err := retryPolicyFromJob(cp.Job)
	require.NoError(t, err)
	require.ErrorIs(t, ctl.runWithRetryPolicy(ctx, cp.Job, policy), signal.ErrInfrastructureStop)
	require.Zero(t, countClientActions(client, "create", "jobs"))
	require.NoError(t, release())
	require.NoError(t, store.Get(ctx, &record))
	require.Equal(t, workflowconfig.JobSchedulingReleased, record.SchedulingState)
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

func TestJobAdmissionRecoveryConfirmationReleasesOnlyAuthoritativeLoss(t *testing.T) {
	transientErr := errors.New("temporary Kubernetes API failure")
	for _, tc := range []struct {
		name         string
		missing      bool
		replacement  bool
		transient    bool
		wantReleased bool
	}{
		{name: "missing", missing: true, wantReleased: true},
		{name: "replacement", replacement: true, wantReleased: true},
		{name: "transient API error", transient: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, scopedStore, _ := evaluationScopeStore(t)
			store := &sqlstore.Driver{Client: *db}
			task := evaluationScopeTask(t, scopedStore, "")
			task.Status, task.Attempt = config.StatusRunning, 1
			live := task.JobInfo.(*batchv1.Job)
			live.UID, live.ResourceVersion = "recorded-runner", "1"
			stampJobExecutionIdentity(task, live)
			owner := &model.WorkflowQueue{TaskID: task.TaskID}
			ctx := context.Background()
			require.NoError(t, store.Get(ctx, owner))
			record := buildJobInfoRecord(task)
			require.NoError(t, repository.EnqueueJobForScheduling(ctx, store, owner, &record, nil))
			n, err := repository.AdmitQueuedJobs(ctx, store)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: live.DeepCopy(), Attempt: 1,
				CurrentUID: live.UID, Deadline: time.Now().Add(time.Hour).UnixNano()}
			raw, err := json.Marshal(cp)
			require.NoError(t, err)
			task.InternalInfo = string(raw)
			require.NoError(t, store.Get(ctx, &record))
			record.InternalInfo, record.Attempt, record.Status = task.InternalInfo, task.Attempt, string(task.Status)
			require.NoError(t, store.Put(ctx, &record))

			owner.RunGeneration++
			owner.RunToken, owner.WorkerID = "replacement-token", "replacement-worker"
			require.NoError(t, store.Put(ctx, owner))
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = owner.RunGeneration, owner.RunToken, owner.WorkerID
			client := fake.NewSimpleClientset()
			if !tc.missing {
				observed := live.DeepCopy()
				if tc.replacement {
					observed.UID = "replacement-runner"
				}
				require.NoError(t, client.Tracker().Add(observed))
			}
			if tc.transient {
				client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, transientErr
				})
			}

			release, err := waitForJobAdmission(ctx, store, task, client)
			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			if tc.transient {
				require.ErrorIs(t, err, transientErr)
			}
			require.NoError(t, release())
			require.NoError(t, store.Get(ctx, &record))
			if tc.wantReleased {
				require.Equal(t, workflowconfig.JobSchedulingReleased, record.SchedulingState)
			} else {
				require.Equal(t, workflowconfig.JobSchedulingAdmitted, record.SchedulingState)
			}
			require.Zero(t, countClientActions(client, "create", "jobs"))
		})
	}
}

func TestExpiredRecoveredJobSettlesWithoutAdmission(t *testing.T) {
	for _, kind := range []config.JobType{config.JobCommand, config.JobEval} {
		for _, tc := range []struct {
			name           string
			noUID          bool
			missing        bool
			replacement    bool
			cancelled      bool
			expiredLease   bool
			revokedOwner   bool
			infrastructure bool
		}{
			{name: "confirmed UID"},
			{name: "unconfirmed create", noUID: true},
			{name: "never created", noUID: true, missing: true},
			{name: "missing recorded execution", missing: true},
			{name: "replacement object", replacement: true},
			{name: "cancellation takes precedence", cancelled: true},
			{name: "expired lease before reaper", expiredLease: true},
			{name: "revoked lease", expiredLease: true, revokedOwner: true},
			{name: "infrastructure stop", infrastructure: true},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				db, scopedStore, _ := evaluationScopeStore(t)
				store := &sqlstore.Driver{Client: *db}
				task := evaluationScopeTask(t, scopedStore, "")
				task.JobType = string(kind)
				task.Status, task.Attempt = config.StatusRunning, 1
				live := task.JobInfo.(*batchv1.Job)
				live.UID, live.ResourceVersion = "original-execution", "1"
				stampJobExecutionIdentity(task, live)
				cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: live.DeepCopy(),
					Attempt: 1, CurrentUID: live.UID, Deadline: time.Now().Add(-time.Second).UnixNano()}
				if tc.noUID {
					cp.CurrentUID = ""
				}
				raw, err := json.Marshal(cp)
				require.NoError(t, err)
				task.InternalInfo = string(raw)
				record := buildJobInfoRecord(task)
				ctx := context.Background()
				require.NoError(t, store.Add(ctx, &record))
				owner := &model.WorkflowQueue{TaskID: task.TaskID}
				require.NoError(t, store.Get(ctx, owner))
				owner.RunGeneration, owner.RunToken, owner.WorkerID = 2, "replacement-token", "replacement-worker"
				task.OwnerRunGeneration, task.RunToken, task.WorkerID = owner.RunGeneration, owner.RunToken, owner.WorkerID
				if tc.expiredLease {
					expired := time.Now().Add(-time.Second)
					owner.LeaseExpiresAt = &expired
				}
				if tc.revokedOwner {
					owner.RunGeneration++
					owner.RunToken, owner.WorkerID = "newer-token", "newer-worker"
				}
				if tc.cancelled {
					owner.Status = config.StatusCancelled
				}
				require.NoError(t, store.Put(ctx, owner))
				client := fake.NewSimpleClientset()
				if !tc.missing {
					observed := live.DeepCopy()
					if tc.replacement {
						observed.UID = "replacement-object"
					}
					require.NoError(t, client.Tracker().Add(observed))
				}
				if tc.infrastructure {
					stopped, cancel := context.WithCancelCause(ctx)
					cancel(signal.ErrInfrastructureStop)
					ctx = stopped
				}
				err = runJob(ctx, task, client, store, func() {}, nil)
				saved := &model.JobInfo{ID: record.ID}
				require.NoError(t, store.Get(context.Background(), saved))
				if tc.revokedOwner || tc.infrastructure {
					if tc.revokedOwner {
						require.ErrorIs(t, err, signal.ErrInfrastructureStop)
					}
					require.Equal(t, string(config.StatusRunning), saved.Status)
					require.Zero(t, saved.EndTime)
					require.Empty(t, client.Actions())
					return
				}
				require.NoError(t, err)
				wantStatus := config.StatusTimeout
				if tc.cancelled {
					wantStatus = config.StatusCancelled
				}
				require.Equal(t, wantStatus, task.Status)
				require.Equal(t, string(wantStatus), saved.Status)
				require.Positive(t, saved.EndTime)
				require.Zero(t, countClientActions(client, "create", "jobs"))
				if tc.replacement || tc.missing {
					require.Zero(t, countClientActions(client, "delete", "jobs"))
				} else {
					require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
					for _, action := range client.Actions() {
						if action.GetVerb() == "delete" && action.GetResource().Resource == "jobs" {
							options := action.(k8stesting.DeleteAction).GetDeleteOptions()
							require.Equal(t, live.UID, *options.Preconditions.UID)
						}
					}
				}
				// Terminal persistence prevents another takeover from repeating work.
				before := len(client.Actions())
				require.NoError(t, runJob(ctx, task, client, store, func() {}, nil))
				require.Len(t, client.Actions(), before)
			})
		}
	}
}
