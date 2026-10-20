package jobinstant

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type retryCheckpointStore struct {
	jobtest.NoopStore
	record    *model.JobInfo
	afterSave func(*model.JobInfo) error
}

func (*retryCheckpointStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	return time.Now().UTC(), nil
}

func TestRetryCreationBudgetPersistsAcrossCommandAndEvaluationControllers(t *testing.T) {
	store := jobtest.NewRetryCreationBudgetStore(t, &retryCheckpointStore{})
	policy := workflowconfig.DefaultJobSchedulerPolicy()
	policy.ResourceCreationQPS, policy.ResourceCreationBurst = 0.1, 1
	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	require.NoError(t, store.CreationStore.Put(context.Background(), &model.SystemSetting{
		Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded,
	}))
	command := retryTestTask(t, nil)
	command.JobType = string(config.JobCommand)
	client := fake.NewSimpleClientset()
	require.NoError(t, NewInstantJobCtl(command, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}}).waitRetryCreationBudget(context.Background()))
	budget := &model.ResourceCreationBudget{ID: "jobs-and-sandboxes"}
	require.NoError(t, store.CreationStore.Get(context.Background(), budget))
	require.False(t, budget.AvailableAt.IsZero())
	availableAt := budget.AvailableAt

	// A new controller shares persisted debt, even through a separate fixture
	// wrapper. Cancellation must stop waiting without extending that debt.
	evaluationStore := &jobtest.RetryCreationBudgetStore{DataStore: &retryCheckpointStore{}, CreationStore: store.CreationStore}
	evaluation := retryTestTask(t, nil)
	evaluation.JobType = string(config.JobEval)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = NewInstantJobCtl(evaluation, &jobexec.Runtime{Client: client, Store: evaluationStore, Ack: func() {}}).waitRetryCreationBudget(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, store.CreationStore.Get(context.Background(), budget))
	require.True(t, availableAt.Equal(budget.AvailableAt), "waiting cannot consume a permit")
	require.Empty(t, client.Actions())
}

func (s *retryCheckpointStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	if s.record == nil {
		return nil, nil
	}
	copy := *s.record
	return []datastore.Entity{&copy}, nil
}

func (s *retryCheckpointStore) Add(_ context.Context, entity datastore.Entity) error {
	record := *entity.(*model.JobInfo)
	if s.afterSave != nil {
		if err := s.afterSave(&record); err != nil {
			return err
		}
	}
	s.record = &record
	return nil
}

func (s *retryCheckpointStore) CompareAndSwapWithConditions(_ context.Context, _ datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if s.record == nil || s.record.Attempt != conditions["attempt"] || s.record.Status != conditions["status"] {
		return false, nil
	}
	record := *s.record
	record.Attempt = updates["attempt"].(uint)
	record.Status = updates["status"].(string)
	record.Info = updates["info"].(string)
	record.InternalInfo = updates["internal_info"].(string)
	if s.afterSave != nil {
		if err := s.afterSave(&record); err != nil {
			return false, err
		}
	}
	s.record = &record
	return true, nil
}

func retryTestTask(t *testing.T, policy *workflowconfig.JobRetryPolicy) *model.JobTask {
	t.Helper()
	component := &model.ApplicationComponent{Name: "retry-job", Namespace: "ns", Image: "busybox:1.37"}
	desired := jobexec.BuildJob(component, &model.Properties{JobRetryPolicy: policy}, jobexec.JobBuildOptions{})
	desired.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi"), corev1.ResourceCPU: resource.MustParse("100m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi"), corev1.ResourceCPU: resource.MustParse("200m")},
	}
	task := &model.JobTask{Name: desired.Name, Namespace: "ns", JobInfo: desired, JobType: string(config.JobDeployInstant),
		TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 1, Attempt: 1, Timeout: 10}
	jobresult.StampJobExecutionIdentity(task, desired)
	return task
}

func TestInstantJobOOMRetryPolicies(t *testing.T) {
	for _, tt := range []struct {
		name        string
		policy      *workflowconfig.JobRetryPolicy
		reason      string
		failures    int
		wantCreates int
		wantError   string
		wantMemory  string
	}{
		{"resize", jobtest.RetryTestPolicy(), "OOMKilled", 1, 2, "", "512Mi"},
		{"retry same resources", &workflowconfig.JobRetryPolicy{OnOOM: "retry", MaxRetries: 1, BackoffSeconds: 1}, "OOMKilled", 1, 2, "", "256Mi"},
		{"explicit stop", &workflowconfig.JobRetryPolicy{OnOOM: "stop"}, "OOMKilled", 1, 1, "job failed after attempt 1", "256Mi"},
		{"ordinary exit 137 stops", jobtest.RetryTestPolicy(), "Error", 1, 1, "non-OOM", "256Mi"},
		{"bounded retries", jobtest.RetryTestPolicy(), "OOMKilled", 2, 2, "job failed after attempt 2", "512Mi"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := retryTestTask(t, tt.policy)
			store := &retryCheckpointStore{}
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			jobtest.InstallRetryJobReactor(t, client, tt.failures, tt.reason, &created)
			ctl := newObservedInstantJobCtl(t, task, client, jobtest.NewRetryCreationBudgetStore(t, jobtest.WithJobTestOwner(store, task)), func() {})
			err := ctl.Run(jobexec.WithCleanupTracker(context.Background()))
			if tt.wantError == "" {
				require.NoError(t, err)
				require.Equal(t, config.StatusCompleted, task.Status)
			} else {
				require.ErrorContains(t, err, tt.wantError)
			}
			require.Len(t, created, tt.wantCreates)
			resources := created[len(created)-1].Spec.Template.Spec.Containers[0].Resources
			require.Equal(t, tt.wantMemory, resources.Limits.Memory().String())
			require.Equal(t, "200m", resources.Limits.Cpu().String())
			require.Equal(t, uint(tt.wantCreates), task.Attempt)
			require.Equal(t, task.InternalInfo, store.record.InternalInfo)
		})
	}
}

func TestInstantJobRetryCheckpointResumesWithoutDoubleGrowth(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	client := fake.NewSimpleClientset()
	var created []*batchv1.Job
	jobtest.InstallRetryJobReactor(t, client, 1, "OOMKilled", &created)
	ctx, cancel := context.WithCancel(context.Background())
	store := &retryCheckpointStore{afterSave: func(record *model.JobInfo) error {
		if record.Attempt == 2 {
			cancel()
		}
		return nil
	}}
	require.ErrorIs(t, newObservedInstantJobCtl(t, task, client, jobtest.NewRetryCreationBudgetStore(t, jobtest.WithJobTestOwner(store, task)), func() {}).Run(ctx), context.Canceled)
	require.Len(t, created, 1)
	require.Equal(t, uint(2), store.record.Attempt)
	store.afterSave = nil
	recovered := retryTestTask(t, jobtest.RetryTestPolicy())
	recovered.InternalInfo = store.record.InternalInfo
	recovered.Attempt = store.record.Attempt
	require.NoError(t, RestoreInstantJobRetryCheckpoint(recovered))
	require.NoError(t, newObservedInstantJobCtl(t, recovered, client, jobtest.NewRetryCreationBudgetStore(t, jobtest.WithJobTestOwner(store, recovered)), func() {}).Run(context.Background()))
	require.Len(t, created, 2)
	require.Equal(t, "512Mi", created[1].Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
	require.Equal(t, 1, jobtest.CountClientActions(client, "delete", "jobs"))
}

func TestInstantJobRetryCheckpointFailurePreventsDeletion(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	client := fake.NewSimpleClientset()
	var created []*batchv1.Job
	jobtest.InstallRetryJobReactor(t, client, 1, "OOMKilled", &created)
	store := &retryCheckpointStore{afterSave: func(record *model.JobInfo) error {
		if record.Attempt == 2 {
			return errors.New("database unavailable")
		}
		return nil
	}}
	err := newObservedInstantJobCtl(t, task, client, jobtest.NewRetryCreationBudgetStore(t, jobtest.WithJobTestOwner(store, task)), func() {}).Run(context.Background())
	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
	require.Len(t, created, 1)
	require.Equal(t, uint(1), store.record.Attempt)
}

func TestRetryRejectsForeignOOMAndPreviousTermination(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	job.UID = "expected"
	for _, tt := range []struct {
		name   string
		change func(*corev1.Pod)
	}{
		{"different owner UID", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "other" }},
		{"not controller", func(p *corev1.Pod) { no := false; p.OwnerReferences[0].Controller = &no }},
		{"last terminated state", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].LastTerminationState = p.Status.ContainerStatuses[0].State
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{}
		}},
		{"exit137", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.Reason = "Error" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := jobtest.RetryTestPod(job, "OOMKilled")
			tt.change(pod)
			containers, err := oomJobContainers(context.Background(), fake.NewSimpleClientset(pod), job)
			require.NoError(t, err)
			require.Empty(t, containers)
		})
	}
}

func TestResizeResourcesRespectCapsAndExplicitCPU(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	container := job.Spec.Template.Spec.Containers[0].Name
	policy := jobtest.RetryTestPolicy()
	policy.CPUGrowthFactor = 2
	policy.MaxResources[corev1.ResourceCPU] = resource.MustParse("1")
	next, err := growOOMJobResources(job, policy, map[string]bool{container: true})
	require.NoError(t, err)
	require.Equal(t, "200m", next.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String())
	require.Equal(t, "400m", next.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().String())
	policy.MaxResources[corev1.ResourceMemory] = resource.MustParse("256Mi")
	_, err = growOOMJobResources(job, policy, map[string]bool{container: true})
	require.ErrorContains(t, err, "exceeds maxResources")
	require.Equal(t, "256Mi", job.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
}

func TestRetryAttemptWaitsForPreviousOwnedPods(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	job := task.JobInfo.(*batchv1.Job).DeepCopy()
	job.UID = "previous"
	pod := jobtest.RetryTestPod(job, "OOMKilled")
	pod.Status.Phase = corev1.PodRunning
	client := fake.NewSimpleClientset(pod)
	cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: task.JobInfo.(*batchv1.Job), Attempt: 2,
		PreviousUID: job.UID, RetryAt: time.Now().Add(-time.Second).UnixNano(), Deadline: time.Now().Add(time.Hour).UnixNano()}
	cp.Job.Annotations[workflowconfig.AnnotationJobAttempt] = "2"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: &retryCheckpointStore{}, Ack: func() {}}).ensureRetryAttempt(ctx, cp)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, jobtest.CountClientActions(client, "create", "jobs"))
}

func TestRetryCheckpointCannotReplayMissingOrForeignJob(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(strconv.FormatBool(exists), func(t *testing.T) {
			task := retryTestTask(t, jobtest.RetryTestPolicy())
			job := task.JobInfo.(*batchv1.Job)
			job.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			cp := &jobinfo.InstantJobRetryCheckpoint{Job: job, Attempt: 1, CurrentUID: "expected"}
			client := fake.NewSimpleClientset()
			if exists {
				foreign := job.DeepCopy()
				foreign.UID = "replacement"
				require.NoError(t, client.Tracker().Add(foreign))
			}
			err := NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: &retryCheckpointStore{}, Ack: func() {}}).ensureRetryAttempt(context.Background(), cp)
			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.NotErrorIs(t, err, errJobAdmissionRecoveryExecutionLost, "resize retries do not use the stop-policy admission recovery exemption")
			require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
			require.Zero(t, jobtest.CountClientActions(client, "create", "jobs"))
		})
	}
}

func TestRestoreRetryCheckpointRejectsMalformedIdentity(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	job.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: job, Attempt: 1, Deadline: time.Now().Add(time.Hour).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo = string(raw)
	require.NoError(t, RestoreInstantJobRetryCheckpoint(task))
	task.ExecutionKey = "wrong-execution"
	require.Error(t, RestoreInstantJobRetryCheckpoint(task))
	require.True(t, jobinfo.HasInstantJobRetryCheckpoint(&model.JobInfo{Type: task.JobType, InternalInfo: "invalid json"}))
}

func TestRetryRecoveryHonorsExpiredDeadline(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	job.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: job, Attempt: 1, Deadline: time.Now().Add(-time.Second).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo = string(raw)
	client := fake.NewSimpleClientset()
	err = NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: &retryCheckpointStore{}, Ack: func() {}}).Run(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, config.StatusTimeout, task.Status)
	require.Empty(t, client.Actions())
}

func TestRetryDeletionRechecksWorkflowFence(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	task.OwnerRunGeneration = 1
	task.RunToken = "token-1"
	task.WorkerID = "worker-1"
	current := model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}
	replacement := current
	replacement.RunGeneration = 2
	store := &jobtest.WorkflowOwnershipStore{NoopStore: &jobtest.NoopStore{}, Tasks: []model.WorkflowQueue{current, replacement}}
	previous := task.JobInfo.(*batchv1.Job).DeepCopy()
	previous.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	previous.UID = "old-job"
	previous.ResourceVersion = "9"
	previous.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	next := task.JobInfo.(*batchv1.Job).DeepCopy()
	next.Annotations[workflowconfig.AnnotationJobAttempt] = "2"
	cp := &jobinfo.InstantJobRetryCheckpoint{Job: next, Attempt: 2, PreviousUID: previous.UID}
	client := fake.NewSimpleClientset(previous)
	err := NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}}).ensureRetryAttempt(context.Background(), cp)
	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
	require.Zero(t, jobtest.CountClientActions(client, "create", "jobs"))
}

func TestRetryDeletionUsesUIDAndResourceVersion(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	live := task.JobInfo.(*batchv1.Job).DeepCopy()
	live.UID = "old-job"
	live.ResourceVersion = "9"
	client := fake.NewSimpleClientset(live)
	require.NoError(t, NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: &retryCheckpointStore{}, Ack: func() {}}).deleteRetryJob(context.Background(), live))
	options := client.Actions()[0].(k8stesting.DeleteAction).GetDeleteOptions()
	require.Equal(t, live.UID, *options.Preconditions.UID)
	require.Equal(t, live.ResourceVersion, *options.Preconditions.ResourceVersion)
	require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
}

func TestResizeOnlyOOMContainerAndRejectsInjectedContainer(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	init := job.Spec.Template.Spec.Containers[0].DeepCopy()
	init.Name = "init"
	job.Spec.Template.Spec.InitContainers = []corev1.Container{*init}
	next, err := growOOMJobResources(job, jobtest.RetryTestPolicy(), map[string]bool{"init": true})
	require.NoError(t, err)
	require.Equal(t, "512Mi", next.Spec.Template.Spec.InitContainers[0].Resources.Limits.Memory().String())
	require.Equal(t, "256Mi", next.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
	_, err = growOOMJobResources(job, jobtest.RetryTestPolicy(), map[string]bool{"injected": true})
	require.ErrorContains(t, err, "absent from the submitted Job template")
}

func TestRetryCleanupRecoveredTimeoutUsesCheckpointOwnership(t *testing.T) {
	for _, pendingRetry := range []bool{false, true} {
		t.Run(strconv.FormatBool(pendingRetry), func(t *testing.T) {
			task := retryTestTask(t, jobtest.RetryTestPolicy())
			desired := task.JobInfo.(*batchv1.Job)
			desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			live := desired.DeepCopy()
			live.UID = "created-before-worker-restart"
			cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired,
				Attempt: 1, CurrentUID: live.UID, Deadline: time.Now().Add(-time.Second).UnixNano()}
			if pendingRetry {
				cp.Attempt, task.Attempt = 2, 2
				cp.CurrentUID = ""
				cp.PreviousUID = live.UID
				cp.RetryAt = time.Now().Add(time.Minute).UnixNano()
				desired.Annotations[workflowconfig.AnnotationJobAttempt] = "2"
				live.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
			}
			raw, err := json.Marshal(cp)
			require.NoError(t, err)
			task.InternalInfo = string(raw)
			client := fake.NewSimpleClientset(live)
			ctl := newObservedInstantJobCtl(t, task, client, &retryCheckpointStore{}, func() {})
			require.ErrorIs(t, ctl.Run(context.Background()), context.DeadlineExceeded)
			ctl.Clean(context.Background())
			require.Equal(t, 1, jobtest.CountClientActions(client, "delete", "jobs"))
		})
	}
}

func TestRetryCleanupRejectsForeignExecutionOrUID(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*batchv1.Job)
	}{
		{"replacement UID", func(job *batchv1.Job) { job.UID = "different" }},
		{"replacement generation", func(job *batchv1.Job) { job.Annotations[config.AnnotationJobRunGeneration] = "2" }},
		{"replacement attempt", func(job *batchv1.Job) { job.Annotations[workflowconfig.AnnotationJobAttempt] = "2" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := retryTestTask(t, jobtest.RetryTestPolicy())
			desired := task.JobInfo.(*batchv1.Job)
			desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
				CurrentUID: "owned", Deadline: time.Now().Add(time.Hour).UnixNano()}
			raw, err := json.Marshal(cp)
			require.NoError(t, err)
			task.InternalInfo = string(raw)
			live := desired.DeepCopy()
			live.UID = "owned"
			tt.change(live)
			client := fake.NewSimpleClientset(live)
			err = NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: &retryCheckpointStore{}, Ack: func() {}}).cleanRetryAttempt(context.Background())
			require.ErrorIs(t, err, jobinfo.ErrJobExecutionIdentityChanged)
			require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
		})
	}
}

type retryOwnedCheckpointStore struct {
	retryCheckpointStore
	owner           model.WorkflowQueue
	ownershipChecks int
}

func (s *retryOwnedCheckpointStore) Get(_ context.Context, entity datastore.Entity) error {
	if owner, ok := entity.(*model.WorkflowQueue); ok {
		*owner = s.owner
		return nil
	}
	return datastore.ErrRecordNotExist
}

// The fixture models an explicit transaction boundary; SQL tests verify isolation.
func (s *retryOwnedCheckpointStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.WithTransaction(ctx, fn)
}

func (s *retryOwnedCheckpointStore) WithTransaction(_ context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *retryOwnedCheckpointStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.ownershipChecks++
		return conditions["run_generation"] == s.owner.RunGeneration && conditions["run_token"] == s.owner.RunToken &&
			conditions["worker_id"] == s.owner.WorkerID && conditions["status"] == s.owner.Status, nil
	}
	return s.retryCheckpointStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func TestRetryRuntimeResumesOldGenerationUnderCurrentLease(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 4, "token-4", "worker-4"
	previous := task.JobInfo.(*batchv1.Job).DeepCopy()
	previous.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	previous.UID = "generation-1-attempt-1"
	previous.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	next, err := growOOMJobResources(task.JobInfo.(*batchv1.Job), jobtest.RetryTestPolicy(), map[string]bool{previous.Spec.Template.Spec.Containers[0].Name: true})
	require.NoError(t, err)
	next.Annotations[workflowconfig.AnnotationJobAttempt] = "2"
	cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: next, Attempt: 2, PreviousUID: previous.UID,
		RetryAt: time.Now().Add(-time.Second).UnixNano(), Deadline: time.Now().Add(time.Hour).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo, task.Attempt = string(raw), 2
	require.NoError(t, RestoreInstantJobRetryCheckpoint(task))
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 4,
		RunToken: task.RunToken, WorkerID: task.WorkerID}}
	client := fake.NewSimpleClientset(previous, jobtest.RetryTestPod(previous, "OOMKilled"))
	var created []*batchv1.Job
	jobtest.InstallRetryJobReactor(t, client, 0, "", &created)
	require.NoError(t, newObservedInstantJobCtl(t, task, client, store, func() {}).Run(context.Background()))
	require.Len(t, created, 1)
	require.Equal(t, "1", created[0].Annotations[config.AnnotationJobRunGeneration])
	require.Equal(t, uint64(1), store.record.RunGeneration)
	require.Equal(t, uint64(4), task.OwnerRunGeneration)
	require.Positive(t, store.ownershipChecks)
	require.Equal(t, "512Mi", created[0].Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
}

func TestRetryCancellationRecognizesCommittedParentBeforeSignal(t *testing.T) {
	for _, signalReceived := range []bool{false, true} {
		t.Run(strconv.FormatBool(signalReceived), func(t *testing.T) {
			task := retryTestTask(t, jobtest.RetryTestPolicy())
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
			desired := task.JobInfo.(*batchv1.Job)
			desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			live := desired.DeepCopy()
			live.UID = "owned"
			cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
				CurrentUID: live.UID, Deadline: time.Now().Add(time.Hour).UnixNano()}
			raw, err := json.Marshal(cp)
			require.NoError(t, err)
			task.InternalInfo = string(raw)
			store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusCancelled,
				RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
			client := fake.NewSimpleClientset(live)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if signalReceived {
				cancel()
			} else {
				// The polling loop can observe Cancelled before Redis delivers it.
				_, err := newObservedInstantJobCtl(t, task, client, store, func() {}).waitRetryAttempt(ctx, cp)
				require.ErrorIs(t, err, context.Canceled)
				require.NotErrorIs(t, err, signal.ErrInfrastructureStop)
			}
			ctl := newObservedInstantJobCtl(t, task, client, store, func() {})
			ctl.Clean(ctx)
			require.Equal(t, 1, jobtest.CountClientActions(client, "delete", "jobs"))
			require.Equal(t, config.StatusCancelled, task.OwnerStatus)
		})
	}
}

func TestRetryCancellationCleanupRejectsDifferentLease(t *testing.T) {
	for _, changed := range []string{"generation", "token", "worker"} {
		t.Run(changed, func(t *testing.T) {
			task := retryTestTask(t, jobtest.RetryTestPolicy())
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
			owner := model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusCancelled, RunGeneration: 1,
				RunToken: task.RunToken, WorkerID: task.WorkerID}
			switch changed {
			case "generation":
				owner.RunGeneration++
			case "token":
				owner.RunToken = "token-2"
			case "worker":
				owner.WorkerID = "worker-2"
			}
			live := task.JobInfo.(*batchv1.Job).DeepCopy()
			live.UID = "owned"
			client := fake.NewSimpleClientset(live)
			store := &retryOwnedCheckpointStore{owner: owner}
			err := NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: func() {}}).deleteRetryJob(context.Background(), live)
			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.Zero(t, jobtest.CountClientActions(client, "delete", "jobs"))
		})
	}
}

func TestRetryRecoveryAddsRetentionToExistingCheckpointAndJob(t *testing.T) {
	task := retryTestTask(t, jobtest.RetryTestPolicy())
	desired := task.JobInfo.(*batchv1.Job)
	desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	live := desired.DeepCopy()
	live.UID = "old-worker-job"
	live.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	cp := &jobinfo.InstantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
		CurrentUID: live.UID, Deadline: time.Now().Add(time.Hour).UnixNano()}
	client := fake.NewSimpleClientset(live)
	store := &retryCheckpointStore{}
	require.NoError(t, NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: jobtest.WithJobTestOwner(store, task), Ack: func() {}}).ensureRetryAttempt(context.Background(), cp))
	retained, err := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, retained.Spec.TTLSecondsAfterFinished)
	require.GreaterOrEqual(t, *retained.Spec.TTLSecondsAfterFinished, int32(3*time.Hour/time.Second)+config.DefaultJobTTLSeconds)
	require.Equal(t, cp.Job.Spec.TTLSecondsAfterFinished, retained.Spec.TTLSecondsAfterFinished)
	require.Equal(t, cp.CurrentUID, retained.UID)
	require.Zero(t, jobtest.CountClientActions(client, "create", "jobs"))
}
