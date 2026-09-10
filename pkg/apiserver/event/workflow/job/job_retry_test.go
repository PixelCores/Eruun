package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type retryCheckpointStore struct {
	noopStore
	record    *model.JobInfo
	afterSave func(*model.JobInfo) error
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

func retryTestPolicy() *workflowconfig.JobRetryPolicy {
	return &workflowconfig.JobRetryPolicy{OnOOM: "resize", MaxRetries: 1, BackoffSeconds: 1, MemoryGrowthFactor: 2,
		MaxResources: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}
}

func retryTestTask(t *testing.T, policy *workflowconfig.JobRetryPolicy) *model.JobTask {
	t.Helper()
	component := &model.ApplicationComponent{Name: "retry-job", Namespace: "ns", Image: "busybox:1.37"}
	desired := buildJob(component, &model.Properties{JobRetryPolicy: policy}, jobBuildOptions{})
	desired.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi"), corev1.ResourceCPU: resource.MustParse("100m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi"), corev1.ResourceCPU: resource.MustParse("200m")},
	}
	task := &model.JobTask{Name: desired.Name, Namespace: "ns", JobInfo: desired, JobType: string(config.JobDeployInstant),
		TaskID: "task-1", ExecutionKey: "execution-1", RunGeneration: 1, Attempt: 1, Timeout: 10}
	stampJobExecutionIdentity(task, desired)
	return task
}

func retryTestPod(job *batchv1.Job, reason string) *corev1.Pod {
	controller := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-" + string(job.UID), Namespace: job.Namespace,
		Labels:          map[string]string{batchv1.JobNameLabel: job.Name},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: job.Spec.Template.Spec.Containers[0].Name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: reason, ExitCode: 137}}}}}}
}

func installRetryJobReactor(t *testing.T, client *fake.Clientset, failAttempts int, reason string, created *[]*batchv1.Job) {
	t.Helper()
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.UID = types.UID(fmt.Sprintf("job-%d", len(*created)+1))
		job.ResourceVersion = "1"
		*created = append(*created, job.DeepCopy())
		if len(*created) <= failAttempts {
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
			require.NoError(t, client.Tracker().Add(retryTestPod(job, reason)))
		} else {
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		}
		require.NoError(t, client.Tracker().Add(job))
		return true, job, nil
	})
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
		{"resize", retryTestPolicy(), "OOMKilled", 1, 2, "", "512Mi"},
		{"retry same resources", &workflowconfig.JobRetryPolicy{OnOOM: "retry", MaxRetries: 1, BackoffSeconds: 1}, "OOMKilled", 1, 2, "", "256Mi"},
		{"explicit stop", &workflowconfig.JobRetryPolicy{OnOOM: "stop"}, "OOMKilled", 1, 1, "job failed after attempt 1", "256Mi"},
		{"ordinary exit 137 stops", retryTestPolicy(), "Error", 1, 1, "non-OOM", "256Mi"},
		{"bounded retries", retryTestPolicy(), "OOMKilled", 2, 2, "job failed after attempt 2", "512Mi"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := retryTestTask(t, tt.policy)
			store := &retryCheckpointStore{}
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			installRetryJobReactor(t, client, tt.failures, tt.reason, &created)
			ctl := NewInstantJobCtl(task, client, store, func() {})
			err := ctl.Run(WithCleanupTracker(context.Background()))
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
	task := retryTestTask(t, retryTestPolicy())
	client := fake.NewSimpleClientset()
	var created []*batchv1.Job
	installRetryJobReactor(t, client, 1, "OOMKilled", &created)
	ctx, cancel := context.WithCancel(context.Background())
	store := &retryCheckpointStore{afterSave: func(record *model.JobInfo) error {
		if record.Attempt == 2 {
			cancel()
		}
		return nil
	}}
	require.ErrorIs(t, NewInstantJobCtl(task, client, store, func() {}).Run(ctx), context.Canceled)
	require.Len(t, created, 1)
	require.Equal(t, uint(2), store.record.Attempt)
	store.afterSave = nil
	recovered := retryTestTask(t, retryTestPolicy())
	recovered.InternalInfo = store.record.InternalInfo
	recovered.Attempt = store.record.Attempt
	require.NoError(t, RestoreInstantJobRetryCheckpoint(recovered))
	require.NoError(t, NewInstantJobCtl(recovered, client, store, func() {}).Run(context.Background()))
	require.Len(t, created, 2)
	require.Equal(t, "512Mi", created[1].Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
}

func TestInstantJobRetryCheckpointFailurePreventsDeletion(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	client := fake.NewSimpleClientset()
	var created []*batchv1.Job
	installRetryJobReactor(t, client, 1, "OOMKilled", &created)
	store := &retryCheckpointStore{afterSave: func(record *model.JobInfo) error {
		if record.Attempt == 2 {
			return errors.New("database unavailable")
		}
		return nil
	}}
	err := NewInstantJobCtl(task, client, store, func() {}).Run(context.Background())
	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Zero(t, countClientActions(client, "delete", "jobs"))
	require.Len(t, created, 1)
	require.Equal(t, uint(1), store.record.Attempt)
}

func TestRetryRejectsForeignOOMAndPreviousTermination(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
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
			pod := retryTestPod(job, "OOMKilled")
			tt.change(pod)
			containers, err := oomJobContainers(context.Background(), fake.NewSimpleClientset(pod), job)
			require.NoError(t, err)
			require.Empty(t, containers)
		})
	}
}

func TestResizeResourcesRespectCapsAndExplicitCPU(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	container := job.Spec.Template.Spec.Containers[0].Name
	policy := retryTestPolicy()
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
	task := retryTestTask(t, retryTestPolicy())
	job := task.JobInfo.(*batchv1.Job).DeepCopy()
	job.UID = "previous"
	pod := retryTestPod(job, "OOMKilled")
	pod.Status.Phase = corev1.PodRunning
	client := fake.NewSimpleClientset(pod)
	cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: task.JobInfo.(*batchv1.Job), Attempt: 2,
		PreviousUID: job.UID, RetryAt: time.Now().Add(-time.Second).UnixNano(), Deadline: time.Now().Add(time.Hour).UnixNano()}
	cp.Job.Annotations[workflowconfig.AnnotationJobAttempt] = "2"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {}).ensureRetryAttempt(ctx, cp)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, countClientActions(client, "create", "jobs"))
}

func TestRetryCheckpointCannotReplayMissingOrForeignJob(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(strconv.FormatBool(exists), func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			job := task.JobInfo.(*batchv1.Job)
			job.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			cp := &instantJobRetryCheckpoint{Job: job, Attempt: 1, CurrentUID: "expected"}
			client := fake.NewSimpleClientset()
			if exists {
				foreign := job.DeepCopy()
				foreign.UID = "replacement"
				require.NoError(t, client.Tracker().Add(foreign))
			}
			err := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {}).ensureRetryAttempt(context.Background(), cp)
			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.Zero(t, countClientActions(client, "delete", "jobs"))
			require.Zero(t, countClientActions(client, "create", "jobs"))
		})
	}
}

func TestRestoreRetryCheckpointRejectsMalformedIdentity(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	job.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: job, Attempt: 1, Deadline: time.Now().Add(time.Hour).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo = string(raw)
	require.NoError(t, RestoreInstantJobRetryCheckpoint(task))
	task.ExecutionKey = "wrong-execution"
	require.Error(t, RestoreInstantJobRetryCheckpoint(task))
	require.True(t, HasInstantJobRetryCheckpoint(&model.JobInfo{Type: task.JobType, InternalInfo: "invalid json"}))
}

func TestRetryRecoveryHonorsExpiredDeadline(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	job.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: job, Attempt: 1, Deadline: time.Now().Add(-time.Second).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo = string(raw)
	client := fake.NewSimpleClientset()
	err = NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {}).Run(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, config.StatusTimeout, task.Status)
	require.Empty(t, client.Actions())
}

func TestRetryDeletionRechecksWorkflowFence(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	task.OwnerRunGeneration = 1
	task.RunToken = "token-1"
	task.WorkerID = "worker-1"
	current := model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}
	replacement := current
	replacement.RunGeneration = 2
	store := &workflowOwnershipStore{noopStore: &noopStore{}, tasks: []model.WorkflowQueue{current, replacement}}
	previous := task.JobInfo.(*batchv1.Job).DeepCopy()
	previous.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	previous.UID = "old-job"
	previous.ResourceVersion = "9"
	previous.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	next := task.JobInfo.(*batchv1.Job).DeepCopy()
	next.Annotations[workflowconfig.AnnotationJobAttempt] = "2"
	cp := &instantJobRetryCheckpoint{Job: next, Attempt: 2, PreviousUID: previous.UID}
	client := fake.NewSimpleClientset(previous)
	err := NewInstantJobCtl(task, client, store, func() {}).ensureRetryAttempt(context.Background(), cp)
	require.ErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Zero(t, countClientActions(client, "delete", "jobs"))
	require.Zero(t, countClientActions(client, "create", "jobs"))
}

func TestRetryDeletionUsesUIDAndResourceVersion(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	live := task.JobInfo.(*batchv1.Job).DeepCopy()
	live.UID = "old-job"
	live.ResourceVersion = "9"
	client := fake.NewSimpleClientset(live)
	require.NoError(t, NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {}).deleteRetryJob(context.Background(), live))
	options := client.Actions()[0].(k8stesting.DeleteAction).GetDeleteOptions()
	require.Equal(t, live.UID, *options.Preconditions.UID)
	require.Equal(t, live.ResourceVersion, *options.Preconditions.ResourceVersion)
	require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
}

func TestResizeOnlyOOMContainerAndRejectsInjectedContainer(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	job := task.JobInfo.(*batchv1.Job)
	init := job.Spec.Template.Spec.Containers[0].DeepCopy()
	init.Name = "init"
	job.Spec.Template.Spec.InitContainers = []corev1.Container{*init}
	next, err := growOOMJobResources(job, retryTestPolicy(), map[string]bool{"init": true})
	require.NoError(t, err)
	require.Equal(t, "512Mi", next.Spec.Template.Spec.InitContainers[0].Resources.Limits.Memory().String())
	require.Equal(t, "256Mi", next.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
	_, err = growOOMJobResources(job, retryTestPolicy(), map[string]bool{"injected": true})
	require.ErrorContains(t, err, "absent from the submitted Job template")
}

func TestRetryCleanupRecoveredTimeoutUsesCheckpointOwnership(t *testing.T) {
	for _, pendingRetry := range []bool{false, true} {
		t.Run(strconv.FormatBool(pendingRetry), func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
			desired := task.JobInfo.(*batchv1.Job)
			desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			live := desired.DeepCopy()
			live.UID = "created-before-worker-restart"
			cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired,
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
			ctl := NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {})
			require.ErrorIs(t, ctl.Run(context.Background()), context.DeadlineExceeded)
			ctl.Clean(context.Background())
			require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
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
			task := retryTestTask(t, retryTestPolicy())
			desired := task.JobInfo.(*batchv1.Job)
			desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
				CurrentUID: "owned", Deadline: time.Now().Add(time.Hour).UnixNano()}
			raw, err := json.Marshal(cp)
			require.NoError(t, err)
			task.InternalInfo = string(raw)
			live := desired.DeepCopy()
			live.UID = "owned"
			tt.change(live)
			client := fake.NewSimpleClientset(live)
			err = NewInstantJobCtl(task, client, &retryCheckpointStore{}, func() {}).cleanRetryAttempt(context.Background())
			require.ErrorIs(t, err, errJobExecutionIdentityChanged)
			require.Zero(t, countClientActions(client, "delete", "jobs"))
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
	task := retryTestTask(t, retryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 4, "token-4", "worker-4"
	previous := task.JobInfo.(*batchv1.Job).DeepCopy()
	previous.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	previous.UID = "generation-1-attempt-1"
	previous.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	next, err := growOOMJobResources(task.JobInfo.(*batchv1.Job), retryTestPolicy(), map[string]bool{previous.Spec.Template.Spec.Containers[0].Name: true})
	require.NoError(t, err)
	next.Annotations[workflowconfig.AnnotationJobAttempt] = "2"
	cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: next, Attempt: 2, PreviousUID: previous.UID,
		RetryAt: time.Now().Add(-time.Second).UnixNano(), Deadline: time.Now().Add(time.Hour).UnixNano()}
	raw, err := json.Marshal(cp)
	require.NoError(t, err)
	task.InternalInfo, task.Attempt = string(raw), 2
	require.NoError(t, RestoreInstantJobRetryCheckpoint(task))
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning, RunGeneration: 4,
		RunToken: task.RunToken, WorkerID: task.WorkerID}}
	client := fake.NewSimpleClientset(previous, retryTestPod(previous, "OOMKilled"))
	var created []*batchv1.Job
	installRetryJobReactor(t, client, 0, "", &created)
	require.NoError(t, NewInstantJobCtl(task, client, store, func() {}).Run(context.Background()))
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
			task := retryTestTask(t, retryTestPolicy())
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
			desired := task.JobInfo.(*batchv1.Job)
			desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			live := desired.DeepCopy()
			live.UID = "owned"
			cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
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
				_, err := NewInstantJobCtl(task, client, store, func() {}).waitRetryAttempt(ctx, cp)
				require.ErrorIs(t, err, context.Canceled)
				require.NotErrorIs(t, err, signal.ErrInfrastructureStop)
			}
			ctl := NewInstantJobCtl(task, client, store, func() {})
			ctl.Clean(ctx)
			require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
			require.Equal(t, config.StatusCancelled, task.OwnerStatus)
			task.Status = config.StatusCancelled
			require.NoError(t, persistTerminalJobState(context.Background(), ctl, task, store, nil))
			require.Equal(t, string(config.StatusCancelled), store.record.Status)
		})
	}
}

func TestRetryCancellationCleanupRejectsDifferentLease(t *testing.T) {
	for _, changed := range []string{"generation", "token", "worker"} {
		t.Run(changed, func(t *testing.T) {
			task := retryTestTask(t, retryTestPolicy())
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
			err := NewInstantJobCtl(task, client, store, func() {}).deleteRetryJob(context.Background(), live)
			require.ErrorIs(t, err, signal.ErrInfrastructureStop)
			require.Zero(t, countClientActions(client, "delete", "jobs"))
		})
	}
}

func TestRetryCancellationDuringCreatedUIDCheckpoint(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
		RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
	store.afterSave = func(record *model.JobInfo) error {
		var cp instantJobRetryCheckpoint
		require.NoError(t, json.Unmarshal([]byte(record.InternalInfo), &cp))
		if cp.CurrentUID != "" && record.Status == string(config.StatusRunning) {
			store.owner.Status = config.StatusCancelled
			return repository.ErrWorkflowOwnershipLost
		}
		return nil
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		live := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		live.UID = "created-before-cancel"
		require.NoError(t, client.Tracker().Add(live))
		return true, live, nil
	})
	ctl := NewInstantJobCtl(task, client, store, func() {})
	err := ctl.Run(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, signal.ErrInfrastructureStop)
	require.Equal(t, config.StatusCancelled, task.OwnerStatus)
	ctl.Clean(context.Background())
	require.Equal(t, 1, countClientActions(client, "create", "jobs"))
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
	task.Status = config.StatusCancelled
	require.NoError(t, persistTerminalJobState(context.Background(), ctl, task, store, nil))
	require.Equal(t, string(config.StatusCancelled), store.record.Status)
}

func TestRetryCancellationWithoutLiveJobPersistsTerminal(t *testing.T) {
	for _, attempt := range []uint{1, 2} {
		for _, tc := range []struct {
			name               string
			changeOwner        func(*model.WorkflowQueue)
			infrastructureStop bool
			wantOwnerStatus    config.Status
			wantStoredStatus   config.Status
			wantSaveErr        error
		}{
			{name: "cancelled owner", wantOwnerStatus: config.StatusCancelled, wantStoredStatus: config.StatusCancelled},
			{name: "running owner", changeOwner: func(owner *model.WorkflowQueue) { owner.Status = config.StatusRunning },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusCancelled},
			{name: "new generation", changeOwner: func(owner *model.WorkflowQueue) { owner.RunGeneration++ },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning, wantSaveErr: repository.ErrWorkflowOwnershipLost},
			{name: "new token", changeOwner: func(owner *model.WorkflowQueue) { owner.RunToken = "replacement" },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning, wantSaveErr: repository.ErrWorkflowOwnershipLost},
			{name: "new worker", changeOwner: func(owner *model.WorkflowQueue) { owner.WorkerID = "replacement" },
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning, wantSaveErr: repository.ErrWorkflowOwnershipLost},
			{name: "infrastructure stop", infrastructureStop: true,
				wantOwnerStatus: config.StatusRunning, wantStoredStatus: config.StatusRunning},
		} {
			t.Run(fmt.Sprintf("attempt-%d/%s", attempt, tc.name), func(t *testing.T) {
				task := retryTestTask(t, retryTestPolicy())
				task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
				task.OwnerStatus, task.Status, task.Attempt = config.StatusRunning, config.StatusRunning, attempt
				desired := task.JobInfo.(*batchv1.Job)
				desired.Annotations[workflowconfig.AnnotationJobAttempt] = strconv.FormatUint(uint64(attempt), 10)
				cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired,
					Attempt: attempt, Deadline: time.Now().Add(time.Hour).UnixNano()}
				if attempt > 1 {
					cp.PreviousUID, cp.RetryAt = "deleted-attempt-1", time.Now().Add(-time.Second).UnixNano()
				}
				raw, err := json.Marshal(cp)
				require.NoError(t, err)
				task.InternalInfo = string(raw)
				record := buildJobInfoRecord(task)
				store := &retryOwnedCheckpointStore{retryCheckpointStore: retryCheckpointStore{record: &record},
					owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusCancelled,
						RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
				if tc.changeOwner != nil {
					tc.changeOwner(&store.owner)
				}
				client := fake.NewSimpleClientset()
				ctl := NewInstantJobCtl(task, client, store, func() {})
				ctx, cancel := context.WithCancelCause(context.Background())
				if tc.infrastructureStop {
					cancel(signal.ErrInfrastructureStop)
				} else {
					cancel(context.Canceled)
				}
				// The signal arrives between attempts: the checkpoint exists,
				// but no live Job can trigger cleanup's ownership check.
				require.ErrorIs(t, ctl.Run(ctx), context.Canceled)
				if !tc.infrastructureStop {
					ctl.Clean(ctx)
					// runJob sets the terminal cancellation status after cleanup.
					task.Status = config.StatusCancelled
				}
				err = persistTerminalJobState(ctx, ctl, task, store, nil)
				if tc.wantSaveErr != nil {
					require.ErrorIs(t, err, tc.wantSaveErr)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.wantOwnerStatus, task.OwnerStatus)
				require.Equal(t, string(tc.wantStoredStatus), store.record.Status)
				require.Equal(t, attempt, store.record.Attempt)
				require.Zero(t, countClientActions(client, "create", "jobs"))
				require.Zero(t, countClientActions(client, "delete", "jobs"))
			})
		}
	}
}

func TestRetrySuccessKeepsEvidenceUntilResultAndLogsAreSaved(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
	client := fake.NewSimpleClientset()
	var created []*batchv1.Job
	installRetryJobReactor(t, client, 0, "", &created)
	store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
		RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
	ctl := NewInstantJobCtl(task, client, store, func() {})
	require.NoError(t, ctl.Run(context.Background()))
	var cp instantJobRetryCheckpoint
	require.NoError(t, json.Unmarshal([]byte(store.record.InternalInfo), &cp))
	require.Equal(t, created[0].UID, cp.CurrentUID)
	require.Equal(t, string(config.StatusRunning), store.record.Status)
	require.NotNil(t, created[0].Spec.TTLSecondsAfterFinished)
	require.GreaterOrEqual(t, int64(*created[0].Spec.TTLSecondsAfterFinished), task.Timeout+int64(config.DefaultJobTTLSeconds))
	pod := retryTestPod(created[0], "Completed")
	pod.Status.Phase = corev1.PodSucceeded
	pod.Spec.Containers = created[0].Spec.Template.Spec.Containers
	require.NoError(t, client.Tracker().Add(pod))
	finalizeCompletedJobIfNeeded(context.Background(), client, task)
	require.NotEmpty(t, task.Info)
	require.Zero(t, countClientActions(client, "delete", "jobs"))
	store.afterSave = func(*model.JobInfo) error { return errors.New("database unavailable during terminal save") }
	require.ErrorContains(t, persistTerminalJobState(context.Background(), ctl, task, store, nil), "database unavailable")
	require.Equal(t, string(config.StatusRunning), store.record.Status)
	require.Zero(t, countClientActions(client, "delete", "jobs"))
	store.afterSave = nil

	// A new lease resumes the same execution and completed attempt without a
	// create, resource increase, fresh deadline, or fresh retry budget.
	recovered := retryTestTask(t, retryTestPolicy())
	recovered.ExecutionKey, recovered.RunGeneration = *store.record.ExecutionKey, store.record.RunGeneration
	recovered.OwnerRunGeneration, recovered.RunToken, recovered.WorkerID = 2, "token-2", "worker-2"
	store.owner.RunGeneration, store.owner.RunToken, store.owner.WorkerID = 2, recovered.RunToken, recovered.WorkerID
	recovered.InternalInfo, recovered.Attempt = store.record.InternalInfo, store.record.Attempt
	require.NoError(t, RestoreInstantJobRetryCheckpoint(recovered))
	recoveredCtl := NewInstantJobCtl(recovered, client, store, func() {})
	require.NoError(t, recoveredCtl.Run(context.Background()))
	finalizeCompletedJobIfNeeded(context.Background(), client, recovered)
	require.Zero(t, countClientActions(client, "delete", "jobs"))
	require.Equal(t, task.Info, recovered.Info)
	require.NoError(t, persistTerminalJobState(context.Background(), recoveredCtl, recovered, store, nil))
	require.Equal(t, string(config.StatusCompleted), store.record.Status)
	require.Equal(t, recovered.Info, store.record.Info)
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
	require.Len(t, created, 1)
	var restored instantJobRetryCheckpoint
	require.NoError(t, json.Unmarshal([]byte(store.record.InternalInfo), &restored))
	require.Equal(t, cp.Deadline, restored.Deadline)
	require.Equal(t, cp.Attempt, restored.Attempt)
	require.Equal(t, cp.CurrentUID, restored.CurrentUID)
}

func TestRetryFailureKeepsEvidenceUntilResultIsSaved(t *testing.T) {
	for _, interruption := range []string{"none", "terminal save failure", "process exit before save"} {
		t.Run(interruption, func(t *testing.T) {
			policy := &workflowconfig.JobRetryPolicy{OnOOM: "stop"}
			task := retryTestTask(t, policy)
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			installRetryJobReactor(t, client, 1, "OOMKilled", &created)
			store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
				RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
			ctl := NewInstantJobCtl(task, client, store, func() {})
			require.ErrorContains(t, ctl.Run(context.Background()), "job failed after attempt 1")
			require.Equal(t, config.StatusFailed, task.Status)
			originalCheckpoint := store.record.InternalInfo
			// runJob invokes Clean before its deferred terminal persistence.
			ctl.Clean(context.Background())
			require.Zero(t, countClientActions(client, "delete", "jobs"))
			require.Equal(t, string(config.StatusRunning), store.record.Status)

			if interruption == "terminal save failure" {
				store.afterSave = func(*model.JobInfo) error { return errors.New("database unavailable during terminal save") }
				require.ErrorContains(t, persistTerminalJobState(context.Background(), ctl, task, store, nil), "database unavailable")
				require.Equal(t, string(config.StatusRunning), store.record.Status)
				require.Zero(t, countClientActions(client, "delete", "jobs"))
				store.afterSave = nil
			}
			if interruption != "none" {
				recovered := retryTestTask(t, policy)
				recovered.ExecutionKey, recovered.RunGeneration = *store.record.ExecutionKey, store.record.RunGeneration
				recovered.OwnerRunGeneration, recovered.RunToken, recovered.WorkerID = 2, "token-2", "worker-2"
				store.owner.RunGeneration, store.owner.RunToken, store.owner.WorkerID = 2, recovered.RunToken, recovered.WorkerID
				recovered.InternalInfo, recovered.Attempt = store.record.InternalInfo, store.record.Attempt
				require.NoError(t, RestoreInstantJobRetryCheckpoint(recovered))
				ctl = NewInstantJobCtl(recovered, client, store, func() {})
				err := ctl.Run(context.Background())
				require.ErrorContains(t, err, "job failed after attempt 1")
				require.NotErrorIs(t, err, signal.ErrInfrastructureStop)
				require.Equal(t, task.Error, recovered.Error)
				ctl.Clean(context.Background())
				task = recovered
			}
			require.Equal(t, originalCheckpoint, task.InternalInfo, "recovery must keep its attempt, UID, resources and deadline")
			require.Zero(t, countClientActions(client, "delete", "jobs"))
			require.NoError(t, persistTerminalJobState(context.Background(), ctl, task, store, nil))
			require.Equal(t, string(config.StatusFailed), store.record.Status)
			require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
			require.Len(t, created, 1, "terminal recovery must not execute the workload again")
		})
	}
}

func TestRetryFailedTerminalSaveRejectsDifferentLease(t *testing.T) {
	for _, changed := range []string{"generation", "token", "worker"} {
		t.Run(changed, func(t *testing.T) {
			task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
			task.OwnerRunGeneration, task.RunToken, task.WorkerID = 1, "token-1", "worker-1"
			client := fake.NewSimpleClientset()
			var created []*batchv1.Job
			installRetryJobReactor(t, client, 1, "OOMKilled", &created)
			store := &retryOwnedCheckpointStore{owner: model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusRunning,
				RunGeneration: 1, RunToken: task.RunToken, WorkerID: task.WorkerID}}
			ctl := NewInstantJobCtl(task, client, store, func() {})
			require.ErrorContains(t, ctl.Run(context.Background()), "job failed after attempt 1")
			switch changed {
			case "generation":
				store.owner.RunGeneration++
			case "token":
				store.owner.RunToken = "token-2"
			case "worker":
				store.owner.WorkerID = "worker-2"
			}
			ctl.Clean(context.Background())
			require.ErrorIs(t, persistTerminalJobState(context.Background(), ctl, task, store, nil), repository.ErrWorkflowOwnershipLost)
			require.Equal(t, string(config.StatusRunning), store.record.Status)
			require.Zero(t, countClientActions(client, "delete", "jobs"))
		})
	}
}

func TestRetryRecoveryAddsRetentionToExistingCheckpointAndJob(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	desired := task.JobInfo.(*batchv1.Job)
	desired.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	live := desired.DeepCopy()
	live.UID = "old-worker-job"
	live.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	cp := &instantJobRetryCheckpoint{Kind: "instant_job_retry", Version: 1, Job: desired, Attempt: 1,
		CurrentUID: live.UID, Deadline: time.Now().Add(time.Hour).UnixNano()}
	client := fake.NewSimpleClientset(live)
	store := &retryCheckpointStore{}
	require.NoError(t, NewInstantJobCtl(task, client, store, func() {}).ensureRetryAttempt(context.Background(), cp))
	retained, err := client.BatchV1().Jobs(live.Namespace).Get(context.Background(), live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, retained.Spec.TTLSecondsAfterFinished)
	require.GreaterOrEqual(t, *retained.Spec.TTLSecondsAfterFinished, int32(3*time.Hour/time.Second)+config.DefaultJobTTLSeconds)
	require.Equal(t, cp.Job.Spec.TTLSecondsAfterFinished, retained.Spec.TTLSecondsAfterFinished)
	require.Equal(t, cp.CurrentUID, retained.UID)
	require.Zero(t, countClientActions(client, "create", "jobs"))
}
