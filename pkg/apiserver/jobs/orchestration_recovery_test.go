package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func newRecoverableEvaluation(t *testing.T) (*runnerFixture, *model.JobTask, *model.JobCheckpoint) {
	t.Helper()
	f := newRunnerFixture(t)
	claimRunner(t, f)
	ctx := context.Background()
	info, err := decodeEvaluationInfo(f.record.EvaluationInfo)
	require.NoError(t, err)
	info.Traits.Evaluation.Agent, info.Traits.Evaluation.Model = "codex", "openai/model"
	info.Traits.Evaluation.Recovery = &spec.EvaluationRecoverySpec{AgentVersion: spec.CodexRecoveryVersion, ReplaySafe: true}
	raw, err := json.Marshal(info)
	require.NoError(t, err)
	f.record.EvaluationInfo, f.record.Status = string(raw), string(config.StatusFailed)
	require.NoError(t, f.raw.Put(ctx, f.record))
	f.pod.Status.Phase = corev1.PodFailed
	for _, container := range f.pod.Spec.Containers {
		f.pod.Status.ContainerStatuses = append(f.pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: container.Name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.Now(), ExitCode: 1, Reason: "Error"}}})
	}
	_, err = f.service.Kube.CoreV1().Pods(f.pod.Namespace).UpdateStatus(ctx, f.pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.service.SandboxClient = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	_, deadline, err := decodeRunnerState(f.record)
	require.NoError(t, err)
	point := &model.JobCheckpoint{ID: "complete-recovery-point", WorkspaceID: "space", TaskID: f.parent.TaskID, ExecutionKey: *f.record.ExecutionKey, State: "ready", SourceDeadline: time.Unix(0, deadline), ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, f.raw.Add(ctx, point))
	task := &model.JobTask{Name: f.workload.Name, Namespace: f.workload.Namespace, WorkspaceID: "space", TaskID: f.parent.TaskID, JobType: string(config.JobEval), Status: config.StatusFailed,
		ExecutionKey: *f.record.ExecutionKey, RunGeneration: f.record.RunGeneration, OwnerRunGeneration: f.parent.RunGeneration, RunToken: f.parent.RunToken, WorkerID: f.parent.WorkerID,
		EvaluationInfo: f.record.EvaluationInfo, InternalInfo: f.record.InternalInfo, JobInfo: f.workload}
	return f, task, point
}

func TestEvaluationRecoveryReservesNewIdentityAndOriginalDeadline(t *testing.T) {
	f, task, point := newRecoverableEvaluation(t)
	ctx := context.Background()
	oldKey, oldInfo := task.ExecutionKey, task.EvaluationInfo
	again, err := f.service.RecoverEvaluation(ctx, task)
	require.NoError(t, err)
	require.True(t, again)
	require.NotEqual(t, oldKey, task.ExecutionKey)
	require.Equal(t, config.StatusQueued, task.Status)
	require.Empty(t, task.InternalInfo)
	info, err := decodeEvaluationInfo(task.EvaluationInfo)
	require.NoError(t, err)
	require.True(t, info.RecoveryIsolated)
	require.Equal(t, oldKey, info.RootExecutionKey)
	require.Equal(t, oldKey, info.RecoveryOfExecutionKey)
	require.Equal(t, point.SourceDeadline.UnixNano(), info.ExecutionDeadline)
	require.Equal(t, point.ID, info.ResumeCheckpointID)
	require.NoError(t, f.raw.Get(ctx, f.record))
	require.NotEqual(t, oldInfo, f.record.EvaluationInfo, "reservation must revoke the source Runner token in the same transaction")
	require.NoError(t, f.raw.Get(ctx, point))
	require.Equal(t, task.ExecutionKey, point.ReferencedByExecutionKey)
	workload := task.JobInfo.(*batchv1.Job)
	require.NotEqual(t, f.workload.Name, workload.Name)
	require.Equal(t, task.Name, workload.Name)
	require.Equal(t, info.ExecutionDeadline, mustParseRecoveryDeadline(t, workload.Annotations[workflowjob.EvaluationDeadlineAnnotation]))
	require.LessOrEqual(t, *workload.Spec.ActiveDeadlineSeconds, int64(time.Until(point.SourceDeadline).Seconds())+1)
	_, err = f.service.RunnerEvent(ctx, f.identity, RunnerEvent{ProtocolVersion: RunnerProtocolVersion, Sequence: 2, Kind: "heartbeat"})
	require.Error(t, err, "old source token must not publish after successor reservation")
	count, err := f.raw.Count(ctx, &model.JobInfo{TaskID: task.TaskID}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func mustParseRecoveryDeadline(t *testing.T, value string) int64 {
	t.Helper()
	var deadline int64
	require.NoError(t, json.Unmarshal([]byte(value), &deadline))
	return deadline
}

func TestEvaluationRecoveryDispatchesSuccessorThroughAdmission(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		name := "after source failure"
		if takeover {
			name = "after worker takeover"
		}
		t.Run(name, func(t *testing.T) {
			f, task, _ := newRecoverableEvaluation(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			leaseUntil := time.Now().Add(time.Hour)
			f.parent.LeaseExpiresAt = &leaseUntil
			if takeover {
				again, err := f.service.RecoverEvaluation(ctx, task)
				require.NoError(t, err)
				require.True(t, again)
				reserved, err := evaluationRecord(ctx, f.service.Store, task, task.ExecutionKey)
				require.NoError(t, err)
				task.Status, task.EvaluationInfo = config.Status(reserved.Status), reserved.EvaluationInfo
				f.parent.RunGeneration++
				f.parent.RunToken, f.parent.WorkerID = "takeover-token", "takeover-worker"
				task.OwnerRunGeneration, task.RunToken, task.WorkerID = f.parent.RunGeneration, f.parent.RunToken, f.parent.WorkerID
			}
			require.NoError(t, f.raw.Put(ctx, f.parent))
			client := f.service.Kube.(*fake.Clientset)
			created := make(chan *batchv1.Job, 1)
			client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
				workload := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				workload.UID, workload.ResourceVersion = "recovery-job", "1"
				workload.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
				if err := client.Tracker().Add(workload); err != nil {
					return true, nil, err
				}
				created <- workload.DeepCopy()
				return true, workload, nil
			})
			observer := informer.NewKubernetesWorkloadObserver(client)
			require.NoError(t, observer.Start(ctx))
			ctx = workflowjob.WithEvaluationRecovery(ctx, f.service.RecoverEvaluation)
			finished := make(chan error, 1)
			go func() {
				finished <- workflowjob.RunJobs(ctx, []*model.JobTask{task}, 1, client, nil, f.service.Store, func() {}, true, nil, nil, nil, observer, nil)
			}()
			require.Eventually(t, func() bool {
				var count int64
				return f.raw.Client.Model(&model.JobInfo{}).Where("task_id = ? AND scheduling_state = ?", f.parent.TaskID, "queued").Count(&count).Error == nil && count == 1
			}, time.Second, 10*time.Millisecond, "the successor must enter admission")
			require.Empty(t, created, "the successor cannot launch before admission")
			count, err := repository.AdmitQueuedJobs(ctx, f.service.Store)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("recovery execution did not finish")
			}
			select {
			case workload := <-created:
				require.NotEqual(t, f.workload.Name, workload.Name)
				require.Equal(t, task.ExecutionKey, workload.Annotations[config.AnnotationJobExecutionKey])
			default:
				t.Fatal("recovery did not launch its Runner")
			}
			require.Equal(t, config.StatusFailed, task.Status, "the simulated Runner failure must reach terminal persistence")
			saved, err := evaluationRecord(ctx, f.service.Store, task, task.ExecutionKey)
			require.NoError(t, err)
			require.Equal(t, string(config.StatusFailed), saved.Status)
			require.Equal(t, task.Name, saved.ServiceName)
			require.Equal(t, "released", saved.SchedulingState)
			require.NotEmpty(t, saved.InternalInfo, "the started successor must retain its execution checkpoint")
		})
	}
}

func TestEvaluationRecoveryReservationPreservesAcceptedTimeoutPolicy(t *testing.T) {
	f, task, point := newRecoverableEvaluation(t)
	ctx := context.Background()
	info, err := decodeEvaluationInfo(task.EvaluationInfo)
	require.NoError(t, err)
	info.Traits.Evaluation.TimeoutSeconds = 7200
	encoded, err := json.Marshal(info)
	require.NoError(t, err)
	task.EvaluationInfo, f.record.EvaluationInfo = string(encoded), string(encoded)
	require.NoError(t, f.raw.Put(ctx, f.record))
	policy := workflowconfig.DefaultJobSchedulerPolicy()
	policy.MaxEvaluationTimeoutSeconds = 3600
	encoded, err = json.Marshal(policy)
	require.NoError(t, err)
	require.NoError(t, f.raw.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded}))
	again, err := f.service.RecoverEvaluation(ctx, task)
	require.NoError(t, err, "a queued recovery retains the accepted evaluation policy")
	require.True(t, again)
	require.Equal(t, config.StatusQueued, task.Status)
	require.LessOrEqual(t, task.Timeout, int64(time.Until(point.SourceDeadline).Seconds())+1)
}

func TestEvaluationRecoveryPreservesApplicationComponentIdentity(t *testing.T) {
	f, task, _ := newRecoverableEvaluation(t)
	ctx := context.Background()
	task.AppID, f.parent.AppID, f.record.AppID = "app", "app", "app"
	f.record.ServiceName = "benchmark"
	f.workload.Annotations[config.AnnotationComponentName] = "benchmark"
	require.NoError(t, f.raw.Put(ctx, f.parent))
	require.NoError(t, f.raw.Put(ctx, f.record))
	again, err := f.service.RecoverEvaluation(ctx, task)
	require.NoError(t, err)
	require.True(t, again)
	reserved, err := evaluationRecord(ctx, f.service.Store, task, task.ExecutionKey)
	require.NoError(t, err)
	require.Equal(t, "benchmark", reserved.ServiceName)
	require.Equal(t, "benchmark", task.JobInfo.(*batchv1.Job).Annotations[config.AnnotationComponentName])
}

func TestEvaluationRecoveryReservationSurvivesUnconfirmedSource(t *testing.T) {
	for _, condition := range []string{"running", "missing", "different UID", "node lost", "missing termination", "unknown termination", "missing finish time", "unterminated init", "unterminated ephemeral"} {
		t.Run(condition, func(t *testing.T) {
			f, task, point := newRecoverableEvaluation(t)
			ctx := context.Background()
			client := f.service.Kube.(*fake.Clientset)
			switch condition {
			case "running":
				f.pod.Status.Phase = corev1.PodRunning
				_, err := client.CoreV1().Pods(f.pod.Namespace).UpdateStatus(ctx, f.pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "missing":
				require.NoError(t, client.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
			case "different UID":
				f.pod.UID = "replacement-pod"
				_, err := client.CoreV1().Pods(f.pod.Namespace).Update(ctx, f.pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			default:
				switch condition {
				case "node lost":
					f.pod.Status.Reason = "NodeLost"
				case "missing termination":
					f.pod.Status.ContainerStatuses = nil
				case "unknown termination":
					f.pod.Status.ContainerStatuses[0].State.Terminated.Reason = "ContainerStatusUnknown"
				case "missing finish time":
					f.pod.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.Time{}
				case "unterminated init":
					f.pod.Spec.InitContainers = []corev1.Container{{Name: "unconfirmed-init"}}
				case "unterminated ephemeral":
					f.pod.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "unconfirmed-ephemeral"}}}
				}
				_, err := client.CoreV1().Pods(f.pod.Namespace).Update(ctx, f.pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			again, err := f.service.RecoverEvaluation(ctx, task)
			require.ErrorContains(t, err, "isolate source Runner")
			require.False(t, again)
			require.Equal(t, config.StatusQueued, task.Status)
			require.NoError(t, f.raw.Get(ctx, point))
			require.Equal(t, task.ExecutionKey, point.ReferencedByExecutionKey)
			reserved, err := evaluationRecord(ctx, f.service.Store, task, task.ExecutionKey)
			require.NoError(t, err)
			info, err := decodeEvaluationInfo(reserved.EvaluationInfo)
			require.NoError(t, err)
			require.False(t, info.RecoveryIsolated)
			for _, action := range client.Actions() {
				require.NotEqual(t, "create", action.GetVerb(), "unconfirmed source must not create a Runner")
			}
		})
	}
}

func TestEvaluationRecoveryClosesTerminalSandboxBeforeClone(t *testing.T) {
	f, task, _ := newRecoverableEvaluation(t)
	ctx := context.Background()
	row := &model.JobSandbox{ID: sandboxID(task.WorkspaceID, task.ExecutionKey, "trial-1"), WorkspaceID: task.WorkspaceID, TaskID: task.TaskID,
		JobID: f.record.ID, ExecutionKey: task.ExecutionKey, TrialID: "trial-1", Namespace: task.Namespace, State: sandboxReady,
		SandboxName: "source-sandbox", SandboxUID: "source-sandbox-uid", PodName: "source-trial", PodUID: "source-trial-uid",
		RunnerUID: string(f.pod.UID), RunnerPodName: f.pod.Name, SlotReserved: true, Deadline: time.Now().Add(time.Hour)}
	retention := time.Now().Add(sandboxRetention)
	leaseUntil := time.Now().Add(sandboxLeaseDuration)
	row.LeaseToken, row.LeaseUntil, row.RetainUntil, row.ReleaseRequested = "previous-maintenance-lease", &leaseUntil, &retention, true
	staleMaintenance := *row
	require.NoError(t, f.raw.Add(ctx, row))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: row.PodName, Namespace: row.Namespace, UID: "source-trial-uid"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: "main",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.Now(), ExitCode: 1, Reason: "Error"}}}}}}
	require.NoError(t, f.service.Kube.(*fake.Clientset).Tracker().Add(pod))
	sandbox := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.kruise.io/v1alpha1", "kind": "Sandbox", "spec": map[string]any{"shutdownTime": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)},
	}}
	sandbox.SetName(row.SandboxName)
	sandbox.SetNamespace(row.Namespace)
	sandbox.SetUID("source-sandbox-uid")
	sandbox.SetLabels(sandboxLabels(row))
	sandbox.SetAnnotations(map[string]string{sandboxDigestAnnotation: row.RequestDigest, sandboxRunnerAnnotation: row.RunnerUID})
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	require.NoError(t, dynamicClient.Tracker().Create(SandboxGVR, sandbox, row.Namespace))
	f.service.SandboxClient = dynamicClient
	again, err := f.service.RecoverEvaluation(ctx, task)
	require.NoError(t, err)
	require.True(t, again)
	closed, err := dynamicClient.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	shutdown, _, err := unstructured.NestedString(closed.Object, "spec", "shutdownTime")
	require.NoError(t, err)
	stopAt, err := time.Parse(time.RFC3339, shutdown)
	require.NoError(t, err)
	require.False(t, stopAt.After(time.Now()), "a terminal Pod does not close its Sandbox controller; shutdownTime must also stop it")
	// A cleanup worker may already hold a copy of the old retention lease when
	// the recovery transaction revokes it. Its cloud write must also be fenced.
	_ = f.service.cleanupSandbox(ctx, nil, &staleMaintenance)
	closed, err = dynamicClient.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	require.NoError(t, err)
	shutdown, _, err = unstructured.NestedString(closed.Object, "spec", "shutdownTime")
	require.NoError(t, err)
	staleStop, err := time.Parse(time.RFC3339, shutdown)
	require.NoError(t, err)
	require.False(t, staleStop.After(stopAt), "a revoked maintenance lease must not extend cloud shutdownTime")
	require.NoError(t, f.service.maintainSandbox(ctx, row))
	closed, err = dynamicClient.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, row.SandboxName, metav1.GetOptions{})
	if err != nil {
		require.True(t, k8serrors.IsNotFound(err), "only completed cleanup may remove the isolated source")
	}
	if err == nil {
		shutdown, _, err = unstructured.NestedString(closed.Object, "spec", "shutdownTime")
		require.NoError(t, err)
		maintainedStop, err := time.Parse(time.RFC3339, shutdown)
		require.NoError(t, err)
		require.False(t, maintainedStop.After(stopAt), "maintenance must not reopen or extend an isolated source")
	}
}

func TestEvaluationRecoveryDoesNotReusePreviousRunnerTerminationProof(t *testing.T) {
	f, task, point := newRecoverableEvaluation(t)
	ctx := context.Background()
	info, err := decodeEvaluationInfo(task.EvaluationInfo)
	require.NoError(t, err)
	info.RootExecutionKey, info.RecoveryIndex, info.ResumeCheckpointID = "root-before-first-recovery", 1, point.ID
	info.RecoveryIsolated, info.RecoveryRunnerStopped = true, true
	encoded, err := json.Marshal(info)
	require.NoError(t, err)
	task.EvaluationInfo, f.record.EvaluationInfo = string(encoded), string(encoded)
	require.NoError(t, f.raw.Put(ctx, f.record))
	f.pod.Status.Phase = corev1.PodRunning
	_, err = f.service.Kube.CoreV1().Pods(f.pod.Namespace).UpdateStatus(ctx, f.pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	again, err := f.service.RecoverEvaluation(ctx, task)
	require.ErrorContains(t, err, "isolate source Runner")
	require.False(t, again)
	reserved, err := evaluationRecord(ctx, f.service.Store, task, task.ExecutionKey)
	require.NoError(t, err)
	current, err := decodeEvaluationInfo(reserved.EvaluationInfo)
	require.NoError(t, err)
	require.Equal(t, 2, current.RecoveryIndex)
	require.False(t, current.RecoveryRunnerStopped)
	require.False(t, current.RecoveryIsolated)
}

func TestEvaluationRecoveryTakeoverUsesDurableSourceTerminationProof(t *testing.T) {
	f, task, _ := newRecoverableEvaluation(t)
	ctx := context.Background()
	again, err := f.service.RecoverEvaluation(ctx, task)
	require.NoError(t, err)
	require.True(t, again)
	reserved, err := evaluationRecord(ctx, f.service.Store, task, task.ExecutionKey)
	require.NoError(t, err)
	info, err := decodeEvaluationInfo(reserved.EvaluationInfo)
	require.NoError(t, err)
	require.True(t, info.RecoveryRunnerStopped)
	// Reproduce a crash after the durable Runner stop proof but before the
	// all-members-isolated commit, followed by Kubernetes garbage collection.
	info.RecoveryIsolated = false
	encoded, err := json.Marshal(info)
	require.NoError(t, err)
	reserved.EvaluationInfo, task.EvaluationInfo = string(encoded), string(encoded)
	require.NoError(t, f.raw.Put(ctx, reserved))
	require.NoError(t, f.service.Kube.CoreV1().Pods(f.pod.Namespace).Delete(ctx, f.pod.Name, metav1.DeleteOptions{}))
	again, err = f.service.RecoverEvaluation(ctx, task)
	require.NoError(t, err)
	require.False(t, again, "takeover continues the reserved execution instead of creating another identity")
	current, err := decodeEvaluationInfo(task.EvaluationInfo)
	require.NoError(t, err)
	require.True(t, current.RecoveryIsolated)
	count, err := f.raw.Count(ctx, &model.JobInfo{TaskID: task.TaskID}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestEvaluationRecoveryNeverReplaysCancelledOrExpiredWork(t *testing.T) {
	for _, condition := range []string{"cancelled", "expired", "incomplete point", "no replay consent"} {
		t.Run(condition, func(t *testing.T) {
			f, task, point := newRecoverableEvaluation(t)
			ctx := context.Background()
			switch condition {
			case "cancelled":
				f.parent.Status = config.StatusCancelled
				require.NoError(t, f.raw.Put(ctx, f.parent))
			case "expired":
				point.ExpiresAt = time.Now().Add(-time.Second)
				require.NoError(t, f.raw.Put(ctx, point))
			case "incomplete point":
				point.State = "pending"
				require.NoError(t, f.raw.Put(ctx, point))
			case "no replay consent":
				info, err := decodeEvaluationInfo(task.EvaluationInfo)
				require.NoError(t, err)
				info.Traits.Evaluation.Recovery = nil
				raw, err := json.Marshal(info)
				require.NoError(t, err)
				task.EvaluationInfo = string(raw)
			}
			original := task.ExecutionKey
			again, err := f.service.RecoverEvaluation(ctx, task)
			if condition == "cancelled" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.False(t, again)
			require.Equal(t, original, task.ExecutionKey)
			count, err := f.raw.Count(ctx, &model.JobInfo{TaskID: task.TaskID}, nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
		})
	}
}

func TestEvaluationExecutionRankRejectsUnrelatedLineage(t *testing.T) {
	f, task, _ := newRecoverableEvaluation(t)
	root := task.ExecutionKey
	rank, ok := EvaluationExecutionRank(f.record, root)
	require.True(t, ok)
	require.Zero(t, rank)
	info, err := decodeEvaluationInfo(task.EvaluationInfo)
	require.NoError(t, err)
	info.RootExecutionKey, info.RecoveryIndex, info.ResumeCheckpointID = root, 2, "point"
	raw, err := json.Marshal(info)
	require.NoError(t, err)
	successor := &model.JobInfo{Type: string(config.JobEval), ExecutionKey: ptr.To(recoveryExecutionKey(root, 2)), EvaluationInfo: string(raw)}
	rank, ok = EvaluationExecutionRank(successor, root)
	require.True(t, ok)
	require.Equal(t, 2, rank)
	_, ok = EvaluationExecutionRank(successor, "other-root")
	require.False(t, ok)
	successor.ExecutionKey = ptr.To("forged-lineage-key")
	_, ok = EvaluationExecutionRank(successor, root)
	require.False(t, ok)
}
