package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestFinalizeLogContent_NoTruncate(t *testing.T) {
	content := []byte("line-1\nline-2\n")
	got, truncated := finalizeLogContent(content, int64(len(content)+10))
	if truncated {
		t.Fatalf("expected not truncated")
	}
	if got != string(content) {
		t.Fatalf("expected %q, got %q", string(content), got)
	}
}

func TestFinalizeLogContent_TruncateAddsMarker(t *testing.T) {
	content := []byte("line-1\nline-2\n")
	got, truncated := finalizeLogContent(content, int64(len(content)))
	if !truncated {
		t.Fatalf("expected truncated")
	}
	want := "line-1\nline-2\n" + logTruncatedMarker
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestDeleteCompletedPodsForJobDeletesOnlyOwnedSucceededPods(t *testing.T) {
	ctx := context.Background()
	jobObj := jobForPodFallback("cleanup-job", nil)
	ownedSucceeded := succeededPodForJob(jobObj, "cleanup-job-owned", jobObj.UID)
	mismatchedOwner := succeededPodForJob(jobObj, "cleanup-job-mismatch", types.UID("other-job-uid"))
	running := podForJobWithPhase(jobObj, "cleanup-job-running", jobObj.UID, corev1.PodRunning)
	failed := podForJobWithPhase(jobObj, "cleanup-job-failed", jobObj.UID, corev1.PodFailed)
	client := fake.NewSimpleClientset(jobObj, ownedSucceeded, mismatchedOwner, running, failed)

	deleted, err := deleteCompletedPodsForJob(ctx, client, jobObj.Namespace, jobObj)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	_, err = client.CoreV1().Pods(jobObj.Namespace).Get(ctx, ownedSucceeded.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))

	for _, pod := range []*corev1.Pod{mismatchedOwner, running, failed} {
		_, err = client.CoreV1().Pods(jobObj.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		require.NoError(t, err)
	}
}

func TestDeleteCompletedJobAndPodsDeletesJobBeforePodsWithUIDPrecondition(t *testing.T) {
	ctx := context.Background()
	jobObj := jobForPodFallback("ordered-cleanup-job", nil)
	ownedSucceeded := succeededPodForJob(jobObj, "ordered-cleanup-job-owned", jobObj.UID)
	client := fake.NewSimpleClientset(jobObj, ownedSucceeded)

	err := deleteCompletedJobAndPods(ctx, client, jobObj.Namespace, jobObj.Name, jobObj)
	require.NoError(t, err)

	deleteActions := make([]k8stesting.DeleteAction, 0, 2)
	for _, action := range client.Actions() {
		if action.GetVerb() != "delete" {
			continue
		}
		deleteAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		deleteActions = append(deleteActions, deleteAction)
	}
	require.Len(t, deleteActions, 2)
	require.Equal(t, "jobs", deleteActions[0].GetResource().Resource)
	require.Equal(t, "pods", deleteActions[1].GetResource().Resource)

	deleteOptions := deleteActions[0].GetDeleteOptions()
	require.NotNil(t, deleteOptions.Preconditions)
	require.NotNil(t, deleteOptions.Preconditions.UID)
	require.Equal(t, jobObj.UID, *deleteOptions.Preconditions.UID)
	require.NotNil(t, deleteOptions.PropagationPolicy)
	require.Equal(t, metav1.DeletePropagationBackground, *deleteOptions.PropagationPolicy)
}

func TestDeleteCompletedJobAndPodsKeepsPodWhenJobDeleteFails(t *testing.T) {
	ctx := context.Background()
	jobObj := jobForPodFallback("failed-delete-job", nil)
	ownedSucceeded := succeededPodForJob(jobObj, "failed-delete-job-owned", jobObj.UID)
	client := fake.NewSimpleClientset(jobObj, ownedSucceeded)
	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("delete denied")
	})

	err := deleteCompletedJobAndPods(ctx, client, jobObj.Namespace, jobObj.Name, jobObj)
	require.ErrorContains(t, err, "delete denied")

	_, err = client.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Pods(jobObj.Namespace).Get(ctx, ownedSucceeded.Name, metav1.GetOptions{})
	require.NoError(t, err)
	for _, action := range client.Actions() {
		require.False(t, action.Matches("delete", "pods"), "pod delete must not run after job delete failure")
	}
}

func TestDeleteCompletedJobAndPodsKeepsReplacementJobAndPod(t *testing.T) {
	ctx := context.Background()
	oldJob := jobForPodFallback("reused-job", nil)
	newJob := oldJob.DeepCopy()
	newJob.UID = types.UID("reused-job-new-uid")
	oldPod := succeededPodForJob(oldJob, "reused-job-old-pod", oldJob.UID)
	newPod := succeededPodForJob(newJob, "reused-job-new-pod", newJob.UID)
	client := fake.NewSimpleClientset(newJob, oldPod, newPod)
	client.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		deleteOptions := deleteAction.GetDeleteOptions()
		require.NotNil(t, deleteOptions.Preconditions)
		require.NotNil(t, deleteOptions.Preconditions.UID)
		require.Equal(t, oldJob.UID, *deleteOptions.Preconditions.UID)
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Group: batchv1.GroupName, Resource: "jobs"},
			oldJob.Name,
			errors.New("UID precondition failed"),
		)
	})

	err := deleteCompletedJobAndPods(ctx, client, oldJob.Namespace, oldJob.Name, oldJob)
	require.NoError(t, err)

	storedJob, err := client.BatchV1().Jobs(oldJob.Namespace).Get(ctx, oldJob.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newJob.UID, storedJob.UID)
	_, err = client.CoreV1().Pods(oldJob.Namespace).Get(ctx, oldPod.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Pods(oldJob.Namespace).Get(ctx, newPod.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestDeleteCompletedJobAndPodsKeepsPodWhenUIDDeletionWaitTimesOut(t *testing.T) {
	jobObj := jobForPodFallback("deletion-timeout-job", nil)
	ownedSucceeded := succeededPodForJob(jobObj, "deletion-timeout-job-owned", jobObj.UID)
	client := fake.NewSimpleClientset(jobObj, ownedSucceeded)
	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := deleteCompletedJobAndPods(ctx, client, jobObj.Namespace, jobObj.Name, jobObj)
	require.ErrorContains(t, err, "wait for job")

	_, err = client.CoreV1().Pods(jobObj.Namespace).Get(context.Background(), ownedSucceeded.Name, metav1.GetOptions{})
	require.NoError(t, err)
	for _, action := range client.Actions() {
		require.False(t, action.Matches("delete", "pods"), "pod delete must not run before the old job UID disappears")
	}
}

func TestFinalizeCompletedJobDeletesCompletedOwnedPods(t *testing.T) {
	ctx := context.Background()
	liveJob := jobForPodFallback("finalize-job", nil)
	ownedSucceeded := succeededPodForJob(liveJob, "finalize-job-owned", liveJob.UID)
	client := fake.NewSimpleClientset(liveJob, ownedSucceeded)
	task := &model.JobTask{
		Name:      liveJob.Name,
		Namespace: liveJob.Namespace,
		Status:    config.StatusCompleted,
	}

	finalizeCompletedJob(ctx, client, task, &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      liveJob.Name,
			Namespace: liveJob.Namespace,
		},
	})

	_, err := client.CoreV1().Pods(liveJob.Namespace).Get(ctx, ownedSucceeded.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.BatchV1().Jobs(liveJob.Namespace).Get(ctx, liveJob.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
}

func TestProcessJobResultDeletesCompletedOwnedPods(t *testing.T) {
	ctx := context.Background()
	liveJob := jobForPodFallback("result-job", nil)
	taskID := "task-result"
	stampTestResultJob(liveJob, taskID)
	liveJob.Status.Conditions = []batchv1.JobCondition{{
		Type:   batchv1.JobComplete,
		Status: corev1.ConditionTrue,
	}}
	ownedSucceeded := resultTestPod(liveJob)
	client := fake.NewSimpleClientset(liveJob, ownedSucceeded)
	payload := &JobResultPayload{
		Name:           liveJob.Name,
		Namespace:      liveJob.Namespace,
		TaskID:         taskID,
		ExecutionKey:   testResultExecutionKey(taskID),
		RunGeneration:  1,
		JobType:        string(config.JobDeployInstant),
		ServiceName:    "mysql-migrate",
		TimeoutSeconds: 1,
	}
	store := newResultOutboxTestStore()
	jobInfo := testResultJobInfo(1, payload)
	jobInfo.Status = string(config.StatusRunning)
	require.NoError(t, store.Add(ctx, jobInfo))

	err := processClaimedTestJobResult(t, ctx, client, store, payload)
	require.NoError(t, err)

	_, err = client.CoreV1().Pods(liveJob.Namespace).Get(ctx, ownedSucceeded.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.BatchV1().Jobs(liveJob.Namespace).Get(ctx, liveJob.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))

	var stored model.JobInfo
	stored.ID = 1
	require.NoError(t, store.Get(ctx, &stored))
	require.Equal(t, string(config.StatusCompleted), stored.Status)
}

func TestProcessJobResultStopsWhenSameNameJobIsReplaced(t *testing.T) {
	oldJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-job",
			Namespace: "default",
			UID:       types.UID("old-uid"),
			Annotations: map[string]string{
				config.AnnotationJobTaskID:        "task-old",
				config.AnnotationJobExecutionKey:  "execution-old",
				config.AnnotationJobRunGeneration: "1",
			},
		},
	}
	newJob := oldJob.DeepCopy()
	newJob.UID = types.UID("new-uid")
	newJob.Annotations[config.AnnotationJobExecutionKey] = "execution-new"
	newJob.Annotations[config.AnnotationJobRunGeneration] = "2"
	client := fake.NewSimpleClientset(oldJob)
	getCalls := 0
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		getCalls++
		if getCalls >= 2 {
			return true, newJob.DeepCopy(), nil
		}
		return true, oldJob.DeepCopy(), nil
	})
	store := newResultOutboxTestStore()
	key := "execution-old"
	require.NoError(t, store.Add(context.Background(), &model.JobInfo{ID: 1, TaskID: "task-old", Status: string(config.StatusRunning), Type: string(config.JobDeployScheduled), ExecutionKey: &key, RunGeneration: 1}))
	started := time.Now()
	err := processClaimedTestJobResult(t, context.Background(), client, store, &JobResultPayload{
		TaskID:         "task-old",
		JobType:        string(config.JobDeployScheduled),
		Namespace:      oldJob.Namespace,
		Name:           oldJob.Name,
		ExecutionKey:   "execution-old",
		RunGeneration:  1,
		TimeoutSeconds: 60,
	})

	require.NoError(t, err)
	require.GreaterOrEqual(t, getCalls, 2)
	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.Equal(t, string(config.StatusRunning), store.jobInfoByTaskID("task-old").Status, "stale result must not update execution state")
}

func TestProcessJobResultKeepsCompletedStatusWhenJobCleanupFails(t *testing.T) {
	ctx := context.Background()
	liveJob := jobForPodFallback("result-cleanup-failure-job", nil)
	taskID := "task-result-cleanup-failure"
	stampTestResultJob(liveJob, taskID)
	liveJob.Status.Conditions = []batchv1.JobCondition{{
		Type:   batchv1.JobComplete,
		Status: corev1.ConditionTrue,
	}}
	ownedSucceeded := resultTestPod(liveJob)
	client := fake.NewSimpleClientset(liveJob, ownedSucceeded)
	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("delete denied")
	})
	payload := &JobResultPayload{
		Name:           liveJob.Name,
		Namespace:      liveJob.Namespace,
		TaskID:         taskID,
		ExecutionKey:   testResultExecutionKey(taskID),
		RunGeneration:  1,
		JobType:        string(config.JobDeployInstant),
		ServiceName:    "mysql-migrate",
		TimeoutSeconds: 1,
	}
	store := newResultOutboxTestStore()
	jobInfo := testResultJobInfo(2, payload)
	jobInfo.Status = string(config.StatusRunning)
	require.NoError(t, store.Add(ctx, jobInfo))

	err := processClaimedTestJobResult(t, ctx, client, store, payload)
	require.ErrorContains(t, err, "delete denied")

	_, err = client.BatchV1().Jobs(liveJob.Namespace).Get(ctx, liveJob.Name, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Pods(liveJob.Namespace).Get(ctx, ownedSucceeded.Name, metav1.GetOptions{})
	require.NoError(t, err)

	var stored model.JobInfo
	stored.ID = 2
	require.NoError(t, store.Get(ctx, &stored))
	require.Equal(t, string(config.StatusCompleted), stored.Status)
}

func podForJobWithPhase(jobObj *batchv1.Job, name string, ownerUID types.UID, phase corev1.PodPhase) *corev1.Pod {
	pod := succeededPodForJob(jobObj, name, ownerUID)
	pod.Status.Phase = phase
	return pod
}

func TestDeleteCompletedPodsForJobRetainsReplacementAfterList(t *testing.T) {
	jobObj := jobForPodFallback("pod-name-reused", nil)
	listed := succeededPodForJob(jobObj, "reused-pod", jobObj.UID)
	listed.UID = "listed-pod-uid"
	replacement := listed.DeepCopy()
	replacement.UID = "replacement-pod-uid"
	client := fake.NewSimpleClientset(replacement)
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: []corev1.Pod{*listed}}, nil
	})
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions != nil && options.Preconditions.UID != nil && *options.Preconditions.UID == listed.UID {
			return true, nil, k8serrors.NewConflict(schema.GroupResource{Resource: "pods"}, listed.Name, errors.New("UID precondition failed"))
		}
		return false, nil, nil
	})
	deleted, err := deleteCompletedPodsForJob(context.Background(), client, jobObj.Namespace, jobObj)
	require.ErrorContains(t, err, "UID precondition failed")
	require.Zero(t, deleted)
	current, err := client.CoreV1().Pods(jobObj.Namespace).Get(context.Background(), replacement.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, replacement.UID, current.UID)
}

func TestProcessJobResultRejectsPodIdentityChangeDuringLogRead(t *testing.T) {
	for _, change := range []string{"replaced", "deleted", "verification unavailable"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			payload, liveJob, pod, store := completedResultFixture(t)
			pod.UID = "listed-pod-uid"
			client := fake.NewSimpleClientset(liveJob, pod)
			logsRead := false
			client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "log" {
					if logsRead && change == "verification unavailable" {
						return true, nil, errors.New("pod identity verification unavailable")
					}
					return false, nil, nil
				}
				logsRead = true
				resource := corev1.SchemeGroupVersion.WithResource("pods")
				switch change {
				case "replaced":
					replacement := pod.DeepCopy()
					replacement.UID = "replacement-pod-uid"
					require.NoError(t, client.Tracker().Update(resource, replacement, pod.Namespace))
				case "deleted":
					require.NoError(t, client.Tracker().Delete(resource, pod.Namespace, pod.Name))
				}
				return true, &runtime.Unknown{Raw: []byte("logs whose pod identity must be verified")}, nil
			})

			err := processClaimedTestJobResult(t, ctx, client, store, payload)
			require.Error(t, err)
			require.True(t, logsRead)
			stored := &model.JobInfo{ID: 1}
			require.NoError(t, store.Get(ctx, stored))
			require.Equal(t, string(config.StatusDistributed), stored.Status)
			require.Empty(t, stored.Info)
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb(), "unverified logs must not trigger cleanup")
			}
		})
	}
}

func TestCollectResultLogsOmitsOnlyKnownNeverStartedContainers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*corev1.Pod)
		omit   bool
	}{
		{name: "waiting without runtime history", omit: true},
		{name: "never-started init container", omit: true, change: func(p *corev1.Pod) {
			p.Spec.InitContainers, p.Spec.Containers = p.Spec.Containers, nil
			p.Status.InitContainerStatuses, p.Status.ContainerStatuses = p.Status.ContainerStatuses, nil
		}},
		{name: "missing status", change: func(p *corev1.Pod) { p.Status.ContainerStatuses = nil }},
		{name: "unknown state", change: func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State = corev1.ContainerState{} }},
		{name: "missing pod UID", change: func(p *corev1.Pod) { p.UID = "" }},
		{name: "nonterminal pod", change: func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending }},
		{name: "successful pod", change: func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }},
		{name: "runtime ID", change: func(p *corev1.Pod) { p.Status.ContainerStatuses[0].ContainerID = "containerd://previous" }},
		{name: "restart count", change: func(p *corev1.Pod) { p.Status.ContainerStatuses[0].RestartCount = 1 }},
		{name: "previous termination", change: func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ExitCode: 1}
		}},
		{name: "currently terminated", change: func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}
		}},
		{name: "currently running", change: func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{name: "started flag", change: func(p *corev1.Pod) {
			started := true
			p.Status.ContainerStatuses[0].Started = &started
		}},
		{name: "ready flag", change: func(p *corev1.Pod) { p.Status.ContainerStatuses[0].Ready = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, live, pod, _ := completedResultFixture(t)
			pod.UID = "failed-pod"
			pod.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", ContainerStatuses: []corev1.ContainerStatus{{
				Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			}}}
			if tc.change != nil {
				tc.change(pod)
			}
			client := fake.NewSimpleClientset(live, pod)
			reads := 0
			client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() == "log" {
					reads++
					return true, nil, k8serrors.NewBadRequest("container logs unavailable")
				}
				return false, nil, nil
			})
			logs, err := collectJobPodLogsForJob(context.Background(), client, live.Namespace, live.Name, live)
			if tc.omit {
				require.NoError(t, err)
				require.Zero(t, reads)
				require.Contains(t, logs, "container never started")
				require.Contains(t, logs, "Evicted")
			} else {
				require.ErrorContains(t, err, "container logs unavailable")
				require.Equal(t, 1, reads)
				require.NotContains(t, logs, "container never started")
			}
		})
	}
}

func TestCollectResultLogsRevalidatesNeverStartedContainer(t *testing.T) {
	for _, change := range []string{"UID replaced", "owner changed", "pod resumed", "container started", "status lost", "deleted", "API unavailable"} {
		t.Run(change, func(t *testing.T) {
			payload, live, pod, store := completedResultFixture(t)
			pod.UID = "failed-pod"
			pod.Status = corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{
				Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
			}}}
			current := pod.DeepCopy()
			switch change {
			case "UID replaced":
				current.UID = "replacement-pod"
			case "owner changed":
				current.OwnerReferences[0].UID = "other-job"
			case "pod resumed":
				current.Status.Phase = corev1.PodRunning
			case "container started":
				current.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}
			case "status lost":
				current.Status.ContainerStatuses = nil
			}
			client := fake.NewSimpleClientset(live, pod)
			verified := false
			client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				require.NotEqual(t, "log", action.GetSubresource(), "a never-started container has no log request")
				verified = true
				switch change {
				case "deleted":
					return true, nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "pods"}, pod.Name)
				case "API unavailable":
					return true, nil, errors.New("temporary Pod verification failure")
				}
				return true, current, nil
			})
			require.Error(t, processClaimedTestJobResult(t, context.Background(), client, store, payload))
			require.True(t, verified)
			record := store.jobInfoByTaskID(payload.TaskID)
			require.Equal(t, string(config.StatusDistributed), record.Status)
			require.Empty(t, record.Info)
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb(), "unverified omissions cannot authorize cleanup")
			}
		})
	}
}

func TestCollectResultLogsRetainsTransientFailureForExecutedContainer(t *testing.T) {
	payload, live, pod, store := completedResultFixture(t)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", ContainerID: "containerd://executed", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
	client := fake.NewSimpleClientset(live, pod)
	client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "log" {
			return true, nil, errors.New("temporary log transport failure")
		}
		return false, nil, nil
	})
	require.ErrorContains(t, processClaimedTestJobResult(t, context.Background(), client, store, payload), "temporary log transport failure")
	record := store.jobInfoByTaskID(payload.TaskID)
	require.Equal(t, string(config.StatusDistributed), record.Status)
	require.Empty(t, record.Info)
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb())
	}
}
