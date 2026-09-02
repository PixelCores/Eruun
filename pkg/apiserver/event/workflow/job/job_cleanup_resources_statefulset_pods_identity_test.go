package job

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestCleanupResourcesJobCtlPinsOwnerJobAndPodDeleteIdentity(t *testing.T) {
	ctx := context.Background()
	component, ownerJob, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	requireRequiredStatefulSetOwnerJobDeleteIdentity(t, client, ownerJob)
	require.Equal(t, 0, countClientActions(client, "delete", "pods"), "Pod deletion must wait until the owner Job is confirmed gone")

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	requireRequiredStatefulSetPodDeleteIdentity(t, client, pod)

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
}

func TestCleanupResourcesJobCtlConvergesTerminatingPodOwnerJobBeforeImmediateReplacement(t *testing.T) {
	ctx := context.Background()
	component, ownerJob, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	now := metav1.Now()
	currentPod, err := client.CoreV1().Pods(component.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	currentPod.DeletionTimestamp = &now
	require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), currentPod, component.Namespace))

	replacement := pod.DeepCopy()
	replacement.Name = pod.Name + "-replacement"
	replacement.UID = types.UID("replacement-owner-pod-uid")
	replacement.ResourceVersion = "32"
	replacement.DeletionTimestamp = nil
	replacementCreated := false
	client.Fake.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		requireRequiredStatefulSetOwnerJobDeleteAction(t, action, ownerJob)
		if !replacementCreated {
			replacementCreated = true
			require.NoError(t, client.Tracker().Add(replacement), "model the Job controller creating a replacement after the Pod starts terminating")
		}
		return false, nil, nil
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.True(t, replacementCreated)
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"), "a terminating Pod must not be deleted again")

	require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name))
	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.Equal(t, 0, countClientActions(client, "delete", "pods"), "the replacement must be checkpointed before deletion")

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	requireRequiredStatefulSetPodDeleteIdentity(t, client, replacement)

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
}

func TestCleanupResourcesJobCtlRestartsAfterPodDisappearsAndConvergesCheckpointedOwnerJob(t *testing.T) {
	ctx := context.Background()
	component, ownerJob, pod, client, _ := newRequiredStatefulSetOwnerJobPodController(t)
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName, Namespace: component.Namespace,
		UID: types.UID("statefulset-uid"), ResourceVersion: "11",
	}}
	require.NoError(t, client.Tracker().Add(statefulSet))

	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
	}}}
	firstJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	firstCtl := NewCleanupResourcesJobCtl(firstJob, client, store, nil)
	require.NotNil(t, firstCtl)
	require.NoError(t, firstCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, checkpoint.OwnerJobsCaptured)
	require.Equal(t, []requiredStatefulSetPodOwnerJobCheckpoint{{
		PodNames: []string{pod.Name}, Name: ownerJob.Name, UID: ownerJob.UID,
	}}, checkpoint.OwnerJobs)
	checkpoint.OwnerJobs[0].PodName = checkpoint.OwnerJobs[0].PodNames[0]
	checkpoint.OwnerJobs[0].PodNames = nil
	legacyCheckpoint, err := marshalRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo, checkpoint)
	require.NoError(t, err)
	store.jobInfo.InternalInfo = legacyCheckpoint

	require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSet.Name))
	require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name))
	client.ClearActions()

	retryJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	retryCtl := NewCleanupResourcesJobCtl(retryJob, client, store, nil)
	require.NotNil(t, retryCtl)
	gone, err := retryCtl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone, "a restarted worker must not succeed while the checkpointed owner Job is still live")
	requireRequiredStatefulSetOwnerJobDeleteIdentity(t, client, ownerJob)
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))

	gone, err = retryCtl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
}

func TestCleanupResourcesJobCtlRechecksLatePodShareBeforeCheckpointedOwnerJobDelete(t *testing.T) {
	ctx := context.Background()
	component, _, pod, client, _ := newRequiredStatefulSetOwnerJobPodController(t)
	nonCanonicalPod := pod.DeepCopy()
	nonCanonicalPod.Name = "zz-" + pod.Name
	nonCanonicalPod.UID = types.UID("non-canonical-owner-pod-uid")
	nonCanonicalPod.ResourceVersion = "32"
	require.NoError(t, client.Tracker().Add(nonCanonicalPod))
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 11, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-late-share", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
	}}}
	firstJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-late-share", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	firstCtl := NewCleanupResourcesJobCtl(firstJob, client, store, nil)
	require.NotNil(t, firstCtl)
	require.NoError(t, firstCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, checkpoint.OwnerJobsCaptured)
	require.Len(t, checkpoint.OwnerJobs, 1)
	require.Equal(t, []string{pod.Name, nonCanonicalPod.Name}, checkpoint.OwnerJobs[0].PodNames)
	client.ClearActions()

	shareInjected := false
	client.Fake.PrependReactor("list", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		canonicalObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name)
		require.NoError(t, getErr)
		canonicalSnapshot := canonicalObject.(*corev1.Pod).DeepCopy()
		currentObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, nonCanonicalPod.Name)
		require.NoError(t, getErr)
		current := currentObject.(*corev1.Pod).DeepCopy()
		staleNonCanonicalSnapshot := current.DeepCopy()
		if !shareInjected {
			shareInjected = true
			current.Labels = protectedRequiredStatefulSetCleanupLabels("late-share")
			now := metav1.Now()
			current.DeletionTimestamp = &now
			require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), current, component.Namespace))
		}
		return true, &corev1.PodList{Items: []corev1.Pod{*canonicalSnapshot, *staleNonCanonicalSnapshot}}, nil
	})
	retryJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-late-share", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	retryCtl := NewCleanupResourcesJobCtl(retryJob, client, store, nil)
	require.NotNil(t, retryCtl)

	gone, err := retryCtl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.True(t, shareInjected)
	require.Contains(t, err.Error(), "protected by live share labels")
	require.Contains(t, err.Error(), nonCanonicalPod.Name)
	require.Equal(t, 0, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlRechecksNonCanonicalPodUIDBeforeCheckpointedOwnerJobDelete(t *testing.T) {
	ctx := context.Background()
	component, _, pod, client, _ := newRequiredStatefulSetOwnerJobPodController(t)
	nonCanonicalPod := pod.DeepCopy()
	nonCanonicalPod.Name = "zz-" + pod.Name
	nonCanonicalPod.UID = types.UID("non-canonical-owner-pod-uid")
	nonCanonicalPod.ResourceVersion = "32"
	require.NoError(t, client.Tracker().Add(nonCanonicalPod))
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 12, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-late-replacement", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
	}}}
	firstJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-late-replacement", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	firstCtl := NewCleanupResourcesJobCtl(firstJob, client, store, nil)
	require.NotNil(t, firstCtl)
	require.NoError(t, firstCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{pod.Name, nonCanonicalPod.Name}, checkpoint.OwnerJobs[0].PodNames)
	client.ClearActions()

	replacementInjected := false
	client.Fake.PrependReactor("list", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		canonicalObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name)
		require.NoError(t, getErr)
		canonicalSnapshot := canonicalObject.(*corev1.Pod).DeepCopy()
		currentObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, nonCanonicalPod.Name)
		require.NoError(t, getErr)
		current := currentObject.(*corev1.Pod).DeepCopy()
		staleNonCanonicalSnapshot := current.DeepCopy()
		if !replacementInjected {
			replacementInjected = true
			require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, current.Name))
			replacement := current.DeepCopy()
			replacement.UID = types.UID("replacement-non-canonical-owner-pod-uid")
			replacement.ResourceVersion = "33"
			require.NoError(t, client.Tracker().Add(replacement))
		}
		return true, &corev1.PodList{Items: []corev1.Pod{*canonicalSnapshot, *staleNonCanonicalSnapshot}}, nil
	})
	retryJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-late-replacement", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	retryCtl := NewCleanupResourcesJobCtl(retryJob, client, store, nil)
	require.NotNil(t, retryCtl)

	gone, err := retryCtl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.True(t, replacementInjected)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), nonCanonicalPod.Name)
	require.Contains(t, err.Error(), "Pod UID changed")
	require.Equal(t, 0, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlPreflightsAllOwnerJobsBeforeAnyDelete(t *testing.T) {
	tests := []struct {
		name         string
		mutate       func(*testing.T, *fake.Clientset, string, *corev1.Pod)
		wantConflict bool
		wantDetail   string
	}{
		{
			name: "later Job Pod gains live share",
			mutate: func(t *testing.T, client *fake.Clientset, namespace string, pod *corev1.Pod) {
				t.Helper()
				pod.Labels = protectedRequiredStatefulSetCleanupLabels("later-job-share")
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, namespace))
			},
			wantDetail: "protected by live share labels",
		},
		{
			name: "later Job Pod is replaced",
			mutate: func(t *testing.T, client *fake.Clientset, namespace string, pod *corev1.Pod) {
				t.Helper()
				require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), namespace, pod.Name))
				pod.UID = types.UID("later-job-replacement-pod-uid")
				pod.ResourceVersion = "53"
				require.NoError(t, client.Tracker().Add(pod))
			},
			wantConflict: true,
			wantDetail:   "Pod UID changed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, firstOwnerJob, firstPod, client, _ := newRequiredStatefulSetOwnerJobPodController(t)
			laterOwnerJob := firstOwnerJob.DeepCopy()
			laterOwnerJob.Name = "zz-" + firstOwnerJob.Name
			laterOwnerJob.UID = types.UID("later-owner-job-uid")
			laterOwnerJob.ResourceVersion = "41"
			require.NoError(t, client.Tracker().Add(laterOwnerJob))
			laterPod := firstPod.DeepCopy()
			laterPod.Name = "zz-" + firstPod.Name
			laterPod.UID = types.UID("later-owner-pod-uid")
			laterPod.ResourceVersion = "51"
			laterPod.OwnerReferences[0].Name = laterOwnerJob.Name
			laterPod.OwnerReferences[0].UID = laterOwnerJob.UID
			require.NoError(t, client.Tracker().Add(laterPod))

			marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
			taskID := "task-global-preflight-" + tt.name
			store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
				ID: 13, Type: string(config.JobCleanupResources), AppID: component.AppID,
				TaskID: taskID, Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
			}}}
			firstJob := &model.JobTask{
				Name: component.Name, AppID: component.AppID, TaskID: taskID, JobType: string(config.JobCleanupResources),
				JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
			}
			firstCtl := NewCleanupResourcesJobCtl(firstJob, client, store, nil)
			require.NotNil(t, firstCtl)
			require.NoError(t, firstCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
			checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, checkpoint.OwnerJobs, 2)
			require.Equal(t, firstOwnerJob.Name, checkpoint.OwnerJobs[0].Name)
			require.Equal(t, laterOwnerJob.Name, checkpoint.OwnerJobs[1].Name)
			client.ClearActions()

			mutationInjected := false
			client.Fake.PrependReactor("list", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				firstObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, firstPod.Name)
				require.NoError(t, getErr)
				firstSnapshot := firstObject.(*corev1.Pod).DeepCopy()
				laterObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, laterPod.Name)
				require.NoError(t, getErr)
				laterCurrent := laterObject.(*corev1.Pod).DeepCopy()
				laterSnapshot := laterCurrent.DeepCopy()
				if !mutationInjected {
					mutationInjected = true
					tt.mutate(t, client, component.Namespace, laterCurrent)
				}
				return true, &corev1.PodList{Items: []corev1.Pod{*firstSnapshot, *laterSnapshot}}, nil
			})
			retryJob := &model.JobTask{
				Name: component.Name, AppID: component.AppID, TaskID: taskID, JobType: string(config.JobCleanupResources),
				JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
			}
			retryCtl := NewCleanupResourcesJobCtl(retryJob, client, store, nil)
			require.NotNil(t, retryCtl)

			gone, err := retryCtl.componentPodsGone(ctx, component)
			require.False(t, gone)
			require.Error(t, err)
			require.True(t, mutationInjected)
			if tt.wantConflict {
				require.True(t, k8serrors.IsConflict(err))
			}
			require.Contains(t, err.Error(), laterPod.Name)
			require.Contains(t, err.Error(), tt.wantDetail)
			require.Equal(t, 0, countClientActions(client, "delete", "jobs"), "global preflight must finish before deleting the earlier Job")
			require.Equal(t, 0, countClientActions(client, "delete", "pods"))
		})
	}
}

func TestCleanupResourcesJobCtlReconcilesStaleLocalCheckpointAfterConcurrentExpansion(t *testing.T) {
	tests := []struct {
		name              string
		addLocalExtension bool
	}{
		{name: "persisted flag remains true"},
		{name: "local target also expands", addLocalExtension: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, firstOwnerJob, firstPod, client, _ := newRequiredStatefulSetOwnerJobPodController(t)
			marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
			taskID := "task-concurrent-checkpoint-" + tt.name
			store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
				ID: 14, Type: string(config.JobCleanupResources), AppID: component.AppID,
				TaskID: taskID, Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
			}}}
			newTask := func() *model.JobTask {
				return &model.JobTask{
					Name: component.Name, AppID: component.AppID, TaskID: taskID, JobType: string(config.JobCleanupResources),
					JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
				}
			}
			staleCtl := NewCleanupResourcesJobCtl(newTask(), client, store, nil)
			require.NotNil(t, staleCtl)
			require.NoError(t, staleCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
			require.True(t, staleCtl.requiredStatefulSetPodTarget.checkpointPersisted)
			require.True(t, staleCtl.requiredStatefulSetPodTarget.checkpointEverPersisted)
			concurrentCtl := NewCleanupResourcesJobCtl(newTask(), client, store, nil)
			require.NotNil(t, concurrentCtl)
			require.NoError(t, concurrentCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))

			concurrentOwnerJob := firstOwnerJob.DeepCopy()
			concurrentOwnerJob.Name = "concurrent-" + firstOwnerJob.Name
			concurrentOwnerJob.UID = types.UID("concurrent-owner-job-uid")
			concurrentOwnerJob.ResourceVersion = "61"
			require.NoError(t, client.Tracker().Add(concurrentOwnerJob))
			concurrentPod := firstPod.DeepCopy()
			concurrentPod.Name = "concurrent-" + firstPod.Name
			concurrentPod.UID = types.UID("concurrent-owner-pod-uid")
			concurrentPod.ResourceVersion = "62"
			concurrentPod.OwnerReferences[0].Name = concurrentOwnerJob.Name
			concurrentPod.OwnerReferences[0].UID = concurrentOwnerJob.UID
			require.NoError(t, client.Tracker().Add(concurrentPod))
			require.NoError(t, concurrentCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
			persisted, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, requiredStatefulSetPodCheckpointContainsPod(persisted, concurrentPod.Name, concurrentPod.UID))
			require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, concurrentPod.Name))

			var localPod *corev1.Pod
			if tt.addLocalExtension {
				localOwnerJob := firstOwnerJob.DeepCopy()
				localOwnerJob.Name = "local-" + firstOwnerJob.Name
				localOwnerJob.UID = types.UID("local-owner-job-uid")
				localOwnerJob.ResourceVersion = "71"
				require.NoError(t, client.Tracker().Add(localOwnerJob))
				localPod = firstPod.DeepCopy()
				localPod.Name = "local-" + firstPod.Name
				localPod.UID = types.UID("local-owner-pod-uid")
				localPod.ResourceVersion = "72"
				localPod.OwnerReferences[0].Name = localOwnerJob.Name
				localPod.OwnerReferences[0].UID = localOwnerJob.UID
				require.NoError(t, client.Tracker().Add(localPod))
			}
			client.ClearActions()

			var persistErr error
			if tt.addLocalExtension {
				persistErr = staleCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component)
			} else {
				persistErr = staleCtl.persistRequiredStatefulSetPodTarget(ctx)
			}
			after, found, parseErr := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
			require.NoError(t, parseErr)
			require.True(t, found)
			require.True(t, requiredStatefulSetPodCheckpointContainsPod(after, concurrentPod.Name, concurrentPod.UID), "the concurrent DB extension must not be overwritten")
			if tt.addLocalExtension {
				require.Error(t, persistErr)
				require.True(t, k8serrors.IsConflict(persistErr))
				require.Contains(t, persistErr.Error(), "checkpoint forked concurrently")
				require.False(t, staleCtl.requiredStatefulSetPodTarget.checkpointPersisted, "a true checkpoint fork must invalidate the local persisted flag")
				require.True(t, staleCtl.requiredStatefulSetPodTarget.checkpointEverPersisted, "observing a persisted checkpoint must be monotonic")
				require.False(t, requiredStatefulSetPodCheckpointContainsPod(after, localPod.Name, localPod.UID), "a divergent local extension must not replace the DB checkpoint")
			} else {
				require.NoError(t, persistErr)
				require.True(t, staleCtl.requiredStatefulSetPodTarget.checkpointPersisted)
				require.True(t, staleCtl.requiredStatefulSetPodTarget.checkpointEverPersisted)
				require.Equal(t, concurrentPod.UID, staleCtl.requiredStatefulSetPodTarget.podUIDs[concurrentPod.Name], "a persisted strict superset must replace the stale local target")
				require.Contains(t, staleCtl.requiredStatefulSetPodTarget.ownerJobs, concurrentOwnerJob.Name)
			}
			require.Equal(t, 0, countClientActions(client, "delete", "jobs"))
			require.Equal(t, 0, countClientActions(client, "delete", "pods"))
		})
	}
}

func TestCleanupResourcesJobCtlDoesNotCompleteAcrossConcurrentPodCheckpointExpansion(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	checkpointA, err := marshalRequiredStatefulSetPodDeletionCheckpoint(marker, requiredStatefulSetPodDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName, OwnerJobsCaptured: true,
		Pods: []requiredStatefulSetPodIdentityCheckpoint{{Name: "owner-a-pod", UID: types.UID("owner-a-pod-uid")}},
	})
	require.NoError(t, err)
	checkpointAB, err := marshalRequiredStatefulSetPodDeletionCheckpoint(checkpointA, requiredStatefulSetPodDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName, OwnerJobsCaptured: true,
		Pods: []requiredStatefulSetPodIdentityCheckpoint{
			{Name: "owner-a-pod", UID: types.UID("owner-a-pod-uid")},
			{Name: "owner-b-pod", UID: types.UID("owner-b-pod-uid")},
		},
	})
	require.NoError(t, err)
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 17, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-complete-checkpoint-fence", Status: string(config.StatusRunning),
		InternalInfo: checkpointA, ServiceName: component.Name,
	}}
	store.beforeConditionalCAS = func() {
		store.jobInfo.InternalInfo = checkpointAB
	}
	job := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: store.jobInfo.TaskID,
		JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: checkpointA, Status: config.StatusCompleted,
	}
	ackCount := 0
	ctl := NewCleanupResourcesJobCtl(job, fake.NewSimpleClientset(), store, func() { ackCount++ })
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.Equal(t, config.StatusFailed, job.Status)
	require.Equal(t, string(config.StatusFailed), store.jobInfo.Status)
	require.Equal(t, checkpointAB, job.InternalInfo)
	require.Equal(t, checkpointAB, store.jobInfo.InternalInfo)
	require.Contains(t, job.Error, "checkpoint advanced before completion")
	require.Equal(t, 1, ackCount, "the workflow snapshot must observe the recoverable failure")

	checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, requiredStatefulSetPodCheckpointContainsPod(checkpoint, "owner-b-pod", types.UID("owner-b-pod-uid")))
}

func TestCleanupResourcesJobCtlKeepsExistingCompletedCheckpointAuthoritative(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	checkpointA, err := marshalRequiredStatefulSetPodDeletionCheckpoint(marker, requiredStatefulSetPodDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName, OwnerJobsCaptured: true,
		Pods: []requiredStatefulSetPodIdentityCheckpoint{{Name: "owner-a-pod", UID: types.UID("owner-a-pod-uid")}},
	})
	require.NoError(t, err)
	checkpointAB, err := marshalRequiredStatefulSetPodDeletionCheckpoint(checkpointA, requiredStatefulSetPodDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName, OwnerJobsCaptured: true,
		Pods: []requiredStatefulSetPodIdentityCheckpoint{
			{Name: "owner-a-pod", UID: types.UID("owner-a-pod-uid")},
			{Name: "owner-b-pod", UID: types.UID("owner-b-pod-uid")},
		},
	})
	require.NoError(t, err)
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 18, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-existing-completed-checkpoint", Status: string(config.StatusCompleted),
		InternalInfo: checkpointAB, ServiceName: component.Name,
	}}
	job := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: store.jobInfo.TaskID,
		JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: checkpointA, Status: config.StatusCompleted,
	}
	ackCount := 0
	ctl := NewCleanupResourcesJobCtl(job, fake.NewSimpleClientset(), store, func() { ackCount++ })
	require.NotNil(t, ctl)

	require.NoError(t, ctl.SaveInfo(context.Background()))
	require.True(t, ctl.skipSaveInfo)
	require.Equal(t, config.StatusCompleted, job.Status)
	require.Equal(t, checkpointAB, job.InternalInfo)
	require.Equal(t, string(config.StatusCompleted), store.jobInfo.Status)
	require.Equal(t, checkpointAB, store.jobInfo.InternalInfo)
	require.Equal(t, 0, ackCount)
}

func TestCleanupResourcesJobCtlDoesNotRecreateDisappearedCheckpointAfterLocalExpansion(t *testing.T) {
	ctx := context.Background()
	component, firstOwnerJob, firstPod, client, _ := newRequiredStatefulSetOwnerJobPodController(t)
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	pvcSibling, err := marshalRequiredStatefulSetPVCDeletionCheckpoint(marker, requiredStatefulSetPVCDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName, Templates: []string{"data"},
	})
	require.NoError(t, err)
	store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 15, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-disappeared-checkpoint", Status: string(config.StatusRunning), InternalInfo: pvcSibling, ServiceName: component.Name,
	}}}
	newTask := func() *model.JobTask {
		return &model.JobTask{
			Name: component.Name, AppID: component.AppID, TaskID: "task-disappeared-checkpoint", JobType: string(config.JobCleanupResources),
			JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
		}
	}
	writerCtl := NewCleanupResourcesJobCtl(newTask(), client, store, nil)
	require.NotNil(t, writerCtl)
	require.NoError(t, writerCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	_, podCheckpointFound, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, podCheckpointFound)
	_, pvcCheckpointFound, err := parseRequiredStatefulSetPVCDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, pvcCheckpointFound)

	restoredCtl := NewCleanupResourcesJobCtl(newTask(), client, store, nil)
	require.NotNil(t, restoredCtl)
	require.NoError(t, restoredCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	require.True(t, restoredCtl.requiredStatefulSetPodTarget.checkpointPersisted)
	require.True(t, restoredCtl.requiredStatefulSetPodTarget.checkpointEverPersisted)

	localOwnerJob := firstOwnerJob.DeepCopy()
	localOwnerJob.Name = "local-dirty-" + firstOwnerJob.Name
	localOwnerJob.UID = types.UID("local-dirty-owner-job-uid")
	localOwnerJob.ResourceVersion = "81"
	require.NoError(t, client.Tracker().Add(localOwnerJob))
	localPod := firstPod.DeepCopy()
	localPod.Name = "local-dirty-" + firstPod.Name
	localPod.UID = types.UID("local-dirty-owner-pod-uid")
	localPod.ResourceVersion = "82"
	localPod.OwnerReferences[0].Name = localOwnerJob.Name
	localPod.OwnerReferences[0].UID = localOwnerJob.UID
	require.NoError(t, client.Tracker().Add(localPod))
	checkpointRemoved := false
	client.Fake.PrependReactor("list", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		firstObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, firstPod.Name)
		require.NoError(t, getErr)
		localObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, localPod.Name)
		require.NoError(t, getErr)
		if !checkpointRemoved {
			checkpointRemoved = true
			store.jobInfo.InternalInfo = pvcSibling
		}
		return true, &corev1.PodList{Items: []corev1.Pod{
			*firstObject.(*corev1.Pod).DeepCopy(),
			*localObject.(*corev1.Pod).DeepCopy(),
		}}, nil
	})
	store.casJobInfo = nil
	client.ClearActions()

	err = restoredCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "persisted pod identity checkpoint disappeared")
	require.True(t, checkpointRemoved)
	require.False(t, restoredCtl.requiredStatefulSetPodTarget.checkpointPersisted)
	require.True(t, restoredCtl.requiredStatefulSetPodTarget.checkpointEverPersisted)
	require.Nil(t, store.casJobInfo, "a disappeared durable checkpoint must fail before CAS")
	require.Equal(t, pvcSibling, store.jobInfo.InternalInfo, "the PVC sibling must remain without recreating the Pod checkpoint")
	_, podCheckpointFound, err = parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.False(t, podCheckpointFound)
	_, pvcCheckpointFound, err = parseRequiredStatefulSetPVCDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, pvcCheckpointFound)
	require.Equal(t, 0, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlRejectsRestartAfterPodDisappearsWithoutOwnerJobCheckpoint(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	checkpointed, err := marshalRequiredStatefulSetPodDeletionCheckpoint(marker, requiredStatefulSetPodDeletionCheckpoint{
		Namespace:           component.Namespace,
		StatefulSetName:     buildStoreSeverName(component.Name, component.ResourceNameKey()),
		StatefulSetUID:      types.UID("statefulset-uid"),
		StatefulSetWasFound: true,
		Pods: []requiredStatefulSetPodIdentityCheckpoint{{
			Name: "mysql-owner-pod", UID: types.UID("owner-pod-uid"),
		}},
		OwnerJobsCaptured: false,
	})
	require.NoError(t, err)
	store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 11, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-legacy", Status: string(config.StatusRunning), InternalInfo: checkpointed, ServiceName: component.Name,
	}}}
	job := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-legacy", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	client := fake.NewSimpleClientset()
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)

	gone, err := ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "Pod disappeared before its owner Job identity was checkpointed")
	require.Equal(t, 0, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlRetriesOwnerJobDeleteAfterBenignResourceVersionConflict(t *testing.T) {
	ctx := context.Background()
	component, ownerJob, _, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	deleteAttempts := 0
	client.Fake.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deleteAttempts++
		switch deleteAttempts {
		case 1:
			requireRequiredStatefulSetOwnerJobDeleteAction(t, action, ownerJob)
			currentObject, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), component.Namespace, ownerJob.Name)
			require.NoError(t, err)
			current := currentObject.(*batchv1.Job).DeepCopy()
			current.ResourceVersion = "22"
			current.Status.Active = 1
			require.NoError(t, client.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), current, component.Namespace))
			return true, nil, k8serrors.NewConflict(
				schema.GroupResource{Group: "batch", Resource: "jobs"},
				ownerJob.Name,
				fmt.Errorf("resourceVersion precondition no longer matches"),
			)
		case 2:
			latest := ownerJob.DeepCopy()
			latest.ResourceVersion = "22"
			requireRequiredStatefulSetOwnerJobDeleteAction(t, action, latest)
			return false, nil, nil
		default:
			require.Failf(t, "unexpected owner Job delete", "attempt %d", deleteAttempts)
			return true, nil, nil
		}
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"), "Pod deletion must wait for the pinned owner Job retry")

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.Equal(t, 2, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"), "Pod deletion must wait until the owner Job is confirmed gone")

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.Equal(t, 1, countClientActions(client, "delete", "pods"))

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
}

func TestCleanupResourcesJobCtlRetriesPodDeleteAfterBenignResourceVersionConflict(t *testing.T) {
	ctx := context.Background()
	component, _, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	deleteAttempts := 0
	client.Fake.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deleteAttempts++
		switch deleteAttempts {
		case 1:
			requireRequiredStatefulSetPodDeleteAction(t, action, pod)
			currentObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name)
			require.NoError(t, err)
			current := currentObject.(*corev1.Pod).DeepCopy()
			current.ResourceVersion = "32"
			current.Status.Phase = corev1.PodRunning
			require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), current, component.Namespace))
			return true, nil, k8serrors.NewConflict(
				schema.GroupResource{Resource: "pods"},
				pod.Name,
				fmt.Errorf("resourceVersion precondition no longer matches"),
			)
		case 2:
			latest := pod.DeepCopy()
			latest.ResourceVersion = "32"
			requireRequiredStatefulSetPodDeleteAction(t, action, latest)
			return false, nil, nil
		default:
			require.Failf(t, "unexpected Pod delete", "attempt %d", deleteAttempts)
			return true, nil, nil
		}
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"), "retry must accept the already-gone pinned owner Job")
	require.Equal(t, 1, countClientActions(client, "delete", "pods"))

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)
	require.Equal(t, 2, countClientActions(client, "delete", "pods"))

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
}

func TestCleanupResourcesJobCtlRejectsOwnerJobLabelResourceVersionRace(t *testing.T) {
	ctx := context.Background()
	component, ownerJob, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	client.Fake.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		requireRequiredStatefulSetOwnerJobDeleteAction(t, action, ownerJob)
		currentObject, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), component.Namespace, ownerJob.Name)
		require.NoError(t, err)
		current := currentObject.(*batchv1.Job).DeepCopy()
		current.Labels = protectedRequiredStatefulSetCleanupLabels("late-shared-owner")
		current.ResourceVersion = "22"
		require.NoError(t, client.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), current, component.Namespace))
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Group: "batch", Resource: "jobs"},
			ownerJob.Name,
			fmt.Errorf("resourceVersion precondition no longer matches"),
		)
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.Contains(t, err.Error(), "owner job "+ownerJob.Name+" is protected")
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
	currentPod, getErr := client.CoreV1().Pods(component.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, pod.UID, currentPod.UID)
}

func TestCleanupResourcesJobCtlRejectsOwnerJobReplacementAfterDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	component, ownerJob, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	client.Fake.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		requireRequiredStatefulSetOwnerJobDeleteAction(t, action, ownerJob)
		require.NoError(t, client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), component.Namespace, ownerJob.Name))
		replacement := ownerJob.DeepCopy()
		replacement.UID = types.UID("replacement-owner-job-uid")
		replacement.ResourceVersion = "22"
		require.NoError(t, client.Tracker().Add(replacement))
		return true, nil, k8serrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, ownerJob.Name)
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "Job UID changed")
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
	currentPod, getErr := client.CoreV1().Pods(component.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, pod.UID, currentPod.UID)
}

func TestCleanupResourcesJobCtlRechecksPodAfterOwnerJobGC(t *testing.T) {
	ctx := context.Background()
	component, ownerJob, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	client.Fake.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		requireRequiredStatefulSetOwnerJobDeleteAction(t, action, ownerJob)
		require.NoError(t, client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), component.Namespace, ownerJob.Name))
		currentObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name)
		require.NoError(t, err)
		current := currentObject.(*corev1.Pod).DeepCopy()
		current.Labels = protectedRequiredStatefulSetCleanupLabels("late-shared-pod")
		current.ResourceVersion = "32"
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), current, component.Namespace))
		return true, nil, k8serrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, ownerJob.Name)
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)

	gone, err = ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pod "+component.Namespace+"/"+pod.Name+" is protected")
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlRejectsPodLabelResourceVersionRace(t *testing.T) {
	ctx := context.Background()
	component, _, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	client.Fake.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		requireRequiredStatefulSetPodDeleteAction(t, action, pod)
		currentObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name)
		require.NoError(t, err)
		current := currentObject.(*corev1.Pod).DeepCopy()
		current.Labels = protectedRequiredStatefulSetCleanupLabels("late-shared-pod")
		current.ResourceVersion = "32"
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), current, component.Namespace))
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Resource: "pods"},
			pod.Name,
			fmt.Errorf("resourceVersion precondition no longer matches"),
		)
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)

	gone, err = ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pod "+component.Namespace+"/"+pod.Name+" is protected")
	current, getErr := client.CoreV1().Pods(component.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, types.UID("owner-pod-uid"), current.UID)
}

func TestCleanupResourcesJobCtlRetriesPodCleanupAfterOwnerJobIsGone(t *testing.T) {
	ctx := context.Background()
	component, _, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
	failFirstPodDelete := true
	client.Fake.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if !failFirstPodDelete {
			return false, nil, nil
		}
		failFirstPodDelete = false
		requireRequiredStatefulSetPodDeleteAction(t, action, pod)
		return true, nil, k8serrors.NewInternalError(fmt.Errorf("transient pod delete failure"))
	})

	gone, err := ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.NoError(t, err)
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))

	gone, err = ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transient pod delete failure")
	require.Equal(t, 1, countClientActions(client, "delete", "jobs"), "retry must accept the already-gone pinned owner Job")

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
}

func TestCleanupResourcesJobCtlRejectsEmptyOwnerJobOrPodDeleteIdentity(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*batchv1.Job, *corev1.Pod)
		wantDetail string
	}{
		{
			name: "empty Pod UID",
			mutate: func(_ *batchv1.Job, pod *corev1.Pod) {
				pod.UID = ""
			},
			wantDetail: "Pod UID changed",
		},
		{
			name: "empty Pod resourceVersion",
			mutate: func(_ *batchv1.Job, pod *corev1.Pod) {
				pod.ResourceVersion = ""
			},
			wantDetail: "live Pod has an empty resourceVersion",
		},
		{
			name: "empty owner reference UID",
			mutate: func(_ *batchv1.Job, pod *corev1.Pod) {
				pod.OwnerReferences[0].UID = ""
			},
			wantDetail: "owner reference has an empty UID",
		},
		{
			name: "empty live Job UID",
			mutate: func(job *batchv1.Job, _ *corev1.Pod) {
				job.UID = ""
			},
			wantDetail: "live Job has an empty UID",
		},
		{
			name: "empty live Job resourceVersion",
			mutate: func(job *batchv1.Job, _ *corev1.Pod) {
				job.ResourceVersion = ""
			},
			wantDetail: "live Job has an empty resourceVersion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, ownerJob, pod, client, ctl := newRequiredStatefulSetOwnerJobPodController(t)
			currentJob, err := client.BatchV1().Jobs(component.Namespace).Get(ctx, ownerJob.Name, metav1.GetOptions{})
			require.NoError(t, err)
			currentPod, err := client.CoreV1().Pods(component.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			tt.mutate(currentJob, currentPod)
			require.NoError(t, client.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), currentJob, component.Namespace))
			require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), currentPod, component.Namespace))

			gone, err := ctl.componentPodsGone(ctx, component)
			require.False(t, gone)
			require.Error(t, err)
			require.True(t, k8serrors.IsConflict(err))
			require.Contains(t, err.Error(), tt.wantDetail)
			require.Equal(t, 0, countClientActions(client, "delete", "jobs"))
			require.Equal(t, 0, countClientActions(client, "delete", "pods"))
		})
	}
}

func newRequiredStatefulSetOwnerJobPodController(t *testing.T) (
	*model.ApplicationComponent,
	*batchv1.Job,
	*corev1.Pod,
	*fake.Clientset,
	*CleanupResourcesJobCtl,
) {
	t.Helper()
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName, Namespace: component.Namespace,
		UID: types.UID("statefulset-uid"), ResourceVersion: "11",
	}}
	ownerJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "mysql-owner", Namespace: component.Namespace,
		UID: types.UID("owner-job-uid"), ResourceVersion: "21",
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "mysql-owner-pod", Namespace: component.Namespace,
		UID: types.UID("owner-pod-uid"), ResourceVersion: "31",
		Labels: map[string]string{
			config.LabelAppID:         component.AppID,
			config.LabelComponentName: component.Name,
		},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "batch/v1", Kind: "Job", Name: ownerJob.Name, UID: ownerJob.UID,
		}},
	}}
	client := fake.NewSimpleClientset(statefulSet, ownerJob, pod)
	task := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	require.NoError(t, ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	require.True(t, ctl.requiredStatefulSetPodTarget.checkpointPersisted)
	require.False(t, ctl.requiredStatefulSetPodTarget.checkpointEverPersisted, "nonpersistent helpers must not claim a durable checkpoint")
	require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSet.Name))
	return component, ownerJob, pod, client, ctl
}

func requiredStatefulSetPodCheckpointContainsPod(
	checkpoint requiredStatefulSetPodDeletionCheckpoint,
	name string,
	uid types.UID,
) bool {
	for _, pod := range checkpoint.Pods {
		if pod.Name == name && pod.UID == uid {
			return true
		}
	}
	return false
}

func protectedRequiredStatefulSetCleanupLabels(shareName string) map[string]string {
	return map[string]string{
		config.LabelShareName:     shareName,
		config.LabelShareStrategy: string(config.ShareStrategyDefault),
	}
}

func requireRequiredStatefulSetOwnerJobDeleteIdentity(t *testing.T, client *fake.Clientset, job *batchv1.Job) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "jobs" {
			requireRequiredStatefulSetOwnerJobDeleteAction(t, action, job)
			return
		}
	}
	require.Fail(t, "owner Job delete action was not recorded")
}

func requireRequiredStatefulSetOwnerJobDeleteAction(t *testing.T, action k8stesting.Action, job *batchv1.Job) {
	t.Helper()
	deleteAction, ok := action.(k8stesting.DeleteAction)
	require.True(t, ok)
	require.Equal(t, job.Name, deleteAction.GetName())
	options := deleteAction.GetDeleteOptions()
	require.NotNil(t, options.PropagationPolicy)
	require.Equal(t, metav1.DeletePropagationOrphan, *options.PropagationPolicy)
	require.NotNil(t, options.Preconditions)
	require.NotNil(t, options.Preconditions.UID)
	require.Equal(t, job.UID, *options.Preconditions.UID)
	require.NotNil(t, options.Preconditions.ResourceVersion)
	require.Equal(t, job.ResourceVersion, *options.Preconditions.ResourceVersion)
}

func requireRequiredStatefulSetPodDeleteIdentity(t *testing.T, client *fake.Clientset, pod *corev1.Pod) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			requireRequiredStatefulSetPodDeleteAction(t, action, pod)
			return
		}
	}
	require.Fail(t, "Pod delete action was not recorded")
}

func requireRequiredStatefulSetPodDeleteAction(t *testing.T, action k8stesting.Action, pod *corev1.Pod) {
	t.Helper()
	deleteAction, ok := action.(k8stesting.DeleteAction)
	require.True(t, ok)
	require.Equal(t, pod.Name, deleteAction.GetName())
	options := deleteAction.GetDeleteOptions()
	require.NotNil(t, options.Preconditions)
	require.NotNil(t, options.Preconditions.UID)
	require.Equal(t, pod.UID, *options.Preconditions.UID)
	require.NotNil(t, options.Preconditions.ResourceVersion)
	require.Equal(t, pod.ResourceVersion, *options.Preconditions.ResourceVersion)
}
