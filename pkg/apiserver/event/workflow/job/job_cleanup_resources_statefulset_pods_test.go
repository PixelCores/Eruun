package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func TestCleanupResourcesJobCtlDeletesPinnedStatefulSetPodAfterLabelDrift(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, pod, client, ctl := newRequiredStatefulSetPodController(t)
	pod.Labels = map[string]string{"drifted": "true"}
	require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, component.Namespace))

	require.NoError(t, ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSet.Name))

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone, "a successful delete requires one confirming poll")
	requirePodDeleteUsesUIDPrecondition(t, client, pod.Name, pod.UID)

	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
	require.Equal(t, 1, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlRechecksPinnedPodProtectionAfterOrphan(t *testing.T) {
	t.Run("live pod share labels", func(t *testing.T) {
		ctx := context.Background()
		component, statefulSet, pod, client, ctl := newRequiredStatefulSetPodController(t)
		require.NoError(t, ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
		require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSet.Name))

		current, err := client.CoreV1().Pods(component.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		require.NoError(t, err)
		current.Labels = map[string]string{
			config.LabelShareName:     "late-shared-mysql",
			config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
		}
		_, err = client.CoreV1().Pods(component.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		require.NoError(t, err)

		gone, err := ctl.componentPodsGone(ctx, component)
		require.False(t, gone)
		require.Error(t, err)
		require.Contains(t, err.Error(), "pod "+component.Namespace+"/"+pod.Name+" is protected")
		require.Equal(t, 0, countClientActions(client, "delete", "pods"))
	})

	t.Run("live owner job share labels", func(t *testing.T) {
		ctx := context.Background()
		component, statefulSet, _, client, ctl := newRequiredStatefulSetPodController(t)
		ownerJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: "mysql-owner", Namespace: component.Namespace,
			UID: types.UID("owner-job-uid"), ResourceVersion: "21",
		}}
		jobPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "mysql-owner-pod", Namespace: component.Namespace,
			UID: types.UID("owner-pod-uid"), ResourceVersion: "31",
			Labels: map[string]string{
				config.LabelAppID:         component.AppID,
				config.LabelComponentName: component.Name,
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: ownerJob.Name, UID: ownerJob.UID}},
		}}
		require.NoError(t, client.Tracker().Add(ownerJob))
		require.NoError(t, client.Tracker().Add(jobPod))
		require.NoError(t, ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
		require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSet.Name))

		currentPod, err := client.CoreV1().Pods(component.Namespace).Get(ctx, jobPod.Name, metav1.GetOptions{})
		require.NoError(t, err)
		currentPod.Labels = nil
		_, err = client.CoreV1().Pods(component.Namespace).Update(ctx, currentPod, metav1.UpdateOptions{})
		require.NoError(t, err)
		currentJob, err := client.BatchV1().Jobs(component.Namespace).Get(ctx, ownerJob.Name, metav1.GetOptions{})
		require.NoError(t, err)
		currentJob.Labels = map[string]string{
			config.LabelShareName:     "late-shared-owner",
			config.LabelShareStrategy: string(domainspec.ShareStrategyIgnore),
		}
		_, err = client.BatchV1().Jobs(component.Namespace).Update(ctx, currentJob, metav1.UpdateOptions{})
		require.NoError(t, err)

		gone, err := ctl.componentPodsGone(ctx, component)
		require.False(t, gone)
		require.Error(t, err)
		require.Contains(t, err.Error(), "owner job "+ownerJob.Name+" is protected")
		require.Equal(t, 0, countClientActions(client, "delete", "jobs"))
		require.Equal(t, 0, countClientActions(client, "delete", "pods"))
	})
}

func TestCleanupResourcesJobCtlRejectsPinnedStatefulSetPodReplacement(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, pod, client, ctl := newRequiredStatefulSetPodController(t)
	require.NoError(t, ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSet.Name))
	require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), component.Namespace, pod.Name))
	replacement := pod.DeepCopy()
	replacement.UID = types.UID("replacement-pod-uid")
	require.NoError(t, client.Tracker().Add(replacement))

	gone, err := ctl.componentPodsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "Pod UID changed")
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlFailsClosedForOrphanOrdinalPodOnFreshRetry(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName + "-0", Namespace: component.Namespace, UID: types.UID("unknown-orphan-pod"),
		Labels: map[string]string{
			config.LabelAppID:         component.AppID,
			config.LabelComponentName: component.Name,
		},
	}}
	client := fake.NewSimpleClientset(pod)
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)

	err := ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "cannot be proven")
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlRestoresPinnedPodIdentityOnSameTaskRetry(t *testing.T) {
	tests := []struct {
		name           string
		replacementUID types.UID
		wantConflict   bool
	}{
		{name: "original orphan Pod", replacementUID: "statefulset-pod-uid"},
		{name: "same-name replacement", replacementUID: "replacement-pod-uid", wantConflict: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, statefulSet, pod, client, _ := newRequiredStatefulSetPodController(t)
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
			require.Contains(t, store.jobInfo.InternalInfo, requiredStatefulSetPodCheckpointKey)

			require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSet.Name))
			currentPod, err := client.CoreV1().Pods(component.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			currentPod.UID = tt.replacementUID
			currentPod.Labels = nil
			currentPod.OwnerReferences = nil
			require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), currentPod, component.Namespace))

			retryJob := &model.JobTask{
				Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
				JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
			}
			retryCtl := NewCleanupResourcesJobCtl(retryJob, client, store, nil)
			require.NotNil(t, retryCtl)
			err = retryCtl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component)
			if tt.wantConflict {
				require.Error(t, err)
				require.True(t, k8serrors.IsConflict(err))
				require.Equal(t, 0, countClientActions(client, "delete", "pods"))
				return
			}
			require.NoError(t, err)

			gone, err := retryCtl.componentPodsGone(ctx, component)
			require.NoError(t, err)
			require.False(t, gone)
			requirePodDeleteUsesUIDPrecondition(t, client, pod.Name, pod.UID)
		})
	}
}

func TestCleanupResourcesJobCtlStopsWhenCheckpointPersistRacesCancellation(t *testing.T) {
	ctx := context.Background()
	component, _, _, client, _ := newRequiredStatefulSetPodController(t)
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
	}}}
	store.beforeCheckpointCAS = func(updates map[string]interface{}) {
		store.jobInfo.InternalInfo, _ = updates["internal_info"].(string)
		store.jobInfo.Status = string(config.StatusCancelled)
	}
	job := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)

	err := ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component)
	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)
	require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
}

func TestCleanupResourcesJobCtlStopsWhenWorkflowTaskIsCancelledButJobInfoIsRunning(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSetUID := types.UID("statefulset-uid")
	controller := true
	labels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: statefulSetName, Namespace: component.Namespace, UID: statefulSetUID,
				ResourceVersion: "11", Labels: labels,
			},
			Spec: appsv1.StatefulSetSpec{
				PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			},
		},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName + "-0", Namespace: component.Namespace, UID: types.UID("pod-uid"), Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID}},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace, ResourceVersion: "21",
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID, Controller: &controller,
			}},
		}},
	)
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	store := &cleanupComponentStore{
		component:    component,
		workflowTask: &model.WorkflowQueue{TaskID: "task-1", Status: config.StatusCancelled},
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
		},
	}
	job := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: marker, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)
	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)
	require.Equal(t, config.StatusCancelled, job.Status)
	require.False(t, ctl.skipSaveInfo)
	require.NoError(t, ctl.SaveInfo(ctx))
	require.Equal(t, string(config.StatusCancelled), store.jobInfo.Status)
	for _, resource := range []string{"statefulsets", "pods", "persistentvolumeclaims"} {
		for _, verb := range []string{"update", "patch", "delete"} {
			require.Equal(t, 0, countClientActions(client, verb, resource), "%s %s must be fenced after persisted cancellation", verb, resource)
		}
	}
}

func TestCleanupResourcesJobCtlStopsOldGenerationBeforeStatefulSetCleanup(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSetUID := types.UID("statefulset-uid")
	controller := true
	labels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name: statefulSetName, Namespace: component.Namespace, UID: statefulSetUID,
				ResourceVersion: "11", Labels: labels,
			},
			Spec: appsv1.StatefulSetSpec{
				PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
			},
		},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName + "-0", Namespace: component.Namespace, UID: types.UID("pod-uid"), Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID}},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace, ResourceVersion: "21",
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID, Controller: &controller,
			}},
		}},
	)
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	oldExecutionKey := "task-1:cleanup:mysql:g1:a1"
	store := &cleanupComponentStore{
		component:    component,
		workflowTask: &model.WorkflowQueue{TaskID: "task-1", Status: config.StatusRunning, RunGeneration: 2},
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
			ExecutionKey: &oldExecutionKey, RunGeneration: 1, Attempt: 1,
		},
	}
	job := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
		ExecutionKey: oldExecutionKey, RunGeneration: 1, Attempt: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)
	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)
	require.Equal(t, config.StatusCancelled, job.Status)
	require.True(t, ctl.skipSaveInfo)
	require.NoError(t, ctl.SaveInfo(ctx))
	require.Nil(t, store.casJobInfo)
	require.Equal(t, string(config.StatusRunning), store.jobInfo.Status)
	require.Equal(t, uint64(1), store.jobInfo.RunGeneration)
	require.NotNil(t, store.jobInfo.ExecutionKey)
	require.Equal(t, oldExecutionKey, *store.jobInfo.ExecutionKey)
	for _, resource := range []string{"statefulsets", "pods", "persistentvolumeclaims"} {
		for _, verb := range []string{"update", "patch", "delete"} {
			require.Equal(t, 0, countClientActions(client, verb, resource), "%s %s must be fenced after generation takeover", verb, resource)
		}
	}
}

func TestCleanupResourcesJobCtlKeepsCompletedRequiredStatefulSetCleanupIdempotent(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	client := fake.NewSimpleClientset(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName, Namespace: component.Namespace,
	}})
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	store := &cleanupComponentStore{
		component:    component,
		workflowTask: &model.WorkflowQueue{TaskID: "task-1", Status: config.StatusCompleted},
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusCompleted), InternalInfo: marker, ServiceName: component.Name,
		},
	}
	job := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component, InternalInfo: marker,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, job.Status)
	require.True(t, ctl.skipSaveInfo)
	require.NoError(t, ctl.SaveInfo(ctx))
	require.Equal(t, string(config.StatusCompleted), store.jobInfo.Status)
	require.Nil(t, store.putJobInfo)
	for _, resource := range []string{"statefulsets", "pods", "persistentvolumeclaims"} {
		for _, verb := range []string{"update", "patch", "delete"} {
			require.Equal(t, 0, countClientActions(client, verb, resource))
		}
	}
}

func newRequiredStatefulSetPodController(t *testing.T) (
	*model.ApplicationComponent,
	*appsv1.StatefulSet,
	*corev1.Pod,
	*fake.Clientset,
	*CleanupResourcesJobCtl,
) {
	t.Helper()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSetUID := types.UID("statefulset-uid")
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName, Namespace: component.Namespace, UID: statefulSetUID, ResourceVersion: "11",
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName + "-0", Namespace: component.Namespace,
		UID: types.UID("statefulset-pod-uid"), ResourceVersion: "21",
		Labels: map[string]string{
			config.LabelAppID:         component.AppID,
			config.LabelComponentName: component.Name,
		},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID,
		}},
	}}
	client := fake.NewSimpleClientset(statefulSet, pod)
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	return component, statefulSet, pod, client, ctl
}

func requirePodDeleteUsesUIDPrecondition(t *testing.T, client *fake.Clientset, name string, uid types.UID) {
	t.Helper()
	found := false
	for _, action := range client.Actions() {
		if action.GetVerb() != "delete" || action.GetResource().Resource != "pods" {
			continue
		}
		deleteAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		if deleteAction.GetName() != name {
			continue
		}
		found = true
		options := deleteAction.GetDeleteOptions()
		require.NotNil(t, options.Preconditions)
		require.NotNil(t, options.Preconditions.UID)
		require.Equal(t, uid, *options.Preconditions.UID)
	}
	require.True(t, found, "Pod %s was not deleted", name)
}

type requiredStatefulSetPodCheckpointStore struct {
	cleanupComponentStore
	beforeCheckpointCAS func(map[string]interface{})
}

func (s *requiredStatefulSetPodCheckpointStore) CompareAndSwapWithConditions(
	ctx context.Context,
	entity datastore.Entity,
	conditions map[string]interface{},
	updates map[string]interface{},
) (bool, error) {
	if s.beforeCheckpointCAS != nil {
		hook := s.beforeCheckpointCAS
		s.beforeCheckpointCAS = nil
		hook(updates)
	}
	return s.cleanupComponentStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}
