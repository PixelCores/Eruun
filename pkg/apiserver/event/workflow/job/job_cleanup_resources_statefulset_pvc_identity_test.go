package job

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
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
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestCleanupResourcesJobCtlRejectsPVCCheckpointWriteAfterExecutionTakeover(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	ref, err := requiredStatefulSetCleanupRef(component)
	require.NoError(t, err)
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	oldExecutionKey := "task-1:cleanup:mysql:g1:a1"
	newExecutionKey := "task-1:cleanup:mysql:g2:a1"
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
		ExecutionKey: &newExecutionKey, RunGeneration: 2, Attempt: 1,
	}}
	job := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: marker, Status: config.StatusRunning,
		ExecutionKey: oldExecutionKey, RunGeneration: 1, Attempt: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)
	ctl.requiredStatefulSetPVCTarget = &requiredStatefulSetPVCDeletionTarget{
		ref: ref, templates: []string{"data"},
		pvcUIDs: map[string]types.UID{"data-" + ref.name + "-0": types.UID("pvc-uid")},
	}

	err = ctl.persistRequiredStatefulSetPVCTarget(ctx)
	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)
	require.True(t, ctl.skipSaveInfo)
	require.Nil(t, store.casJobInfo)
	require.Equal(t, marker, store.jobInfo.InternalInfo)
	require.Equal(t, uint64(2), store.jobInfo.RunGeneration)
	require.NotNil(t, store.jobInfo.ExecutionKey)
	require.Equal(t, newExecutionKey, *store.jobInfo.ExecutionKey)
}

func TestCleanupResourcesJobCtlPVCDeletePreconditionsRejectLateProtection(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace,
		UID: types.UID("original-pvc-uid"), ResourceVersion: "1",
	}}
	client := fake.NewSimpleClientset(pvc)
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data"), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	require.NoError(t, ctl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component))

	client.Fake.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deleteAction := action.(k8stesting.DeleteAction)
		options := deleteAction.GetDeleteOptions()
		require.NotNil(t, options.Preconditions)
		require.NotNil(t, options.Preconditions.UID)
		require.Equal(t, pvc.UID, *options.Preconditions.UID)
		require.NotNil(t, options.Preconditions.ResourceVersion)
		require.Equal(t, pvc.ResourceVersion, *options.Preconditions.ResourceVersion)

		currentObject, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), component.Namespace, pvc.Name)
		require.NoError(t, err)
		current := currentObject.(*corev1.PersistentVolumeClaim).DeepCopy()
		current.Labels = map[string]string{
			config.LabelShareName:     "late-shared-data",
			config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
		}
		current.ResourceVersion = "2"
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), current, component.Namespace))
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Resource: "persistentvolumeclaims"},
			pvc.Name,
			fmt.Errorf("resourceVersion precondition no longer matches"),
		)
	})

	gone, err := ctl.requiredStatefulSetPVCsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	current, getErr := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	_, protected := cleanupResourceShareProtected(current.Labels)
	require.True(t, protected)
}

func TestCleanupResourcesJobCtlPVCDeleteNotFoundRejectsSameNameReplacement(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace,
		UID: types.UID("original-pvc-uid"), ResourceVersion: "1",
	}}
	client := fake.NewSimpleClientset(pvc)
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data"), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	require.NoError(t, ctl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component))

	client.Fake.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deleteAction := action.(k8stesting.DeleteAction)
		options := deleteAction.GetDeleteOptions()
		require.NotNil(t, options.Preconditions)
		require.NotNil(t, options.Preconditions.UID)
		require.Equal(t, pvc.UID, *options.Preconditions.UID)
		require.NotNil(t, options.Preconditions.ResourceVersion)
		require.Equal(t, pvc.ResourceVersion, *options.Preconditions.ResourceVersion)
		require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), component.Namespace, pvc.Name))
		replacement := pvc.DeepCopy()
		replacement.UID = types.UID("replacement-pvc-uid")
		replacement.ResourceVersion = "2"
		require.NoError(t, client.Tracker().Add(replacement))
		return true, nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, pvc.Name)
	})

	gone, err := ctl.requiredStatefulSetPVCsGone(ctx, component)
	require.False(t, gone)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "PVC UID changed")
	current, getErr := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, types.UID("replacement-pvc-uid"), current.UID)
}

func TestCleanupResourcesJobCtlRejectsMissingPVCDeleteIdentity(t *testing.T) {
	tests := []struct {
		name            string
		uid             types.UID
		resourceVersion string
		wantDetail      string
		failPreflight   bool
	}{
		{name: "empty UID checkpoint", resourceVersion: "1", wantDetail: "empty UID", failPreflight: true},
		{name: "empty live resourceVersion", uid: types.UID("pvc-uid"), wantDetail: "empty resourceVersion"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace,
				UID: tt.uid, ResourceVersion: tt.resourceVersion,
			}}
			client := fake.NewSimpleClientset(pvc)
			marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
			store := &cleanupComponentStore{jobInfo: &model.JobInfo{
				ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
				TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker,
				ServiceName: component.Name,
			}}
			job := &model.JobTask{
				Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
				JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
			}
			ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
			require.NotNil(t, ctl)

			err := ctl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component)
			if tt.failPreflight {
				require.Error(t, err)
				require.True(t, k8serrors.IsConflict(err))
				require.Contains(t, err.Error(), tt.wantDetail)
			} else {
				require.NoError(t, err)
				gone, deleteErr := ctl.requiredStatefulSetPVCsGone(ctx, component)
				require.False(t, gone)
				require.Error(t, deleteErr)
				require.True(t, k8serrors.IsConflict(deleteErr))
				require.Contains(t, deleteErr.Error(), tt.wantDetail)
			}
			require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
		})
	}
}

func TestCleanupResourcesJobCtlRestoresPVCIdentityAndRejectsReplacementOnSameTaskRetry(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace,
		UID: types.UID("original-pvc-uid"), ResourceVersion: "1",
	}}
	client := fake.NewSimpleClientset(pvc)
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	markerWithPodCheckpoint, err := marshalRequiredStatefulSetPodDeletionCheckpoint(marker, requiredStatefulSetPodDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName,
	})
	require.NoError(t, err)
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: markerWithPodCheckpoint,
		ServiceName: component.Name,
	}}
	firstJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: markerWithPodCheckpoint, Status: config.StatusRunning, Timeout: 1,
	}
	firstCtl := NewCleanupResourcesJobCtl(firstJob, client, store, nil)
	require.NotNil(t, firstCtl)
	require.NoError(t, firstCtl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component))
	require.Contains(t, store.jobInfo.InternalInfo, requiredStatefulSetPVCCheckpointKey)
	_, podCheckpointFound, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, podCheckpointFound, "persisting PVC identity must retain the Pod checkpoint sibling")

	require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), component.Namespace, pvc.Name))
	replacement := pvc.DeepCopy()
	replacement.UID = types.UID("replacement-pvc-uid")
	replacement.ResourceVersion = "2"
	require.NoError(t, client.Tracker().Add(replacement))

	retryJob := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	retryCtl := NewCleanupResourcesJobCtl(retryJob, client, store, nil)
	require.NotNil(t, retryCtl)
	err = retryCtl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "PVC UID changed")
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
}

func TestCleanupResourcesJobCtlDoesNotOverwriteConcurrentPodCheckpoint(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace,
		UID: types.UID("pvc-uid"), ResourceVersion: "1",
	}}
	client := fake.NewSimpleClientset(pvc)
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	concurrentInternalInfo, err := marshalRequiredStatefulSetPodDeletionCheckpoint(marker, requiredStatefulSetPodDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName,
	})
	require.NoError(t, err)
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker,
		ServiceName: component.Name,
	}}
	store.beforeConditionalCAS = func() {
		store.jobInfo.InternalInfo = concurrentInternalInfo
	}
	job := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)

	err = ctl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(err))
	checkpointErr := err
	require.Equal(t, concurrentInternalInfo, store.jobInfo.InternalInfo)
	_, podCheckpointFound, parseErr := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, parseErr)
	require.True(t, podCheckpointFound)
	_, pvcCheckpointFound, parseErr := parseRequiredStatefulSetPVCDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, parseErr)
	require.False(t, pvcCheckpointFound)
	newerInternalInfo, err := marshalRequiredStatefulSetPVCDeletionCheckpoint(concurrentInternalInfo, requiredStatefulSetPVCDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSetName, Templates: []string{"data"},
		PVCs: []requiredStatefulSetPVCIdentityCheckpoint{{Name: pvc.Name, UID: pvc.UID}},
	})
	require.NoError(t, err)
	store.beforeConditionalCAS = func() {
		store.jobInfo.InternalInfo = newerInternalInfo
	}
	job.Status = config.StatusFailed
	job.Error = checkpointErr.Error()
	require.NoError(t, ctl.SaveInfo(ctx))
	require.Equal(t, string(config.StatusFailed), store.jobInfo.Status)
	require.Equal(t, newerInternalInfo, store.jobInfo.InternalInfo)
	_, podCheckpointFound, parseErr = parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, parseErr)
	require.True(t, podCheckpointFound)
	_, pvcCheckpointFound, parseErr = parseRequiredStatefulSetPVCDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, parseErr)
	require.True(t, pvcCheckpointFound)
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
}

func TestCleanupResourcesJobCtlPodCheckpointDoesNotOverwriteConcurrentPVCCheckpoint(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, _, client, _ := newRequiredStatefulSetPodController(t)
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	pvcName := "data-" + statefulSet.Name + "-0"
	concurrentInternalInfo, err := marshalRequiredStatefulSetPVCDeletionCheckpoint(marker, requiredStatefulSetPVCDeletionCheckpoint{
		Namespace: component.Namespace, StatefulSetName: statefulSet.Name, Templates: []string{"data"},
		PVCs: []requiredStatefulSetPVCIdentityCheckpoint{{Name: pvcName, UID: types.UID("pvc-uid")}},
	})
	require.NoError(t, err)
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker,
		ServiceName: component.Name,
	}}
	store.beforeConditionalCAS = func() {
		store.jobInfo.InternalInfo = concurrentInternalInfo
	}
	job := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.ensureRequiredStatefulSetPodDeletionAllowed(ctx, component))
	_, pvcCheckpointFound, parseErr := parseRequiredStatefulSetPVCDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, parseErr)
	require.True(t, pvcCheckpointFound)
	_, podCheckpointFound, parseErr := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
	require.NoError(t, parseErr)
	require.True(t, podCheckpointFound)
	require.Equal(t, string(config.StatusRunning), store.jobInfo.Status)
	for _, resource := range []string{"statefulsets", "pods", "persistentvolumeclaims"} {
		for _, verb := range []string{"update", "patch", "delete"} {
			require.Equal(t, 0, countClientActions(client, verb, resource))
		}
	}
}

func TestCleanupResourcesJobCtlBoundsCheckpointChurnDuringSaveInfo(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	marker := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker,
		ServiceName: component.Name,
	}}
	job := &model.JobTask{
		Name: component.Name, AppID: component.AppID, TaskID: "task-1", JobType: string(config.JobCleanupResources),
		JobInfo: component, InternalInfo: marker, Status: config.StatusFailed, Error: "checkpoint conflict",
	}
	ctl := NewCleanupResourcesJobCtl(job, fake.NewSimpleClientset(), store, nil)
	require.NotNil(t, ctl)

	attempts := 0
	var churn func()
	churn = func() {
		attempts++
		next, err := marshalRequiredStatefulSetPodDeletionCheckpoint(marker, requiredStatefulSetPodDeletionCheckpoint{
			Namespace: component.Namespace, StatefulSetName: statefulSetName,
			Pods: []requiredStatefulSetPodIdentityCheckpoint{{
				Name: fmt.Sprintf("%s-%d", statefulSetName, attempts), UID: types.UID(fmt.Sprintf("pod-uid-%d", attempts)),
			}},
		})
		require.NoError(t, err)
		store.jobInfo.InternalInfo = next
		store.beforeConditionalCAS = churn
	}
	store.beforeConditionalCAS = churn

	err := ctl.SaveInfo(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("did not converge after %d attempts", jobInfoSaveMaxAttempts))
	require.Equal(t, jobInfoSaveMaxAttempts, attempts)
	require.Equal(t, string(config.StatusRunning), store.jobInfo.Status)
	require.Equal(t, store.jobInfo.InternalInfo, job.InternalInfo)
}

func TestCleanupResourcesJobCtlRejectsExtraLabelMatchedStatefulSetBeforeMutation(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	componentLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	requiredStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName, Namespace: component.Namespace, UID: types.UID("required-statefulset-uid"),
		ResourceVersion: "1", Labels: componentLabels,
	}}
	extraStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName + "-unexpected", Namespace: component.Namespace, UID: types.UID("extra-statefulset-uid"),
		ResourceVersion: "2", Labels: componentLabels,
	}}
	client := fake.NewSimpleClientset(requiredStatefulSet, extraStatefulSet)
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	store := &cleanupComponentStore{
		component: component,
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusQueued), InternalInfo: marker,
			ServiceName: component.Name,
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
	require.True(t, k8serrors.IsConflict(err))
	require.Contains(t, err.Error(), "is not the required deletion target")
	require.Equal(t, marker, store.jobInfo.InternalInfo, "unexpected target must be rejected before identity checkpoints mutate JobInfo")
	for _, resource := range []string{"statefulsets", "pods", "persistentvolumeclaims"} {
		for _, verb := range []string{"update", "patch", "delete"} {
			require.Equal(t, 0, countClientActions(client, verb, resource), "%s %s must not run before preflight closes", verb, resource)
		}
	}
	_, err = client.AppsV1().StatefulSets(component.Namespace).Get(ctx, requiredStatefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.AppsV1().StatefulSets(component.Namespace).Get(ctx, extraStatefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
}
