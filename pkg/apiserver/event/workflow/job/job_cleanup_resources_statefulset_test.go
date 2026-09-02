package job

import (
	"context"

	"errors"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestCleanupResourcesJobCtlEnforcesRequiredStatefulSetDeletion(t *testing.T) {
	tests := []struct {
		name          string
		shareStrategy spec.ShareStrategy
		wantBlocked   bool
	}{
		{name: "live default share labels block before deletion", shareStrategy: spec.ShareStrategyDefault, wantBlocked: true},
		{name: "live force share labels allow deletion", shareStrategy: spec.ShareStrategyForce},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				ID:            1,
				Name:          "mysql",
				AppID:         "app-1",
				Namespace:     "default",
				ComponentType: config.StoreJob,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			templatePVCName := "data-" + statefulSetName + "-0"
			shareLabels := map[string]string{
				config.LabelAppID:         component.AppID,
				config.LabelComponentName: component.Name,
				config.LabelShareName:     "legacy-shared-mysql",
				config.LabelShareStrategy: string(tt.shareStrategy),
			}
			client := fake.NewSimpleClientset(
				&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
					Name: statefulSetName, Namespace: component.Namespace, UID: types.UID("statefulset-uid"), Labels: shareLabels,
				}},
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
					Name: "mysql-config", Namespace: component.Namespace,
					Labels: map[string]string{
						config.LabelAppID:         component.AppID,
						config.LabelComponentName: component.Name,
					},
				}},
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
					Name: templatePVCName, Namespace: component.Namespace,
				}},
			)
			internalInfo := versionUpdateRequireStatefulSetDeletionInternalInfo()
			store := &cleanupComponentStore{
				component: component,
				jobInfo: &model.JobInfo{
					ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
					TaskID: "task-1", Status: string(config.StatusQueued), InternalInfo: internalInfo,
					ServiceName: component.Name,
				},
			}
			task := &model.JobTask{
				Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
				TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
				InternalInfo: internalInfo, Timeout: 1,
			}
			ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
			require.NotNil(t, ctl)

			err := ctl.Run(ctx)
			if tt.wantBlocked {
				require.Error(t, err)
				require.Contains(t, err.Error(), "required StatefulSet deletion blocked")
				require.Contains(t, err.Error(), "protected by live share labels")
				require.Equal(t, config.StatusFailed, task.Status)
				require.Nil(t, store.putComponent)
				_, err = client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
				require.NoError(t, err)
				_, err = client.CoreV1().ConfigMaps(component.Namespace).Get(ctx, "mysql-config", metav1.GetOptions{})
				require.NoError(t, err)
				_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, templatePVCName, metav1.GetOptions{})
				require.NoError(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, config.StatusCompleted, task.Status)
			require.NotNil(t, store.putComponent)
			_, err = client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
			require.True(t, k8serrors.IsNotFound(err))
			_, err = client.CoreV1().ConfigMaps(component.Namespace).Get(ctx, "mysql-config", metav1.GetOptions{})
			require.True(t, k8serrors.IsNotFound(err))
			_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, templatePVCName, metav1.GetOptions{})
			require.NoError(t, err)
		})
	}
}

func TestCleanupResourcesJobCtlRetriesAffectedStatefulSetPVCDeletionAndWaitsForNotFound(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "mysql",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	targetNames := []string{
		"data-" + statefulSetName + "-0",
		"data-" + statefulSetName + "-4",
	}
	similarName := "data-" + statefulSetName + "-backup-0"
	standaloneName := "standalone-data"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: targetNames[0], Namespace: component.Namespace, UID: types.UID("target-pvc-0"), ResourceVersion: "1"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: targetNames[1], Namespace: component.Namespace, UID: types.UID("target-pvc-4"), ResourceVersion: "2"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: similarName, Namespace: component.Namespace}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: standaloneName, Namespace: component.Namespace}},
	)
	deleteCalls := make(chan string, len(targetNames)+1)
	client.Fake.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deleteAction := action.(k8stesting.DeleteAction)
		select {
		case deleteCalls <- deleteAction.GetName():
		default:
		}
		return true, nil, nil
	})
	internalInfo := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	store := &cleanupComponentStore{
		component: component,
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: internalInfo,
			ServiceName: component.Name,
		},
	}
	task := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: internalInfo, Timeout: 3,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	result := make(chan error, 1)
	go func() {
		result <- ctl.Run(ctx)
	}()

	deletedNames := make(map[string]struct{}, len(targetNames))
	deadline := time.After(2 * time.Second)
	for len(deletedNames) < len(targetNames) {
		select {
		case name := <-deleteCalls:
			deletedNames[name] = struct{}{}
		case err := <-result:
			require.FailNow(t, "cleanup returned before PVCs disappeared", "error: %v", err)
		case <-deadline:
			require.FailNow(t, "cleanup did not request all target PVC deletions")
		}
	}
	require.ElementsMatch(t, targetNames, mapKeys(deletedNames))
	select {
	case err := <-result:
		require.FailNow(t, "cleanup returned before PVCs disappeared", "error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	pvcResource := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
	for _, name := range targetNames {
		require.NoError(t, client.Tracker().Delete(pvcResource, component.Namespace, name))
	}
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "cleanup did not finish after target PVCs disappeared")
	}
	require.Equal(t, config.StatusCompleted, task.Status)
	for _, name := range targetNames {
		_, err := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, name, metav1.GetOptions{})
		require.True(t, k8serrors.IsNotFound(err))
	}
	_, err := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, similarName, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, standaloneName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlDeletesStatefulSetPVCsAfterWorkloadAndPods(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	templatePVCName := "data-" + statefulSetName + "-0"
	componentLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName, Namespace: component.Namespace, UID: types.UID("statefulset-uid"),
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "mysql-0", Namespace: component.Namespace, UID: types.UID("mysql-pod-uid"), ResourceVersion: "1", Labels: componentLabels,
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: component.Namespace, UID: types.UID("template-pvc-uid"), ResourceVersion: "1"}},
	)
	internalInfo := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	store := &cleanupComponentStore{
		component: component,
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusQueued), InternalInfo: internalInfo,
			ServiceName: component.Name,
		},
	}
	task := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: internalInfo, Timeout: 3,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))

	deleteIndexes := map[string]int{}
	firstPodListIndex := -1
	for index, action := range client.Actions() {
		if action.GetVerb() == "list" && action.GetResource().Resource == "pods" && firstPodListIndex < 0 {
			firstPodListIndex = index
		}
		if action.GetVerb() != "delete" {
			continue
		}
		deleteAction, ok := action.(k8stesting.DeleteAction)
		if ok {
			deleteIndexes[action.GetResource().Resource+"/"+deleteAction.GetName()] = index
		}
	}
	require.Contains(t, deleteIndexes, "statefulsets/"+statefulSetName)
	require.Contains(t, deleteIndexes, "pods/mysql-0")
	require.Contains(t, deleteIndexes, "persistentvolumeclaims/"+templatePVCName)
	statefulSetDelete := deleteIndexes["statefulsets/"+statefulSetName]
	podDelete := deleteIndexes["pods/mysql-0"]
	pvcDelete := deleteIndexes["persistentvolumeclaims/"+templatePVCName]
	require.NotEqual(t, -1, firstPodListIndex)
	require.Positive(t, pvcDelete)
	require.Less(t, firstPodListIndex, statefulSetDelete)
	require.Less(t, statefulSetDelete, pvcDelete)
	require.Less(t, podDelete, pvcDelete)
}

func TestCleanupResourcesJobCtlBlocksStatefulSetPVCDeletionForProtectedPVC(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	templatePVCName := "data-" + statefulSetName + "-0"
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: templatePVCName, Namespace: component.Namespace, UID: types.UID("protected-pvc-uid"),
		Labels: map[string]string{
			config.LabelShareName:     "shared-mysql-data",
			config.LabelShareStrategy: string(spec.ShareStrategyDefault),
		},
	}})
	internalInfo := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	store := &cleanupComponentStore{
		component: component,
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusQueued), InternalInfo: internalInfo,
			ServiceName: component.Name,
		},
	}
	task := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: internalInfo, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)

	require.Error(t, err)
	require.Contains(t, err.Error(), "required StatefulSet PVC deletion blocked")
	_, getErr := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, templatePVCName, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
}

func TestCleanupResourcesJobCtlPreflightsProtectedPodsBeforeAnyDelete(t *testing.T) {
	tests := []struct {
		name          string
		podLabels     map[string]string
		ownerJob      *batchv1.Job
		ownerRefs     []metav1.OwnerReference
		wantErrDetail string
	}{
		{
			name: "pod labels",
			podLabels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: "mysql",
				config.LabelShareName:     "shared-mysql",
				config.LabelShareStrategy: string(spec.ShareStrategyIgnore),
			},
			wantErrDetail: "pod default/mysql-0 is protected",
		},
		{
			name: "owner job labels",
			podLabels: map[string]string{
				config.LabelAppID:         "app-1",
				config.LabelComponentName: "mysql",
			},
			ownerJob: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "mysql-owner", Namespace: "default",
				UID: types.UID("mysql-owner-uid"), ResourceVersion: "21",
				Labels: map[string]string{
					config.LabelShareName:     "shared-mysql-owner",
					config.LabelShareStrategy: string(spec.ShareStrategyDefault),
				},
			}},
			ownerRefs: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "mysql-owner", UID: types.UID("mysql-owner-uid")},
			},
			wantErrDetail: "owner job mysql-owner is protected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			templatePVCName := "data-" + statefulSetName + "-0"
			objects := []k8sruntime.Object{
				&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: statefulSetName, Namespace: component.Namespace}},
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: component.Namespace}},
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name: "mysql-0", Namespace: component.Namespace,
					UID: types.UID("mysql-pod-uid"), ResourceVersion: "31",
					Labels: tt.podLabels, OwnerReferences: tt.ownerRefs,
				}},
			}
			if tt.ownerJob != nil {
				objects = append(objects, tt.ownerJob)
			}
			client := fake.NewSimpleClientset(objects...)
			internalInfo := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
			store := &cleanupComponentStore{
				component: component,
				jobInfo: &model.JobInfo{
					ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
					TaskID: "task-1", Status: string(config.StatusQueued), InternalInfo: internalInfo,
					ServiceName: component.Name,
				},
			}
			task := &model.JobTask{
				Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
				TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
				InternalInfo: internalInfo, Timeout: 1,
			}
			ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
			require.NotNil(t, ctl)

			err := ctl.Run(ctx)

			require.Error(t, err)
			require.Contains(t, err.Error(), "required StatefulSet deletion blocked")
			require.Contains(t, err.Error(), tt.wantErrDetail)
			require.Equal(t, config.StatusFailed, task.Status)
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb(), "unexpected delete action for %s", action.GetResource().Resource)
			}
		})
	}
}

func TestCleanupResourcesJobCtlRechecksProtectedPodsBeforePVCDeletion(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	templatePVCName := "data-" + statefulSetName + "-0"
	componentLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName, Namespace: component.Namespace, UID: types.UID("statefulset-uid"),
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: component.Namespace, UID: types.UID("template-pvc-uid"), ResourceVersion: "1"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "mysql-0", Namespace: component.Namespace,
			UID: types.UID("mysql-pod-uid"), ResourceVersion: "31", Labels: componentLabels,
		}},
	)
	client.Fake.PrependReactor("delete", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		podResource := corev1.SchemeGroupVersion.WithResource("pods")
		obj, err := client.Tracker().Get(podResource, component.Namespace, "mysql-0")
		if err != nil {
			return true, nil, err
		}
		pod := obj.(*corev1.Pod).DeepCopy()
		pod.Labels[config.LabelShareName] = "late-shared-mysql"
		pod.Labels[config.LabelShareStrategy] = string(spec.ShareStrategyDefault)
		if err := client.Tracker().Update(podResource, pod, component.Namespace); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	internalInfo := versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
	store := &cleanupComponentStore{
		component: component,
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusQueued), InternalInfo: internalInfo,
			ServiceName: component.Name,
		},
	}
	task := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: internalInfo, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	err := ctl.Run(ctx)

	require.Error(t, err)
	require.Contains(t, err.Error(), "required StatefulSet deletion blocked")
	require.Contains(t, err.Error(), "pod default/mysql-0 is protected")
	require.Equal(t, 1, countClientActions(client, "delete", "statefulsets"))
	require.Equal(t, 0, countClientActions(client, "delete", "pods"))
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
}

func TestCleanupResourcesJobCtlRejectsUnversionedStatefulSetPVCDeletionMarker(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `","requireStatefulSetDeletion":true,"statefulSetPVCTemplatesToDelete":["data"]}`,
	}
	ctl := NewCleanupResourcesJobCtl(job, fake.NewSimpleClientset(), &noopStore{}, nil)
	require.NotNil(t, ctl)

	err := ctl.ensureRequiredStatefulSetDeletionAllowed(context.Background(), component)

	require.Error(t, err)
	require.Contains(t, err.Error(), "marker is missing version")
}

func TestCleanupResourcesJobCtlDoesNotSilentlySkipRequiredStatefulSetDeletion(t *testing.T) {
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	client := fake.NewSimpleClientset(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: statefulSetName, Namespace: component.Namespace,
		Labels: map[string]string{
			config.LabelShareName:     "shared-mysql",
			config.LabelShareStrategy: string(spec.ShareStrategyDefault),
		},
	}})
	task := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(),
	}
	ctl := NewCleanupResourcesJobCtl(task, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	deleted := cleanupResourceSet{seen: make(map[string]struct{})}
	deleteCalled := false

	ctl.deleteTrackedResource(context.Background(), &deleted, spec.ResourceStatefulSet, component.Namespace, statefulSetName, false, func(context.Context) error {
		deleteCalled = true
		return nil
	})

	require.False(t, deleteCalled)
	err := errors.Join(deleted.errs...)
	require.Error(t, err)
	require.Contains(t, err.Error(), "required StatefulSet deletion blocked")
}

func TestCleanupResourcesJobCtlPreservesNestedStoragePVCs(t *testing.T) {
	ctx := context.Background()
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)

	traits, err := model.NewJSONStructByStruct(spec.Traits{
		Init: []spec.InitTraitSpec{{
			Name:  "migrate",
			Image: "busybox:1.36",
			Traits: spec.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "init-data",
					Type:      "persistent",
					MountPath: "/init-data",
					Size:      "1Gi",
				}},
			},
		}},
		Sidecar: []spec.SidecarTraitsSpec{{
			Name:  "logger",
			Image: "busybox:1.36",
			Traits: spec.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "sidecar-data",
					Type:      "persistent",
					MountPath: "/sidecar-data",
					Size:      "1Gi",
				}},
			},
		}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "web",
		AppID:         "app-reset",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits:        traits,
	}
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: buildWebServiceName(component.Name, component.AppID), Namespace: component.Namespace}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "init-data", Namespace: component.Namespace}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "sidecar-data", Namespace: component.Namespace}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, "init-data", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, "sidecar-data", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlPreservesExplicitClaimNamePVC(t *testing.T) {
	ctx := context.Background()
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)

	traits, err := model.NewJSONStructByStruct(spec.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:      "logs",
			Type:      "persistent",
			MountPath: "/logs",
			ClaimName: "shared-logs",
		}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "web",
		AppID:         "app-reset",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits:        traits,
	}
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: buildWebServiceName(component.Name, component.AppID), Namespace: component.Namespace}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "shared-logs", Namespace: component.Namespace}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, "shared-logs", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlDeletesStatefulSetWithoutForcingTemplatePVCRetention(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "mysql",
		AppID:         "app-reset",
		Namespace:     "default",
		ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: statefulSetName, Namespace: component.Namespace},
		Spec: appsv1.StatefulSetSpec{
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
			}},
		},
	}
	templatePVCName := "data-" + statefulSetName + "-0"
	client := fake.NewSimpleClientset(
		statefulSet,
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: component.Namespace}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	_, err := client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, templatePVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
	require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
	require.Equal(t, 0, countClientActions(client, "update", "persistentvolumeclaims"))
}
