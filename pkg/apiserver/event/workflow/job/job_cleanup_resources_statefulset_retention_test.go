package job

import (
	"context"
	"encoding/json"
	"errors"
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
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestCleanupResourcesJobCtlRetainsAllStatefulSetPVCsBeforeRequiredDeletion(t *testing.T) {
	tests := []struct {
		name             string
		internalInfo     func(*testing.T) string
		wantDataDeleted  bool
		wantPVCDeleteOps int
	}{
		{
			name: "v2 rebuild retains every claim",
			internalInfo: func(*testing.T) string {
				return versionUpdateRequireStatefulSetDeletionInternalInfo()
			},
		},
		{
			name: "v3 rebuild deletes only planned claim",
			internalInfo: func(t *testing.T) string {
				return versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
			},
			wantDataDeleted:  true,
			wantPVCDeleteOps: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			statefulSetUID := types.UID("statefulset-uid")
			dataPVCName := "data-" + statefulSetName + "-0"
			cachePVCName := "cache-" + statefulSetName + "-0"
			controller := true
			statefulSet := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: statefulSetName, Namespace: component.Namespace, UID: statefulSetUID,
					ResourceVersion: "11", Generation: 1,
				},
				Spec: appsv1.StatefulSetSpec{
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
					},
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
						{ObjectMeta: metav1.ObjectMeta{Name: "data"}},
						{ObjectMeta: metav1.ObjectMeta{Name: "cache"}},
					},
				},
				Status: appsv1.StatefulSetStatus{ObservedGeneration: 1},
			}
			client := fake.NewSimpleClientset(
				statefulSet,
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
					Name: dataPVCName, Namespace: component.Namespace, UID: types.UID("data-pvc-uid"), ResourceVersion: "21",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1", Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID, Controller: &controller,
					}},
				}},
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
					Name: cachePVCName, Namespace: component.Namespace, ResourceVersion: "22",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1", Kind: "Pod", Name: statefulSetName + "-0", UID: types.UID("pod-uid"), Controller: &controller,
					}},
				}},
			)

			statefulSetResource := appsv1.SchemeGroupVersion.WithResource("statefulsets")
			pvcResource := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
			client.Fake.PrependReactor("update", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
				updateAction := action.(k8stesting.UpdateAction)
				updated := updateAction.GetObject().(*appsv1.StatefulSet).DeepCopy()
				updated.Generation = 2
				updated.Status.ObservedGeneration = 2
				if err := client.Tracker().Update(statefulSetResource, updated, component.Namespace); err != nil {
					return true, nil, err
				}
				return true, updated, nil
			})
			client.Fake.PrependReactor("delete", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				obj, err := client.Tracker().Get(statefulSetResource, component.Namespace, statefulSetName)
				if err != nil {
					return true, nil, err
				}
				current := obj.(*appsv1.StatefulSet)
				unsafe := !statefulSetPVCRetentionIsRetain(current)
				for _, pvcName := range []string{dataPVCName, cachePVCName} {
					pvcObj, getErr := client.Tracker().Get(pvcResource, component.Namespace, pvcName)
					if getErr != nil {
						return true, nil, getErr
					}
					unsafe = unsafe || statefulSetPVCDeletionOwnerReferencePresent(current, pvcObj.(*corev1.PersistentVolumeClaim))
				}
				if unsafe {
					for _, pvcName := range []string{dataPVCName, cachePVCName} {
						if err := client.Tracker().Delete(pvcResource, component.Namespace, pvcName); err != nil {
							return true, nil, err
						}
					}
				}
				return false, nil, nil
			})

			internalInfo := tt.internalInfo(t)
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
				InternalInfo: internalInfo, Timeout: 2,
			}
			ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
			require.NotNil(t, ctl)

			require.NoError(t, ctl.Run(ctx))
			require.Equal(t, 1, countClientActions(client, "update", "statefulsets"))
			require.Equal(t, 2, countClientActions(client, "patch", "persistentvolumeclaims"))
			requirePVCOwnerReferencePatchesUseResourceVersion(t, client)
			requireStatefulSetDeleteUsesOrphanPropagation(t, client, statefulSetName, statefulSetUID, "11")
			require.Equal(t, tt.wantPVCDeleteOps, countClientActions(client, "delete", "persistentvolumeclaims"))
			dataPVC, dataErr := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, dataPVCName, metav1.GetOptions{})
			if tt.wantDataDeleted {
				require.True(t, k8serrors.IsNotFound(dataErr))
			} else {
				require.NoError(t, dataErr)
				require.Empty(t, dataPVC.OwnerReferences)
			}
			cachePVC, cacheErr := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, cachePVCName, metav1.GetOptions{})
			require.NoError(t, cacheErr)
			require.Empty(t, cachePVC.OwnerReferences)
		})
	}
}

func TestCleanupResourcesJobCtlRetriesAndIdempotentlyAppliesStatefulSetPVCRetention(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	client := fake.NewSimpleClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: statefulSetName, Namespace: component.Namespace},
		Spec: appsv1.StatefulSetSpec{PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		}},
	})
	updateAttempts := 0
	client.Fake.PrependReactor("update", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		updateAttempts++
		if updateAttempts == 1 {
			return true, nil, k8serrors.NewConflict(
				schema.GroupResource{Group: "apps", Resource: "statefulsets"},
				statefulSetName,
				errors.New("conflict"),
			)
		}
		return false, nil, nil
	})
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 2,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	ref, err := requiredStatefulSetCleanupRef(component)
	require.NoError(t, err)

	require.NoError(t, ctl.ensureStatefulSetPVCRetention(ctx, ref))
	require.Equal(t, 2, updateAttempts)
	updated, err := client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	require.NoError(t, err)
	require.True(t, statefulSetPVCRetentionIsRetain(updated))

	require.NoError(t, ctl.ensureStatefulSetPVCRetention(ctx, ref))
	require.Equal(t, 2, updateAttempts, "already-retained StatefulSet must not be updated again")
}

func TestCleanupResourcesJobCtlTreatsMissingStatefulSetAsRetentionConverged(t *testing.T) {
	component := &model.ApplicationComponent{Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob}
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
	}
	client := fake.NewSimpleClientset()
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	ref, err := requiredStatefulSetCleanupRef(component)
	require.NoError(t, err)

	require.NoError(t, ctl.ensureStatefulSetPVCRetention(context.Background(), ref))
	require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
}

func TestCleanupResourcesJobCtlClearsStatefulSetPVCOwnerReferencesWithoutLivePods(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSetUID := types.UID("statefulset-uid")
	controller := true
	dataPVCName := "data-" + statefulSetName + "-0"
	cachePVCName := "cache-" + statefulSetName + "-7"
	unrelatedStatefulSetOwner := metav1.OwnerReference{Kind: "StatefulSet", Name: "other", UID: types.UID("other-statefulset")}
	unrelatedPodOwner := metav1.OwnerReference{Kind: "Pod", Name: "other-7", UID: types.UID("other-pod")}
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: statefulSetName, Namespace: component.Namespace, UID: statefulSetUID, Generation: 2},
			Spec: appsv1.StatefulSetSpec{
				Replicas: int32Ptr(1),
				PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
					{ObjectMeta: metav1.ObjectMeta{Name: "data"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "cache"}},
				},
			},
			Status: appsv1.StatefulSetStatus{ObservedGeneration: 2},
		},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: dataPVCName, Namespace: component.Namespace, ResourceVersion: "31",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID, Controller: &controller},
				unrelatedStatefulSetOwner,
			},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: cachePVCName, Namespace: component.Namespace, ResourceVersion: "32",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Pod", Name: statefulSetName + "-7", UID: types.UID("scaled-down-pod"), Controller: &controller},
				unrelatedPodOwner,
			},
		}},
	)
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	ref, err := requiredStatefulSetCleanupRef(component)
	require.NoError(t, err)

	require.NoError(t, ctl.ensureStatefulSetPVCRetention(ctx, ref))
	require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
	require.Equal(t, 2, countClientActions(client, "patch", "persistentvolumeclaims"))
	requirePVCOwnerReferencePatchesUseResourceVersion(t, client)
	require.Equal(t, 0, countClientActions(client, "delete", "pods"), "scaled-down ordinal has no Pod to delete")
	dataPVC, err := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, dataPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []metav1.OwnerReference{unrelatedStatefulSetOwner}, dataPVC.OwnerReferences)
	cachePVC, err := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, cachePVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []metav1.OwnerReference{unrelatedPodOwner}, cachePVC.OwnerReferences)
}

func TestCleanupResourcesJobCtlFailsClosedWhenStatefulSetPVCOwnerReferencePatchFails(t *testing.T) {
	tests := []struct {
		name            string
		patchError      func(string) error
		wantError       string
		wantStatus      config.Status
		wantMinAttempts int
	}{
		{
			name: "conflict is retried until timeout",
			patchError: func(name string) error {
				return k8serrors.NewConflict(
					schema.GroupResource{Resource: "persistentvolumeclaims"},
					name,
					errors.New("owner-reference patch conflict"),
				)
			},
			wantError:       "owner-reference patch conflict",
			wantStatus:      config.StatusTimeout,
			wantMinAttempts: 2,
		},
		{
			name: "forbidden fails immediately",
			patchError: func(name string) error {
				return k8serrors.NewForbidden(
					schema.GroupResource{Resource: "persistentvolumeclaims"},
					name,
					errors.New("owner-reference patch forbidden"),
				)
			},
			wantError:       "owner-reference patch forbidden",
			wantStatus:      config.StatusFailed,
			wantMinAttempts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			statefulSetUID := types.UID("statefulset-uid")
			controller := true
			pvcName := "data-" + statefulSetName + "-0"
			client := fake.NewSimpleClientset(
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: statefulSetName, Namespace: component.Namespace, UID: statefulSetUID, Generation: 2},
					Spec: appsv1.StatefulSetSpec{
						PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
							WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
							WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						},
						VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
					},
					Status: appsv1.StatefulSetStatus{ObservedGeneration: 2},
				},
				&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
					Name: pvcName, Namespace: component.Namespace, ResourceVersion: "41",
					OwnerReferences: []metav1.OwnerReference{{
						Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID, Controller: &controller,
					}},
				}},
			)
			patchAttempts := 0
			client.Fake.PrependReactor("patch", "persistentvolumeclaims", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
				patchAttempts++
				return true, nil, tt.patchError(action.(k8stesting.PatchAction).GetName())
			})
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
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantError)
			require.Equal(t, tt.wantStatus, task.Status)
			require.GreaterOrEqual(t, patchAttempts, tt.wantMinAttempts)
			requirePVCOwnerReferencePatchesUseResourceVersion(t, client)
			require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
			require.Equal(t, 0, countClientActions(client, "delete", "pods"))
			require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
			_, getErr := client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
			require.NoError(t, getErr)
			pvc, getErr := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.NotEmpty(t, pvc.OwnerReferences)
		})
	}
}

func TestCleanupResourcesJobCtlBlocksExtraProtectedLabeledStatefulSetBeforeAnyDelete(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	componentLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	extraLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
		config.LabelShareName:     "shared-mysql",
		config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
	}
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName, Namespace: component.Namespace, Labels: componentLabels,
		}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-shared-mysql", Namespace: component.Namespace, Labels: extraLabels,
		}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: "mysql-config", Namespace: component.Namespace, Labels: componentLabels,
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

	require.Error(t, err)
	require.Contains(t, err.Error(), "required StatefulSet deletion blocked")
	require.Contains(t, err.Error(), "default/legacy-shared-mysql")
	require.Equal(t, config.StatusFailed, task.Status)
	for _, action := range client.Actions() {
		require.NotEqual(t, "delete", action.GetVerb(), "preflight must finish before deleting %s", action.GetResource().Resource)
	}
	_, err = client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.AppsV1().StatefulSets(component.Namespace).Get(ctx, "legacy-shared-mysql", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().ConfigMaps(component.Namespace).Get(ctx, "mysql-config", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlRechecksRequiredStatefulSetShareLabelsAfterRetention(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	client := fake.NewSimpleClientset(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName, Namespace: component.Namespace,
			UID: types.UID("statefulset-uid"), ResourceVersion: "61",
		},
		Spec: appsv1.StatefulSetSpec{PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		}},
	})
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))

	statefulSetResource := appsv1.SchemeGroupVersion.WithResource("statefulsets")
	getAfterPreflight := 0
	client.Fake.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		getAfterPreflight++
		if getAfterPreflight != 2 {
			return false, nil, nil
		}
		obj, err := client.Tracker().Get(statefulSetResource, component.Namespace, statefulSetName)
		if err != nil {
			return true, nil, err
		}
		statefulSet := obj.(*appsv1.StatefulSet).DeepCopy()
		statefulSet.ResourceVersion = "62"
		statefulSet.Labels = map[string]string{
			config.LabelShareName:     "late-shared-mysql",
			config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
		}
		if err := client.Tracker().Update(statefulSetResource, statefulSet, component.Namespace); err != nil {
			return true, nil, err
		}
		return true, statefulSet, nil
	})

	err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSetName)

	require.Error(t, err)
	require.Contains(t, err.Error(), "required StatefulSet deletion blocked")
	require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
	statefulSet, err := client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "late-shared-mysql", statefulSet.Labels[config.LabelShareName])
}

func TestCleanupResourcesJobCtlFailsClosedIfRequiredStatefulSetChangesAfterRetention(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*appsv1.StatefulSet)
		want   string
	}{
		{
			name: "replacement has a different UID",
			mutate: func(statefulSet *appsv1.StatefulSet) {
				statefulSet.UID = types.UID("replacement-statefulset")
				statefulSet.ResourceVersion = "72"
			},
			want: "changed after PVC retention convergence",
		},
		{
			name: "new generation is not observed",
			mutate: func(statefulSet *appsv1.StatefulSet) {
				statefulSet.ResourceVersion = "73"
				statefulSet.Generation = 2
				statefulSet.Status.ObservedGeneration = 1
			},
			want: "PVC retention is not observed as Retain/Retain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			client := fake.NewSimpleClientset(&appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: statefulSetName, Namespace: component.Namespace,
					UID: types.UID("statefulset-uid"), ResourceVersion: "71", Generation: 1,
				},
				Spec: appsv1.StatefulSetSpec{PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				}},
				Status: appsv1.StatefulSetStatus{ObservedGeneration: 1},
			})
			job := &model.JobTask{
				Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
				InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
			}
			ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
			require.NotNil(t, ctl)
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))

			statefulSetResource := appsv1.SchemeGroupVersion.WithResource("statefulsets")
			getAfterPreflight := 0
			client.Fake.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
				getAfterPreflight++
				if getAfterPreflight != 2 {
					return false, nil, nil
				}
				obj, err := client.Tracker().Get(statefulSetResource, component.Namespace, statefulSetName)
				if err != nil {
					return true, nil, err
				}
				statefulSet := obj.(*appsv1.StatefulSet).DeepCopy()
				tt.mutate(statefulSet)
				if err := client.Tracker().Update(statefulSetResource, statefulSet, component.Namespace); err != nil {
					return true, nil, err
				}
				return true, statefulSet, nil
			})

			err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSetName)

			require.Error(t, err)
			require.True(t, k8serrors.IsConflict(err))
			require.Contains(t, err.Error(), tt.want)
			require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
		})
	}
}

func TestCleanupResourcesJobCtlRemembersMarkerTemplatesWhenStatefulSetAlreadyMissing(t *testing.T) {
	ctx := context.Background()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	pvcName := "retired-" + statefulSetName + "-7"
	controller := true
	unrelatedOwner := metav1.OwnerReference{Kind: "ConfigMap", Name: "keep-me", UID: types.UID("config-uid")}
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: pvcName, Namespace: component.Namespace, ResourceVersion: "51",
		OwnerReferences: []metav1.OwnerReference{
			{Kind: "Pod", Name: statefulSetName + "-7", UID: types.UID("old-pod"), Controller: &controller},
			unrelatedOwner,
		},
	}})
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "retired"), Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
	ref, err := requiredStatefulSetCleanupRef(component)
	require.NoError(t, err)
	target, ok := ctl.statefulSetRetentionTargets[ref.namespace+"/"+ref.name]
	require.True(t, ok)
	require.Contains(t, target.templates, "retired")

	gone, err := ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone, "owner-reference update requires one confirming poll")
	gone, err = ctl.componentPodsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
	require.Equal(t, 1, countClientActions(client, "patch", "persistentvolumeclaims"))
	requirePVCOwnerReferencePatchesUseResourceVersion(t, client)
	pvc, err := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []metav1.OwnerReference{unrelatedOwner}, pvc.OwnerReferences)
}

func TestCleanupResourcesJobCtlProtectsPodsForEveryRequiredStatefulSetDeletion(t *testing.T) {
	tests := []struct {
		name      string
		podLabels map[string]string
		ownerJob  *batchv1.Job
		ownerRefs []metav1.OwnerReference
		want      string
	}{
		{
			name: "protected pod",
			podLabels: map[string]string{
				config.LabelAppID: "app-1", config.LabelComponentName: "mysql",
				config.LabelShareName: "shared-mysql", config.LabelShareStrategy: string(domainspec.ShareStrategyDefault),
			},
			want: "pod default/mysql-0 is protected",
		},
		{
			name: "protected owner job",
			podLabels: map[string]string{
				config.LabelAppID: "app-1", config.LabelComponentName: "mysql",
			},
			ownerJob: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "mysql-owner", Namespace: "default",
				UID: types.UID("mysql-owner-uid"), ResourceVersion: "21",
				Labels: map[string]string{
					config.LabelShareName: "shared-mysql-owner", config.LabelShareStrategy: string(domainspec.ShareStrategyIgnore),
				},
			}},
			ownerRefs: []metav1.OwnerReference{{Kind: "Job", Name: "mysql-owner", UID: types.UID("mysql-owner-uid")}},
			want:      "owner job mysql-owner is protected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component := &model.ApplicationComponent{Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob}
			client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "mysql-0", Namespace: component.Namespace,
				UID: types.UID("mysql-pod-uid"), ResourceVersion: "31",
				Labels: tt.podLabels, OwnerReferences: tt.ownerRefs,
			}})
			if tt.ownerJob != nil {
				require.NoError(t, client.Tracker().Add(tt.ownerJob))
			}
			job := &model.JobTask{
				Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
				InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 1,
			}
			ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
			require.NotNil(t, ctl)

			preflightErr := ctl.ensureRequiredStatefulSetPodDeletionAllowed(context.Background(), component)
			require.Error(t, preflightErr)
			require.Contains(t, preflightErr.Error(), "required StatefulSet deletion blocked")
			require.Contains(t, preflightErr.Error(), tt.want)

			gone, waitErr := ctl.componentPodsGone(context.Background(), component)
			require.False(t, gone)
			require.Error(t, waitErr)
			require.Contains(t, waitErr.Error(), "required StatefulSet deletion blocked")
			require.Contains(t, waitErr.Error(), tt.want)
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb(), "protected Pod/owner must not be deleted")
			}
		})
	}
}

func requireStatefulSetDeleteUsesOrphanPropagation(t *testing.T, client *fake.Clientset, name string, uid types.UID, resourceVersion string) {
	t.Helper()
	found := false
	for _, action := range client.Actions() {
		if action.GetVerb() != "delete" || action.GetResource().Resource != "statefulsets" {
			continue
		}
		deleteAction, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		if deleteAction.GetName() != name {
			continue
		}
		found = true
		options := deleteAction.GetDeleteOptions()
		require.NotNil(t, options.PropagationPolicy)
		require.Equal(t, metav1.DeletePropagationOrphan, *options.PropagationPolicy)
		require.NotNil(t, options.Preconditions)
		require.NotNil(t, options.Preconditions.UID)
		require.Equal(t, uid, *options.Preconditions.UID)
		require.NotNil(t, options.Preconditions.ResourceVersion)
		require.Equal(t, resourceVersion, *options.Preconditions.ResourceVersion)
	}
	require.True(t, found, "StatefulSet %s was not deleted", name)
}

func requirePVCOwnerReferencePatchesUseResourceVersion(t *testing.T, client *fake.Clientset) {
	t.Helper()
	found := false
	for _, action := range client.Actions() {
		if action.GetVerb() != "patch" || action.GetResource().Resource != "persistentvolumeclaims" {
			continue
		}
		patchAction, ok := action.(k8stesting.PatchAction)
		require.True(t, ok)
		require.Equal(t, types.MergePatchType, patchAction.GetPatchType())
		var payload struct {
			Metadata struct {
				ResourceVersion string                  `json:"resourceVersion"`
				OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
			} `json:"metadata"`
		}
		require.NoError(t, json.Unmarshal(patchAction.GetPatch(), &payload))
		require.NotEmpty(t, payload.Metadata.ResourceVersion)
		require.NotNil(t, payload.Metadata.OwnerReferences)
		found = true
	}
	require.True(t, found, "PVC owner-reference patch was not issued")
}
