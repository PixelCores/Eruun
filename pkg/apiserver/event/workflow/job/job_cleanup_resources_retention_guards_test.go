package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestCleanupResourcesJobCtlPinsStatefulSetUIDAcrossRetentionPolls(t *testing.T) {
	t.Run("replacement fails closed before it is mutated", func(t *testing.T) {
		ctx := context.Background()
		component := &model.ApplicationComponent{
			Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
		}
		statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
		initialUID := types.UID("initial-statefulset")
		replacementUID := types.UID("replacement-statefulset")
		initial := retentionGuardStatefulSet(component.Namespace, statefulSetName, initialUID)
		initial.Generation = 2
		initial.Status.ObservedGeneration = 1
		replacement := retentionGuardStatefulSet(component.Namespace, statefulSetName, replacementUID)
		replacement.ResourceVersion = "2"
		replacement.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		}
		replacementPVCName := "data-" + statefulSetName + "-0"
		replacementPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: replacementPVCName, Namespace: component.Namespace, ResourceVersion: "11",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "StatefulSet", Name: statefulSetName, UID: replacementUID,
			}},
		}}
		client := fake.NewSimpleClientset(initial, replacementPVC)
		statefulSetResource := appsv1.SchemeGroupVersion.WithResource("statefulsets")
		getCount := 0
		client.Fake.PrependReactor("get", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
			getCount++
			if getCount != 2 {
				return false, nil, nil
			}
			if err := client.Tracker().Update(statefulSetResource, replacement, component.Namespace); err != nil {
				return true, nil, err
			}
			return true, replacement.DeepCopy(), nil
		})
		job := &model.JobTask{
			Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
			InternalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(), Timeout: 2,
		}
		ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
		require.NotNil(t, ctl)

		err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSetName)

		require.Error(t, err)
		require.True(t, k8serrors.IsConflict(err))
		require.Contains(t, err.Error(), "changed during PVC retention convergence")
		require.Equal(t, 2, getCount)
		target, ok := ctl.statefulSetRetentionTargets[component.Namespace+"/"+statefulSetName]
		require.True(t, ok)
		require.Equal(t, initialUID, target.uid, "the first observed UID must remain pinned")
		require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
		require.Equal(t, 0, countClientActions(client, "patch", "persistentvolumeclaims"))
		require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
		require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))

		liveObject, getErr := client.Tracker().Get(statefulSetResource, component.Namespace, statefulSetName)
		require.NoError(t, getErr)
		liveReplacement := liveObject.(*appsv1.StatefulSet)
		require.Equal(t, replacementUID, liveReplacement.UID)
		require.False(t, statefulSetPVCRetentionIsRetain(liveReplacement), "replacement must not be updated")
		pvcObject, getErr := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), component.Namespace, replacementPVCName)
		require.NoError(t, getErr)
		require.Equal(t, replacementUID, pvcObject.(*corev1.PersistentVolumeClaim).OwnerReferences[0].UID)
	})

	t.Run("the same UID may converge on a later poll", func(t *testing.T) {
		ctx := context.Background()
		component := &model.ApplicationComponent{
			Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
		}
		statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
		statefulSetUID := types.UID("stable-statefulset")
		initial := retentionGuardStatefulSet(component.Namespace, statefulSetName, statefulSetUID)
		initial.Generation = 2
		initial.Status.ObservedGeneration = 1
		client := fake.NewSimpleClientset(initial)
		statefulSetResource := appsv1.SchemeGroupVersion.WithResource("statefulsets")
		getCount := 0
		client.Fake.PrependReactor("get", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
			getCount++
			if getCount != 2 {
				return false, nil, nil
			}
			observed := initial.DeepCopy()
			observed.ResourceVersion = "2"
			observed.Status.ObservedGeneration = observed.Generation
			if err := client.Tracker().Update(statefulSetResource, observed, component.Namespace); err != nil {
				return true, nil, err
			}
			return true, observed, nil
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
		require.Equal(t, 2, getCount)
		target, ok := ctl.statefulSetRetentionTargets[component.Namespace+"/"+statefulSetName]
		require.True(t, ok)
		require.Equal(t, statefulSetUID, target.uid)
		require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
	})
}

func TestCleanupResourcesJobCtlPinsPreflightStatefulSetIdentityBeforeFirstRetentionPoll(t *testing.T) {
	tests := []struct {
		name                string
		statefulSetWasFound bool
		wantError           string
	}{
		{
			name:                "different UID replaces preflight target",
			statefulSetWasFound: true,
			wantError:           "UID changed",
		},
		{
			name:      "StatefulSet appears after missing preflight target",
			wantError: "appeared after the required pod identity scan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			initialUID := types.UID("preflight-statefulset")
			var objects []k8sruntime.Object
			if tt.statefulSetWasFound {
				objects = append(objects, retentionGuardStatefulSet(component.Namespace, statefulSetName, initialUID))
			}
			client := fake.NewSimpleClientset(objects...)
			marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
			store := &requiredStatefulSetPodCheckpointStore{cleanupComponentStore: cleanupComponentStore{
				component: component,
				jobInfo: &model.JobInfo{
					ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
					TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
				},
			}}
			job := &model.JobTask{
				Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
				TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
				InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
			}
			ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
			require.NotNil(t, ctl)
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))

			checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, tt.statefulSetWasFound, checkpoint.StatefulSetWasFound)
			if tt.statefulSetWasFound {
				require.Equal(t, initialUID, checkpoint.StatefulSetUID)
				require.NoError(t, client.Tracker().Delete(appsv1.SchemeGroupVersion.WithResource("statefulsets"), component.Namespace, statefulSetName))
			} else {
				require.Empty(t, checkpoint.StatefulSetUID)
			}

			replacementUID := types.UID("replacement-statefulset")
			replacement := retentionGuardStatefulSet(component.Namespace, statefulSetName, replacementUID)
			replacement.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			}
			replacementPVCName := "data-" + statefulSetName + "-0"
			replacementPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Name: replacementPVCName, Namespace: component.Namespace, ResourceVersion: "11",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "StatefulSet", Name: statefulSetName, UID: replacementUID,
				}},
			}}
			require.NoError(t, client.Tracker().Add(replacement))
			require.NoError(t, client.Tracker().Add(replacementPVC))

			err = ctl.prepareRequiredStatefulSetDeletion(ctx, component)
			require.Error(t, err)
			require.True(t, k8serrors.IsConflict(err))
			require.Contains(t, err.Error(), tt.wantError)
			require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
			require.Equal(t, 0, countClientActions(client, "patch", "persistentvolumeclaims"))
			require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
			require.Equal(t, 0, countClientActions(client, "delete", "pods"))
			require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))

			liveReplacement, getErr := client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.Equal(t, replacementUID, liveReplacement.UID)
			require.False(t, statefulSetPVCRetentionIsRetain(liveReplacement))
			livePVC, getErr := client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, replacementPVCName, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.Equal(t, replacementUID, livePVC.OwnerReferences[0].UID)
		})
	}
}

func TestCleanupResourcesJobCtlRejectsTerminatingRetainedPVCs(t *testing.T) {
	tests := []struct {
		name         string
		internalInfo func(*testing.T) string
		pvcTemplate  string
		wantError    bool
	}{
		{
			name: "v2 retains every PVC",
			internalInfo: func(*testing.T) string {
				return versionUpdateRequireStatefulSetDeletionInternalInfo()
			},
			pvcTemplate: "data",
			wantError:   true,
		},
		{
			name: "v3 retains an unplanned PVC",
			internalInfo: func(t *testing.T) string {
				return versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
			},
			pvcTemplate: "cache",
			wantError:   true,
		},
		{
			name: "v3 may continue for a planned PVC",
			internalInfo: func(t *testing.T) string {
				return versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
			},
			pvcTemplate: "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" during preflight", func(t *testing.T) {
			component, _, client, ctl := newRetentionGuardController(t, tt.internalInfo(t), tt.pvcTemplate, true)
			ctx := context.Background()
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))

			err := ctl.prepareRequiredStatefulSetDeletion(ctx, component)

			if tt.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "PVC is already terminating and is not planned for deletion")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
			require.Equal(t, 0, countClientActions(client, "delete", "pods"))
			require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
		})

		t.Run(tt.name+" before pod deletion", func(t *testing.T) {
			component, statefulSetName, client, ctl := newRetentionGuardController(t, tt.internalInfo(t), tt.pvcTemplate, false)
			ctx := context.Background()
			require.NoError(t, client.Tracker().Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: statefulSetName + "-0", Namespace: component.Namespace,
				UID: types.UID("statefulset-pod-0"), ResourceVersion: "21",
				Labels: map[string]string{
					config.LabelAppID:         component.AppID,
					config.LabelComponentName: component.Name,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "StatefulSet", Name: statefulSetName, UID: types.UID("statefulset-uid"),
				}},
			}}))
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
			require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))

			pvcName := tt.pvcTemplate + "-" + statefulSetName + "-0"
			pvcResource := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
			pvcObject, err := client.Tracker().Get(pvcResource, component.Namespace, pvcName)
			require.NoError(t, err)
			terminatingPVC := pvcObject.(*corev1.PersistentVolumeClaim).DeepCopy()
			now := metav1.Now()
			terminatingPVC.DeletionTimestamp = &now
			terminatingPVC.ResourceVersion = "12"
			require.NoError(t, client.Tracker().Update(pvcResource, terminatingPVC, component.Namespace))

			gone, err := ctl.componentPodsGone(ctx, component)

			if tt.wantError {
				require.False(t, gone)
				require.Error(t, err)
				require.Contains(t, err.Error(), "PVC is already terminating and is not planned for deletion")
				require.Equal(t, 0, countClientActions(client, "delete", "pods"))
			} else {
				require.NoError(t, err)
				require.False(t, gone, "the allowed Pod deletion still requires a confirming poll")
				require.Equal(t, 1, countClientActions(client, "delete", "pods"))
			}
		})
	}
}

func newRetentionGuardController(
	t *testing.T,
	internalInfo string,
	pvcTemplate string,
	terminating bool,
) (*model.ApplicationComponent, string, *fake.Clientset, *CleanupResourcesJobCtl) {
	t.Helper()
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: pvcTemplate + "-" + statefulSetName + "-0", Namespace: component.Namespace, ResourceVersion: "11",
	}}
	if terminating {
		now := metav1.Now()
		pvc.DeletionTimestamp = &now
	}
	client := fake.NewSimpleClientset(
		retentionGuardStatefulSet(component.Namespace, statefulSetName, types.UID("statefulset-uid")),
		pvc,
	)
	job := &model.JobTask{
		Name: component.Name, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: internalInfo, Timeout: 2,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, &noopStore{}, nil)
	require.NotNil(t, ctl)
	return component, statefulSetName, client, ctl
}

func retentionGuardStatefulSet(namespace, name string, uid types.UID) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace, UID: uid, ResourceVersion: "1", Generation: 1,
		},
		Spec: appsv1.StatefulSetSpec{
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{ObjectMeta: metav1.ObjectMeta{Name: "data"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "cache"}},
			},
		},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 1},
	}
}
