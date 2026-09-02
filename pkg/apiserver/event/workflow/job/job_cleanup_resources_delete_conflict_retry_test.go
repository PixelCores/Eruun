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
)

func TestCleanupResourcesJobCtlRetriesStatefulSetDeleteAfterResourceVersionConflict(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
	require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
	require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
	client.ClearActions()

	var deleteResourceVersions []string
	client.Fake.PrependReactor("delete", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		require.NotNil(t, options.Preconditions)
		require.NotNil(t, options.Preconditions.UID)
		require.Equal(t, statefulSet.UID, *options.Preconditions.UID)
		require.NotNil(t, options.Preconditions.ResourceVersion)
		deleteResourceVersions = append(deleteResourceVersions, *options.Preconditions.ResourceVersion)
		if len(deleteResourceVersions) != 1 {
			return false, nil, nil
		}

		current := requiredStatefulSetFromTracker(t, client, component.Namespace, statefulSet.Name).DeepCopy()
		current.ResourceVersion = "12"
		require.NoError(t, client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), current, component.Namespace))
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Group: "apps", Resource: "statefulsets"},
			statefulSet.Name,
			fmt.Errorf("resourceVersion precondition no longer matches"),
		)
	})

	err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSet.Name)

	require.NoError(t, err)
	require.Equal(t, []string{"11", "12"}, deleteResourceVersions)
	_, getErr := client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSet.Name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(getErr))
}

func TestCleanupResourcesJobCtlStatefulSetDeleteConflictStillFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*appsv1.StatefulSet)
		wantError  string
		wantUID    types.UID
		wantShared bool
	}{
		{
			name: "replacement UID",
			mutate: func(current *appsv1.StatefulSet) {
				current.UID = types.UID("replacement-statefulset-uid")
				current.ResourceVersion = "12"
			},
			wantError: "changed after PVC retention convergence",
			wantUID:   types.UID("replacement-statefulset-uid"),
		},
		{
			name: "late share protection",
			mutate: func(current *appsv1.StatefulSet) {
				current.ResourceVersion = "12"
				current.Labels[config.LabelShareName] = "late-shared-mysql"
				current.Labels[config.LabelShareStrategy] = string(config.ShareStrategyDefault)
			},
			wantError:  "protected by live share labels",
			wantUID:    types.UID("required-statefulset-uid"),
			wantShared: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
			require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
			client.ClearActions()

			client.Fake.PrependReactor("delete", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				current := requiredStatefulSetFromTracker(t, client, component.Namespace, statefulSet.Name).DeepCopy()
				tt.mutate(current)
				require.NoError(t, client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), current, component.Namespace))
				return true, nil, k8serrors.NewConflict(
					schema.GroupResource{Group: "apps", Resource: "statefulsets"},
					statefulSet.Name,
					fmt.Errorf("resourceVersion precondition no longer matches"),
				)
			})

			err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSet.Name)

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantError)
			require.Equal(t, 1, countClientActions(client, "delete", "statefulsets"))
			live, getErr := client.AppsV1().StatefulSets(component.Namespace).Get(ctx, statefulSet.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.Equal(t, tt.wantUID, live.UID)
			_, protected := cleanupResourceShareProtected(live.Labels)
			require.Equal(t, tt.wantShared, protected)
		})
	}
}

func TestCleanupResourcesJobCtlStatefulSetDeleteConflictRechecksCancellation(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
	require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
	require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
	client.ClearActions()
	store := ctl.store.(*cleanupComponentStore)

	client.Fake.PrependReactor("delete", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		current := requiredStatefulSetFromTracker(t, client, component.Namespace, statefulSet.Name).DeepCopy()
		current.ResourceVersion = "12"
		require.NoError(t, client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), current, component.Namespace))
		store.workflowTask.Status = config.StatusCancelled
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Group: "apps", Resource: "statefulsets"},
			statefulSet.Name,
			fmt.Errorf("resourceVersion precondition no longer matches"),
		)
	})

	err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSet.Name)

	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)
	require.Equal(t, 1, countClientActions(client, "delete", "statefulsets"))
}

func TestCleanupResourcesJobCtlStatefulSetDeleteConflictRetryIsBoundedAndCancellationAware(t *testing.T) {
	tests := []struct {
		name       string
		cancel     bool
		wantStatus config.Status
		wantError  string
	}{
		{name: "bounded while active", wantError: "did not converge after 3 attempts"},
		{name: "cancellation wins at retry exhaustion", cancel: true, wantStatus: config.StatusCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
			require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
			client.ClearActions()
			store := ctl.store.(*cleanupComponentStore)
			deleteAttempts := 0

			client.Fake.PrependReactor("delete", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				deleteAttempts++
				current := requiredStatefulSetFromTracker(t, client, component.Namespace, statefulSet.Name).DeepCopy()
				current.ResourceVersion = fmt.Sprintf("%d", 11+deleteAttempts)
				require.NoError(t, client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), current, component.Namespace))
				if tt.cancel && deleteAttempts == requiredStatefulSetSafetyRefreshMaxAttempts {
					store.workflowTask.Status = config.StatusCancelled
				}
				return true, nil, k8serrors.NewConflict(
					schema.GroupResource{Group: "apps", Resource: "statefulsets"},
					statefulSet.Name,
					fmt.Errorf("resourceVersion precondition no longer matches"),
				)
			})

			err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSet.Name)

			require.Error(t, err)
			require.Equal(t, requiredStatefulSetSafetyRefreshMaxAttempts, deleteAttempts)
			require.Equal(t, requiredStatefulSetSafetyRefreshMaxAttempts, countClientActions(client, "delete", "statefulsets"))
			if tt.wantStatus != "" {
				statusErr, ok := ExtractStatusError(err)
				require.True(t, ok)
				require.Equal(t, tt.wantStatus, statusErr.Status)
			} else {
				require.True(t, k8serrors.IsConflict(err))
				require.Contains(t, err.Error(), tt.wantError)
			}
		})
	}
}

func TestCleanupResourcesJobCtlRetriesPVCDeleteAfterResourceVersionConflict(t *testing.T) {
	ctx := context.Background()
	component, pvc, client, ctl := newRequiredStatefulSetPVCConflictRetryController(t)
	require.NoError(t, ctl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component))
	client.ClearActions()

	var deleteResourceVersions []string
	client.Fake.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		options := action.(k8stesting.DeleteAction).GetDeleteOptions()
		require.NotNil(t, options.Preconditions)
		require.NotNil(t, options.Preconditions.UID)
		require.Equal(t, pvc.UID, *options.Preconditions.UID)
		require.NotNil(t, options.Preconditions.ResourceVersion)
		deleteResourceVersions = append(deleteResourceVersions, *options.Preconditions.ResourceVersion)
		if len(deleteResourceVersions) != 1 {
			return false, nil, nil
		}

		current := requiredStatefulSetPVCFromTracker(t, client, component.Namespace, pvc.Name).DeepCopy()
		current.ResourceVersion = "2"
		require.NoError(t, client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), current, component.Namespace))
		return true, nil, k8serrors.NewConflict(
			schema.GroupResource{Resource: "persistentvolumeclaims"},
			pvc.Name,
			fmt.Errorf("resourceVersion precondition no longer matches"),
		)
	})

	gone, err := ctl.requiredStatefulSetPVCsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone)

	gone, err = ctl.requiredStatefulSetPVCsGone(ctx, component)
	require.NoError(t, err)
	require.False(t, gone, "a successful delete remains pending until NotFound is observed")

	gone, err = ctl.requiredStatefulSetPVCsGone(ctx, component)
	require.NoError(t, err)
	require.True(t, gone)
	require.Equal(t, []string{"1", "2"}, deleteResourceVersions)
}

func TestCleanupResourcesJobCtlPVCDeleteConflictStillFailsClosed(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*corev1.PersistentVolumeClaim)
		wantError  string
		wantUID    types.UID
		wantShared bool
	}{
		{
			name: "replacement UID",
			mutate: func(current *corev1.PersistentVolumeClaim) {
				current.UID = types.UID("replacement-pvc-uid")
				current.ResourceVersion = "2"
			},
			wantError: "PVC UID changed",
			wantUID:   types.UID("replacement-pvc-uid"),
		},
		{
			name: "late share protection",
			mutate: func(current *corev1.PersistentVolumeClaim) {
				current.ResourceVersion = "2"
				current.Labels = map[string]string{
					config.LabelShareName:     "late-shared-data",
					config.LabelShareStrategy: string(config.ShareStrategyIgnore),
				}
			},
			wantError:  "protected by live share labels",
			wantUID:    types.UID("original-pvc-uid"),
			wantShared: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, pvc, client, ctl := newRequiredStatefulSetPVCConflictRetryController(t)
			require.NoError(t, ctl.ensureRequiredStatefulSetPVCDeletionAllowed(ctx, component))
			client.ClearActions()

			client.Fake.PrependReactor("delete", "persistentvolumeclaims", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				current := requiredStatefulSetPVCFromTracker(t, client, component.Namespace, pvc.Name).DeepCopy()
				tt.mutate(current)
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
			require.Contains(t, err.Error(), tt.wantError)
			require.Equal(t, 1, countClientActions(client, "delete", "persistentvolumeclaims"))
			live := requiredStatefulSetPVCFromTracker(t, client, component.Namespace, pvc.Name)
			require.Equal(t, tt.wantUID, live.UID)
			_, protected := cleanupResourceShareProtected(live.Labels)
			require.Equal(t, tt.wantShared, protected)
		})
	}
}

func newRequiredStatefulSetPVCConflictRetryController(
	t *testing.T,
) (*model.ApplicationComponent, *corev1.PersistentVolumeClaim, *fake.Clientset, *CleanupResourcesJobCtl) {
	t.Helper()
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
	return component, pvc, client, ctl
}

func requiredStatefulSetPVCFromTracker(
	t *testing.T,
	client *fake.Clientset,
	namespace, name string,
) *corev1.PersistentVolumeClaim {
	t.Helper()
	object, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), namespace, name)
	require.NoError(t, err)
	pvc, ok := object.(*corev1.PersistentVolumeClaim)
	require.True(t, ok)
	return pvc
}
