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
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestCleanupResourcesJobCtlRefreshesStatefulSetSafetyBeforeRetentionMutation(t *testing.T) {
	tests := []struct {
		name      string
		drift     requiredStatefulSetSafetyDrift
		boundary  string
		wantError string
	}{
		{name: "default share before retention update", drift: requiredStatefulSetDefaultShareDrift, boundary: "update", wantError: "protected by live share labels"},
		{name: "ignore share before PVC owner patch", drift: requiredStatefulSetIgnoreShareDrift, boundary: "patch", wantError: "protected by live share labels"},
		{name: "extra target before retention update", drift: requiredStatefulSetExtraTargetDrift, boundary: "update", wantError: "is not the required deletion target"},
		{name: "extra target before PVC owner patch", drift: requiredStatefulSetExtraTargetDrift, boundary: "patch", wantError: "is not the required deletion target"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			retain := tt.boundary == "patch"
			component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, retain, retain)
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
			client.ClearActions()

			switch tt.boundary {
			case "update":
				getCount := 0
				client.Fake.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
					getAction := action.(k8stesting.GetAction)
					if getAction.GetName() != statefulSet.Name {
						return false, nil, nil
					}
					getCount++
					if getCount != 2 {
						return false, nil, nil
					}
					original := requiredStatefulSetFromTracker(t, client, component.Namespace, statefulSet.Name).DeepCopy()
					applyRequiredStatefulSetSafetyDrift(t, client, component, statefulSet, tt.drift)
					return true, original, nil
				})
			case "patch":
				mutated := false
				client.Fake.PrependReactor("list", "persistentvolumeclaims", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
					if !mutated {
						mutated = true
						applyRequiredStatefulSetSafetyDrift(t, client, component, statefulSet, tt.drift)
					}
					return false, nil, nil
				})
			default:
				t.Fatalf("unsupported mutation boundary %q", tt.boundary)
			}

			err := ctl.prepareRequiredStatefulSetDeletion(ctx, component)

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantError)
			requireNoRequiredStatefulSetSafetyMutation(t, client)
		})
	}
}

func TestCleanupResourcesJobCtlRefreshesStatefulSetSafetyBeforeFinalDelete(t *testing.T) {
	tests := []struct {
		name      string
		drift     requiredStatefulSetSafetyDrift
		wantError string
	}{
		{name: "default share", drift: requiredStatefulSetDefaultShareDrift, wantError: "protected by live share labels"},
		{name: "ignore share", drift: requiredStatefulSetIgnoreShareDrift, wantError: "protected by live share labels"},
		{name: "extra target", drift: requiredStatefulSetExtraTargetDrift, wantError: "is not the required deletion target"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
			require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
			require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
			client.ClearActions()

			getCount := 0
			client.Fake.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
				getAction := action.(k8stesting.GetAction)
				if getAction.GetName() != statefulSet.Name {
					return false, nil, nil
				}
				getCount++
				if getCount != 3 {
					return false, nil, nil
				}
				original := requiredStatefulSetFromTracker(t, client, component.Namespace, statefulSet.Name).DeepCopy()
				applyRequiredStatefulSetSafetyDrift(t, client, component, statefulSet, tt.drift)
				return true, original, nil
			})

			err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSet.Name)

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantError)
			requireNoRequiredStatefulSetSafetyMutation(t, client)
		})
	}
}

func TestCleanupResourcesJobCtlRetriesTransientSafetyRefreshBeforeFinalStatefulSetDelete(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
	require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
	require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
	client.ClearActions()

	getCount := 0
	client.Fake.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		getAction := action.(k8stesting.GetAction)
		if getAction.GetName() != statefulSet.Name {
			return false, nil, nil
		}
		getCount++
		if getCount == 4 {
			return true, nil, k8serrors.NewTimeoutError("transient final safety refresh timeout", 0)
		}
		return false, nil, nil
	})

	err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSet.Name)

	require.NoError(t, err)
	require.GreaterOrEqual(t, getCount, 5)
	require.Equal(t, 1, countClientActions(client, "delete", "statefulsets"))
}

func TestCleanupResourcesJobCtlSafetyRefreshRetryStopsOnPersistedCancellation(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
	require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
	require.NoError(t, ctl.prepareRequiredStatefulSetDeletion(ctx, component))
	client.ClearActions()
	store, ok := ctl.store.(*cleanupComponentStore)
	require.True(t, ok)

	getCount := 0
	client.Fake.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		getAction := action.(k8stesting.GetAction)
		if getAction.GetName() != statefulSet.Name {
			return false, nil, nil
		}
		getCount++
		if getCount == 4 {
			store.workflowTask.Status = config.StatusCancelled
			return true, nil, k8serrors.NewTimeoutError("transient final safety refresh timeout", 0)
		}
		return false, nil, nil
	})

	err := ctl.deleteStatefulSet(ctx, component.Namespace, statefulSet.Name)

	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusCancelled, statusErr.Status)
	require.Equal(t, 4, getCount, "cancellation must stop before another Kubernetes safety read")
	require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
}

func TestCleanupResourcesJobCtlRequiredStatefulSetSafetyRefreshRejectsNilClient(t *testing.T) {
	ctx := context.Background()
	component, _, _, ctl := newRequiredStatefulSetSafetyRefreshController(t, true, false)
	require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
	ctl.client = nil

	err := ctl.ensureRequiredStatefulSetSafetyCurrent(ctx)

	require.EqualError(t, err, "refresh required StatefulSet safety: client is nil")
}

func TestCleanupResourcesJobCtlRetriesTransientStatefulSetSafetyRefresh(t *testing.T) {
	ctx := context.Background()
	component, statefulSet, client, ctl := newRequiredStatefulSetSafetyRefreshController(t, false, false)
	require.NoError(t, ctl.ensureRequiredStatefulSetDeletionAllowed(ctx, component))
	client.ClearActions()

	transient := true
	client.Fake.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		getAction := action.(k8stesting.GetAction)
		if getAction.GetName() != statefulSet.Name || !transient {
			return false, nil, nil
		}
		transient = false
		return true, nil, k8serrors.NewTimeoutError("transient safety refresh timeout", 0)
	})

	err := ctl.prepareRequiredStatefulSetDeletion(ctx, component)

	require.NoError(t, err)
	require.Equal(t, 1, countClientActions(client, "update", "statefulsets"))
}

type requiredStatefulSetSafetyDrift string

const (
	requiredStatefulSetDefaultShareDrift requiredStatefulSetSafetyDrift = "default-share"
	requiredStatefulSetIgnoreShareDrift  requiredStatefulSetSafetyDrift = "ignore-share"
	requiredStatefulSetExtraTargetDrift  requiredStatefulSetSafetyDrift = "extra-target"
)

func newRequiredStatefulSetSafetyRefreshController(
	t *testing.T,
	retainPolicy bool,
	withDeletionOwnerReference bool,
) (*model.ApplicationComponent, *appsv1.StatefulSet, *fake.Clientset, *CleanupResourcesJobCtl) {
	t.Helper()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSetUID := types.UID("required-statefulset-uid")
	policy := &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
	}
	if retainPolicy {
		policy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		}
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName, Namespace: component.Namespace, UID: statefulSetUID, ResourceVersion: "11",
			Labels: requiredStatefulSetSafetyComponentLabels(component),
		},
		Spec: appsv1.StatefulSetSpec{
			PersistentVolumeClaimRetentionPolicy: policy,
			VolumeClaimTemplates:                 []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
		},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 1},
	}
	statefulSet.Generation = 1

	objects := []k8sruntime.Object{statefulSet}
	if withDeletionOwnerReference {
		controller := true
		objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + statefulSetName + "-0", Namespace: component.Namespace, UID: types.UID("pvc-uid"), ResourceVersion: "21",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "StatefulSet", Name: statefulSetName, UID: statefulSetUID, Controller: &controller,
			}},
		}})
	}
	client := fake.NewSimpleClientset(objects...)
	marker := versionUpdateRequireStatefulSetDeletionInternalInfo()
	store := &cleanupComponentStore{
		component:    component,
		workflowTask: &model.WorkflowQueue{TaskID: "task-1", Status: config.StatusRunning},
		jobInfo: &model.JobInfo{
			ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
			TaskID: "task-1", Status: string(config.StatusRunning), InternalInfo: marker, ServiceName: component.Name,
		},
	}
	job := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: "task-1", JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: marker, Status: config.StatusRunning, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(job, client, store, nil)
	require.NotNil(t, ctl)
	return component, statefulSet, client, ctl
}

func applyRequiredStatefulSetSafetyDrift(
	t *testing.T,
	client *fake.Clientset,
	component *model.ApplicationComponent,
	statefulSet *appsv1.StatefulSet,
	drift requiredStatefulSetSafetyDrift,
) {
	t.Helper()
	switch drift {
	case requiredStatefulSetDefaultShareDrift, requiredStatefulSetIgnoreShareDrift:
		current := requiredStatefulSetFromTracker(t, client, component.Namespace, statefulSet.Name).DeepCopy()
		current.ResourceVersion = "12"
		current.Labels = requiredStatefulSetSafetyComponentLabels(component)
		current.Labels[config.LabelShareName] = "late-shared-mysql"
		if drift == requiredStatefulSetDefaultShareDrift {
			current.Labels[config.LabelShareStrategy] = string(domainspec.ShareStrategyDefault)
		} else {
			current.Labels[config.LabelShareStrategy] = string(domainspec.ShareStrategyIgnore)
		}
		require.NoError(t, client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), current, component.Namespace))
	case requiredStatefulSetExtraTargetDrift:
		extra := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: statefulSet.Name + "-unexpected", Namespace: component.Namespace,
			UID: types.UID("extra-statefulset-uid"), ResourceVersion: "31",
			Labels: requiredStatefulSetSafetyComponentLabels(component),
		}}
		require.NoError(t, client.Tracker().Add(extra))
	default:
		t.Fatalf("unsupported StatefulSet safety drift %q", drift)
	}
}

func requiredStatefulSetFromTracker(t *testing.T, client *fake.Clientset, namespace, name string) *appsv1.StatefulSet {
	t.Helper()
	object, err := client.Tracker().Get(appsv1.SchemeGroupVersion.WithResource("statefulsets"), namespace, name)
	require.NoError(t, err)
	statefulSet, ok := object.(*appsv1.StatefulSet)
	require.True(t, ok)
	return statefulSet
}

func requiredStatefulSetSafetyComponentLabels(component *model.ApplicationComponent) map[string]string {
	return map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
}

func requireNoRequiredStatefulSetSafetyMutation(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		switch action.GetVerb() {
		case "update", "patch", "delete":
			require.Failf(t, "unexpected Kubernetes mutation", "%s %s ran after StatefulSet safety became stale", action.GetVerb(), action.GetResource().Resource)
		}
	}
}
