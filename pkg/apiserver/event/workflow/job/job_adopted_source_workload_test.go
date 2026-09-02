package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainadoption "github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestDeployJobCtlRunAdoptedNoopPreservesUnknownLiveFields(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("deployment-uid")
	live := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-backend", Namespace: "ops", UID: uid,
			Labels: map[string]string{"platform.example/owner": "team-a"},
		}, Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(2),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "legacy"}}, Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "backend", Image: "api:v1", SecurityContext: &corev1.SecurityContext{ReadOnlyRootFilesystem: boolPtr(true)}}, {Name: "sidecar", Image: "sidecar:v1"}},
			}},
		},
	}
	desired := live.DeepCopy()
	desired.Name = "generated-name"
	client := fake.NewSimpleClientset(live)
	snapshotResource := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{
		component: sourceComponent("app-1", "backend", "Deployment", live.Name, uid),
		app:       adoptedApplication(t, "app-1", "ops", snapshotResource),
	}
	jobTask := &model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploy), JobInfo: desired}
	ctl := NewDeployJobCtl(jobTask, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	require.NoError(t, ctl.run(ctx))
	got, err := client.AppsV1().Deployments("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "team-a", got.Labels["platform.example/owner"])
	require.NotNil(t, got.Spec.Template.Spec.Containers[0].SecurityContext)
	require.True(t, *got.Spec.Template.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem)
	require.Equal(t, 0, countClientActions(client, "update", "deployments"))
}

func TestAdoptedDeploymentUpdatePreservesLiveManagedByOutsideWorkloadSelector(t *testing.T) {
	current := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
			"app": "api",
		}},
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			"app":                 "api",
			config.LabelManagedBy: "Helm",
		}}},
	}}
	desired := current.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		"app":                 "api",
		config.LabelManagedBy: config.ManagedByEruun,
		config.LabelAppID:     "app-1",
	}

	updated := adoptedDeploymentForExistingUpdate(current, desired)

	require.Equal(t, "Helm", updated.Spec.Template.Labels[config.LabelManagedBy])
	require.Equal(t, "app-1", updated.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, current.Spec.Selector, updated.Spec.Selector)
	serviceSelector := labels.SelectorFromSet(map[string]string{
		"app":                 "api",
		config.LabelManagedBy: "Helm",
	})
	require.True(t, serviceSelector.Matches(labels.Set(updated.Spec.Template.Labels)))
	policySelector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: map[string]string{
		"app":                 "api",
		config.LabelManagedBy: "Helm",
	}})
	require.NoError(t, err)
	require.True(t, policySelector.Matches(labels.Set(updated.Spec.Template.Labels)))
	require.False(t, adoptedDeploymentNeedsUpdate(updated, desired))
}

func TestAdoptedStatefulSetUpdatePreservesLiveManagedByOutsideWorkloadSelector(t *testing.T) {
	current := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{
		Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "app",
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{"api"},
		}}},
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			"app":                 "api",
			config.LabelManagedBy: "Helm",
		}}},
	}}
	desired := current.DeepCopy()
	desired.Spec.Template.Labels = map[string]string{
		"app":                 "api",
		config.LabelManagedBy: config.ManagedByEruun,
		config.LabelAppID:     "app-1",
	}

	updated, err := adoptedStatefulSetForExistingUpdate(current, desired)

	require.NoError(t, err)
	require.Equal(t, "Helm", updated.Spec.Template.Labels[config.LabelManagedBy])
	require.Equal(t, "app-1", updated.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, current.Spec.Selector, updated.Spec.Selector)
	serviceSelector := labels.SelectorFromSet(map[string]string{
		"app":                 "api",
		config.LabelManagedBy: "Helm",
	})
	require.True(t, serviceSelector.Matches(labels.Set(updated.Spec.Template.Labels)))
	needsUpdate, err := adoptedStatefulSetNeedsUpdate(updated, desired)
	require.NoError(t, err)
	require.False(t, needsUpdate)
}

func TestDeployJobCtlRunAdoptedRejectsUIDReplacementWithoutWrite(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	live := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-backend", Namespace: "ops", UID: types.UID("replacement")},
	}
	desired := live.DeepCopy()
	client := fake.NewSimpleClientset(live)
	baseline := live.DeepCopy()
	baseline.UID = types.UID("original")
	snapshotResource := adoptedSnapshotResource(
		t,
		baseline,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{
		component: sourceComponent("app-1", "backend", "Deployment", live.Name, types.UID("original")),
		app:       adoptedApplication(t, "app-1", "ops", snapshotResource),
	}
	ctl := NewDeployJobCtl(&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploy), JobInfo: desired}, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ownership conflict")
	require.Equal(t, 0, countClientActions(client, "update", "deployments"))
	require.Equal(t, 0, countClientActions(client, "create", "deployments"))
}

func TestDeployJobCtlRunAdoptedMapsLogicalComponentToFirstContainer(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("deployment-uid")
	live := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend",
			Namespace: "ops",
			UID:       uid,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "legacy"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "main", Image: "api:v1", Env: []corev1.EnvVar{{Name: "PRESERVE", Value: "yes"}}},
					{Name: "backend", Image: "sidecar:v1"},
				}},
			},
		},
	}
	snapshotResource := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	component := sourceComponent("app-1", "backend", "Deployment", live.Name, uid)
	store := &adoptedSourceStore{
		component: component,
		app:       adoptedApplication(t, "app-1", "ops", snapshotResource),
	}
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "generated", Namespace: "ops"},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "backend",
					Image: "api:v2",
					Env:   []corev1.EnvVar{{Name: "NEW_VALUE", Value: "enabled"}},
				}}},
			},
		},
	}
	client := fake.NewSimpleClientset(live.DeepCopy())
	jobTask := &model.JobTask{
		Name:      "backend",
		AppID:     "app-1",
		Namespace: "ops",
		TaskID:    "version-task-1",
		JobType:   string(config.JobDeploy),
		JobInfo:   desired,
	}
	ctl := NewDeployJobCtl(jobTask, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	require.NoError(t, ctl.run(ctx))
	got, err := client.AppsV1().Deployments("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "api:v2", got.Spec.Template.Spec.Containers[0].Image)
	require.Equal(t, "sidecar:v1", got.Spec.Template.Spec.Containers[1].Image, "logical component name must not select a same-name sidecar")
	require.Contains(t, got.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "PRESERVE", Value: "yes"})
	require.Contains(t, got.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "NEW_VALUE", Value: "enabled"})
	require.Equal(t, "version-task-1", got.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, 1, countClientActions(client, "update", "deployments"))
}

func TestDeployJobCtlRunAdoptedMissingSourceBindingNeverFallsBackToGeneratedName(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend",
			Namespace: "ops",
			UID:       types.UID("source-uid"),
		},
	}
	snapshotResource := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	corruptBinding := &model.ApplicationComponent{
		ID:            7,
		AppID:         "app-1",
		Name:          "backend",
		Namespace:     "ops",
		ComponentType: config.ServerJob,
	}
	desired := source.DeepCopy()
	desired.Name = "generated-backend"
	unrelated := desired.DeepCopy()
	unrelated.UID = types.UID("unrelated-uid")
	client := fake.NewSimpleClientset(unrelated)
	store := &adoptedSourceStore{
		component: corruptBinding,
		app:       adoptedApplication(t, "app-1", "ops", snapshotResource),
	}
	ctl := NewDeployJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   string(config.JobDeploy),
			JobInfo:   desired,
		},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.ErrorContains(t, err, "no complete source workload binding")
	require.Empty(t, client.Actions(), "corrupt adopted source state must fail before any Kubernetes access")
}

func TestDeployJobCtlRunAdoptedMissingSourceWithoutSnapshotNeverCreatesReplacement(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("original")
	desired := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "generated", Namespace: "ops"}}
	client := fake.NewSimpleClientset()
	store := &adoptedSourceStore{component: sourceComponent("app-1", "backend", "Deployment", "legacy-backend", uid)}
	ctl := NewDeployJobCtl(&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploy), JobInfo: desired}, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
	require.Equal(t, 0, countClientActions(client, "create", "deployments"))
}

func TestStatefulSetAdoptedPreservesIdentityAndIgnoresSyntheticImmutableFields(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("mysql-uid")
	live := &appsv1.StatefulSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"}, ObjectMeta: metav1.ObjectMeta{Name: "legacy-mysql", Namespace: "ops", UID: uid}, Spec: appsv1.StatefulSetSpec{
		ServiceName:          "mysql-headless",
		PodManagementPolicy:  appsv1.ParallelPodManagement,
		UpdateStrategy:       appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
		Selector:             &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
		Replicas:             int32Ptr(1),
		VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
		Template:             corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mysql"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:8"}}}},
	}}
	desired := live.DeepCopy()
	desired.Name = "generated"
	desired.Spec.ServiceName = "generated-headless"
	desired.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"generated": "selector"}}
	desired.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
	desired.Spec.VolumeClaimTemplates[0].Name = "other-data"
	client := fake.NewSimpleClientset(live)
	snapshotResource := adoptedSnapshotResource(
		t,
		live,
		"mysql",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{
		component: sourceComponent("app-1", "mysql", "StatefulSet", live.Name, uid),
		app:       adoptedApplication(t, "app-1", "ops", snapshotResource),
	}
	ctl := NewDeployStatefulSetJobCtl(&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployStore), JobInfo: desired}, client, store, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
	got, getErr := client.AppsV1().StatefulSets("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, "mysql-headless", got.Spec.ServiceName)
	require.Equal(t, appsv1.ParallelPodManagement, got.Spec.PodManagementPolicy)
	require.Equal(t, appsv1.OnDeleteStatefulSetStrategyType, got.Spec.UpdateStrategy.Type)
}

func TestDeployJobCtlRunAdoptedRecreatesOriginalNameAndPersistsNewUID(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("deployment-old")
	newUID := types.UID("deployment-new")
	baseline := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend",
			Namespace: "ops",
			UID:       oldUID,
			Labels:    map[string]string{"platform.example/owner": "team-a"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(2),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "legacy"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "backend", Image: "api:v1"},
					{Name: "sidecar", Image: "sidecar:v1"},
				}},
			},
		},
	}
	snapshotResource := adoptedSnapshotResource(
		t,
		baseline,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	component := sourceComponent("app-1", "backend", "Deployment", baseline.Name, oldUID)
	store := &adoptedSourceStore{
		component: component,
		app:       adoptedApplication(t, "app-1", "ops", snapshotResource),
	}
	desired := baseline.DeepCopy()
	desired.Name = "generated-backend"
	desired.UID = ""
	desired.Spec.Replicas = int32Ptr(3)
	desired.Spec.Template.Spec.Containers = []corev1.Container{{Name: "backend", Image: "api:v2"}}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create := action.(k8stesting.CreateAction)
		object := create.GetObject().(*appsv1.Deployment)
		object.UID = newUID
		object.ResourceVersion = "7"
		return false, nil, nil
	})
	ctl := NewDeployJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploy), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	recreated, err := client.AppsV1().Deployments("ops").Get(ctx, baseline.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newUID, recreated.UID)
	require.Equal(t, int32(3), *recreated.Spec.Replicas)
	require.Equal(t, "api:v2", recreated.Spec.Template.Spec.Containers[0].Image)
	require.Equal(t, "sidecar:v1", recreated.Spec.Template.Spec.Containers[1].Image)
	require.Equal(t, map[string]string{"app": "legacy"}, recreated.Spec.Selector.MatchLabels)
	require.Equal(t, "team-a", recreated.Labels["platform.example/owner"])
	require.Equal(t, string(newUID), *store.component.SourceWorkloadUID)
	persistedSnapshot := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(newUID), persistedSnapshot.Resources[0].Source.UID)
	require.Equal(t, "7", persistedSnapshot.Resources[0].Source.ResourceVersion)
	require.Equal(t, 1, countClientActions(client, "create", "deployments"))
}

func TestDeployJobCtlRunAdoptedDoesNotOverwriteConcurrentComponentUpdate(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("deployment-old")
	newUID := types.UID("deployment-new")
	source := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "legacy-backend", Namespace: "ops", UID: oldUID,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "backend"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "backend"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "backend", Image: "api:v1"}}},
			},
		},
	}
	component := sourceComponent("app-1", "backend", "Deployment", source.Name, oldUID)
	component.Status = string(config.ComponentStatusRunning)
	component.ReadyReplicas = 1
	component.UpdateTime = time.Unix(100, 0)
	store := &adoptedSourceStore{
		component: component,
		app: adoptedApplication(
			t,
			"app-1",
			"ops",
			adoptedSnapshotResource(t, source, "backend", "workload", domainadoption.OwnershipExclusive, domainadoption.DispositionManaged),
		),
		beforeWorkloadComponentCAS: func(current *model.ApplicationComponent) {
			current.Status = string(config.ComponentStatusStopped)
			current.ReadyReplicas = 0
			current.LastAbnormal = "new informer state"
			current.UpdateTime = time.Unix(200, 0)
		},
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*appsv1.Deployment)
		object.UID = newUID
		return false, nil, nil
	})
	ctl := NewDeployJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeploy), JobInfo: source.DeepCopy()},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.ErrorIs(t, err, errAdoptedWorkloadComponentConflict)
	require.ErrorContains(t, err, "pending claim retained")
	require.Equal(t, string(oldUID), *store.component.SourceWorkloadUID)
	require.Equal(t, string(config.ComponentStatusStopped), store.component.Status)
	require.Zero(t, store.component.ReadyReplicas)
	require.Equal(t, "new informer state", store.component.LastAbnormal)
	require.Equal(t, time.Unix(200, 0), store.component.UpdateTime)
	require.Equal(t, 1, countClientActions(client, "create", "deployments"))
	require.Equal(t, 0, countClientActions(client, "delete", "deployments"))
	live, getErr := client.AppsV1().Deployments("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, newUID, live.UID)
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(oldUID), persisted.Resources[0].Source.UID)
	require.NotNil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(
		t,
		persisted.Resources[0].PendingRecreation.Token,
		live.Annotations[config.AnnotationAdoptedRecreationToken],
	)
}

func TestDeployStatefulSetJobCtlRunAdoptedRecreatesWithOriginalStorageIdentity(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("statefulset-old")
	newUID := types.UID("statefulset-new")
	baseline := &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-mysql", Namespace: "ops", UID: oldUID},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         "mysql-headless",
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
			Replicas:            int32Ptr(1),
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					}},
				},
			}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mysql"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:8"}}},
			},
		},
	}
	snapshotResource := adoptedSnapshotResource(
		t,
		baseline,
		"mysql",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{
		component: sourceComponent("app-1", "mysql", "StatefulSet", baseline.Name, oldUID),
		app:       adoptedApplication(t, "app-1", "ops", snapshotResource),
	}
	desired := baseline.DeepCopy()
	desired.Name = "generated-mysql"
	desired.UID = ""
	desired.Spec.Replicas = int32Ptr(2)
	desired.Spec.Template.Spec.Containers[0].Image = "mysql:8.4"
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create := action.(k8stesting.CreateAction)
		object := create.GetObject().(*appsv1.StatefulSet)
		object.UID = newUID
		object.ResourceVersion = "9"
		return false, nil, nil
	})
	ctl := NewDeployStatefulSetJobCtl(
		&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployStore), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	recreated, err := client.AppsV1().StatefulSets("ops").Get(ctx, baseline.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newUID, recreated.UID)
	require.Equal(t, "mysql-headless", recreated.Spec.ServiceName)
	require.Equal(t, appsv1.ParallelPodManagement, recreated.Spec.PodManagementPolicy)
	require.Equal(t, appsv1.OnDeleteStatefulSetStrategyType, recreated.Spec.UpdateStrategy.Type)
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, recreated.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.Equal(t, "data", recreated.Spec.VolumeClaimTemplates[0].Name)
	require.Equal(t, resource.MustParse("10Gi"), recreated.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage])
	require.Equal(t, string(newUID), *store.component.SourceWorkloadUID)
}

func TestDeployPVCJobCtlRunAdoptedExpandsStandaloneBoundPVC(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("pvc-uid")
	storageClassName := "expandable"
	live := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mysql-data",
			Namespace:   "ops",
			UID:         uid,
			Annotations: map[string]string{"storage.example/owner": "database"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClassName,
			VolumeName:       "pv-existing",
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	snapshotResource := adoptedSnapshotResource(
		t,
		live,
		"mysql",
		"pvc",
		domainadoption.OwnershipDataProtected,
		domainadoption.DispositionDataProtected,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", snapshotResource)}
	allowExpansion := true
	client := fake.NewSimpleClientset(
		live,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: storageClassName},
			AllowVolumeExpansion: &allowExpansion,
		},
	)
	desired := live.DeepCopy()
	desired.Spec.VolumeName = ""
	desired.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
	ctl := NewDeployPVCJobCtl(
		&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployPVC), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	updated, err := client.CoreV1().PersistentVolumeClaims("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, resource.MustParse("20Gi"), updated.Spec.Resources.Requests[corev1.ResourceStorage])
	require.Equal(t, "pv-existing", updated.Spec.VolumeName)
	require.Equal(t, "database", updated.Annotations["storage.example/owner"])
	require.Equal(t, 1, countClientActions(client, "update", "persistentvolumeclaims"))
	require.Equal(t, 0, countClientActions(client, "create", "persistentvolumeclaims"))
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
}

func TestDeployPVCJobCtlRunAdoptedRejectsUnsafeChanges(t *testing.T) {
	testCases := []struct {
		name       string
		mutate     func(*corev1.PersistentVolumeClaim)
		allow      bool
		wantError  string
		withVCTSTS bool
	}{
		{
			name: "shrink",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("5Gi")
			},
			allow:     true,
			wantError: "shrink",
		},
		{
			name: "storageclass change",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				other := "other"
				pvc.Spec.StorageClassName = &other
			},
			allow:     true,
			wantError: "storageClassName changes are forbidden",
		},
		{
			name: "access mode change",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			},
			allow:     true,
			wantError: "accessModes changes are forbidden",
		},
		{
			name: "storageclass expansion disabled",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
			},
			allow:     false,
			wantError: "does not allow volume expansion",
		},
		{
			name: "vct resize",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
			},
			allow:      true,
			wantError:  "volumeClaimTemplate pvc resize is forbidden",
			withVCTSTS: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := WithCleanupTracker(context.Background())
			uid := types.UID("pvc-uid")
			storageClassName := "expandable"
			pvcName := "mysql-data"
			if testCase.withVCTSTS {
				pvcName = "data-legacy-mysql-0"
			}
			live := &corev1.PersistentVolumeClaim{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
				ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "ops", UID: uid},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: &storageClassName,
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					}},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
			}
			resources := []domainadoption.ResourceSnapshot{adoptedSnapshotResource(
				t,
				live,
				"mysql",
				"pvc",
				domainadoption.OwnershipDataProtected,
				domainadoption.DispositionDataProtected,
			)}
			if testCase.withVCTSTS {
				statefulSet := &appsv1.StatefulSet{
					TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
					ObjectMeta: metav1.ObjectMeta{Name: "legacy-mysql", Namespace: "ops", UID: types.UID("sts-uid")},
					Spec: appsv1.StatefulSetSpec{VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
						ObjectMeta: metav1.ObjectMeta{Name: "data"},
					}}},
				}
				resources = append(resources, adoptedSnapshotResource(
					t,
					statefulSet,
					"mysql",
					"workload",
					domainadoption.OwnershipExclusive,
					domainadoption.DispositionManaged,
				))
			}
			store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", resources...)}
			client := fake.NewSimpleClientset(
				live,
				&storagev1.StorageClass{
					ObjectMeta:           metav1.ObjectMeta{Name: storageClassName},
					AllowVolumeExpansion: &testCase.allow,
				},
			)
			desired := live.DeepCopy()
			testCase.mutate(desired)
			ctl := NewDeployPVCJobCtl(
				&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployPVC), JobInfo: desired},
				client,
				store,
				func() {},
				locker.NewNoopLocker(shareLockerPrefix),
			)

			err := ctl.run(ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.wantError)
			require.Equal(t, 0, countClientActions(client, "update", "persistentvolumeclaims"))
			require.Equal(t, 0, countClientActions(client, "create", "persistentvolumeclaims"))
			require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
		})
	}
}

func TestDeployPVCJobCtlRunAdoptedMissingPVCNeverRecreates(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("pvc-uid")
	storageClassName := "expandable"
	source := &corev1.PersistentVolumeClaim{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-data", Namespace: "ops", UID: uid},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClassName,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}},
		},
	}
	snapshotResource := adoptedSnapshotResource(
		t,
		source,
		"mysql",
		"pvc",
		domainadoption.OwnershipDataProtected,
		domainadoption.DispositionDataProtected,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", snapshotResource)}
	client := fake.NewSimpleClientset()
	ctl := NewDeployPVCJobCtl(
		&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployPVC), JobInfo: source.DeepCopy()},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "never recreated")
	require.Equal(t, 0, countClientActions(client, "create", "persistentvolumeclaims"))
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
}

func TestDeployServiceJobCtlRunAdoptedUsesLiveBaselineAndSkipsNoop(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("service-uid")
	live := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "backend-service",
			Namespace:   "ops",
			UID:         uid,
			Labels:      map[string]string{"platform.example/owner": "team-a"},
			Annotations: map[string]string{"service.example/preserve": "yes"},
		},
		Spec: corev1.ServiceSpec{
			Type:       corev1.ServiceTypeClusterIP,
			ClusterIP:  "10.0.0.10",
			ClusterIPs: []string{"10.0.0.10"},
			Selector: map[string]string{
				"app":                 "backend",
				config.LabelManagedBy: "Helm",
			},
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromString("backend-http"),
				Protocol:   corev1.ProtocolTCP,
			}, {
				Name:       "metrics",
				Port:       9090,
				TargetPort: intstr.FromInt32(9090),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
	resourceSnapshot := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"service",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", resourceSnapshot)}
	desired := applyv1.Service(live.Name, live.Namespace).
		WithSpec(applyv1.ServiceSpec().
			WithType(corev1.ServiceTypeClusterIP).
			WithSelector(map[string]string{
				"app":                 "backend",
				config.LabelManagedBy: "Helm",
			}).
			WithPorts(applyv1.ServicePort().
				WithName("http").
				WithPort(80).
				WithTargetPort(intstr.FromInt32(8080)).
				WithProtocol(corev1.ProtocolTCP)))
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployServiceJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployService), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 0, countClientActions(client, "update", "services"))
	preserved, err := client.CoreV1().Services("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, corev1.ServiceExternalTrafficPolicyLocal, preserved.Spec.ExternalTrafficPolicy)
	require.Len(t, preserved.Spec.Ports, 2)
	require.Equal(t, intstr.FromString("backend-http"), preserved.Spec.Ports[0].TargetPort)
	require.Equal(t, "yes", preserved.Annotations["service.example/preserve"])
	require.Equal(t, "team-a", preserved.Labels["platform.example/owner"])
}

func TestDeployServiceJobCtlRunAdoptedSharedDependencyIsNeverWritten(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("shared-service-uid")
	live := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: "shared-service", Namespace: "ops", UID: uid},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 80}}},
	}
	resourceSnapshot := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"service",
		domainadoption.OwnershipShared,
		domainadoption.DispositionSharedPreserved,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", resourceSnapshot)}
	desired := applyv1.Service(live.Name, live.Namespace).
		WithSpec(applyv1.ServiceSpec().WithPorts(applyv1.ServicePort().WithName("http").WithPort(81)))
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployServiceJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployService), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	require.Equal(t, 0, countClientActions(client, "update", "services"))
	require.Equal(t, 0, countClientActions(client, "create", "services"))
}

func TestDeployServiceJobCtlRunAdoptedRejectsReplacementUID(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: "backend-service", Namespace: "ops", UID: types.UID("source-uid")},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 80}}},
	}
	resourceSnapshot := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"service",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	replacement := source.DeepCopy()
	replacement.UID = types.UID("replacement-uid")
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", resourceSnapshot)}
	desired := applyv1.Service(source.Name, source.Namespace).
		WithSpec(applyv1.ServiceSpec().WithPorts(applyv1.ServicePort().WithName("http").WithPort(81)))
	client := fake.NewSimpleClientset(replacement)
	ctl := NewDeployServiceJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployService), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ownership conflict")
	require.Equal(t, 0, countClientActions(client, "update", "services"))
	require.Equal(t, 0, countClientActions(client, "create", "services"))
	require.Equal(t, 0, countClientActions(client, "delete", "services"))
}

func TestDeployIngressJobCtlRunAdoptedPreservesUnspecifiedLiveFields(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("ingress-uid")
	pathType := networkingv1.PathTypePrefix
	live := &networkingv1.Ingress{
		TypeMeta: metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "Ingress"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backend-ingress",
			Namespace: "ops",
			UID:       uid,
			Annotations: map[string]string{
				"controller.example/feature":   "preserve",
				config.AnnotationComponentName: "backend",
			},
		},
		Spec: networkingv1.IngressSpec{
			DefaultBackend: &networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
				Name: "fallback",
				Port: networkingv1.ServiceBackendPort{Number: 8080},
			}},
			TLS: []networkingv1.IngressTLS{{SecretName: "tls-existing", Hosts: []string{"example.test"}}},
			Rules: []networkingv1.IngressRule{{
				Host: "example.test",
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path:     "/",
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
							Name: "backend-service",
							Port: networkingv1.ServiceBackendPort{Number: 80},
						}},
					}},
				}},
			}},
		},
	}
	resourceSnapshot := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"ingress",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", resourceSnapshot)}
	desired := live.DeepCopy()
	desired.Name = BuildIngressName(live.Name, "lucky77pro")
	desired.Spec.DefaultBackend = nil
	desired.Spec.TLS = nil
	desired.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number = 8081
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployIngressJobCtl(
		&model.JobTask{
			Name:            desired.Name,
			AppID:           "app-1",
			ResourceAppName: "lucky77pro",
			Namespace:       "ops",
			JobType:         string(config.JobDeployIngress),
			JobInfo:         desired,
		},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	require.NoError(t, ctl.run(ctx))
	updated, err := client.NetworkingV1().Ingresses("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(8081), updated.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number)
	require.NotNil(t, updated.Spec.DefaultBackend)
	require.Equal(t, "fallback", updated.Spec.DefaultBackend.Service.Name)
	require.Equal(t, "tls-existing", updated.Spec.TLS[0].SecretName)
	require.Equal(t, "preserve", updated.Annotations["controller.example/feature"])
	require.Equal(t, 1, countClientActions(client, "update", "ingresses"))
}

func TestDeployConfigMapJobCtlRunAdoptedPreservesUnknownLiveFields(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	uid := types.UID("configmap-uid")
	live := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "backend-config",
			Namespace:   "ops",
			UID:         uid,
			Labels:      map[string]string{"platform.example/owner": "team-a"},
			Annotations: map[string]string{"platform.example/revision": "keep"},
		},
		Data:       map[string]string{"application.yaml": "old", "external.conf": "keep"},
		BinaryData: map[string][]byte{"opaque.bin": {1, 2, 3}},
	}
	resourceSnapshot := adoptedSnapshotResource(
		t,
		live,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", resourceSnapshot)}
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: live.Name, Namespace: live.Namespace},
		Data:       map[string]string{"application.yaml": "new"},
	}
	client := fake.NewSimpleClientset(live)
	ctl := NewDeployConfigMapJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployConfigMap), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
		nil,
	)

	require.NoError(t, ctl.run(ctx))
	updated, err := client.CoreV1().ConfigMaps("ops").Get(ctx, live.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "new", updated.Data["application.yaml"])
	require.Equal(t, "keep", updated.Data["external.conf"])
	require.Equal(t, []byte{1, 2, 3}, updated.BinaryData["opaque.bin"])
	require.Equal(t, "team-a", updated.Labels["platform.example/owner"])
	require.Equal(t, "keep", updated.Annotations["platform.example/revision"])
	require.Equal(t, 1, countClientActions(client, "update", "configmaps"))
}

func TestAdoptedStatefulSetRecreationPersistenceFailureRetainsPendingClaimAndRetries(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("statefulset-old")
	newUID := types.UID("statefulset-new")
	source := &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-mysql", Namespace: "ops", UID: oldUID},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "mysql-headless",
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
			Replicas:    int32Ptr(1),
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
			}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mysql"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "mysql",
					Image: "mysql:8",
				}}},
			},
		},
	}
	saved := adoptedSnapshotResource(t, source, "mysql", "workload", domainadoption.OwnershipExclusive, domainadoption.DispositionManaged)
	store := &adoptedSourceStore{
		component:            sourceComponent("app-1", "mysql", "StatefulSet", source.Name, oldUID),
		app:                  adoptedApplication(t, "app-1", "ops", saved),
		workloadCASConflicts: 1,
	}
	client := fake.NewSimpleClientset()
	createdPolicies := make([]appsv1.PersistentVolumeClaimRetentionPolicyType, 0, 2)
	client.Fake.PrependReactor("create", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*appsv1.StatefulSet)
		require.NotNil(t, object.Spec.PersistentVolumeClaimRetentionPolicy)
		createdPolicies = append(createdPolicies, object.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
		object.UID = newUID
		object.ResourceVersion = "9"
		return false, nil, nil
	})
	desired := source.DeepCopy()
	desired.Name = "generated-mysql"
	desired.UID = ""
	ctl := NewDeployStatefulSetJobCtl(
		&model.JobTask{Name: "mysql", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployStore), JobInfo: desired},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist recreated adopted statefulset binding")
	require.Contains(t, err.Error(), "pending claim retained")
	live, getErr := client.AppsV1().StatefulSets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, newUID, live.UID)
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, live.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.Equal(t, string(appsv1.DeletePersistentVolumeClaimRetentionPolicyType), live.Annotations[config.AnnotationAdoptedStatefulSetRetentionRestore])
	require.Equal(t, []appsv1.PersistentVolumeClaimRetentionPolicyType{
		appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}, createdPolicies)
	require.Equal(t, 0, countClientActions(client, "delete", "statefulsets"))
	require.Equal(t, string(oldUID), *store.component.SourceWorkloadUID)
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(oldUID), persisted.Resources[0].Source.UID)
	require.NotNil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(
		t,
		persisted.Resources[0].PendingRecreation.Token,
		live.Annotations[config.AnnotationAdoptedRecreationToken],
	)

	restoreFailures := 1
	client.Fake.PrependReactor("update", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		if restoreFailures == 0 {
			return false, nil, nil
		}
		restoreFailures--
		return true, nil, errors.New("apiserver unavailable")
	})
	err = ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "restore adopted statefulset PVC retention policy")
	pending, getErr := client.AppsV1().StatefulSets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, pending.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.Equal(t, string(appsv1.DeletePersistentVolumeClaimRetentionPolicyType), pending.Annotations[config.AnnotationAdoptedStatefulSetRetentionRestore])
	require.Equal(t, string(newUID), *store.component.SourceWorkloadUID)

	require.NoError(t, ctl.run(ctx))
	recreated, getErr := client.AppsV1().StatefulSets("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, newUID, recreated.UID)
	require.NotNil(t, recreated.Spec.PersistentVolumeClaimRetentionPolicy)
	require.Equal(t, appsv1.DeletePersistentVolumeClaimRetentionPolicyType, recreated.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.NotContains(t, recreated.Annotations, config.AnnotationAdoptedStatefulSetRetentionRestore)
	require.Equal(t, []appsv1.PersistentVolumeClaimRetentionPolicyType{
		appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}, createdPolicies)
	require.Equal(t, string(newUID), *store.component.SourceWorkloadUID)
	require.Equal(t, string(newUID), decodeTestAdoptionSnapshot(t, store.app).Resources[0].Source.UID)
}
