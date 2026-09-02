package job

import (
	"context"

	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	rbacv1 "k8s.io/api/rbac/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainadoption "github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestAdoptedDependencyRecreationMergesSnapshotUpdatesFromSharedRuntime(t *testing.T) {
	ctx := context.Background()
	firstSource := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "api-config", Namespace: "ops", UID: types.UID("old-api-uid"), ResourceVersion: "1",
		},
	}
	secondSource := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-config", Namespace: "ops", UID: types.UID("old-worker-uid"), ResourceVersion: "1",
		},
	}
	store := &adoptedSourceStore{app: adoptedApplication(
		t,
		"app-1",
		"ops",
		adoptedSnapshotResource(t, firstSource, "api", "configmap", domainadoption.OwnershipExclusive, domainadoption.DispositionManaged),
		adoptedSnapshotResource(t, secondSource, "worker", "configmap", domainadoption.OwnershipExclusive, domainadoption.DispositionManaged),
	)}
	firstBinding, adopted, err := adoptedResourceForJob(
		ctx,
		store,
		&model.JobTask{AppID: "app-1", Name: "api"},
		"ConfigMap",
		"ops",
		firstSource.Name,
	)
	require.NoError(t, err)
	require.True(t, adopted)
	secondBinding, adopted, err := adoptedResourceForJob(
		ctx,
		store,
		&model.JobTask{AppID: "app-1", Name: "worker"},
		"ConfigMap",
		"ops",
		secondSource.Name,
	)
	require.NoError(t, err)
	require.True(t, adopted)
	firstRecreation, err := prepareAdoptedDependencyRecreation(store, firstBinding)
	require.NoError(t, err)
	secondRecreation, err := prepareAdoptedDependencyRecreation(store, secondBinding)
	require.NoError(t, err)

	firstCreated := firstSource.DeepCopy()
	firstCreated.UID = types.UID("new-api-uid")
	firstCreated.ResourceVersion = "2"
	secondCreated := secondSource.DeepCopy()
	secondCreated.UID = types.UID("new-worker-uid")
	secondCreated.ResourceVersion = "2"
	sharedRuntime := &jobRuntime{}
	lockProvider := locker.NewMemoryLocker(shareLockerPrefix)
	firstGuard, err := firstRecreation.adoptedResourceBinding.prepareRecreationCandidate(
		ctx,
		store,
		firstCreated,
		sharedRuntime,
		lockProvider,
	)
	require.NoError(t, err)
	defer firstGuard.release()
	secondGuard, err := secondRecreation.adoptedResourceBinding.prepareRecreationCandidate(
		ctx,
		store,
		secondCreated,
		sharedRuntime,
		lockProvider,
	)
	require.NoError(t, err)
	defer secondGuard.release()

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- firstRecreation.persistCreated(ctx, firstCreated, firstCreated, sharedRuntime)
	}()
	go func() {
		<-start
		results <- secondRecreation.persistCreated(ctx, secondCreated, secondCreated, sharedRuntime)
	}()
	close(start)
	require.NoError(t, <-results)
	require.NoError(t, <-results)

	snapshot := decodeTestAdoptionSnapshot(t, store.app)
	uidsByName := make(map[string]string, len(snapshot.Resources))
	for _, resource := range snapshot.Resources {
		uidsByName[resource.Source.Name] = resource.Source.UID
	}
	require.Equal(t, "new-api-uid", uidsByName[firstSource.Name])
	require.Equal(t, "new-worker-uid", uidsByName[secondSource.Name])
	require.Equal(t, 4, store.applicationCASCount)
}

func TestPendingAdoptedRecreationWrongTokenFailsClosed(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	source := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: rbacv1.SchemeGroupVersion.String(), Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: "ops", UID: types.UID("role-old")},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"role",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	saved.PendingRecreation = &domainadoption.RecreationClaim{Token: "expected-token"}
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	replacement := source.DeepCopy()
	replacement.UID = types.UID("foreign-role")
	replacement.Annotations = map[string]string{config.AnnotationAdoptedRecreationToken: "wrong-token"}
	client := fake.NewSimpleClientset(replacement)
	ctl := NewDeployRoleJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobInfo: source.DeepCopy()},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
	)

	err := ctl.run(ctx)
	require.ErrorContains(t, err, "ownership conflict")
	require.Equal(t, 0, store.applicationCASCount)
	require.Equal(t, 0, countClientActions(client, "create", "roles"))
	require.Equal(t, 0, countClientActions(client, "update", "roles"))
	require.Equal(t, 0, countClientActions(client, "delete", "roles"))
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(source.UID), persisted.Resources[0].Source.UID)
	require.Equal(t, "expected-token", persisted.Resources[0].PendingRecreation.Token)
}

func TestAdoptedDependencyRecreationPersistenceFailureRetainsLiveObjectAndPendingClaim(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("configmap-old")
	newUID := types.UID("configmap-new")
	source := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "backend-config", Namespace: "ops", UID: oldUID},
		Data:       map[string]string{"application.yaml": "source"},
	}
	saved := adoptedSnapshotResource(t, source, "backend", "configmap", domainadoption.OwnershipExclusive, domainadoption.DispositionManaged)
	store := &adoptedSourceStore{
		app:                        adoptedApplication(t, "app-1", "ops", saved),
		applicationCASErrOnAttempt: 2,
		applicationCASErr:          errors.New("database unavailable"),
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		object.UID = newUID
		return false, nil, nil
	})
	ctl := NewDeployConfigMapJobCtl(
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops", JobType: string(config.JobDeployConfigMap), JobInfo: source.DeepCopy()},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
		nil,
	)

	err := ctl.run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist recreated adopted configmap binding")
	require.Contains(t, err.Error(), "pending claim retained")
	live, getErr := client.CoreV1().ConfigMaps("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, newUID, live.UID)
	require.Equal(t, 1, countClientActions(client, "create", "configmaps"))
	require.Equal(t, 0, countClientActions(client, "delete", "configmaps"))
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(oldUID), persisted.Resources[0].Source.UID)
	require.NotNil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(
		t,
		persisted.Resources[0].PendingRecreation.Token,
		live.Annotations[config.AnnotationAdoptedRecreationToken],
	)
}

func TestAdoptedWorkloadRecreationPersistenceFailureRetainsLiveObjectAndPendingClaim(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("deployment-old")
	newUID := types.UID("deployment-new")
	source := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-backend", Namespace: "ops", UID: oldUID},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "backend"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "backend"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "backend", Image: "api:v1"}}},
			},
		},
	}
	saved := adoptedSnapshotResource(t, source, "backend", "workload", domainadoption.OwnershipExclusive, domainadoption.DispositionManaged)
	component := sourceComponent("app-1", "backend", "Deployment", source.Name, oldUID)
	store := &adoptedSourceStore{
		component:            component,
		app:                  adoptedApplication(t, "app-1", "ops", saved),
		workloadCASConflicts: 1,
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
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist recreated adopted deployment binding")
	require.Contains(t, err.Error(), "pending claim retained")
	live, getErr := client.AppsV1().Deployments("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, newUID, live.UID)
	require.Equal(t, 1, countClientActions(client, "create", "deployments"))
	require.Equal(t, 0, countClientActions(client, "delete", "deployments"))
	require.Equal(t, string(oldUID), *store.component.SourceWorkloadUID)
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(oldUID), persisted.Resources[0].Source.UID)
	require.NotNil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(
		t,
		persisted.Resources[0].PendingRecreation.Token,
		live.Annotations[config.AnnotationAdoptedRecreationToken],
	)
}
