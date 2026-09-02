package job

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainadoption "github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

func TestVersionRestartJobCtlAdoptedDeploymentUsesSourceIdentity(t *testing.T) {
	ctx := context.Background()
	sourceDeployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend",
			Namespace: "ops",
			UID:       types.UID("source-uid"),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(2),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "api:v1"}}},
			},
		},
	}
	snapshot := adoptedSnapshotResource(
		t,
		sourceDeployment,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	component := sourceComponent("app-1", "backend", "Deployment", sourceDeployment.Name, sourceDeployment.UID)
	component.ComponentType = config.ServerJob
	component.Image = "api:v1"
	generatedName := buildWebServiceName(component.Name, component.ResourceNameKey())
	require.NotEqual(t, sourceDeployment.Name, generatedName)
	collision := sourceDeployment.DeepCopy()
	collision.Name = generatedName
	collision.UID = types.UID("unrelated-uid")

	store := &adoptedSourceStore{
		component: component,
		app:       adoptedApplication(t, "app-1", "ops", snapshot),
	}
	client := fake.NewSimpleClientset(sourceDeployment.DeepCopy(), collision)
	task := versionRestartTask(component)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)
	restartedAt := "2026-07-27T18:00:00Z"

	target, err := ctl.restartDeployment(ctx, component, restartedAt, nil)
	require.NoError(t, err)
	require.Equal(t, sourceDeployment.Name, target.name)

	updated, err := client.AppsV1().Deployments("ops").Get(ctx, sourceDeployment.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, restartedAt, updated.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, component.AppID, updated.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, component.Name, updated.Spec.Template.Labels[config.LabelComponentName])
	require.Equal(t, strconv.Itoa(component.ID), updated.Spec.Template.Labels[config.LabelComponentID])
	unrelated, err := client.AppsV1().Deployments("ops").Get(ctx, generatedName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, unrelated.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
}

func TestVersionRestartJobCtlAdoptedDeploymentRejectsReplacementUID(t *testing.T) {
	sourceDeployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend",
			Namespace: "ops",
			UID:       types.UID("replacement-uid"),
		},
	}
	baseline := sourceDeployment.DeepCopy()
	baseline.UID = types.UID("expected-uid")
	snapshot := adoptedSnapshotResource(
		t,
		baseline,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	component := sourceComponent("app-1", "backend", "Deployment", sourceDeployment.Name, types.UID("expected-uid"))
	component.ComponentType = config.ServerJob
	store := &adoptedSourceStore{
		component: component,
		app:       adoptedApplication(t, "app-1", "ops", snapshot),
	}
	client := fake.NewSimpleClientset(sourceDeployment.DeepCopy())
	ctl := NewVersionRestartJobCtl(versionRestartTask(component), client, store, nil)

	_, err := ctl.restartDeployment(context.Background(), component, "2026-07-27T18:00:00Z", nil)
	require.ErrorContains(t, err, "UID")
	require.Equal(t, 0, countClientActions(client, "update", "deployments"))
}

func TestVersionRestartJobCtlAdoptedDeploymentRejectsPausedSource(t *testing.T) {
	sourceDeployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-backend",
			Namespace: "ops",
			UID:       types.UID("source-uid"),
		},
		Spec: appsv1.DeploymentSpec{Paused: true},
	}
	snapshot := adoptedSnapshotResource(
		t,
		sourceDeployment,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	component := sourceComponent("app-1", "backend", "Deployment", sourceDeployment.Name, sourceDeployment.UID)
	component.ComponentType = config.ServerJob
	store := &adoptedSourceStore{
		component: component,
		app:       adoptedApplication(t, "app-1", "ops", snapshot),
	}
	client := fake.NewSimpleClientset(sourceDeployment.DeepCopy())
	ctl := NewVersionRestartJobCtl(versionRestartTask(component), client, store, nil)

	_, err := ctl.restartDeployment(context.Background(), component, "2026-07-27T18:00:00Z", nil)
	require.ErrorContains(t, err, "Deployment is paused")
	require.Equal(t, 0, countClientActions(client, "update", "deployments"))
}

func TestVersionRestartJobCtlAdoptedStatefulSetRejectsDeleteOnScale(t *testing.T) {
	statefulSet := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-mysql",
			Namespace: "ops",
			UID:       types.UID("statefulset-uid"),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(1),
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:8"}}},
			},
		},
	}
	snapshot := adoptedSnapshotResource(
		t,
		statefulSet,
		"mysql",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	component := sourceComponent("app-1", "mysql", "StatefulSet", statefulSet.Name, statefulSet.UID)
	component.ComponentType = config.StoreJob
	store := &adoptedSourceStore{
		component: component,
		app:       adoptedApplication(t, "app-1", "ops", snapshot),
	}
	client := fake.NewSimpleClientset(statefulSet.DeepCopy())
	ctl := NewVersionRestartJobCtl(versionRestartTask(component), client, store, nil)

	_, err := ctl.restartStatefulSet(context.Background(), component, "2026-07-27T18:00:00Z", nil)
	require.ErrorContains(t, err, "whenScaled=Delete")
	require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
}

func TestVersionRestartJobCtlAdoptedStatefulSetRejectsNonRollingRestartStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy appsv1.StatefulSetUpdateStrategy
		wantErr  string
	}{
		{
			name:     "on delete",
			strategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			wantErr:  "OnDelete",
		},
		{
			name: "partitioned rolling update",
			strategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: int32Ptr(1),
				},
			},
			wantErr: "partition=1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statefulSet := &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "legacy-mysql",
					Namespace: "ops",
					UID:       types.UID("statefulset-uid"),
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas:       int32Ptr(1),
					UpdateStrategy: test.strategy,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:8"}}},
					},
				},
			}
			snapshot := adoptedSnapshotResource(
				t,
				statefulSet,
				"mysql",
				"workload",
				domainadoption.OwnershipExclusive,
				domainadoption.DispositionManaged,
			)
			component := sourceComponent("app-1", "mysql", "StatefulSet", statefulSet.Name, statefulSet.UID)
			component.ComponentType = config.StoreJob
			store := &adoptedSourceStore{
				component: component,
				app:       adoptedApplication(t, "app-1", "ops", snapshot),
			}
			client := fake.NewSimpleClientset(statefulSet.DeepCopy())
			ctl := NewVersionRestartJobCtl(versionRestartTask(component), client, store, nil)

			_, err := ctl.restartStatefulSet(context.Background(), component, "2026-07-27T18:00:00Z", nil)
			require.ErrorContains(t, err, test.wantErr)
			require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
		})
	}
}

func TestVersionRestartJobCtlRestartsDeployment(t *testing.T) {
	api := databaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)
	waiter := informer.NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	waiter.OnPodAdd(versionRestartReadyPod(api, "api-old", "old-restarted-at"))
	cacheStore := cacheutil.NewMemCache(false)
	cacheKey := cacheutil.ApplicationComponentsKey(api.AppID)
	require.NoError(t, cacheStore.Store(cacheKey, "stale"))
	require.True(t, cacheStore.Exists(cacheKey))
	ctl.setRuntime(newJobRuntime(cacheStore, nil, nil, nil, waiter, nil))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForDeploymentRestartAt(t, client, deployment.Name)
	assertNoVersionRestartResult(t, result, 80*time.Millisecond)
	waiter.OnPodAdd(versionRestartReadyPod(api, "api-new", restartedAt))
	require.NoError(t, readVersionRestartResult(t, result))

	updatedDeployment, err := client.AppsV1().Deployments("default").Get(context.Background(), deployment.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, updatedDeployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, string(config.ComponentStatusRestarting), api.Status)
	require.Equal(t, config.StatusCompleted, task.Status)
	require.False(t, cacheStore.Exists(cacheKey))
}

func TestVersionRestartJobCtlRestartsStatefulSet(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", nil)
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	client := fake.NewSimpleClientset(statefulSet)
	task := versionRestartTask(db)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)
	waiter := informer.NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	waiter.OnPodAdd(versionRestartReadyPod(db, "mysql-old", "old-restarted-at"))
	ctl.setRuntime(newJobRuntime(nil, nil, nil, nil, waiter, nil))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForStatefulSetRestartAt(t, client, statefulSet.Name)
	assertNoVersionRestartResult(t, result, 80*time.Millisecond)
	waiter.OnPodAdd(versionRestartReadyPod(db, "mysql-new", restartedAt))
	require.NoError(t, readVersionRestartResult(t, result))

	updatedStatefulSet, err := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, updatedStatefulSet.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, string(config.ComponentStatusRestarting), db.Status)
	require.Equal(t, config.StatusCompleted, task.Status)
}

func TestVersionRestartJobCtlSkipsStoppedComponent(t *testing.T) {
	api := databaseResetServerComponent(t, "api")
	api.Status = string(config.ComponentStatusStopped)
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)

	require.NoError(t, ctl.Run(context.Background()))

	updatedDeployment, err := client.AppsV1().Deployments("default").Get(context.Background(), deployment.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, updatedDeployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, string(config.ComponentStatusStopped), api.Status)
	require.Equal(t, config.StatusSkipped, task.Status)
}

func TestVersionRestartJobCtlHonorsShareLifecyclePolicy(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		wantSkip bool
	}{
		{name: "default", strategy: string(spec.ShareStrategyDefault), wantSkip: true},
		{name: "ignore", strategy: string(spec.ShareStrategyIgnore), wantSkip: true},
		{name: "unknown", strategy: "future-default", wantSkip: true},
		{name: "force", strategy: string(spec.ShareStrategyForce)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := databaseResetServerComponent(t, "api")
			if !tt.wantSkip {
				api.Replicas = 0
			}
			api.Traits = mustDatabaseResetJSON(t, &spec.Traits{
				Share: &spec.ShareTraitSpec{Strategy: tt.strategy},
			})
			store := newDatabaseResetComponentStore(api)
			deployment := databaseResetDeployment(t, api)
			client := fake.NewSimpleClientset(deployment)
			task := versionRestartTask(api)
			ctl := NewVersionRestartJobCtl(task, client, store, nil)

			require.NoError(t, ctl.Run(context.Background()))

			updatedDeployment, err := client.AppsV1().Deployments("default").Get(context.Background(), deployment.Name, metav1.GetOptions{})
			require.NoError(t, err)
			if tt.wantSkip {
				require.Empty(t, updatedDeployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
				require.Empty(t, api.Status)
				require.Equal(t, config.StatusSkipped, task.Status)
				return
			}
			require.NotEmpty(t, updatedDeployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
			require.Equal(t, string(config.ComponentStatusRestarting), api.Status)
			require.Equal(t, config.StatusCompleted, task.Status)
		})
	}
}

func TestWorkloadRestartPatchContainsKubectlAnnotation(t *testing.T) {
	patch, err := buildWorkloadRestartPatch("2026-06-18T00:00:00Z")
	require.NoError(t, err)
	require.True(t, strings.Contains(string(patch), config.AnnotationWorkloadRestartAt))
}

func TestFormatVersionRestartAtPreservesNanoseconds(t *testing.T) {
	now := time.Date(2026, time.August, 1, 12, 34, 56, 123456789, time.FixedZone("UTC+8", 8*60*60))

	require.Equal(t, "2026-08-01T04:34:56.123456789Z", formatVersionRestartAt(now))
}

func TestVersionRestartJobCtlSkipsMissingDeployment(t *testing.T) {
	api := databaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	client := fake.NewSimpleClientset()
	task := versionRestartTask(api)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)

	require.NoError(t, ctl.Run(context.Background()))
	require.Empty(t, api.Status)
	require.Equal(t, config.StatusSkipped, task.Status)
}

func TestVersionRestartJobCtlFailsOnPatchError(t *testing.T) {
	api := databaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	client.Fake.PrependReactor("patch", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("patch denied")
	})
	task := versionRestartTask(api)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "patch denied")
	require.Empty(t, api.Status)
	require.Equal(t, config.StatusFailed, task.Status)
}

func TestVersionRestartJobCtlTreatsNotFoundPatchAsSkipped(t *testing.T) {
	api := databaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	client.Fake.PrependReactor("patch", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, deployment.Name)
	})
	task := versionRestartTask(api)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)

	require.NoError(t, ctl.Run(context.Background()))
	require.Empty(t, api.Status)
	require.Equal(t, config.StatusSkipped, task.Status)
}

func TestVersionRestartJobCtlFailsWhenRestartedDeploymentPodCrashLoops(t *testing.T) {
	api := databaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	task.Timeout = 1
	ctl := NewVersionRestartJobCtl(task, client, store, nil)
	waiter := informer.NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	waiter.OnPodAdd(versionRestartReadyPod(api, "api-old", "old-restarted-at"))
	ctl.setRuntime(newJobRuntime(nil, nil, nil, nil, waiter, nil))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForDeploymentRestartAt(t, client, deployment.Name)
	waiter.OnPodAdd(versionRestartCrashLoopPod(api, "api-new", restartedAt))

	err := readVersionRestartResult(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
	require.Equal(t, config.StatusFailed, task.Status)
	require.Contains(t, task.Error, "CrashLoopBackOff")
	require.Equal(t, string(config.ComponentStatusRestarting), api.Status)
}

func TestVersionRestartJobCtlFailsWhenRestartedStatefulSetPodCrashLoops(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", nil)
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	client := fake.NewSimpleClientset(statefulSet)
	task := versionRestartTask(db)
	task.Timeout = 1
	ctl := NewVersionRestartJobCtl(task, client, store, nil)
	waiter := informer.NewResourceReadyWaiter()
	t.Cleanup(waiter.Close)
	waiter.OnPodAdd(versionRestartReadyPod(db, "mysql-old", "old-restarted-at"))
	ctl.setRuntime(newJobRuntime(nil, nil, nil, nil, waiter, nil))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForStatefulSetRestartAt(t, client, statefulSet.Name)
	waiter.OnPodAdd(versionRestartCrashLoopPod(db, "mysql-new", restartedAt))

	err := readVersionRestartResult(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
	require.Equal(t, config.StatusFailed, task.Status)
	require.Contains(t, task.Error, "CrashLoopBackOff")
	require.Equal(t, string(config.ComponentStatusRestarting), db.Status)
}

func TestVersionRestartJobCtlFailsWhenResourceWaiterMissing(t *testing.T) {
	api := databaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	ctl := NewVersionRestartJobCtl(task, client, store, nil)

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "resource waiter is required")
	require.Equal(t, string(config.ComponentStatusRestarting), api.Status)
	require.Equal(t, config.StatusFailed, task.Status)
}

func versionRestartTask(component *model.ApplicationComponent) *model.JobTask {
	return &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobVersionRestart),
		Timeout:   5,
		JobInfo: &VersionRestartJobInfo{
			Component: component,
		},
	}
}

func runVersionRestartAsync(ctl *VersionRestartJobCtl) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- ctl.Run(context.Background())
	}()
	return result
}

func readVersionRestartResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for version restart job result")
		return nil
	}
}

func assertNoVersionRestartResult(t *testing.T, result <-chan error, wait time.Duration) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("unexpected version restart result: %v", err)
	case <-time.After(wait):
	}
}

func waitForDeploymentRestartAt(t *testing.T, client *fake.Clientset, name string) string {
	t.Helper()
	var restartedAt string
	require.Eventually(t, func() bool {
		updated, err := client.AppsV1().Deployments("default").Get(context.Background(), name, metav1.GetOptions{})
		if err != nil || updated.Spec.Template.Annotations == nil {
			return false
		}
		restartedAt = updated.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt]
		return restartedAt != ""
	}, time.Second, 10*time.Millisecond)
	return restartedAt
}

func waitForStatefulSetRestartAt(t *testing.T, client *fake.Clientset, name string) string {
	t.Helper()
	var restartedAt string
	require.Eventually(t, func() bool {
		updated, err := client.AppsV1().StatefulSets("default").Get(context.Background(), name, metav1.GetOptions{})
		if err != nil || updated.Spec.Template.Annotations == nil {
			return false
		}
		restartedAt = updated.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt]
		return restartedAt != ""
	}, time.Second, 10*time.Millisecond)
	return restartedAt
}

func versionRestartReadyPod(component *model.ApplicationComponent, name, restartedAt string) *corev1.Pod {
	return versionRestartPod(component, name, restartedAt, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{},
	}, true)
}

func versionRestartCrashLoopPod(component *model.ApplicationComponent, name, restartedAt string) *corev1.Pod {
	return versionRestartPod(component, name, restartedAt, corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
			Message: "back-off restarting failed container",
		},
	}, false)
}

func versionRestartPod(component *model.ApplicationComponent, name, restartedAt string, state corev1.ContainerState, ready bool) *corev1.Pod {
	conditions := []corev1.PodCondition{}
	if ready {
		conditions = append(conditions, corev1.PodCondition{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		})
	}
	annotations := map[string]string{}
	if restartedAt != "" {
		annotations[config.AnnotationWorkloadRestartAt] = restartedAt
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			Annotations: annotations,
			Labels: map[string]string{
				config.LabelAppID:         component.AppID,
				config.LabelComponentName: component.Name,
				config.LabelComponentID:   strconv.Itoa(component.ID),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "app",
				Image: component.Image,
			}},
		},
		Status: corev1.PodStatus{
			Conditions: conditions,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "app",
				State: state,
			}},
		},
	}
}
