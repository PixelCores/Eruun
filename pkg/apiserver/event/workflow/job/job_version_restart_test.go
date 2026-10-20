package job

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdeploy"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/cache"
)

func TestVersionRestartJobCtlRestartsDeployment(t *testing.T) {
	api := jobtest.DatabaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	_, err := client.CoreV1().Pods("default").Create(context.Background(), versionRestartReadyPod(api, "api-old", "old-restarted-at"), metav1.CreateOptions{})
	require.NoError(t, err)
	waiter := jobtest.StartWorkloadTestObserver(t, client)
	cacheStore := cacheutil.NewMemCache()
	cacheKey := cacheutil.ApplicationComponentsKey(api.AppID)
	require.NoError(t, cacheStore.Store(context.Background(), cacheKey, "stale"))
	require.NotEmpty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
	ctl := jobdeploy.NewVersionRestartJobCtl(task, newJobRuntime(&jobexec.Runtime{
		Client:         client,
		Store:          store,
		Cache:          cacheStore,
		ResourceWaiter: waiter,
	}))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForDeploymentRestartAt(t, client, deployment.Name)
	assertNoVersionRestartResult(t, result, 80*time.Millisecond)
	_, err = client.CoreV1().Pods("default").Create(context.Background(), versionRestartReadyPod(api, "api-new", restartedAt), metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, readVersionRestartResult(t, result))

	updatedDeployment, err := client.AppsV1().Deployments("default").Get(context.Background(), deployment.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, updatedDeployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, string(config.ComponentStatusRestarting), api.Status)
	require.Equal(t, config.StatusCompleted, task.Status)
	require.Empty(t, cachedValue(t, cacheStore, context.Background(), cacheKey))
}

func TestVersionRestartJobCtlRestartsStatefulSet(t *testing.T) {
	db := jobtest.DatabaseResetStoreComponent(t, "mysql", nil)
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	client := fake.NewSimpleClientset(statefulSet)
	task := versionRestartTask(db)
	_, err := client.CoreV1().Pods("default").Create(context.Background(), versionRestartReadyPod(db, "mysql-old", "old-restarted-at"), metav1.CreateOptions{})
	require.NoError(t, err)
	waiter := jobtest.StartWorkloadTestObserver(t, client)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, newJobRuntime(&jobexec.Runtime{
		Client:         client,
		Store:          store,
		ResourceWaiter: waiter,
	}))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForStatefulSetRestartAt(t, client, statefulSet.Name)
	assertNoVersionRestartResult(t, result, 80*time.Millisecond)
	_, err = client.CoreV1().Pods("default").Create(context.Background(), versionRestartReadyPod(db, "mysql-new", restartedAt), metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, readVersionRestartResult(t, result))

	updatedStatefulSet, err := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, updatedStatefulSet.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, string(config.ComponentStatusRestarting), db.Status)
	require.Equal(t, config.StatusCompleted, task.Status)
}

func TestVersionRestartJobCtlSkipsStoppedComponent(t *testing.T) {
	api := jobtest.DatabaseResetServerComponent(t, "api")
	api.Status = string(config.ComponentStatusStopped)
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: nil})

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
			api := jobtest.DatabaseResetServerComponent(t, "api")
			if !tt.wantSkip {
				api.Replicas = 0
			}
			api.Traits = jobtest.MustDatabaseResetJSON(t, &spec.Traits{
				Share: &spec.ShareTraitSpec{Strategy: tt.strategy},
			})
			store := newDatabaseResetComponentStore(api)
			deployment := databaseResetDeployment(t, api)
			client := fake.NewSimpleClientset(deployment)
			task := versionRestartTask(api)
			ctl := jobdeploy.NewVersionRestartJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: nil})

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

func TestVersionRestartJobCtlSkipsMissingDeployment(t *testing.T) {
	api := jobtest.DatabaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	client := fake.NewSimpleClientset()
	task := versionRestartTask(api)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: nil})

	require.NoError(t, ctl.Run(context.Background()))
	require.Empty(t, api.Status)
	require.Equal(t, config.StatusSkipped, task.Status)
}

func TestVersionRestartJobCtlFailsOnPatchError(t *testing.T) {
	api := jobtest.DatabaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	client.Fake.PrependReactor("patch", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("patch denied")
	})
	task := versionRestartTask(api)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: nil})

	err := ctl.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "patch denied")
	require.Empty(t, api.Status)
	require.Equal(t, config.StatusFailed, task.Status)
}

func TestVersionRestartJobCtlTreatsNotFoundPatchAsSkipped(t *testing.T) {
	api := jobtest.DatabaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	client.Fake.PrependReactor("patch", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, deployment.Name)
	})
	task := versionRestartTask(api)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: nil})

	require.NoError(t, ctl.Run(context.Background()))
	require.Empty(t, api.Status)
	require.Equal(t, config.StatusSkipped, task.Status)
}

func TestVersionRestartJobCtlFailsWhenRestartedDeploymentPodCrashLoops(t *testing.T) {
	api := jobtest.DatabaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	task.Timeout = 3
	_, err := client.CoreV1().Pods("default").Create(context.Background(), versionRestartReadyPod(api, "api-old", "old-restarted-at"), metav1.CreateOptions{})
	require.NoError(t, err)
	waiter := jobtest.StartWorkloadTestObserver(t, client)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, newJobRuntime(&jobexec.Runtime{
		Client:         client,
		Store:          store,
		ResourceWaiter: waiter,
	}))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForDeploymentRestartAt(t, client, deployment.Name)
	_, err = client.CoreV1().Pods("default").Create(context.Background(), versionRestartCrashLoopPod(api, "api-new", restartedAt), metav1.CreateOptions{})
	require.NoError(t, err)

	err = readVersionRestartResult(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
	require.Equal(t, config.StatusFailed, task.Status)
	require.Contains(t, task.Error, "CrashLoopBackOff")
	require.Equal(t, string(config.ComponentStatusRestarting), api.Status)
}

func TestVersionRestartJobCtlFailsWhenRestartedStatefulSetPodCrashLoops(t *testing.T) {
	db := jobtest.DatabaseResetStoreComponent(t, "mysql", nil)
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	client := fake.NewSimpleClientset(statefulSet)
	task := versionRestartTask(db)
	task.Timeout = 3
	_, err := client.CoreV1().Pods("default").Create(context.Background(), versionRestartReadyPod(db, "mysql-old", "old-restarted-at"), metav1.CreateOptions{})
	require.NoError(t, err)
	waiter := jobtest.StartWorkloadTestObserver(t, client)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, newJobRuntime(&jobexec.Runtime{
		Client:         client,
		Store:          store,
		ResourceWaiter: waiter,
	}))

	result := runVersionRestartAsync(ctl)
	restartedAt := waitForStatefulSetRestartAt(t, client, statefulSet.Name)
	_, err = client.CoreV1().Pods("default").Create(context.Background(), versionRestartCrashLoopPod(db, "mysql-new", restartedAt), metav1.CreateOptions{})
	require.NoError(t, err)

	err = readVersionRestartResult(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
	require.Equal(t, config.StatusFailed, task.Status)
	require.Contains(t, task.Error, "CrashLoopBackOff")
	require.Equal(t, string(config.ComponentStatusRestarting), db.Status)
}

func TestVersionRestartJobCtlFailsWhenResourceWaiterMissing(t *testing.T) {
	api := jobtest.DatabaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(api)
	deployment := databaseResetDeployment(t, api)
	client := fake.NewSimpleClientset(deployment)
	task := versionRestartTask(api)
	ctl := jobdeploy.NewVersionRestartJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: nil})

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
		JobInfo: &jobdeploy.VersionRestartJobInfo{
			Component: component,
		},
	}
}

func runVersionRestartAsync(ctl *jobdeploy.VersionRestartJobCtl) <-chan error {
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
	case <-time.After(5 * time.Second):
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
