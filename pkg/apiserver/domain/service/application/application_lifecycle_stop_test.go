package application

import (
	"context"
	"errors"

	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestStopApplicationDeploymentsScalesDeploymentsToZero(t *testing.T) {
	app := &model.Applications{
		ID:        "app-stop-1",
		Name:      "demo-stop",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      3,
		ReadyReplicas: 3,
		Status:        string(config.ComponentStatusRunning),
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	db := &model.ApplicationComponent{
		Name:          "db",
		AppID:         app.ID,
		Namespace:     "ops",
		ComponentType: config.StoreJob,
		Replicas:      2,
		ReadyReplicas: 2,
		Status:        string(config.ComponentStatusRunning),
		Image:         "mysql:8",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web, db},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployReplicas := int32(3)
	statefulReplicas := int32(2)
	deployName := naming.WebServiceName(web.Name, app.Name)
	statefulName := naming.StoreServerName(db.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &deployReplicas},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: statefulName, Namespace: "ops"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &statefulReplicas},
		},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	resp, err := svc.StopApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.NotEmpty(t, resp.TaskID)
	require.NotEmpty(t, resp.StoppedAt)
	require.Contains(t, resp.StoppedResources, "Deployment:default/"+deployName)
	require.Empty(t, resp.FailedResources)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(0), *deploy.Spec.Replicas)

	statefulSet, err := clientset.AppsV1().StatefulSets("ops").Get(context.Background(), statefulName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, statefulSet.Spec.Replicas)
	require.Equal(t, int32(2), *statefulSet.Spec.Replicas)

	require.Equal(t, int32(3), web.Replicas)
	require.Equal(t, int32(0), web.ReadyReplicas)
	require.Equal(t, string(config.ComponentStatusStopped), web.Status)
	require.Equal(t, int32(2), db.Replicas)
	require.Equal(t, string(config.ComponentStatusRunning), db.Status)
}

func TestStopApplicationDeploymentsTriggersRequestCallback(t *testing.T) {
	callbackServer, received := newLifecycleCallbackServer(t)
	app := &model.Applications{
		ID:        "app-stop-callback",
		Name:      "demo-stop-callback",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      2,
		ReadyReplicas: 2,
		Status:        string(config.ComponentStatusRunning),
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	replicas := int32(2)
	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
	)
	queueRepo := &mockWorkflowQueueRepo{}
	svc := &applicationsServiceImpl{
		ScheduleLocker:            locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:                clientset,
		Store:                     store,
		AppRepo:                   &mockCleanupAppRepo{store: store},
		ComponentRepo:             &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo:         queueRepo,
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}),
	}

	resp, err := svc.StopApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, resp.TaskID, queueRepo.lastQueue.TaskID)
	require.Equal(t, config.WorkflowTaskTypeStop, queueRepo.lastQueue.Type)
	requireWorkflowCallbackSuccess(t, queueRepo.lastQueue.Callback, callbackServer.URL)
	requireLifecycleCallback(t, received, "success", string(config.StatusCompleted), resp.TaskID, config.WorkflowTaskTypeStop)
}

func TestStopApplicationDeploymentsReturnsErrorWhenCallbackTaskCreateFails(t *testing.T) {
	callbackServer, received := newLifecycleCallbackServer(t)
	app := &model.Applications{
		ID:        "app-stop-callback-create-fail",
		Name:      "demo-stop-callback-create-fail",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      2,
		ReadyReplicas: 2,
		Status:        string(config.ComponentStatusRunning),
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	replicas := int32(2)
	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
	)
	queueRepo := &mockWorkflowQueueRepo{createErr: errors.New("queue create failed")}
	svc := &applicationsServiceImpl{
		ScheduleLocker:            locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:                clientset,
		Store:                     store,
		AppRepo:                   &mockCleanupAppRepo{store: store},
		ComponentRepo:             &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo:         queueRepo,
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}),
	}

	resp, err := svc.StopApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "record stop callback task")
	require.Contains(t, err.Error(), "queue create failed")
	require.Nil(t, resp)
	require.Nil(t, queueRepo.lastQueue)
	require.Empty(t, queueRepo.queues)
	requireNoCallbackReceived(t, received)

	deploy, getErr := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(0), *deploy.Spec.Replicas)
	require.Equal(t, string(config.ComponentStatusStopped), web.Status)
}

func TestStopApplicationDeploymentsRejectsInvalidCallbackBeforeMutatingWorkloads(t *testing.T) {
	app := &model.Applications{
		ID:        "app-stop-invalid-callback",
		Name:      "demo-stop-invalid-callback",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      2,
		ReadyReplicas: 2,
		Status:        string(config.ComponentStatusRunning),
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	replicas := int32(2)
	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
	)
	queueRepo := &mockWorkflowQueueRepo{}
	svc := &applicationsServiceImpl{
		ScheduleLocker:            locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:                clientset,
		Store:                     store,
		AppRepo:                   &mockCleanupAppRepo{store: store},
		ComponentRepo:             &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo:         queueRepo,
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}),
	}

	resp, err := svc.StopApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Success: "ftp://example.com/callback"},
	})

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrWorkflowConfig)
	require.Nil(t, queueRepo.lastQueue)

	deploy, getErr := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(2), *deploy.Spec.Replicas)
	require.Equal(t, string(config.ComponentStatusRunning), web.Status)
}

func TestStopApplicationDeploymentsSkipsSharedComponent(t *testing.T) {
	app := &model.Applications{
		ID:        "app-stop-2",
		Name:      "demo-stop-shared",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "shared-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		ReadyReplicas: 1,
		Status:        string(config.ComponentStatusRunning),
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(config.ShareStrategyDefault)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployReplicas := int32(1)
	deployName := naming.WebServiceName(web.Name, web.ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &deployReplicas},
		},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:     clientset,
		Store:          store,
		AppRepo:        &mockCleanupAppRepo{store: store},
		ComponentRepo:  &mockCleanupComponentRepo{store: store},
	}

	resp, err := svc.StopApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.Empty(t, resp.StoppedResources)
	require.Contains(t, resp.SkippedResources, "Deployment:default/"+deployName)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(1), *deploy.Spec.Replicas)
	require.Equal(t, int32(1), web.Replicas)
	require.Equal(t, string(config.ComponentStatusRunning), web.Status)
}
