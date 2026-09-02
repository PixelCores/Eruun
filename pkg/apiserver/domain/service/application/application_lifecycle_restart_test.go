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

	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestRestartApplicationWorkloadsRestartsWorkloads(t *testing.T) {
	app := &model.Applications{
		ID:        "app-3",
		Name:      "demo-3",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	db := &model.ApplicationComponent{
		Name:          "db",
		AppID:         app.ID,
		Namespace:     "ops",
		ComponentType: config.StoreJob,
		Replicas:      1,
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

	deployName := naming.WebServiceName(web.Name, app.Name)
	statefulName := naming.StoreServerName(db.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: statefulName, Namespace: "ops"}},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	resp, err := svc.RestartApplicationWorkloads(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.NotEmpty(t, resp.TaskID)
	require.NotEmpty(t, resp.RestartedAt)
	require.Contains(t, resp.RestartedResources, "Deployment:default/"+deployName)
	require.Contains(t, resp.RestartedResources, "StatefulSet:ops/"+statefulName)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, deploy.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])

	statefulSet, err := clientset.AppsV1().StatefulSets("ops").Get(context.Background(), statefulName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, statefulSet.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])

	require.Equal(t, string(config.ComponentStatusRestarting), web.Status)
	require.Equal(t, string(config.ComponentStatusRestarting), db.Status)
}

func TestRestartApplicationWorkloadsTriggersRequestCallback(t *testing.T) {
	callbackServer, received := newLifecycleCallbackServer(t)
	app := &model.Applications{
		ID:        "app-restart-callback",
		Name:      "demo-restart-callback",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
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

	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
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

	resp, err := svc.RestartApplicationWorkloads(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, resp.TaskID, queueRepo.lastQueue.TaskID)
	require.Equal(t, config.WorkflowTaskTypeRestart, queueRepo.lastQueue.Type)
	requireWorkflowCallbackSuccess(t, queueRepo.lastQueue.Callback, callbackServer.URL)
	requireLifecycleCallback(t, received, "success", string(config.StatusCompleted), resp.TaskID, config.WorkflowTaskTypeRestart)
}

func TestRestartApplicationWorkloadsReturnsErrorWhenCallbackTaskCreateFails(t *testing.T) {
	callbackServer, received := newLifecycleCallbackServer(t)
	app := &model.Applications{
		ID:        "app-restart-callback-create-fail",
		Name:      "demo-restart-callback-create-fail",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
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

	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
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

	resp, err := svc.RestartApplicationWorkloads(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "record restart callback task")
	require.Contains(t, err.Error(), "queue create failed")
	require.Nil(t, resp)
	require.Nil(t, queueRepo.lastQueue)
	require.Empty(t, queueRepo.queues)
	requireNoCallbackReceived(t, received)

	deploy, getErr := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.NotEmpty(t, deploy.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
}

func TestRestartApplicationWorkloadsReturnsErrorWhenMarkRestartingFails(t *testing.T) {
	app := &model.Applications{
		ID:        "app-restart-mark-fail",
		Name:      "demo-restart-mark-fail",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
		runtimeUpdateErr: errors.New("status store unavailable"),
	}

	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)
	queueRepo := &mockWorkflowQueueRepo{}

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: queueRepo,
	}

	resp, err := svc.RestartApplicationWorkloads(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mark components restarting")
	require.Contains(t, err.Error(), "status store unavailable")
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.TaskID)
	require.NotNil(t, queueRepo.lastQueue)
	require.Len(t, queueRepo.queues, 1)
	require.Equal(t, resp.TaskID, queueRepo.lastQueue.TaskID)
	require.Equal(t, config.WorkflowTaskTypeRestart, queueRepo.lastQueue.Type)
	require.Equal(t, config.StatusFailed, queueRepo.lastQueue.Status)
	require.Len(t, resp.FailedResources, 1)
	require.Contains(t, resp.FailedResources[0], "ComponentStatus:default/"+app.ID)
	require.Contains(t, resp.FailedResources[0], "mark components restarting")
	require.Contains(t, resp.FailedResources[0], "status store unavailable")

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, deploy.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
}

func TestRestartApplicationWorkloadsSkipsSharedComponent(t *testing.T) {
	app := &model.Applications{
		ID:        "app-4",
		Name:      "demo-4",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "shared-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
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

	deployName := naming.WebServiceName(web.Name, web.ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:     clientset,
		Store:          store,
		AppRepo:        &mockCleanupAppRepo{store: store},
		ComponentRepo:  &mockCleanupComponentRepo{store: store},
	}

	resp, err := svc.RestartApplicationWorkloads(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.Empty(t, resp.RestartedResources)
	require.Contains(t, resp.SkippedResources, "Deployment:default/"+deployName)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, deploy.Spec.Template.Annotations)
	require.NotEqual(t, string(config.ComponentStatusRestarting), web.Status)
}

func TestRestartApplicationWorkloadsDoesNotSkipSharedForce(t *testing.T) {
	app := &model.Applications{
		ID:        "app-5",
		Name:      "demo-5",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "force-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(config.ShareStrategyForce)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployName := naming.WebServiceName(web.Name, web.ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:     clientset,
		Store:          store,
		AppRepo:        &mockCleanupAppRepo{store: store},
		ComponentRepo:  &mockCleanupComponentRepo{store: store},
	}

	resp, err := svc.RestartApplicationWorkloads(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.Contains(t, resp.RestartedResources, "Deployment:default/"+deployName)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, deploy.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	require.Equal(t, string(config.ComponentStatusRestarting), web.Status)
}

func TestRestartApplicationWorkloadsSkipsStoppedComponents(t *testing.T) {
	app := &model.Applications{
		ID:        "app-restart-stopped",
		Name:      "demo-restart-stopped",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      3,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
		LastAbnormal:  "stopped web",
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	db := &model.ApplicationComponent{
		Name:          "db",
		AppID:         app.ID,
		Namespace:     "ops",
		ComponentType: config.StoreJob,
		Replicas:      2,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
		LastAbnormal:  "stopped db",
		Image:         "mysql:8",
		Properties:    mustJSONStruct(&model.Properties{}),
	}
	forceWeb := &model.ApplicationComponent{
		Name:          "force-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      4,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
		LastAbnormal:  "stopped force",
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(config.ShareStrategyForce)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web, db, forceWeb},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	webReplicas := int32(0)
	dbReplicas := int32(0)
	forceReplicas := int32(0)
	deployName := naming.WebServiceName(web.Name, app.Name)
	statefulName := naming.StoreServerName(db.Name, app.Name)
	forceDeployName := naming.WebServiceName(forceWeb.Name, forceWeb.ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &webReplicas},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: statefulName, Namespace: "ops"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &dbReplicas},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: forceDeployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &forceReplicas},
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

	resp, err := svc.RestartApplicationWorkloads(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.RestartedResources)
	require.Contains(t, resp.SkippedResources, "Deployment:default/"+deployName)
	require.Contains(t, resp.SkippedResources, "StatefulSet:ops/"+statefulName)
	require.Contains(t, resp.SkippedResources, "Deployment:default/"+forceDeployName)
	require.Empty(t, clientset.Actions())

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, deploy.Spec.Template.Annotations)
	statefulSet, err := clientset.AppsV1().StatefulSets("ops").Get(context.Background(), statefulName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, statefulSet.Spec.Template.Annotations)
	forceDeploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), forceDeployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, forceDeploy.Spec.Template.Annotations)

	require.Equal(t, int32(3), web.Replicas)
	require.Equal(t, int32(0), web.ReadyReplicas)
	require.Equal(t, string(config.ComponentStatusStopped), web.Status)
	require.Equal(t, "stopped web", web.LastAbnormal)
	require.Equal(t, int32(2), db.Replicas)
	require.Equal(t, int32(0), db.ReadyReplicas)
	require.Equal(t, string(config.ComponentStatusStopped), db.Status)
	require.Equal(t, "stopped db", db.LastAbnormal)
	require.Equal(t, int32(4), forceWeb.Replicas)
	require.Equal(t, int32(0), forceWeb.ReadyReplicas)
	require.Equal(t, string(config.ComponentStatusStopped), forceWeb.Status)
	require.Equal(t, "stopped force", forceWeb.LastAbnormal)
}
