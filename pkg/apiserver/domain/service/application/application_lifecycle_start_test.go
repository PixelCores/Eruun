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

func TestStartApplicationDeploymentsRestoresStoredReplicas(t *testing.T) {
	app := &model.Applications{
		ID:        "app-start-1",
		Name:      "demo-start",
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

	deployReplicas := int32(0)
	statefulReplicas := int32(0)
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

	resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.NotEmpty(t, resp.TaskID)
	require.NotEmpty(t, resp.StartedAt)
	require.Contains(t, resp.StartedResources, "Deployment:default/"+deployName)
	require.Empty(t, resp.FailedResources)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(3), *deploy.Spec.Replicas)

	statefulSet, err := clientset.AppsV1().StatefulSets("ops").Get(context.Background(), statefulName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, statefulSet.Spec.Replicas)
	require.Equal(t, int32(0), *statefulSet.Spec.Replicas)

	require.Equal(t, int32(3), web.Replicas)
	require.Equal(t, int32(0), web.ReadyReplicas)
	require.Equal(t, string(config.ComponentStatusStarting), web.Status)
	require.Equal(t, int32(2), db.Replicas)
	require.Equal(t, string(config.ComponentStatusStopped), db.Status)
}

func TestStartApplicationDeploymentsTriggersRequestCallback(t *testing.T) {
	callbackServer, received := newLifecycleCallbackServer(t)
	app := &model.Applications{
		ID:        "app-start-callback",
		Name:      "demo-start-callback",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      2,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
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

	replicas := int32(0)
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

	resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, resp.TaskID, queueRepo.lastQueue.TaskID)
	require.Equal(t, config.WorkflowTaskTypeStart, queueRepo.lastQueue.Type)
	requireWorkflowCallbackSuccess(t, queueRepo.lastQueue.Callback, callbackServer.URL)
	requireLifecycleCallback(t, received, "success", string(config.StatusCompleted), resp.TaskID, config.WorkflowTaskTypeStart)
}

func TestStartApplicationDeploymentsReturnsErrorWhenCallbackTaskCreateFails(t *testing.T) {
	callbackServer, received := newLifecycleCallbackServer(t)
	app := &model.Applications{
		ID:        "app-start-callback-create-fail",
		Name:      "demo-start-callback-create-fail",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      2,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
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

	replicas := int32(0)
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

	resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Success: callbackServer.URL},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "record start callback task")
	require.Contains(t, err.Error(), "queue create failed")
	require.Nil(t, resp)
	require.Nil(t, queueRepo.lastQueue)
	require.Empty(t, queueRepo.queues)
	requireNoCallbackReceived(t, received)

	deploy, getErr := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(2), *deploy.Spec.Replicas)
	require.Equal(t, string(config.ComponentStatusStarting), web.Status)
}

func TestStartApplicationDeploymentsRejectsMissingStoredReplicas(t *testing.T) {
	app := &model.Applications{
		ID:        "app-start-2",
		Name:      "demo-start-invalid",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      0,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
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

	deployReplicas := int32(0)
	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &deployReplicas},
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

	resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.StartedResources)
	require.Len(t, resp.FailedResources, 1)
	require.Contains(t, resp.FailedResources[0], "stored replicas must be greater than 0")

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(0), *deploy.Spec.Replicas)
	require.Equal(t, int32(0), web.Replicas)
	require.Equal(t, string(config.ComponentStatusStopped), web.Status)
}

func TestStartApplicationDeploymentsTriggersFailureCallbackForPartialFailure(t *testing.T) {
	callbackServer, received := newLifecycleCallbackServer(t)
	app := &model.Applications{
		ID:        "app-start-failure-callback",
		Name:      "demo-start-failure-callback",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      0,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
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

	deployReplicas := int32(0)
	deployName := naming.WebServiceName(web.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: &deployReplicas},
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

	resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{
		Callback: &apisv1.WorkflowCallback{Failure: callbackServer.URL},
	})

	require.Error(t, err)
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.TaskID)
	require.Len(t, resp.FailedResources, 1)
	require.NotNil(t, queueRepo.lastQueue)
	require.Equal(t, config.StatusFailed, queueRepo.lastQueue.Status)
	var callback model.WorkflowCallback
	require.NoError(t, decodeJSONStruct(queueRepo.lastQueue.Callback, &callback))
	require.Equal(t, callbackServer.URL, callback.Failure)
	requireLifecycleCallback(t, received, "failure", string(config.StatusFailed), resp.TaskID, config.WorkflowTaskTypeStart)
}

func TestStartApplicationDeploymentsSkipsNonStoppedComponent(t *testing.T) {
	cases := []struct {
		name          string
		status        config.ComponentStatus
		replicas      int32
		readyReplicas int32
	}{
		{
			name:          "running",
			status:        config.ComponentStatusRunning,
			replicas:      3,
			readyReplicas: 3,
		},
		{
			name:          "pending retry",
			status:        config.ComponentStatusPending,
			replicas:      3,
			readyReplicas: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := &model.Applications{
				ID:        "app-start-non-stopped",
				Name:      "demo-start-non-stopped",
				Namespace: "default",
			}
			web := &model.ApplicationComponent{
				Name:          "web",
				AppID:         app.ID,
				Namespace:     "default",
				ComponentType: config.ServerJob,
				Replicas:      tc.replicas,
				ReadyReplicas: tc.readyReplicas,
				Status:        string(tc.status),
				LastAbnormal:  "unchanged",
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

			deployReplicas := tc.replicas
			deployName := naming.WebServiceName(web.Name, app.Name)
			clientset := fake.NewSimpleClientset(
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"},
					Spec:       appsv1.DeploymentSpec{Replicas: &deployReplicas},
				},
			)

			svc := &applicationsServiceImpl{
				ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
				KubeClient:     clientset,
				AppRepo:        &mockCleanupAppRepo{store: store},
				ComponentRepo:  &mockCleanupComponentRepo{store: store},
			}

			resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Empty(t, resp.StartedResources)
			require.Contains(t, resp.SkippedResources, "Deployment:default/"+deployName)
			require.Empty(t, clientset.Actions())

			deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotNil(t, deploy.Spec.Replicas)
			require.Equal(t, tc.replicas, *deploy.Spec.Replicas)
			require.Equal(t, tc.replicas, web.Replicas)
			require.Equal(t, tc.readyReplicas, web.ReadyReplicas)
			require.Equal(t, string(tc.status), web.Status)
			require.Equal(t, "unchanged", web.LastAbnormal)
		})
	}
}

func TestStartApplicationDeploymentsSkipsSharedComponent(t *testing.T) {
	app := &model.Applications{
		ID:        "app-start-3",
		Name:      "demo-start-shared",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "shared-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployReplicas := int32(0)
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

	resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.Empty(t, resp.StartedResources)
	require.Contains(t, resp.SkippedResources, "Deployment:default/"+deployName)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(0), *deploy.Spec.Replicas)
	require.Equal(t, int32(1), web.Replicas)
	require.Equal(t, string(config.ComponentStatusStopped), web.Status)
}

func TestStartApplicationDeploymentsDoesNotSkipSharedForce(t *testing.T) {
	app := &model.Applications{
		ID:        "app-start-4",
		Name:      "demo-start-force",
		Namespace: "default",
	}
	web := &model.ApplicationComponent{
		Name:          "force-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      2,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusStopped),
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{web},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployReplicas := int32(0)
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

	resp, err := svc.StartApplicationDeployments(context.Background(), app.ID, apisv1.ApplicationLifecycleRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.Contains(t, resp.StartedResources, "Deployment:default/"+deployName)

	deploy, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, deploy.Spec.Replicas)
	require.Equal(t, int32(2), *deploy.Spec.Replicas)
	require.Equal(t, int32(2), web.Replicas)
	require.Equal(t, string(config.ComponentStatusStarting), web.Status)
}
