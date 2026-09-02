package application

import (
	"context"

	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"

	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestCleanupApplicationResourcesDeletesWorkload(t *testing.T) {
	app := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "default",
	}
	props, err := model.NewJSONStructByStruct(&model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Properties:    props,
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployName := naming.WebServiceName(component.Name, app.Name)
	serviceName := naming.ServiceName(component.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: "default"}},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.NotEmpty(t, resp.TaskID)
	require.Contains(t, resp.DeletedResources, "Deployment:default/"+deployName)
	require.Contains(t, resp.DeletedResources, "Service:default/"+serviceName)
	require.Empty(t, resp.FailedResources)

	// 验证组件状态被更新为 Not Deploy
	require.Equal(t, string(config.ComponentStatusNotDeploy), component.Status)
	require.Equal(t, int32(0), component.ReadyReplicas)
	require.Empty(t, component.LastAbnormal)
}

func TestCleanupApplicationResourcesPreservesStoragePVC(t *testing.T) {
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)

	app := &model.Applications{
		ID:        "app-pvc",
		Name:      "demo-pvc",
		Namespace: "default",
	}
	component := &model.ApplicationComponent{
		Name:          "web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Traits: mustJSONStruct(&spec.Traits{
			Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("logs", "default-logs-pvc", false)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployName := naming.WebServiceName(component.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "default-logs-pvc", Namespace: "default"}},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Contains(t, resp.DeletedResources, "Deployment:default/"+deployName)
	require.NotContains(t, resp.DeletedResources, "PersistentVolumeClaim:default/default-logs-pvc")

	_, err = clientset.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "default-logs-pvc", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupApplicationResourcesPreservesTraitGeneratedRBAC(t *testing.T) {
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)

	app := &model.Applications{ID: "app-rbac", Name: "demo-rbac", Namespace: "ops"}
	component := &model.ApplicationComponent{
		Name: "labeler", AppID: app.ID, Namespace: app.Namespace,
		ComponentType: config.ServerJob, Replicas: 1, Image: "busybox:latest",
		Traits: mustJSONStruct(&spec.Traits{
			RBAC: []spec.RBACPolicySpec{
				{
					ServiceAccount: "pod-labeler-sa", RoleName: "pod-labeler-role", BindingName: "pod-labeler-binding",
					Rules: []spec.RBACRuleSpec{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "patch"}}},
				},
				{
					ServiceAccount: "cluster-labeler-sa", ClusterScope: true,
					RoleName: "pod-labeler-cluster-role", BindingName: "pod-labeler-cluster-binding",
					Rules: []spec.RBACRuleSpec{{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get"}}},
				},
			},
		}),
	}
	store := &cleanupStore{
		app: app, components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{app.ID: app},
	}
	deploymentName := naming.WebServiceName(component.Name, app.Name)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: app.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-sa", Namespace: app.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "cluster-labeler-sa", Namespace: app.Namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-role", Namespace: app.Namespace}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-binding", Namespace: app.Namespace}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-cluster-role"}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-cluster-binding"}},
	)
	svc := &applicationsServiceImpl{
		ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:     clientset, Store: store,
		AppRepo: &mockCleanupAppRepo{store: store}, ComponentRepo: &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.Contains(t, resp.DeletedResources, "Deployment:"+app.Namespace+"/"+deploymentName)
	_, err = clientset.CoreV1().ServiceAccounts(app.Namespace).Get(context.Background(), "pod-labeler-sa", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = clientset.CoreV1().ServiceAccounts(app.Namespace).Get(context.Background(), "cluster-labeler-sa", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = clientset.RbacV1().Roles(app.Namespace).Get(context.Background(), "pod-labeler-role", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = clientset.RbacV1().RoleBindings(app.Namespace).Get(context.Background(), "pod-labeler-binding", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = clientset.RbacV1().ClusterRoles().Get(context.Background(), "pod-labeler-cluster-role", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = clientset.RbacV1().ClusterRoleBindings().Get(context.Background(), "pod-labeler-cluster-binding", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupApplicationResourcesDeletesTraitServicesByLabels(t *testing.T) {
	app := &model.Applications{
		ID:        "app-trait-svc",
		Name:      "demo-trait-svc",
		Namespace: "default",
	}
	component := &model.ApplicationComponent{
		Name:          "Mongo_DB",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Replicas:      1,
		Image:         "mongo:8.0",
		Properties: mustJSONStruct(&model.Properties{
			Env: map[string]string{
				"TZ": "Asia/Shanghai",
			},
		}),
		Traits: mustJSONStruct(&spec.Traits{
			Service: []spec.ServiceTraitSpec{
				{
					Name: "mongo-headless",
					Type: "internal",
					Ports: []spec.ServicePortTraitSpec{
						{Port: 27017, TargetPort: 27017, Protocol: "TCP"},
					},
					Selector: map[string]string{"name": "mongodb"},
					Headless: true,
				},
				{
					Name: "mongo-rw",
					Type: "internal",
					Ports: []spec.ServicePortTraitSpec{
						{Port: 27017, TargetPort: 27017, Protocol: "TCP"},
					},
					Selector: map[string]string{"name": "mongodb"},
				},
			},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	headlessName := naming.ServiceName("mongo-headless", app.Name)
	rwName := naming.ServiceName("mongo-rw", app.Name)
	clientset := fake.NewSimpleClientset(
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      headlessName,
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         app.ID,
					config.LabelComponentName: naming.BoundedLabelValue(component.Name),
				},
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rwName,
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         app.ID,
					config.LabelComponentName: naming.BoundedLabelValue(component.Name),
				},
			},
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

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Contains(t, resp.DeletedResources, "Service:default/"+headlessName)
	require.Contains(t, resp.DeletedResources, "Service:default/"+rwName)

	_, err = clientset.CoreV1().Services("default").Get(context.Background(), headlessName, metav1.GetOptions{})
	require.Error(t, err)
	_, err = clientset.CoreV1().Services("default").Get(context.Background(), rwName, metav1.GetOptions{})
	require.Error(t, err)
}

func TestCleanupApplicationResourcesDeletesIngressByLabels(t *testing.T) {
	app := &model.Applications{
		ID:        "app-trait-ing",
		Name:      "demo-trait-ing",
		Namespace: "default",
	}
	component := &model.ApplicationComponent{
		Name:          "Redis_API",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Replicas:      1,
		Image:         "redis:7",
		Properties: mustJSONStruct(&model.Properties{
			Env: map[string]string{
				"TZ": "Asia/Shanghai",
			},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	ingressName := "legacy-redis-ingress"
	clientset := fake.NewSimpleClientset(
		&networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ingressName,
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         app.ID,
					config.LabelComponentName: naming.BoundedLabelValue(component.Name),
				},
			},
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

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Contains(t, resp.DeletedResources, "Ingress:default/"+ingressName)

	_, err = clientset.NetworkingV1().Ingresses("default").Get(context.Background(), ingressName, metav1.GetOptions{})
	require.Error(t, err)
}

func TestCleanupApplicationResourcesSkipsSharedComponent(t *testing.T) {
	app := &model.Applications{
		ID:        "app-share",
		Name:      "demo-share",
		Namespace: "default",
	}
	props := mustJSONStruct(&model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	component := &model.ApplicationComponent{
		Name:          "shared-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Properties:    props,
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployName := naming.WebServiceName(component.Name, component.ResourceNameKey())
	serviceName := naming.ServiceName(component.Name, component.ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: "default"}},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.NotContains(t, resp.DeletedResources, "Deployment:default/"+deployName)
	require.NotContains(t, resp.DeletedResources, "Service:default/"+serviceName)

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = clientset.CoreV1().Services("default").Get(context.Background(), serviceName, metav1.GetOptions{})
	require.NoError(t, err)

	require.Equal(t, string(config.ComponentStatusNotDeploy), component.Status)
	require.Equal(t, int32(0), component.ReadyReplicas)
	require.Empty(t, component.LastAbnormal)
}

func TestCleanupApplicationResourcesSharedAbnormalPodAllowsDelete(t *testing.T) {
	app := &model.Applications{
		ID:        "app-share-abnormal",
		Name:      "demo-share-abnormal",
		Namespace: "default",
	}
	component := &model.ApplicationComponent{
		Name:          "Shared_Web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployName := naming.WebServiceName(component.Name, component.ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "shared-web-pod",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         app.ID,
					config.LabelComponentName: naming.BoundedLabelValue(component.Name),
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "web",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
						},
					},
				},
			},
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

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Contains(t, resp.DeletedResources, "Deployment:default/"+deployName)

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.Error(t, err)
}

func TestCleanupApplicationResourcesDoesNotSkipSharedForce(t *testing.T) {
	app := &model.Applications{
		ID:        "app-force",
		Name:      "demo-force",
		Namespace: "default",
	}
	props, err := model.NewJSONStructByStruct(&model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "force-web",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
		Properties:    props,
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)},
		}),
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	deployName := naming.WebServiceName(component.Name, component.ResourceNameKey())
	serviceName := naming.ServiceName(component.Name, component.ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: "default"}},
	)

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	resp, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, app.ID, resp.AppID)
	require.Contains(t, resp.DeletedResources, "Deployment:default/"+deployName)
	require.Contains(t, resp.DeletedResources, "Service:default/"+serviceName)

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.Error(t, err)
	_, err = clientset.CoreV1().Services("default").Get(context.Background(), serviceName, metav1.GetOptions{})
	require.Error(t, err)

	require.Equal(t, string(config.ComponentStatusNotDeploy), component.Status)
	require.Equal(t, int32(0), component.ReadyReplicas)
	require.Empty(t, component.LastAbnormal)
}

func TestCleanupApplicationResourcesKeepsCleaningWhenPodsRemain(t *testing.T) {
	app := &model.Applications{
		ID:        "app-2",
		Name:      "demo-2",
		Namespace: "default",
	}
	component := &model.ApplicationComponent{
		ID:            7,
		Name:          "Web_API",
		AppID:         app.ID,
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Replicas:      1,
		Image:         "nginx:latest",
	}
	store := &cleanupStore{
		app:        app,
		components: []*model.ApplicationComponent{component},
		applications: map[string]*model.Applications{
			app.ID: app,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-pod",
			Namespace: "default",
			Labels: map[string]string{
				config.LabelAppID:         app.ID,
				config.LabelComponentName: naming.BoundedLabelValue(component.Name),
				config.LabelComponentID:   "7",
			},
		},
	}
	clientset := fake.NewSimpleClientset(pod)

	svc := &applicationsServiceImpl{
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		KubeClient:        clientset,
		Store:             store,
		AppRepo:           &mockCleanupAppRepo{store: store},
		ComponentRepo:     &mockCleanupComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
	}

	_, err := svc.CleanupApplicationResources(context.Background(), app.ID)
	require.NoError(t, err)
	require.Equal(t, string(config.ComponentStatusCleaning), component.Status)
	require.Equal(t, int32(0), component.ReadyReplicas)
	require.Empty(t, component.LastAbnormal)
}
