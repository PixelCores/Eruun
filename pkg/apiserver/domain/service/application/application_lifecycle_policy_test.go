package application

import (
	"context"

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

func TestSharedLifecycleStrategyForComponent(t *testing.T) {
	tests := []struct {
		name         string
		component    *model.ApplicationComponent
		wantStrategy spec.ShareStrategy
		wantShared   bool
	}{
		{
			name: "nil component is managed",
		},
		{
			name:      "nil traits are managed",
			component: &model.ApplicationComponent{},
		},
		{
			name:      "empty traits are managed",
			component: &model.ApplicationComponent{Traits: &model.JSONStruct{}},
		},
		{
			name: "default strategy is shared",
			component: &model.ApplicationComponent{Traits: &model.JSONStruct{
				"share": map[string]interface{}{"strategy": string(spec.ShareStrategyDefault)},
			}},
			wantStrategy: spec.ShareStrategyDefault,
			wantShared:   true,
		},
		{
			name: "ignore strategy is shared",
			component: &model.ApplicationComponent{Traits: &model.JSONStruct{
				"share": map[string]interface{}{"strategy": string(spec.ShareStrategyIgnore)},
			}},
			wantStrategy: spec.ShareStrategyIgnore,
			wantShared:   true,
		},
		{
			name: "force strategy is managed",
			component: &model.ApplicationComponent{Traits: &model.JSONStruct{
				"share": map[string]interface{}{"strategy": string(spec.ShareStrategyForce)},
			}},
		},
		{
			name: "unknown strategy falls back to shared default",
			component: &model.ApplicationComponent{Traits: &model.JSONStruct{
				"share": map[string]interface{}{"strategy": "future-default"},
			}},
			wantStrategy: spec.ShareStrategyDefault,
			wantShared:   true,
		},
		{
			name: "undecodable traits are managed",
			component: &model.ApplicationComponent{Traits: &model.JSONStruct{
				"share": make(chan int),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotStrategy spec.ShareStrategy
			var gotShared bool
			logOutput := captureLifecycleKlogOutput(t, func() {
				gotStrategy, gotShared = SharedLifecycleStrategyForComponent(tt.component)
			})

			require.Equal(t, tt.wantStrategy, gotStrategy)
			require.Equal(t, tt.wantShared, gotShared)
			require.Empty(t, logOutput)
		})
	}
}

func TestApplicationLifecycleOperationsHonorSharePolicy(t *testing.T) {
	type lifecycleOperation struct {
		name               string
		initialStatus      config.ComponentStatus
		deploymentReplicas int32
		run                func(*applicationsServiceImpl, string) ([]string, error)
	}
	operations := []lifecycleOperation{
		{
			name:               "restart",
			initialStatus:      config.ComponentStatusRunning,
			deploymentReplicas: 2,
			run: func(svc *applicationsServiceImpl, appID string) ([]string, error) {
				resp, err := svc.RestartApplicationWorkloads(context.Background(), appID, apisv1.ApplicationLifecycleRequest{})
				if resp == nil {
					return nil, err
				}
				return resp.SkippedResources, err
			},
		},
		{
			name:               "stop",
			initialStatus:      config.ComponentStatusRunning,
			deploymentReplicas: 2,
			run: func(svc *applicationsServiceImpl, appID string) ([]string, error) {
				resp, err := svc.StopApplicationDeployments(context.Background(), appID, apisv1.ApplicationLifecycleRequest{})
				if resp == nil {
					return nil, err
				}
				return resp.SkippedResources, err
			},
		},
		{
			name:               "start",
			initialStatus:      config.ComponentStatusStopped,
			deploymentReplicas: 0,
			run: func(svc *applicationsServiceImpl, appID string) ([]string, error) {
				resp, err := svc.StartApplicationDeployments(context.Background(), appID, apisv1.ApplicationLifecycleRequest{})
				if resp == nil {
					return nil, err
				}
				return resp.SkippedResources, err
			},
		},
	}
	strategies := []struct {
		name      string
		strategy  string
		protected bool
	}{
		{name: "default", strategy: string(spec.ShareStrategyDefault), protected: true},
		{name: "ignore", strategy: string(spec.ShareStrategyIgnore), protected: true},
		{name: "unknown", strategy: "future-default", protected: true},
		{name: "force", strategy: string(spec.ShareStrategyForce)},
	}

	for _, operation := range operations {
		for _, strategy := range strategies {
			t.Run(operation.name+"_"+strategy.name, func(t *testing.T) {
				app := &model.Applications{ID: "app-lifecycle-share", Name: "demo", Namespace: "default"}
				readyReplicas := operation.deploymentReplicas
				web := &model.ApplicationComponent{
					Name:          "shared-web",
					AppID:         app.ID,
					Namespace:     "default",
					ComponentType: config.ServerJob,
					Replicas:      2,
					ReadyReplicas: readyReplicas,
					Status:        string(operation.initialStatus),
					LastAbnormal:  "unchanged",
					Image:         "nginx:latest",
					Properties:    mustJSONStruct(&model.Properties{}),
					Traits: mustJSONStruct(&spec.Traits{
						Share: &spec.ShareTraitSpec{Strategy: strategy.strategy},
					}),
				}
				store := &cleanupStore{
					app:        app,
					components: []*model.ApplicationComponent{web},
					applications: map[string]*model.Applications{
						app.ID: app,
					},
				}
				deploymentName := naming.WebServiceName(web.Name, web.ResourceNameKey())
				replicas := operation.deploymentReplicas
				clientset := fake.NewSimpleClientset(&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: "default"},
					Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
				})
				svc := &applicationsServiceImpl{
					ScheduleLocker: locker.NewMemoryLocker("test-app-schedule"),
					KubeClient:     clientset,
					Store:          store,
					AppRepo:        &mockCleanupAppRepo{store: store},
					ComponentRepo:  &mockCleanupComponentRepo{store: store},
				}

				skipped, err := operation.run(svc, app.ID)
				require.NoError(t, err)

				deployment, err := clientset.AppsV1().Deployments("default").Get(context.Background(), deploymentName, metav1.GetOptions{})
				require.NoError(t, err)
				resource := "Deployment:default/" + deploymentName
				if strategy.protected {
					require.Contains(t, skipped, resource)
					require.Equal(t, operation.deploymentReplicas, *deployment.Spec.Replicas)
					require.Empty(t, deployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
					require.Equal(t, string(operation.initialStatus), web.Status)
					require.Equal(t, readyReplicas, web.ReadyReplicas)
					require.Equal(t, "unchanged", web.LastAbnormal)
					return
				}

				require.NotContains(t, skipped, resource)
				switch operation.name {
				case "restart":
					require.NotEmpty(t, deployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
					require.Equal(t, string(config.ComponentStatusRestarting), web.Status)
					require.Equal(t, readyReplicas, web.ReadyReplicas)
				case "stop":
					require.Equal(t, int32(0), *deployment.Spec.Replicas)
					require.Equal(t, string(config.ComponentStatusStopped), web.Status)
					require.Equal(t, int32(0), web.ReadyReplicas)
				case "start":
					require.Equal(t, int32(2), *deployment.Spec.Replicas)
					require.Equal(t, string(config.ComponentStatusStarting), web.Status)
					require.Equal(t, int32(0), web.ReadyReplicas)
				}
				require.Empty(t, web.LastAbnormal)
			})
		}
	}
}
