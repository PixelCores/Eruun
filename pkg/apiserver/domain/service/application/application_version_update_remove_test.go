package application

import (
	"context"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBuildVersionUpdateFullCleanupInfoKeepsV2WhenVCTsAreUnchanged(t *testing.T) {
	currentTraits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
		}},
		Service: []spec.ServiceTraitSpec{{
			Name: "mysql-headless", Type: string(spec.ServiceAccessInternal), Headless: true,
			Selector: map[string]string{config.LabelComponentName: "mysql"},
			Ports:    []spec.ServicePortTraitSpec{{Name: "mysql", Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	}
	desiredTraits := currentTraits
	desiredTraits.Service = append([]spec.ServiceTraitSpec(nil), currentTraits.Service...)
	desiredTraits.Service[0].Name = "mysql-headless-v2"
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: config.DefaultNamespace,
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1,
		Traits: mustJSONStruct(&currentTraits),
	}

	cleanupInfo, err := buildVersionUpdateFullCleanupInfo(
		map[string]*model.ApplicationComponent{"mysql": component},
		[]apisv1.ComponentUpdateSpec{{Action: "update", Name: "mysql", Traits: &desiredTraits}},
		0,
		false,
		true,
	)

	require.NoError(t, err)
	require.Equal(t, model.VersionUpdateCleanupInfoVersionStatefulSetDeletion, cleanupInfo.Version)
	require.Len(t, cleanupInfo.Components, 1)
	require.True(t, cleanupInfo.Components[0].RequireStatefulSetDeletion)
	require.Empty(t, cleanupInfo.Components[0].StatefulSetPVCTemplatesToDelete)
}

func TestBuildVersionUpdateFullCleanupInfoKeepsV1WithoutImmutableChanges(t *testing.T) {
	traits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
		}},
	}
	component := &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: config.DefaultNamespace,
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1,
		Traits: mustJSONStruct(&traits),
	}
	tests := []struct {
		name  string
		specs []apisv1.ComponentUpdateSpec
	}{
		{name: "no component update"},
		{name: "mutable image update", specs: []apisv1.ComponentUpdateSpec{{Action: "update", Name: "mysql", Image: "mysql:8.4"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanupInfo, err := buildVersionUpdateFullCleanupInfo(
				map[string]*model.ApplicationComponent{"mysql": component},
				tt.specs,
				0,
				false,
				true,
			)

			require.NoError(t, err)
			require.Equal(t, model.VersionUpdateCleanupInfoVersionV1, cleanupInfo.Version)
			require.Len(t, cleanupInfo.Components, 1)
			require.False(t, cleanupInfo.Components[0].RequireStatefulSetDeletion)
			require.Empty(t, cleanupInfo.Components[0].StatefulSetPVCTemplatesToDelete)
		})
	}
}

func TestUpdateVersionRemoveComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:  "backend",
		AppID: "app-1",
	}
	store.components["legacy-service"] = &model.ApplicationComponent{
		Name:  "legacy-service",
		AppID: "app-1",
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "backend", WorkflowType: config.JobDeploy},
				{Name: "legacy-service", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)

	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "remove",
				Name:   "legacy-service",
			},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Equal(t, "2.0.0", resp.Version)
	require.Contains(t, resp.RemovedComponents, "legacy-service")
	require.Empty(t, resp.UpdatedComponents)
	require.Empty(t, resp.AddedComponents)
}

func TestUpdateVersionRemoveComponentCleansKubeResources(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["mongodb"] = &model.ApplicationComponent{
		Name:          "mongodb",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.StoreJob,
		Image:         "mongo:8.0",
		Replicas:      1,
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

	resourceAppName := applicationResourceNameKey(store.apps["app-1"])
	statefulSetName := naming.StoreServerName("mongodb", resourceAppName)
	headlessServiceName := naming.ServiceName("mongo-headless", resourceAppName)
	rwServiceName := naming.ServiceName("mongo-rw", resourceAppName)
	clientset := fake.NewSimpleClientset(
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: statefulSetName, Namespace: "default"}},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      headlessServiceName,
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "mongodb",
				},
			},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rwServiceName,
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "mongodb",
				},
			},
		},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset

	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "remove",
				Name:   "mongodb",
			},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Contains(t, resp.RemovedComponents, "mongodb")
	require.Nil(t, store.components["mongodb"])

	_, err = clientset.AppsV1().StatefulSets("default").Get(context.Background(), statefulSetName, metav1.GetOptions{})
	require.Error(t, err)
	_, err = clientset.CoreV1().Services("default").Get(context.Background(), headlessServiceName, metav1.GetOptions{})
	require.Error(t, err)
	_, err = clientset.CoreV1().Services("default").Get(context.Background(), rwServiceName, metav1.GetOptions{})
	require.Error(t, err)
}

func TestUpdateVersionRemoveComponentCleansDeploymentByResourceAppName(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:1.27",
		Replicas:      1,
	}

	resourceAppName := applicationResourceNameKey(store.apps["app-1"])
	deployName := naming.WebServiceName("api", resourceAppName)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deployName,
				Namespace: "default",
			},
		},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "remove",
				Name:   "api",
			},
		},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)
	require.Contains(t, resp.RemovedComponents, "api")
	require.Nil(t, store.components["api"])

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.Error(t, err)
	require.NotEqual(t, naming.WebServiceName("api", "app-1"), deployName)
}

func TestUpdateVersionRejectsProtectedSharedComponentRemove(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["shared-backend"] = &model.ApplicationComponent{
		Name:          "shared-backend",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Replicas:      1,
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)},
		}),
	}

	deployName := naming.WebServiceName("shared-backend", "shared-backend")
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "shared-backend-pod",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "shared-backend",
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "backend",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
						},
					},
				},
			},
		},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset

	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "shared-backend"},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["shared-backend"])

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestUpdateVersionRejectsProtectedSharedComponentRemoveWhenPodAbnormal(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["shared-backend"] = &model.ApplicationComponent{
		Name:          "shared-backend",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Replicas:      1,
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)},
		}),
	}

	deployName := naming.WebServiceName("shared-backend", "shared-backend")
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "shared-backend-pod",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "shared-backend",
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "backend",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
						},
					},
				},
			},
		},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset

	req := apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "shared-backend"},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["shared-backend"])

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestUpdateVersionRemoveForceSharedComponentCleansResources(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["shared-backend"] = &model.ApplicationComponent{
		Name:          "shared-backend",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Replicas:      1,
		Properties:    mustJSONStruct(&model.Properties{}),
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)},
		}),
	}

	deployName := naming.WebServiceName("shared-backend", "shared-backend")
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "2.0.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "shared-backend"},
		},
		AutoExec: boolPtr(false),
	})
	require.NoError(t, err)
	require.Contains(t, resp.RemovedComponents, "shared-backend")
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)
	require.Nil(t, store.components["shared-backend"])

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.Error(t, err)
}

func TestUpdateVersionSyncTemplatePhasedWorkflowOnRemove(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["cfg"] = &model.ApplicationComponent{
		Name:          "cfg",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ConfJob,
	}
	store.components["db"] = &model.ApplicationComponent{
		Name:          "db",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.StoreJob,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}

	defaultSteps := convertWorkflowStepByTemplatePhases([]apisv1.CreateComponentRequest{
		{Name: "cfg", ComponentType: config.ConfJob},
		{Name: "db", ComponentType: config.StoreJob},
		{Name: "api", ComponentType: config.ServerJob},
	})
	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps:        mustJSONStruct(defaultSteps),
	}
	store.workflows["wf-update"] = &model.Workflow{
		ID:           "wf-update",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeUpdate,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "legacy-update-step", WorkflowType: config.JobDeploy, Mode: config.WorkflowModeStepByStep},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "db"},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Contains(t, resp.RemovedComponents, "db")

	steps := decodeWorkflowSteps(t, store.workflows["wf-default"].Steps)
	require.Len(t, steps.Steps, 2)
	require.Equal(t, "phase-2-config-secret", steps.Steps[0].Name)
	require.Equal(t, "phase-5-webservice", steps.Steps[1].Name)
	require.ElementsMatch(t, []string{"cfg"}, steps.Steps[0].ComponentNames())
	require.ElementsMatch(t, []string{"api"}, steps.Steps[1].ComponentNames())
	for _, step := range steps.Steps {
		for _, name := range step.ComponentNames() {
			require.NotEqual(t, "db", name)
		}
	}

	updateSteps := decodeWorkflowSteps(t, store.workflows["wf-update"].Steps)
	require.Len(t, updateSteps.Steps, 1)
	require.Equal(t, "legacy-update-step", updateSteps.Steps[0].Name)
}

func TestUpdateVersionSyncTemplatePhasedWorkflowOnRemoveLegacyPhaseNames(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["cfg"] = &model.ApplicationComponent{
		Name:          "cfg",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ConfJob,
	}
	store.components["db"] = &model.ApplicationComponent{
		Name:          "db",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.StoreJob,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
	}

	store.workflows["wf-default"] = &model.Workflow{
		ID:           "wf-default",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{
					Name:         "phase-1-config-secret",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"cfg"}}},
				},
				{
					Name:         "phase-2-store",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"db"}}},
				},
				{
					Name:         "phase-4-webservice",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"api"}}},
				},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "db"},
		},
		AutoExec: boolPtr(false),
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.Contains(t, resp.RemovedComponents, "db")

	steps := decodeWorkflowSteps(t, store.workflows["wf-default"].Steps)
	require.Len(t, steps.Steps, 2)
	require.Equal(t, "phase-2-config-secret", steps.Steps[0].Name)
	require.Equal(t, "phase-5-webservice", steps.Steps[1].Name)
	require.ElementsMatch(t, []string{"cfg"}, steps.Steps[0].ComponentNames())
	require.ElementsMatch(t, []string{"api"}, steps.Steps[1].ComponentNames())
	for _, step := range steps.Steps {
		for _, name := range step.ComponentNames() {
			require.NotEqual(t, "db", name)
		}
	}
}

func TestUpdateVersionRejectsMissingRemoveComponent(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "DemoApp",
		Version: "1.0.0",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Image:         "api:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}

	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "missing"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrComponentNotFound)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["api"])
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func TestUpdateVersionRejectsSameRequestRemoveAddReuse(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["api"] = &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
			},
		}),
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "api"},
			{Action: "add", Name: "API", Image: "nginx:1.28", ComponentType: config.ServerJob},
		},
	})

	require.ErrorIs(t, err, bcode.ErrDuplicateComponentName)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["api"])
	require.Equal(t, "nginx:1.27", store.components["api"].Image)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func TestUpdateVersionRejectsDuplicateRemoveSpec(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["api"] = &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
			},
		}),
	}
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "api"},
			{Action: "remove", Name: " API "},
		},
	})

	require.ErrorIs(t, err, bcode.ErrDuplicateComponentName)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["api"])
	require.Equal(t, "nginx:1.27", store.components["api"].Image)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}
