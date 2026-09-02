package application

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
	"time"
)

func TestUpdateVersionAutoExecRejectsMissingRemoveComponent(t *testing.T) {
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
		Image:         "api:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}

	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "missing"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrComponentNotFound)
	require.Nil(t, resp)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["api"])
	require.Equal(t, string(config.ComponentStatusRunning), store.components["api"].Status)
}

func TestUpdateVersionAutoExecFalseRejectsSameRequestRemoveAddReuse(t *testing.T) {
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
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "api"},
			{Action: "add", Name: "api", Image: "nginx:1.28", ComponentType: config.ServerJob},
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

func TestUpdateVersionAutoExecFalseRejectsSameRequestRemoveUpdate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		components []apisv1.ComponentUpdateSpec
	}{
		{
			name: "remove then default update",
			components: []apisv1.ComponentUpdateSpec{
				{Action: "remove", Name: "api"},
				{Name: "API", Image: "nginx:1.28"},
			},
		},
		{
			name: "default update then remove",
			components: []apisv1.ComponentUpdateSpec{
				{Name: "API", Image: "nginx:1.28"},
				{Action: "remove", Name: "api"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{
				ID:        "app-1",
				Name:      "DemoApp",
				Version:   "1.0.0",
				Namespace: "default",
			}
			store.components["api"] = &model.ApplicationComponent{
				ID:              1,
				Name:            "api",
				AppID:           "app-1",
				Namespace:       "default",
				ResourceAppName: applicationResourceNameKey(store.apps["app-1"]),
				Image:           "nginx:1.27",
				Replicas:        1,
				ComponentType:   config.ServerJob,
				Status:          string(config.ComponentStatusRunning),
			}
			deployName := naming.WebServiceName(store.components["api"].Name, store.components["api"].ResourceNameKey())
			clientset := fake.NewSimpleClientset(
				&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
			)
			svc := newMockServiceWithStore(store)
			svc.KubeClient = clientset

			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:    "1.1.0",
				AutoExec:   boolPtr(false),
				Components: tc.components,
			})

			require.ErrorIs(t, err, bcode.ErrDuplicateComponentName)
			require.Nil(t, resp)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			require.NotNil(t, store.components["api"])
			require.Equal(t, "nginx:1.27", store.components["api"].Image)
			require.Empty(t, store.tasks)
			require.Empty(t, store.jobs)

			_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
			require.NoError(t, err)
		})
	}
}

func TestUpdateVersionAutoExecFalseRejectsDuplicateRemoveSpec(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["api"] = &model.ApplicationComponent{
		ID:              1,
		Name:            "api",
		AppID:           "app-1",
		Namespace:       "default",
		ResourceAppName: applicationResourceNameKey(store.apps["app-1"]),
		Image:           "nginx:1.27",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		Status:          string(config.ComponentStatusRunning),
	}
	deployName := naming.WebServiceName(store.components["api"].Name, store.components["api"].ResourceNameKey())
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)
	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.1.0",
		AutoExec: boolPtr(false),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "api"},
			{Action: "remove", Name: "api"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrDuplicateComponentName)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["api"])
	require.Equal(t, "nginx:1.27", store.components["api"].Image)
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestUpdateVersionAutoExecRemoveQueueCreateFailureDoesNotCleanupKubeResources(t *testing.T) {
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
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
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
	store.addWorkflowQueueErr = errors.New("queue create failed")

	resourceAppName := applicationResourceNameKey(store.apps["app-1"])
	deployName := naming.WebServiceName("api", resourceAppName)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "remove",
				Name:   "api",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "auto exec workflow")
	require.Contains(t, err.Error(), "queue create failed")
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["api"])
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestUpdateVersionAutoExecRemovePersistsCleanupJobAfterCommit(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	removedProperties, err := model.NewJSONStructByStruct(model.Properties{
		Ports:   []model.Ports{{Port: 8080}},
		Env:     map[string]string{"API_TOKEN": "env-secret-token"},
		Conf:    map[string]string{"password": "conf-secret-password"},
		Secret:  map[string]string{"api-key": "secret-payload-value"},
		Command: []string{"sh", "-c", "echo command-secret"},
		Labels:  map[string]string{"credential": "label-secret"},
		Cloud: &spec.CloudSpec{
			Provider: "aliyun",
			Action:   "provision",
			Params: map[string]interface{}{
				"accessKeySecret": "cloud-secret-value",
			},
		},
	})
	require.NoError(t, err)
	removedTraits, err := model.NewJSONStructByStruct(spec.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:      "data",
			Type:      "persistent",
			MountPath: "/data",
			TmpCreate: true,
			Size:      "1Gi",
		}},
		Init: []spec.InitTraitSpec{{
			Name:  "migrate",
			Image: "busybox:1.36",
			Properties: spec.Properties{
				Env:     map[string]string{"INIT_TOKEN": "init-secret-token"},
				Command: []string{"sh", "-c", "echo init-command-secret"},
			},
			Traits: spec.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "init-data",
					Type:      "persistent",
					MountPath: "/init-data",
					TmpCreate: true,
					Size:      "2Gi",
				}},
			},
		}},
		Sidecar: []spec.SidecarTraitsSpec{{
			Name:    "logger",
			Image:   "busybox:1.36",
			Command: []string{"sh", "-c", "echo sidecar-command-secret"},
			Env:     map[string]string{"SIDECAR_TOKEN": "sidecar-secret-token"},
			Traits: spec.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:      "sidecar-data",
					Type:      "persistent",
					MountPath: "/sidecar-data",
					TmpCreate: true,
					Size:      "3Gi",
				}},
			},
		}},
	})
	require.NoError(t, err)
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Properties:    removedProperties,
		Traits:        removedTraits,
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

	resourceAppName := applicationResourceNameKey(store.apps["app-1"])
	deployName := naming.WebServiceName("api", resourceAppName)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "remove",
				Name:   "api",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Contains(t, resp.RemovedComponents, "api")
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Nil(t, store.components["api"])
	require.NotNil(t, store.tasks[resp.TaskID])

	require.Len(t, store.jobs, 1)
	cleanupJob := store.jobs[0]
	require.Equal(t, string(config.JobCleanupResources), cleanupJob.Type)
	require.Equal(t, string(config.StatusQueued), cleanupJob.Status)
	require.Equal(t, resp.TaskID, cleanupJob.TaskID)
	require.Equal(t, "wf-1", cleanupJob.WorkflowID)
	require.Equal(t, "app-1", cleanupJob.AppID)
	require.Equal(t, "api", cleanupJob.ServiceName)
	require.Contains(t, cleanupJob.Info, "cleanup resources")
	require.NotEmpty(t, cleanupJob.InternalInfo)
	require.JSONEq(t, `{"source":"version_update_remove"}`, cleanupJob.InternalInfo)

	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	require.Len(t, cleanupInfo.Components, 1)
	cleanupPayload := requireVersionUpdateCleanupComponent(t, cleanupInfo, "api")
	require.NotNil(t, cleanupPayload.Component)
	require.Equal(t, "api", cleanupPayload.Component.Name)
	require.Equal(t, "app-1", cleanupPayload.Component.AppID)
	require.Equal(t, "default", cleanupPayload.Component.Namespace)
	require.Equal(t, config.ServerJob, cleanupPayload.Component.ComponentType)
	require.Equal(t, applicationResourceNameKey(store.apps["app-1"]), cleanupPayload.ResourceAppName)
	require.Equal(t, 0, cleanupPayload.InsertBeforeStepIndex)
	require.NotContains(t, cleanupJob.InternalInfo, "env-secret-token")
	require.NotContains(t, cleanupJob.InternalInfo, "conf-secret-password")
	require.NotContains(t, cleanupJob.InternalInfo, "secret-payload-value")
	require.NotContains(t, cleanupJob.InternalInfo, "command-secret")
	require.NotContains(t, cleanupJob.InternalInfo, "label-secret")
	require.NotContains(t, cleanupJob.InternalInfo, "cloud-secret-value")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "env-secret-token")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "conf-secret-password")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "secret-payload-value")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "command-secret")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "label-secret")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "cloud-secret-value")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "init-secret-token")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "init-command-secret")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "sidecar-secret-token")
	require.NotContains(t, store.tasks[resp.TaskID].CleanupInfo, "sidecar-command-secret")
	require.NotNil(t, cleanupPayload.Component.Properties)
	var cleanupProperties model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, cleanupPayload.Component.Properties)), &cleanupProperties))
	require.Equal(t, []model.Ports{{Port: 8080}}, cleanupProperties.Ports)
	require.Empty(t, cleanupProperties.Env)
	require.Empty(t, cleanupProperties.Conf)
	require.Empty(t, cleanupProperties.Secret)
	require.Empty(t, cleanupProperties.Command)
	require.Empty(t, cleanupProperties.Labels)
	require.Nil(t, cleanupProperties.Cloud)
	require.NotNil(t, cleanupPayload.Component.Traits)
	var cleanupTraits spec.Traits
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, cleanupPayload.Component.Traits)), &cleanupTraits))
	require.Len(t, cleanupTraits.Storage, 1)
	require.Equal(t, "data", cleanupTraits.Storage[0].Name)
	require.Len(t, cleanupTraits.Init, 1)
	require.Equal(t, "migrate", cleanupTraits.Init[0].Name)
	require.Equal(t, "busybox:1.36", cleanupTraits.Init[0].Image)
	require.Len(t, cleanupTraits.Init[0].Traits.Storage, 1)
	require.Equal(t, "init-data", cleanupTraits.Init[0].Traits.Storage[0].Name)
	require.Empty(t, cleanupTraits.Init[0].Properties.Env)
	require.Empty(t, cleanupTraits.Init[0].Properties.Command)
	require.Len(t, cleanupTraits.Sidecar, 1)
	require.Equal(t, "logger", cleanupTraits.Sidecar[0].Name)
	require.Equal(t, "busybox:1.36", cleanupTraits.Sidecar[0].Image)
	require.Len(t, cleanupTraits.Sidecar[0].Traits.Storage, 1)
	require.Equal(t, "sidecar-data", cleanupTraits.Sidecar[0].Traits.Storage[0].Name)
	require.Empty(t, cleanupTraits.Sidecar[0].Env)
	require.Empty(t, cleanupTraits.Sidecar[0].Command)

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestUpdateVersionAutoExecDelayedCleanupBlocksComponentReuse(t *testing.T) {
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
	removeResp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:   "1.1.0",
		ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "api"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, removeResp.TaskID)
	require.Nil(t, store.components["api"])
	require.Len(t, store.tasks, 1)
	require.Len(t, store.jobs, 1)

	addResp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.2.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action:        "add",
				Name:          "api",
				Image:         "nginx:1.28",
				ComponentType: config.ServerJob,
			},
		},
	})
	require.ErrorIs(t, err, bcode.ErrWorkflowTaskRunning)
	require.Nil(t, addResp)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Nil(t, store.components["api"])
	require.Len(t, store.tasks, 1)
	require.Len(t, store.jobs, 1)
}

func TestUpdateVersionAutoExecRejectsProtectedSharedComponentRemove(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	store.components["shared-api"] = &model.ApplicationComponent{
		Name:          "shared-api",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Traits: mustJSONStruct(&spec.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyDefault)},
		}),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "shared-api", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "shared-api"},
		},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.NotNil(t, store.components["shared-api"])
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func TestUpdateVersionAutoExecRemovePersistsCleanupPlacementAfterApproval(t *testing.T) {
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
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{
					Name:     "approve-release",
					StepType: config.WorkflowStepTypeApproval,
					Approval: &model.WorkflowStepApproval{
						Method: "POST",
					},
				},
				{Name: "api", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "remove",
				Name:   "api",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Len(t, store.jobs, 1)

	require.JSONEq(t, `{"source":"version_update_remove"}`, store.jobs[0].InternalInfo)
	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	cleanupPayload := requireVersionUpdateCleanupComponent(t, cleanupInfo, "api")
	require.Equal(t, 1, cleanupPayload.InsertBeforeStepIndex)
}

func TestUpdateVersionAutoExecRemovePersistsCleanupPlacementAfterMultipleRemovals(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	for _, name := range []string{"a", "b", "c"} {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			Namespace:     "default",
			Image:         "nginx:1.27",
			Replicas:      1,
			ComponentType: config.ServerJob,
		}
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "a", WorkflowType: config.JobDeploy},
				{Name: "b", WorkflowType: config.JobDeploy},
				{Name: "c", WorkflowType: config.JobDeploy},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "a"},
			{Action: "remove", Name: "b"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.ElementsMatch(t, []string{"a", "b"}, resp.RemovedComponents)
	require.Len(t, store.jobs, 2)

	for _, cleanupJob := range store.jobs {
		require.JSONEq(t, `{"source":"version_update_remove"}`, cleanupJob.InternalInfo)
	}
	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, map[string]int{"a": 0, "b": 0}, versionUpdateCleanupIndexes(cleanupInfo))

	steps := decodeWorkflowSteps(t, store.workflows["wf-1"].Steps)
	require.Len(t, steps.Steps, 1)
	require.Equal(t, "c", steps.Steps[0].Name)
}

func TestUpdateVersionAutoExecRemoveCleanupPlacementSkipsEmptiedTemplatePhase(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	componentTypes := map[string]config.JobType{
		"cfg":    config.ConfJob,
		"secret": config.SecretJob,
		"worker": config.InstantJob,
		"api":    config.ServerJob,
	}
	for name, componentType := range componentTypes {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			Namespace:     "default",
			Image:         "nginx:1.27",
			Replicas:      1,
			ComponentType: componentType,
		}
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{
					Name:         "phase-2-config-secret",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"cfg", "secret"}}},
				},
				{
					Name:         "phase-4-job",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"worker"}}},
				},
				{
					Name:         "phase-5-webservice",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"api"}}},
				},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cfg"},
			{Action: "remove", Name: "secret"},
			{Action: "remove", Name: "worker"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.ElementsMatch(t, []string{"cfg", "secret", "worker"}, resp.RemovedComponents)
	require.Len(t, store.jobs, 3)

	for _, cleanupJob := range store.jobs {
		require.JSONEq(t, `{"source":"version_update_remove"}`, cleanupJob.InternalInfo)
	}
	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, map[string]int{"cfg": 0, "secret": 0, "worker": 0}, versionUpdateCleanupIndexes(cleanupInfo))
}

func TestUpdateVersionAutoExecRemoveCleanupPlacementCountsPartiallySurvivingTemplatePhase(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
	}
	componentTypes := map[string]config.JobType{
		"cfg":    config.ConfJob,
		"secret": config.SecretJob,
		"worker": config.InstantJob,
		"api":    config.ServerJob,
	}
	for name, componentType := range componentTypes {
		store.components[name] = &model.ApplicationComponent{
			Name:          name,
			AppID:         "app-1",
			Namespace:     "default",
			Image:         "nginx:1.27",
			Replicas:      1,
			ComponentType: componentType,
		}
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{
					Name:         "phase-2-config-secret",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"cfg", "secret"}}},
				},
				{
					Name:         "phase-4-job",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"worker"}}},
				},
				{
					Name:         "phase-5-webservice",
					WorkflowType: config.JobDeploy,
					Mode:         config.WorkflowModeDAG,
					Properties:   []model.Policies{{Policies: []string{"api"}}},
				},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "secret"},
			{Action: "remove", Name: "worker"},
		},
	})

	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.ElementsMatch(t, []string{"secret", "worker"}, resp.RemovedComponents)
	require.Len(t, store.jobs, 2)

	for _, cleanupJob := range store.jobs {
		require.JSONEq(t, `{"source":"version_update_remove"}`, cleanupJob.InternalInfo)
	}
	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, map[string]int{"secret": 0, "worker": 1}, versionUpdateCleanupIndexes(cleanupInfo))
}

func TestUpdateVersionAutoExecRemovePersistsCleanupJobForRetryableFailureState(t *testing.T) {
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
		Image:         "nginx:1.27",
		Replicas:      1,
		ComponentType: config.ServerJob,
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

	resourceAppName := applicationResourceNameKey(store.apps["app-1"])
	deployName := naming.WebServiceName("api", resourceAppName)
	clientset := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deployName, Namespace: "default"}},
	)

	svc := newMockServiceWithStore(store)
	svc.KubeClient = clientset
	req := apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{
				Action: "remove",
				Name:   "api",
			},
		},
	}

	resp, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Contains(t, resp.RemovedComponents, "api")
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Nil(t, store.components["api"])
	require.NotNil(t, store.tasks[resp.TaskID])
	require.Len(t, store.jobs, 1)
	require.Equal(t, string(config.JobCleanupResources), store.jobs[0].Type)
	require.Equal(t, string(config.StatusQueued), store.jobs[0].Status)
	require.Equal(t, "api", store.jobs[0].ServiceName)
	require.NotEmpty(t, store.jobs[0].InternalInfo)
	require.JSONEq(t, `{"source":"version_update_remove"}`, store.jobs[0].InternalInfo)
	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	cleanupPayload := requireVersionUpdateCleanupComponent(t, cleanupInfo, "api")
	require.Equal(t, 0, cleanupPayload.InsertBeforeStepIndex)

	_, err = clientset.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestUpdateVersionCleanupAllSentinelCleansResourcesAndKeepsComponents(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Empty(t, resp.RemovedComponents)
	require.NotNil(t, store.components["api"])
	require.NotNil(t, store.components["worker"])
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)

	cleanupInfo := requireVersionUpdateCleanupInfo(t, task)
	require.Len(t, cleanupInfo.Components, 2)
	require.True(t, cleanupInfo.CleanupOnly)
	require.Equal(t, map[string]int{"api": 0, "worker": 0}, versionUpdateCleanupIndexes(cleanupInfo))
	requireVersionUpdateCleanupComponent(t, cleanupInfo, "api")
	requireVersionUpdateCleanupComponent(t, cleanupInfo, "worker")

	require.Len(t, store.jobs, 2)
	cleanupJobs := make(map[string]*model.JobInfo, len(store.jobs))
	for _, job := range store.jobs {
		require.Equal(t, string(config.JobCleanupResources), job.Type)
		require.Equal(t, string(config.StatusQueued), job.Status)
		require.Equal(t, resp.TaskID, job.TaskID)
		require.Equal(t, "wf-1", job.WorkflowID)
		require.Equal(t, "app-1", job.AppID)
		require.Contains(t, job.Info, "cleanup resources")
		require.JSONEq(t, `{"source":"version_update_remove"}`, job.InternalInfo)
		cleanupJobs[job.ServiceName] = job
	}
	require.Contains(t, cleanupJobs, "api")
	require.Contains(t, cleanupJobs, "worker")
}

func TestUpdateVersionAddAllSentinelQueuesDeployAll(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "add", Name: "all"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Empty(t, resp.AddedComponents)
	require.Empty(t, resp.UpdatedComponents)
	require.Empty(t, resp.RemovedComponents)
	require.NotNil(t, store.tasks[resp.TaskID])
	require.Empty(t, store.tasks[resp.TaskID].CleanupInfo)
	requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Equal(t, "api:v1", store.components["api"].Image)
}

func TestUpdateVersionAddAllSentinelAllowsOmittedJobTypeDeployCoverage(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "api"},
			{Name: "worker"},
		},
	})
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "add", Name: "all"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Empty(t, store.tasks[resp.TaskID].CleanupInfo)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
}

func TestUpdateVersionAddAllSentinelAllowsSubStepDeployCoverage(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name: "deploy-all",
				SubSteps: []*model.WorkflowSubStep{
					{Name: "api"},
					{Name: "worker", WorkflowType: config.JobDeploy},
				},
			},
		},
	})
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "add", Name: "all"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Empty(t, store.tasks[resp.TaskID].CleanupInfo)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
}

func TestUpdateVersionRestartActionQueuesRestartOnlyTask(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "approve",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: "https://example.com/approve",
				},
			},
			{Name: "api", WorkflowType: config.JobDeploy},
			{Name: "worker", WorkflowType: config.JobDeploy},
		},
	})
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "restart", Name: "api"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, "wf-1", resp.WorkflowID)
	require.Empty(t, resp.UpdatedComponents)
	require.Empty(t, resp.AddedComponents)
	require.Empty(t, resp.RemovedComponents)
	require.Equal(t, []string{"api"}, resp.RestartedComponents)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["api"].Status)

	task := store.tasks[resp.TaskID]
	require.NotNil(t, task)
	require.Empty(t, task.CleanupInfo)
	info := requireVersionUpdateResourceActionInfo(t, task)
	require.True(t, info.RestartOnly)
	require.Equal(t, []string{"api"}, info.RestartComponents)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), info.ImageReadyTimeoutSeconds)
}

func TestUpdateVersionRestartActionCanMixWithOrdinaryUpdate(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Name: "worker", Image: "worker:v2"},
			{Action: "restart", Name: "api"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, []string{"worker"}, resp.UpdatedComponents)
	require.Equal(t, []string{"api"}, resp.RestartedComponents)
	require.Equal(t, "worker:v2", store.components["worker"].Image)

	info := requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.False(t, info.RestartOnly)
	require.Equal(t, []string{"api"}, info.RestartComponents)
	require.Equal(t, []string{"worker"}, info.ImageReadyComponents)
	require.Equal(t, int64(config.DefaultVersionUpdateImageReadyTimeout), info.ImageReadyTimeoutSeconds)
}

func TestUpdateVersionRestartActionRejectsInvalidContracts(t *testing.T) {
	replicas := int32(2)
	tests := []struct {
		name        string
		specs       []apisv1.ComponentUpdateSpec
		autoExec    *bool
		mutateStore func(*inMemoryAppStore)
		expected    error
	}{
		{
			name:     "empty component name",
			specs:    []apisv1.ComponentUpdateSpec{{Action: "restart"}},
			expected: bcode.ErrApplicationConfig,
		},
		{
			name:     "auto exec false",
			specs:    []apisv1.ComponentUpdateSpec{{Action: "restart", Name: "api"}},
			autoExec: boolPtr(false),
			expected: bcode.ErrApplicationConfig,
		},
		{
			name:     "component fields",
			specs:    []apisv1.ComponentUpdateSpec{{Action: "restart", Name: "api", Image: "api:v2", Replicas: &replicas}},
			expected: bcode.ErrApplicationConfig,
		},
		{
			name:     "same component update",
			specs:    []apisv1.ComponentUpdateSpec{{Name: "api", Image: "api:v2"}, {Action: "restart", Name: "api"}},
			expected: bcode.ErrDuplicateComponentName,
		},
		{
			name:     "duplicate restart",
			specs:    []apisv1.ComponentUpdateSpec{{Action: "restart", Name: "api"}, {Action: "restart", Name: "api"}},
			expected: bcode.ErrDuplicateComponentName,
		},
		{
			name:     "mix cleanup all",
			specs:    []apisv1.ComponentUpdateSpec{{Action: "remove", Name: "cleanup_all"}, {Action: "restart", Name: "api"}},
			expected: bcode.ErrApplicationConfig,
		},
		{
			name:  "unsupported component type",
			specs: []apisv1.ComponentUpdateSpec{{Action: "restart", Name: "cfg"}},
			mutateStore: func(store *inMemoryAppStore) {
				store.components["cfg"] = &model.ApplicationComponent{
					Name:          "cfg",
					AppID:         "app-1",
					Namespace:     "default",
					ComponentType: config.ConfJob,
				}
			},
			expected: bcode.ErrApplicationConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			addVersionResetAppFixture(store)
			if tt.mutateStore != nil {
				tt.mutateStore(store)
			}
			svc := newMockServiceWithStore(store)

			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:    "1.1.0",
				AutoExec:   tt.autoExec,
				Components: tt.specs,
			})
			require.ErrorIs(t, err, tt.expected)
			require.Nil(t, resp)
			require.Empty(t, store.tasks)
			require.Empty(t, store.jobs)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
		})
	}
}

func TestUpdateVersionCleanupAllThenAddAllQueuesCleanupBeforeDeploy(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:     "approve",
				StepType: config.WorkflowStepTypeApproval,
				Approval: &model.WorkflowStepApproval{
					NotifyURL: "https://example.com/approve",
				},
			},
			{Name: "api", WorkflowType: config.JobDeploy},
			{Name: "worker", WorkflowType: config.JobDeploy},
		},
	})
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
			{Action: "add", Name: "all"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Empty(t, resp.RemovedComponents)
	require.NotNil(t, store.components["api"])
	require.NotNil(t, store.components["worker"])
	require.Len(t, store.jobs, 2)

	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	requireVersionUpdateResourceActionInfo(t, store.tasks[resp.TaskID])
	require.Len(t, cleanupInfo.Components, 2)
	require.False(t, cleanupInfo.CleanupOnly)
	require.Equal(t, map[string]int{"api": 1, "worker": 1}, versionUpdateCleanupIndexes(cleanupInfo))
	requireVersionUpdateCleanupComponent(t, cleanupInfo, "api")
	requireVersionUpdateCleanupComponent(t, cleanupInfo, "worker")
}

func TestUpdateVersionSentinelsCanMixWithOrdinaryUpdate(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	svc := newMockServiceWithStore(store)

	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
			{Action: "add", Name: "all"},
			{Name: "api", Image: "api:v2"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskID)
	require.Equal(t, []string{"api"}, resp.UpdatedComponents)
	require.Empty(t, resp.AddedComponents)
	require.Empty(t, resp.RemovedComponents)
	require.Equal(t, "api:v2", store.components["api"].Image)

	cleanupInfo := requireVersionUpdateCleanupInfo(t, store.tasks[resp.TaskID])
	require.Len(t, cleanupInfo.Components, 2)
	require.False(t, cleanupInfo.CleanupOnly)
	require.Equal(t, map[string]int{"api": 0, "worker": 0}, versionUpdateCleanupIndexes(cleanupInfo))
}

func TestUpdateVersionFullRebuildRestoresCompletedCleanupWhenWorkflowFails(t *testing.T) {
	store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)

	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusFailed
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusCompleted)

	second, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	cleanupInfo := requireVersionUpdateCleanupInfoVersion(
		t,
		store.tasks[second.TaskID],
		model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
	)
	cleanupComponent := requireVersionUpdateCleanupComponent(t, cleanupInfo, "mysql")
	require.True(t, cleanupComponent.RequireStatefulSetDeletion)
	require.Equal(t, []string{"data", "data-v2"}, cleanupComponent.StatefulSetPVCTemplatesToDelete)
}

func TestUpdateVersionFullRebuildClearsCompletedCleanupAfterWorkflowCompletes(t *testing.T) {
	store, svc, req := newStatefulSetPVCFullRebuildRetryFixture(t)

	first, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	store.tasks[first.TaskID].Status = config.StatusCompleted
	firstJob := requireVersionUpdateCleanupJobForTask(t, store, first.TaskID, "mysql")
	firstJob.Status = string(config.StatusCompleted)

	second, err := svc.UpdateVersion(context.Background(), "app-1", req)
	require.NoError(t, err)
	cleanupInfo := requireVersionUpdateCleanupInfoVersion(t, store.tasks[second.TaskID], model.VersionUpdateCleanupInfoVersionV1)
	cleanupComponent := requireVersionUpdateCleanupComponent(t, cleanupInfo, "mysql")
	require.False(t, cleanupComponent.RequireStatefulSetDeletion)
	require.Empty(t, cleanupComponent.StatefulSetPVCTemplatesToDelete)
}

func TestUpdateVersionSentinelRejectsComponentFields(t *testing.T) {
	store := newInMemoryAppStore()
	addVersionResetAppFixture(store)
	svc := newMockServiceWithStore(store)

	replicas := int32(2)
	resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version: "1.1.0",
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "add", Name: "all", Image: "nginx:latest", Replicas: &replicas},
		},
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Nil(t, resp)
	require.Empty(t, store.tasks)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
}

func TestUpdateVersionSentinelReservedNamesCannotBeOrdinaryTargets(t *testing.T) {
	tests := []struct {
		name string
		spec apisv1.ComponentUpdateSpec
	}{
		{name: "update cleanup all", spec: apisv1.ComponentUpdateSpec{Name: "cleanup_all", Image: "nginx:latest"}},
		{name: "remove all", spec: apisv1.ComponentUpdateSpec{Action: "remove", Name: "all"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			addVersionResetAppFixture(store)
			svc := newMockServiceWithStore(store)

			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:    "1.1.0",
				Components: []apisv1.ComponentUpdateSpec{tt.spec},
			})
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Nil(t, resp)
			require.Empty(t, store.tasks)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
		})
	}
}

func TestUpdateVersionCleanupAllRequiresExecutableWorkflowTask(t *testing.T) {
	tests := []struct {
		name        string
		mutateStore func(*inMemoryAppStore)
		workflowID  string
		autoExec    *bool
		expected    error
	}{
		{
			name: "missing workflow",
			mutateStore: func(store *inMemoryAppStore) {
				delete(store.workflows, "wf-1")
			},
			expected: bcode.ErrWorkflowNotExist,
		},
		{
			name: "empty workflow",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{})
			},
			expected: bcode.ErrWorkflowEmpty,
		},
		{
			name: "disabled explicit workflow",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Disabled = true
			},
			workflowID: "wf-1",
			expected:   bcode.ErrExecWorkflow,
		},
		{
			name: "unsupported workflow job type",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
					Steps: []*model.WorkflowStep{{Name: "unsupported-step", WorkflowType: config.JobType("unsupported_job_type")}},
				})
			},
			expected: bcode.ErrWorkflowConfig,
		},
		{
			name: "no known components",
			mutateStore: func(store *inMemoryAppStore) {
				clear(store.components)
			},
			expected: bcode.ErrApplicationNoComponents,
		},
		{
			name:     "auto exec false",
			autoExec: boolPtr(false),
			expected: bcode.ErrApplicationConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			addVersionResetAppFixture(store)
			if tt.mutateStore != nil {
				tt.mutateStore(store)
			}
			svc := newMockServiceWithStore(store)

			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:    "1.1.0",
				WorkflowID: tt.workflowID,
				AutoExec:   tt.autoExec,
				Components: []apisv1.ComponentUpdateSpec{
					{Action: "remove", Name: "cleanup_all"},
				},
			})
			require.ErrorIs(t, err, tt.expected)
			require.Nil(t, resp)
			require.Empty(t, store.tasks)
			require.Empty(t, store.jobs)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
		})
	}
}

func TestUpdateVersionAddAllRequiresExecutableCoveringWorkflow(t *testing.T) {
	tests := []struct {
		name        string
		mutateStore func(*inMemoryAppStore)
		workflowID  string
		autoExec    *bool
		expected    error
	}{
		{
			name: "missing workflow",
			mutateStore: func(store *inMemoryAppStore) {
				delete(store.workflows, "wf-1")
			},
			expected: bcode.ErrWorkflowNotExist,
		},
		{
			name: "empty workflow",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{})
			},
			expected: bcode.ErrWorkflowEmpty,
		},
		{
			name: "disabled workflow",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Disabled = true
			},
			workflowID: "wf-1",
			expected:   bcode.ErrExecWorkflow,
		},
		{
			name: "coverage incomplete",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
					Steps: []*model.WorkflowStep{{Name: "api", WorkflowType: config.JobDeploy}},
				})
			},
			expected: bcode.ErrWorkflowConfig,
		},
		{
			name: "cleanup resources step does not cover deploy all",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
					Steps: []*model.WorkflowStep{
						{Name: "api", WorkflowType: config.JobDeploy},
						{Name: "worker", WorkflowType: config.JobCleanupResources},
					},
				})
			},
			expected: bcode.ErrWorkflowConfig,
		},
		{
			name: "substep parent component does not cover deploy all",
			mutateStore: func(store *inMemoryAppStore) {
				store.workflows["wf-1"].Steps = mustJSONStruct(&model.WorkflowSteps{
					Steps: []*model.WorkflowStep{
						{
							Name:       "deploy-group",
							Properties: []model.Policies{{Policies: []string{"worker"}}},
							SubSteps: []*model.WorkflowSubStep{
								{Name: "api", WorkflowType: config.JobDeploy},
							},
						},
					},
				})
			},
			expected: bcode.ErrWorkflowConfig,
		},
		{
			name:     "auto exec false",
			autoExec: boolPtr(false),
			expected: bcode.ErrApplicationConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			addVersionResetAppFixture(store)
			if tt.mutateStore != nil {
				tt.mutateStore(store)
			}
			svc := newMockServiceWithStore(store)

			resp, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
				Version:    "1.1.0",
				WorkflowID: tt.workflowID,
				AutoExec:   tt.autoExec,
				Components: []apisv1.ComponentUpdateSpec{
					{Action: "add", Name: "all"},
				},
			})
			require.ErrorIs(t, err, tt.expected)
			require.Nil(t, resp)
			require.Empty(t, store.tasks)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
		})
	}
}
