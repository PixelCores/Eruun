package application

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"strings"
	"testing"
	"time"
)

func newStatefulSetPVCFullRebuildRetryFixture(t *testing.T) (*inMemoryAppStore, *applicationsServiceImpl, apisv1.UpdateVersionRequest) {
	t.Helper()
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "shop", Version: "1.0.0", Namespace: config.DefaultNamespace,
	}
	currentTraits := apisv1.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name: "data", Type: config.StorageTypePersistent, MountPath: "/data", TmpCreate: true, Size: "1Gi",
		}},
		Service: []spec.ServiceTraitSpec{{
			Name: "mysql-headless", Type: string(config.ServiceAccessInternal), Headless: true,
			Selector: map[string]string{config.LabelComponentName: "mysql"},
			Ports:    []spec.ServicePortTraitSpec{{Name: "mysql", Port: 3306, TargetPort: 3306, Protocol: "TCP"}},
		}},
	}
	store.components["mysql"] = &model.ApplicationComponent{
		Name: "mysql", AppID: "app-1", Namespace: config.DefaultNamespace,
		ComponentType: config.StoreJob, Image: "mysql:8", Replicas: 1,
		Status: string(config.ComponentStatusRunning), Traits: mustJSONStruct(&currentTraits),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID: "wf-1", AppID: "app-1", WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{
			{Name: "mysql", WorkflowType: config.JobDeploy},
		}}),
	}
	desiredTraits := currentTraits
	desiredTraits.Storage = append([]spec.StorageTraitSpec(nil), currentTraits.Storage...)
	desiredTraits.Service = append([]spec.ServiceTraitSpec(nil), currentTraits.Service...)
	desiredTraits.Storage[0].Name = "data-v2"
	desiredTraits.Storage[0].Size = "2Gi"
	desiredTraits.Storage[0].StorageClass = "premium"
	desiredTraits.Service[0].Name = "mysql-headless-v2"
	req := apisv1.UpdateVersionRequest{
		Version:   "2.0.0",
		ExecuteAt: time.Now().Add(10 * time.Minute).Unix(),
		Components: []apisv1.ComponentUpdateSpec{
			{Action: "remove", Name: "cleanup_all"},
			{Action: "add", Name: "all"},
			{Action: "update", Name: "mysql", Traits: &desiredTraits},
		},
	}
	return store, newMockServiceWithStore(store), req
}

func requireVersionUpdateCleanupJobForTask(t *testing.T, store *inMemoryAppStore, taskID, componentName string) *model.JobInfo {
	t.Helper()
	for _, job := range store.jobs {
		if job != nil && job.TaskID == taskID && strings.EqualFold(strings.TrimSpace(job.ServiceName), strings.TrimSpace(componentName)) {
			return job
		}
	}
	t.Fatalf("cleanup job for task %s component %s not found", taskID, componentName)
	return nil
}

func addVersionResetAppFixture(store *inMemoryAppStore) {
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "DemoApp",
		Version:   "1.0.0",
		Namespace: "default",
		Project:   "proj-1",
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
	store.components["worker"] = &model.ApplicationComponent{
		Name:          "worker",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "worker:v1",
		Replicas:      1,
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		Name:         "deploy-all",
		AppID:        "app-1",
		ProjectID:    "proj-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: mustJSONStruct(&model.WorkflowSteps{
			Steps: []*model.WorkflowStep{
				{Name: "api", WorkflowType: config.JobDeploy},
				{Name: "worker", WorkflowType: config.JobDeploy},
			},
		}),
	}
}
