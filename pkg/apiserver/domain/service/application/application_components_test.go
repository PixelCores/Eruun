package application

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestListApplicationComponentsReturnsSorted(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo"}
	store.components["old"] = &model.ApplicationComponent{
		Name:  "old",
		AppID: "app-1",
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(10, 0),
		},
	}
	store.components["new"] = &model.ApplicationComponent{
		Name:  "new",
		AppID: "app-1",
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(20, 0),
		},
	}
	store.components["other"] = &model.ApplicationComponent{
		Name:  "other",
		AppID: "other-app",
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(30, 0),
		},
	}

	svc := newMockServiceWithStore(store)
	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, components, 2)
	require.Equal(t, "new", components[0].Name)
	require.Equal(t, "old", components[1].Name)
}

func TestListApplicationComponentsMissingApp(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	_, err := svc.ListApplicationComponents(context.Background(), "missing")
	require.ErrorIs(t, err, bcode.ErrApplicationNotExist)
}

func TestBatchGetApplicationsReturnsComponentsInRequestOrder(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "first",
		Namespace: config.DefaultNamespace,
		Project:   "proj-1",
		BaseModel: model.BaseModel{UpdateTime: time.Unix(30, 0)},
	}
	store.apps["app-2"] = &model.Applications{
		ID:        "app-2",
		Name:      "second",
		Namespace: config.DefaultNamespace,
		Project:   "proj-2",
		BaseModel: model.BaseModel{UpdateTime: time.Unix(10, 0)},
	}
	store.workflows["wf-1"] = &model.Workflow{
		ID:           "wf-1",
		AppID:        "app-1",
		WorkflowType: config.WorkflowTaskTypeWorkflow,
		BaseModel:    model.BaseModel{UpdateTime: time.Unix(20, 0)},
	}
	properties, err := model.NewJSONStructByStruct(model.Properties{
		Ports:  []model.Ports{{Port: 8080}},
		Env:    map[string]string{"TOKEN": "secret"},
		Secret: map[string]string{"PASSWORD": "secret-value"},
	})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(spec.Traits{
		Sidecar: []spec.SidecarTraitsSpec{{Name: "debug", Image: "busybox"}},
	})
	require.NoError(t, err)
	store.components["api"] = &model.ApplicationComponent{
		ID:            1,
		Name:          "api",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		Image:         "nginx:1.24",
		Replicas:      2,
		ComponentType: config.ServerJob,
		Properties:    properties,
		Traits:        traits,
		Status:        string(config.ComponentStatusRunning),
		BaseModel:     model.BaseModel{UpdateTime: time.Unix(20, 0)},
	}
	store.components["worker"] = &model.ApplicationComponent{
		ID:            2,
		Name:          "worker",
		AppID:         "app-2",
		Namespace:     config.DefaultNamespace,
		Replicas:      1,
		ComponentType: config.JobDeployInstant,
		BaseModel:     model.BaseModel{UpdateTime: time.Unix(10, 0)},
	}

	svc := newMockServiceWithStore(store)
	resp, err := svc.BatchGetApplications(context.Background(), []string{"app-2", "app-1", "app-2"})
	require.NoError(t, err)
	require.Len(t, resp.Applications, 3)
	require.Equal(t, "app-2", resp.Applications[0].ID)
	require.Equal(t, "app-1", resp.Applications[1].ID)
	require.Equal(t, "app-2", resp.Applications[2].ID)
	require.Equal(t, "wf-1", resp.Applications[1].WorkflowID)
	require.Len(t, resp.Applications[0].Components, 1)
	require.Equal(t, "worker", resp.Applications[0].Components[0].Name)
	require.Equal(t, config.JobDeployInstant, resp.Applications[0].Components[0].ComponentType)
	require.Len(t, resp.Applications[1].Components, 1)
	require.Equal(t, "api", resp.Applications[1].Components[0].Name)
	require.Equal(t, config.ServerJob, resp.Applications[1].Components[0].ComponentType)
	require.Equal(t, []spec.Ports{{Port: 8080}}, resp.Applications[1].Components[0].Properties.Ports)

	componentJSON, err := json.Marshal(resp.Applications[1].Components[0])
	require.NoError(t, err)
	componentPayload := string(componentJSON)
	require.Contains(t, componentPayload, `"ports":[{"port":8080}]`)
	require.NotContains(t, componentPayload, `"env"`)
	require.NotContains(t, componentPayload, `"conf"`)
	require.NotContains(t, componentPayload, `"secret"`)
	require.NotContains(t, componentPayload, `"command"`)
	require.NotContains(t, componentPayload, `"labels"`)
	require.NotContains(t, componentPayload, "TOKEN")
	require.NotContains(t, componentPayload, "secret-value")
	require.NotContains(t, componentPayload, "traits")
	require.NotContains(t, componentPayload, "status")
	require.NotContains(t, componentPayload, "sidecars")
}

func TestBatchGetApplicationsRejectsInvalidInput(t *testing.T) {
	svc := newMockServiceWithStore(newInMemoryAppStore())

	_, err := svc.BatchGetApplications(context.Background(), nil)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)

	_, err = svc.BatchGetApplications(context.Background(), []string{"app-1", " "})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestBatchGetApplicationsMissingApp(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "first"}
	svc := newMockServiceWithStore(store)

	_, err := svc.BatchGetApplications(context.Background(), []string{"app-1", "missing"})
	require.ErrorIs(t, err, bcode.ErrApplicationNotExist)
}

func TestApplicationsByIDsUsesApplicationRepository(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "first"}
	svc := &applicationsServiceImpl{AppRepo: &mockAppRepo{store: store}}

	applications, err := svc.applicationsByIDs(context.Background(), []string{"app-1"})

	require.NoError(t, err)
	require.Equal(t, "first", applications["app-1"].Name)
}

func TestBatchGetApplicationsRejectsMalformedComponentProperties(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "first"}
	store.components["api"] = &model.ApplicationComponent{
		Name:       "api",
		AppID:      "app-1",
		Properties: &model.JSONStruct{"ports": "invalid"},
	}
	svc := newMockServiceWithStore(store)

	_, err := svc.BatchGetApplications(context.Background(), []string{"app-1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "convert component api properties")
}

func TestListApplicationComponentsCorrectsPendingToRunning(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo"}
	store.components["nginx"] = &model.ApplicationComponent{
		Name:          "nginx",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Replicas:      1,
		ReadyReplicas: 1,
		Status:        string(config.ComponentStatusPending),
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(10, 0),
		},
	}

	svc := newMockServiceWithStore(store)
	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Equal(t, string(config.ComponentStatusRunning), components[0].Status)
	require.Equal(t, string(config.ComponentStatusPending), store.components["nginx"].Status)
}

func TestListApplicationComponentsCorrectsUpdatingToRunning(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo"}
	store.components["nginx"] = &model.ApplicationComponent{
		Name:          "nginx",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Replicas:      1,
		ReadyReplicas: 1,
		Status:        string(config.ComponentStatusUpdating),
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(10, 0),
		},
	}

	svc := newMockServiceWithStore(store)
	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Equal(t, string(config.ComponentStatusRunning), components[0].Status)
	require.Equal(t, string(config.ComponentStatusUpdating), store.components["nginx"].Status)
}

func TestListApplicationComponentsCorrectsDeployingToRunning(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo"}
	store.components["nginx"] = &model.ApplicationComponent{
		Name:          "nginx",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Replicas:      1,
		ReadyReplicas: 1,
		Status:        string(config.ComponentStatusDeploying),
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(10, 0),
		},
	}

	svc := newMockServiceWithStore(store)
	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Equal(t, string(config.ComponentStatusRunning), components[0].Status)
	require.Equal(t, string(config.ComponentStatusDeploying), store.components["nginx"].Status)
}

func TestMarkInitialDeployingWorkflowComponentsScopesToWorkflowDeploySteps(t *testing.T) {
	store := newInMemoryAppStore()
	store.components["empty"] = &model.ApplicationComponent{
		Name:          "empty",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
	}
	store.components["not-deploy"] = &model.ApplicationComponent{
		Name:          "not-deploy",
		AppID:         "app-1",
		ComponentType: config.StoreJob,
		Status:        string(config.ComponentStatusNotDeploy),
		LastAbnormal:  "old reason",
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusNotDeploy),
		LastAbnormal:  "old api reason",
	}
	store.components["secret"] = &model.ApplicationComponent{
		Name:          "secret",
		AppID:         "app-1",
		ComponentType: config.SecretJob,
		Status:        string(config.ComponentStatusNotDeploy),
		LastAbnormal:  "old secret reason",
	}
	store.components["cloud"] = &model.ApplicationComponent{
		Name:          "cloud",
		AppID:         "app-1",
		ComponentType: config.CloudJob,
		Status:        string(config.ComponentStatusNotDeploy),
		LastAbnormal:  "old cloud reason",
	}
	store.components["running"] = &model.ApplicationComponent{
		Name:          "running",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusRunning),
	}
	store.components["stopped"] = &model.ApplicationComponent{
		Name:          "stopped",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusStopped),
	}
	store.components["other"] = &model.ApplicationComponent{
		Name:          "other",
		AppID:         "app-2",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusNotDeploy),
	}
	steps, err := model.NewJSONStructByStruct(model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{Name: "empty"},
			{
				Name:         "deploy-api",
				WorkflowType: config.JobDeploy,
				Properties: []model.Policies{{
					Policies: []string{"API", "api", "cloud", "running", "stopped"},
				}},
			},
			{
				Name:         "deploy-group",
				WorkflowType: config.JobDeploy,
				Properties:   []model.Policies{{Policies: []string{"not-deploy"}}},
				SubSteps: []*model.WorkflowSubStep{
					{
						Name:       "deploy-secret",
						Properties: []model.Policies{{Policies: []string{"Secret"}}},
					},
					{
						Name:         "cleanup-worker",
						WorkflowType: config.JobCleanupResources,
						Properties:   []model.Policies{{Policies: []string{"not-deploy"}}},
					},
				},
			},
			{
				Name:         "approval",
				StepType:     config.WorkflowStepTypeApproval,
				WorkflowType: config.JobDeploy,
				Properties:   []model.Policies{{Policies: []string{"not-deploy"}}},
			},
			{Name: "database-reset", WorkflowType: config.JobDatabaseReset, Properties: []model.Policies{{Policies: []string{"not-deploy"}}}},
			{Name: "log-archive-upload", WorkflowType: config.JobLogArchiveUpload, Properties: []model.Policies{{Policies: []string{"not-deploy"}}}},
		},
	})
	require.NoError(t, err)
	store.workflows["wf-1"] = &model.Workflow{
		ID:    "wf-1",
		AppID: "app-1",
		Steps: steps,
	}

	svc := newMockServiceWithStore(store)
	require.NoError(t, svc.MarkInitialDeployingWorkflowComponents(context.Background(), "app-1", "wf-1"))

	require.Equal(t, string(config.ComponentStatusDeploying), store.components["empty"].Status)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components["not-deploy"].Status)
	require.Equal(t, "old reason", store.components["not-deploy"].LastAbnormal)
	require.Equal(t, string(config.ComponentStatusDeploying), store.components["api"].Status)
	require.Empty(t, store.components["api"].LastAbnormal)
	require.Equal(t, string(config.ComponentStatusDeploying), store.components["secret"].Status)
	require.Empty(t, store.components["secret"].LastAbnormal)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components["cloud"].Status)
	require.Equal(t, "old cloud reason", store.components["cloud"].LastAbnormal)
	require.Equal(t, string(config.ComponentStatusRunning), store.components["running"].Status)
	require.Equal(t, string(config.ComponentStatusStopped), store.components["stopped"].Status)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components["other"].Status)
}

func TestMarkInitialDeployingWorkflowComponentsNoopsForNonDeployWorkflow(t *testing.T) {
	store := newInMemoryAppStore()
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusNotDeploy),
		LastAbnormal:  "old reason",
	}
	steps, err := model.NewJSONStructByStruct(model.WorkflowSteps{
		Steps: []*model.WorkflowStep{
			{
				Name:         "approval",
				StepType:     config.WorkflowStepTypeApproval,
				WorkflowType: config.JobDeploy,
				Properties:   []model.Policies{{Policies: []string{"api"}}},
			},
			{Name: "cleanup", WorkflowType: config.JobCleanupResources, Properties: []model.Policies{{Policies: []string{"api"}}}},
			{Name: "database-reset", WorkflowType: config.JobDatabaseReset, Properties: []model.Policies{{Policies: []string{"api"}}}},
			{Name: "log-archive-upload", WorkflowType: config.JobLogArchiveUpload, Properties: []model.Policies{{Policies: []string{"api"}}}},
		},
	})
	require.NoError(t, err)
	store.workflows["wf-noop"] = &model.Workflow{ID: "wf-noop", AppID: "app-1", Steps: steps}

	svc := newMockServiceWithStore(store)
	require.NoError(t, svc.MarkInitialDeployingWorkflowComponents(context.Background(), "app-1", "wf-noop"))

	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components["api"].Status)
	require.Equal(t, "old reason", store.components["api"].LastAbnormal)
}

func TestMarkInitialDeployingWorkflowComponentsRequiresWorkflowOwnedByApp(t *testing.T) {
	store := newInMemoryAppStore()
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Status:        string(config.ComponentStatusNotDeploy),
	}
	steps, err := model.NewJSONStructByStruct(model.WorkflowSteps{
		Steps: []*model.WorkflowStep{{Name: "api", WorkflowType: config.JobDeploy}},
	})
	require.NoError(t, err)
	store.workflows["wf-other"] = &model.Workflow{ID: "wf-other", AppID: "app-2", Steps: steps}

	svc := newMockServiceWithStore(store)
	require.Error(t, svc.MarkInitialDeployingWorkflowComponents(context.Background(), "app-1", "wf-other"))
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components["api"].Status)

	require.Error(t, svc.MarkInitialDeployingWorkflowComponents(context.Background(), "app-1", "wf-missing"))
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.components["api"].Status)
}

func TestHasImmediateActiveVersionUpdateTaskFiltersActiveVersionTasks(t *testing.T) {
	now := time.Now().Unix()
	marker, err := model.NewJSONStructByStruct(model.VersionUpdateResourceActionInfo{
		Source:  config.JobInfoSourceVersionUpdateAction,
		Version: 1,
	})
	require.NoError(t, err)
	store := newInMemoryAppStore()
	store.tasks["future"] = &model.WorkflowQueue{
		TaskID:             "future",
		AppID:              "app-1",
		Status:             config.StatusWaiting,
		ExecuteAt:          now + 3600,
		ResourceActionInfo: mustJSON(t, marker),
	}
	store.tasks["terminal"] = &model.WorkflowQueue{
		TaskID:             "terminal",
		AppID:              "app-1",
		Status:             config.StatusCompleted,
		ResourceActionInfo: mustJSON(t, marker),
	}
	store.tasks["non-version"] = &model.WorkflowQueue{
		TaskID: "non-version",
		AppID:  "app-1",
		Status: config.StatusRunning,
	}
	store.tasks["other-app"] = &model.WorkflowQueue{
		TaskID:             "other-app",
		AppID:              "app-2",
		Status:             config.StatusRunning,
		ResourceActionInfo: mustJSON(t, marker),
	}
	svc := newMockServiceWithStore(store)

	hasTask, err := svc.HasImmediateActiveVersionUpdateTask(context.Background(), "app-1", now)
	require.NoError(t, err)
	require.False(t, hasTask)

	store.tasks["active"] = &model.WorkflowQueue{
		TaskID:             "active",
		AppID:              "app-1",
		Status:             config.StatusQueued,
		ResourceActionInfo: mustJSON(t, marker),
	}
	hasTask, err = svc.HasImmediateActiveVersionUpdateTask(context.Background(), "app-1", now)
	require.NoError(t, err)
	require.True(t, hasTask)
}

func TestListApplicationComponentsKeepsRestarting(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo"}
	store.components["nginx"] = &model.ApplicationComponent{
		Name:          "nginx",
		AppID:         "app-1",
		ComponentType: config.ServerJob,
		Replicas:      1,
		ReadyReplicas: 1,
		Status:        string(config.ComponentStatusRestarting),
		BaseModel: model.BaseModel{
			UpdateTime: time.Unix(10, 0),
		},
	}

	svc := newMockServiceWithStore(store)
	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Equal(t, string(config.ComponentStatusRestarting), components[0].Status)
	require.Equal(t, string(config.ComponentStatusRestarting), store.components["nginx"].Status)
}

func TestListApplicationComponentsKeepsBase64LookingSecretTextIndependentOfAppMetadata(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:      "app-1",
		Name:    "demo",
		Version: "2.0.0",
		Project: "proj",
	}
	store.components["db-secret"] = &model.ApplicationComponent{
		Name:          "db-secret",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{
				"password": "c2VjcmV0LXB3ZA==",
				"empty":    "",
			},
		}),
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits: mustJSONStruct(&spec.Traits{
			Envs: []spec.SimplifiedEnvSpec{
				{
					Name: "DB_PASSWORD",
					ValueFrom: spec.ValueSource{
						Secret: &spec.SecretSelectorSpec{Name: "db-secret", Key: "password"},
					},
				},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "db-secret", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("decoded-live-value")},
	})

	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)
	require.Len(t, components, 2)

	var secretComponent *model.ApplicationComponent
	for _, component := range components {
		if component != nil && component.Name == "db-secret" {
			secretComponent = component
			break
		}
	}
	require.NotNil(t, secretComponent)

	var secretProps model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, secretComponent.Properties)), &secretProps))
	require.Equal(t, "c2VjcmV0LXB3ZA==", secretProps.Secret["password"])
	require.Equal(t, "", secretProps.Secret["empty"])

	dtos, err := assembler.ConvertComponentModelsToDTO(components)
	require.NoError(t, err)

	var apiComponent *apisv1.ApplicationComponent
	for _, component := range dtos {
		if component != nil && component.Name == "api" {
			apiComponent = component
			break
		}
	}
	require.NotNil(t, apiComponent)
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "DB_PASSWORD", SecretName: "db-secret", Key: "password", Value: "c2VjcmV0LXB3ZA==", Resolved: true},
	}, apiComponent.Credentials)
}

func TestListApplicationComponentsTreatsBinaryLookingSecretTextAsResolvedCredential(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "demo", Version: "2.0.0", Project: "proj"}
	store.components["bin-secret"] = &model.ApplicationComponent{
		Name:          "bin-secret",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{
				"cert": "//4=",
			},
		}),
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits: mustJSONStruct(&spec.Traits{
			Envs: []spec.SimplifiedEnvSpec{
				{
					Name: "TLS_CERT",
					ValueFrom: spec.ValueSource{
						Secret: &spec.SecretSelectorSpec{Name: "bin-secret", Key: "cert"},
					},
				},
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)

	dtos, err := assembler.ConvertComponentModelsToDTO(components)
	require.NoError(t, err)

	var apiComponent *apisv1.ApplicationComponent
	for _, component := range dtos {
		if component != nil && component.Name == "api" {
			apiComponent = component
			break
		}
	}
	require.NotNil(t, apiComponent)
	require.Equal(t, []apisv1.ComponentCredentialInfo{
		{Source: "component.envs", EnvName: "TLS_CERT", SecretName: "bin-secret", Key: "cert", Value: "//4=", Resolved: true},
	}, apiComponent.Credentials)
}
