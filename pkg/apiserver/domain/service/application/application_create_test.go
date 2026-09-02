package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	assembler "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/assembler/v1"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type createReplacementApplicationRepository struct {
	repository.ApplicationRepository
	application *model.Applications
	err         error
}

func (r *createReplacementApplicationRepository) FindByID(context.Context, string) (*model.Applications, error) {
	return r.application, r.err
}

func TestResolveCreateApplicationReplacementByIDErrorContract(t *testing.T) {
	infrastructureErr := errors.New("database unavailable")
	tests := []struct {
		name           string
		application    *model.Applications
		repositoryErr  error
		wantErr        error
		wantErrContext string
	}{
		{
			name:          "not found",
			repositoryErr: datastore.ErrRecordNotExist,
			wantErr:       bcode.ErrApplicationNotExist,
		},
		{
			name:           "infrastructure error",
			repositoryErr:  infrastructureErr,
			wantErr:        infrastructureErr,
			wantErrContext: `find replacement application "app-1"`,
		},
		{
			name:    "nil entity",
			wantErr: bcode.ErrApplicationNotExist,
		},
		{
			name:        "empty entity",
			application: &model.Applications{},
			wantErr:     bcode.ErrApplicationNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &applicationsServiceImpl{AppRepo: &createReplacementApplicationRepository{
				application: tt.application,
				err:         tt.repositoryErr,
			}}
			req := apisv1.CreateApplicationsRequest{ID: "app-1"}

			resolved, err := svc.resolveCreateApplicationReplacement(context.Background(), req)

			require.Equal(t, req, resolved)
			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantErrContext != "" {
				require.Contains(t, err.Error(), tt.wantErrContext)
				require.NotErrorIs(t, err, bcode.ErrApplicationNotExist)
			}
		})
	}
}

func TestCreateApplicationsRejectsDuplicateName(t *testing.T) {
	store := newInMemoryAppStore()
	existing := &model.Applications{
		ID:        "app-1",
		Name:      "m2605081521cctqpk",
		Namespace: config.DefaultNamespace,
	}
	store.apps[existing.ID] = existing

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		Name: "m2605081521cctqpk",
	}

	_, err := svc.CreateApplications(context.Background(), req)
	require.ErrorIs(t, err, bcode.ErrApplicationExist)
	require.Len(t, store.apps, 1)
}

func TestCreateApplicationsWithMutationCommitsInternalStateAtomically(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)
	uid := "deployment-uid"

	response, err := svc.CreateApplicationsWithMutation(
		context.Background(),
		apisv1.CreateApplicationsRequest{
			Name:      "adopted-app",
			Namespace: config.DefaultNamespace,
			Component: []apisv1.CreateComponentRequest{{
				Name:          "backend",
				ComponentType: config.ServerJob,
				Image:         "nginx:stable",
				Replicas:      1,
			}},
		},
		func(_ context.Context, _ datastore.DataStore, app *model.Applications, components []*model.ApplicationComponent) error {
			app.ManagementMode = config.ManagementModeAdopted
			require.Len(t, components, 1)
			components[0].SourceWorkloadAPIVersion = "apps/v1"
			components[0].SourceWorkloadKind = "Deployment"
			components[0].SourceWorkloadName = "legacy-backend"
			components[0].SourceWorkloadUID = &uid
			return nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, config.ManagementModeAdopted, store.apps[response.ID].ManagementMode)
	require.Equal(t, uid, *store.components["backend"].SourceWorkloadUID)

	mutationErr := errors.New("mutation failed")
	_, err = svc.CreateApplicationsWithMutation(
		context.Background(),
		apisv1.CreateApplicationsRequest{
			Name:      "rolled-back-app",
			Namespace: config.DefaultNamespace,
			Component: []apisv1.CreateComponentRequest{{Name: "worker", ComponentType: config.ServerJob, Image: "nginx:stable"}},
		},
		func(context.Context, datastore.DataStore, *model.Applications, []*model.ApplicationComponent) error {
			return mutationErr
		},
	)
	require.ErrorIs(t, err, mutationErr)
	for _, app := range store.apps {
		require.NotEqual(t, "rolled-back-app", app.Name)
	}
	require.NotContains(t, store.components, "worker")
}

func TestCreateApplicationsWithMutationRefreshesManagedApplication(t *testing.T) {
	for _, managementMode := range []config.ManagementMode{
		config.ManagementModeObserve,
		config.ManagementModeAdopted,
	} {
		t.Run(string(managementMode), func(t *testing.T) {
			store := newInMemoryAppStore()
			defaultWorkflowDisabled := true
			if managementMode == config.ManagementModeAdopted {
				defaultWorkflowDisabled = false
			}
			store.apps["managed-app"] = &model.Applications{
				ID:             "managed-app",
				Name:           "imported-app",
				Namespace:      config.DefaultNamespace,
				Version:        "imported",
				Project:        "imported",
				ManagementMode: managementMode,
			}
			store.workflows["wf-default"] = &model.Workflow{
				ID:           "wf-default",
				AppID:        "managed-app",
				Name:         "imported-app-workflow",
				Alias:        "imported-app-workflow",
				WorkflowType: config.WorkflowTaskTypeWorkflow,
				Disabled:     defaultWorkflowDisabled,
			}
			store.workflows["wf-update"] = &model.Workflow{
				ID:           "wf-update",
				AppID:        "managed-app",
				Name:         "imported-app-update-workflow",
				WorkflowType: config.WorkflowTaskTypeUpdate,
				Disabled:     true,
			}
			store.workflows["wf-custom"] = &model.Workflow{
				ID:           "wf-custom",
				AppID:        "managed-app",
				Name:         "custom-check",
				WorkflowType: config.WorkflowTaskTypeTesting,
				Disabled:     true,
			}
			svc := newMockServiceWithStore(store)
			mutationCalled := false

			response, err := svc.CreateApplicationsWithMutation(
				context.Background(),
				apisv1.CreateApplicationsRequest{
					ID:        "managed-app",
					Name:      "imported-app",
					Namespace: config.DefaultNamespace,
					Version:   "imported",
					Project:   "imported",
					Component: []apisv1.CreateComponentRequest{{
						Name:          "backend",
						ComponentType: config.ServerJob,
						Image:         "nginx:stable",
					}},
				},
				func(_ context.Context, _ datastore.DataStore, app *model.Applications, _ []*model.ApplicationComponent) error {
					mutationCalled = true
					require.Equal(t, managementMode, app.EffectiveManagementMode())
					app.ManagementMode = config.ManagementModeAdopted
					return nil
				},
			)

			require.NoError(t, err)
			require.True(t, mutationCalled)
			require.Equal(t, "managed-app", response.ID)
			require.Equal(t, config.ManagementModeAdopted, store.apps["managed-app"].ManagementMode)
			require.Len(t, store.workflows, 3)
			require.False(t, store.workflows["wf-default"].Disabled)
			require.Equal(t, managementMode == config.ManagementModeAdopted, store.workflows["wf-update"].Disabled)
			require.True(t, store.workflows["wf-custom"].Disabled)
		})
	}
}

func TestCreateApplicationsWithMutationRequiresAdoptedResult(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	_, err := svc.CreateApplicationsWithMutation(
		context.Background(),
		apisv1.CreateApplicationsRequest{
			Name:      "managed-app",
			Namespace: config.DefaultNamespace,
		},
		func(context.Context, datastore.DataStore, *model.Applications, []*model.ApplicationComponent) error {
			return nil
		},
	)

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.Empty(t, store.apps)
	require.Empty(t, store.workflows)
}

func TestCreateApplicationsWithMutationRejectsObserveIntent(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)
	mutationCalled := false

	_, err := svc.CreateApplicationsWithMutation(
		context.Background(),
		apisv1.CreateApplicationsRequest{
			Name:            "managed-app",
			Namespace:       config.DefaultNamespace,
			ImportAsObserve: true,
		},
		func(context.Context, datastore.DataStore, *model.Applications, []*model.ApplicationComponent) error {
			mutationCalled = true
			return nil
		},
	)

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.False(t, mutationCalled)
	require.Empty(t, store.apps)
}

func TestCreateApplicationsWithMutationRejectsNativeReplacement(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["native-app"] = &model.Applications{
		ID:             "native-app",
		Name:           "native-app",
		Namespace:      config.DefaultNamespace,
		ManagementMode: config.ManagementModeNative,
	}
	svc := newMockServiceWithStore(store)
	mutationCalled := false

	_, err := svc.CreateApplicationsWithMutation(
		context.Background(),
		apisv1.CreateApplicationsRequest{
			ID:        "native-app",
			Name:      "native-app",
			Namespace: config.DefaultNamespace,
		},
		func(context.Context, datastore.DataStore, *model.Applications, []*model.ApplicationComponent) error {
			mutationCalled = true
			return nil
		},
	)

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.False(t, mutationCalled)
	require.Equal(t, config.ManagementModeNative, store.apps["native-app"].ManagementMode)
}

func TestCreateApplicationsWithMutationRevalidatesManagedModeInTransaction(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["managed-app"] = &model.Applications{
		ID:             "managed-app",
		Name:           "imported-app",
		Namespace:      config.DefaultNamespace,
		Version:        "imported",
		Project:        "imported",
		ManagementMode: config.ManagementModeObserve,
	}
	store.beforeTransaction = func(store *inMemoryAppStore) {
		store.apps["managed-app"].ManagementMode = config.ManagementModeNative
	}
	svc := newMockServiceWithStore(store)
	mutationCalled := false

	_, err := svc.CreateApplicationsWithMutation(
		context.Background(),
		apisv1.CreateApplicationsRequest{
			ID:        "managed-app",
			Name:      "imported-app",
			Namespace: config.DefaultNamespace,
			Version:   "imported",
			Project:   "imported",
		},
		func(context.Context, datastore.DataStore, *model.Applications, []*model.ApplicationComponent) error {
			mutationCalled = true
			return nil
		},
	)

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.False(t, mutationCalled)
	require.Equal(t, config.ManagementModeObserve, store.apps["managed-app"].ManagementMode)
}

func TestCreateApplicationsObserveModeDisablesWorkflowsAtomically(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:             "app-1",
		Name:           "observed-app",
		Namespace:      config.DefaultNamespace,
		ManagementMode: config.ManagementModeNative,
	}
	store.workflows["wf-custom"] = &model.Workflow{
		ID:           "wf-custom",
		AppID:        "app-1",
		Name:         "custom-check",
		WorkflowType: config.WorkflowTaskTypeTesting,
	}
	svc := newMockServiceWithStore(store)

	response, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		ID:              "app-1",
		Name:            "observed-app",
		Namespace:       config.DefaultNamespace,
		ImportAsObserve: true,
		Component: []apisv1.CreateComponentRequest{{
			Name:          "backend",
			ComponentType: config.ServerJob,
			Image:         "nginx:stable",
		}},
	})
	require.NoError(t, err)
	require.Equal(t, config.ManagementModeObserve, store.apps[response.ID].ManagementMode)
	require.NotEmpty(t, store.workflows)
	for _, workflow := range store.workflows {
		require.True(t, workflow.Disabled)
	}
}

func TestCreateApplicationsObserveImportRejectsAdoptedReplacement(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["adopted-app"] = &model.Applications{
		ID:             "adopted-app",
		Name:           "imported-app",
		Namespace:      config.DefaultNamespace,
		ManagementMode: config.ManagementModeAdopted,
	}
	svc := newMockServiceWithStore(store)

	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		ID:              "adopted-app",
		Name:            "imported-app",
		Namespace:       config.DefaultNamespace,
		ImportAsObserve: true,
	})

	require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
	require.Equal(t, config.ManagementModeAdopted, store.apps["adopted-app"].ManagementMode)
}

func TestCreateApplicationsRejectsGenericManagedReplacement(t *testing.T) {
	for _, managementMode := range []config.ManagementMode{
		config.ManagementModeObserve,
		config.ManagementModeAdopted,
	} {
		t.Run(string(managementMode), func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["managed-app"] = &model.Applications{
				ID:             "managed-app",
				Name:           "imported-app",
				Namespace:      config.DefaultNamespace,
				ManagementMode: managementMode,
			}
			svc := newMockServiceWithStore(store)

			response, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
				ID:        "managed-app",
				Name:      "imported-app",
				Namespace: config.DefaultNamespace,
			})

			require.Nil(t, response)
			require.ErrorIs(t, err, bcode.ErrApplicationManagementMode)
			require.Equal(t, managementMode, store.apps["managed-app"].ManagementMode)
			require.Empty(t, store.workflows)
		})
	}
}

func TestCreateApplicationsModeTransitionUsesApplicationLock(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:             "app-1",
		Name:           "native-app",
		Namespace:      config.DefaultNamespace,
		ManagementMode: config.ManagementModeNative,
	}
	svc := newMockServiceWithStore(store)
	releaseLock := holdApplicationTestAppScheduleLock(t, svc.ScheduleLocker, "app-1")
	defer releaseLock()

	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		ID:              "app-1",
		Name:            "native-app",
		Namespace:       config.DefaultNamespace,
		ImportAsObserve: true,
	})
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Equal(t, config.ManagementModeNative, store.apps["app-1"].ManagementMode)
}

func TestCreateApplicationsAllowsDuplicateNormalNameAcrossNamespaces(t *testing.T) {
	store := newInMemoryAppStore()
	existing := &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: "team-a",
	}
	store.apps[existing.ID] = existing

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "demo",
		Namespace: "team-b",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "demo-config",
			ComponentType: config.ConfJob,
			Properties: apisv1.Properties{
				Conf: map[string]string{"app.conf": "debug=true\n"},
			},
			Traits: apisv1.Traits{},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEqual(t, existing.ID, resp.ID)
	require.Len(t, store.apps, 2)
	require.Equal(t, "team-a", store.apps[existing.ID].Namespace)
}

func TestCreateApplicationsReturnsMainComponentResourceSummary(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)
	templateEnabled := true

	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:            "mysql",
		Namespace:       config.DefaultNamespace,
		TemplateEnabled: &templateEnabled,
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "mysql-config",
				ComponentType: config.ConfJob,
				Properties: apisv1.Properties{
					Conf: map[string]string{"master.cnf": "[mysqld]\n"},
				},
				Traits: apisv1.Traits{},
			},
			{
				Name:          "mysql-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"MYSQL_ROOT_PASSWORD": "secret"},
				},
				Traits: apisv1.Traits{},
			},
			{
				Name:          "mysql",
				ComponentType: config.StoreJob,
				Image:         "mysql:5.7",
				Replicas:      1,
				Traits: apisv1.Traits{
					Resources: &spec.ResourceTraitsSpec{
						CPU:    "300m",
						Memory: "600Mi",
					},
				},
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "300m", resp.Resources.CPUReq)
	require.Equal(t, "600Mi", resp.Resources.MemReq)
	require.Equal(t, "300m", resp.Resources.CPULimit)
	require.Equal(t, "600Mi", resp.Resources.MemLimit)
	require.EqualValues(t, 1, resp.Resources.Replicas)
}

func TestCreateApplicationsResourceSummaryKeepsZeroValuesWhenMainComponentHasNoResources(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Component: []apisv1.CreateComponentRequest{{
			Name:          "api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Replicas:      2,
			Traits:        apisv1.Traits{},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.Resources.CPUReq)
	require.Empty(t, resp.Resources.MemReq)
	require.Empty(t, resp.Resources.CPULimit)
	require.Empty(t, resp.Resources.MemLimit)
	require.Zero(t, resp.Resources.Replicas)
}

func TestCreateApplicationsRejectsGeneratedResourceNameCollisionWithinApp(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "game",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Properties: apisv1.Properties{
					Ports: []spec.Ports{{Port: 8080}},
				},
			},
			{
				Name:          "game-api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Properties: apisv1.Properties{
					Ports: []spec.Ports{{Port: 8081}},
				},
			},
		},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate deployment")
	require.Empty(t, store.apps)
}

func TestCreateApplicationsAllowsStandalonePVCReuseWithinApp(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()

	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "game",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
				},
			},
			{
				Name:          "worker",
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Traits: apisv1.Traits{
					Sidecar: []spec.SidecarTraitsSpec{{
						Name:  "metrics",
						Image: "busybox:latest",
						Traits: spec.Traits{
							Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("metrics-cache", "shared-cache", false)},
						},
					}},
				},
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.components, 2)
}

func TestCreateApplicationsAllowsSameComponentStandalonePVCReuseAcrossTraitScopes(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()

	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "game",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Traits: apisv1.Traits{
				Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
				Init: []spec.InitTraitSpec{{
					Name:  "init-cache",
					Image: "busybox:latest",
					Traits: spec.Traits{
						Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("init-cache", "shared-cache", false)},
					},
				}},
				Sidecar: []spec.SidecarTraitsSpec{{
					Name:  "metrics",
					Image: "busybox:latest",
					Traits: spec.Traits{
						Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("metrics-cache", "shared-cache", false)},
					},
				}},
			},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.components, 1)
}

func TestCreateApplicationsRejectsInvalidStandalonePVCName(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "game",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Traits: apisv1.Traits{
				Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "Bad_PVC", false)},
			},
		}},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "traits.storage[0]")
	require.Empty(t, store.apps)
}

func TestCreateApplicationsRejectsStorageSubPathAndSubPathExpr(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "game",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "api",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Traits: apisv1.Traits{
				Storage: []spec.StorageTraitSpec{{
					Name:        "logs",
					Type:        config.StorageTypePersistent,
					MountPath:   "/app/log",
					SubPath:     "fixed/logs",
					SubPathExpr: "$(POD_IP)/logs",
				}},
			},
		}},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "storage subPath and subPathExpr cannot both be set")
	require.Empty(t, store.apps)
	require.Empty(t, store.components)
}

func TestCreateApplicationsRejectsGeneratedResourceNameCollisionAcrossApps(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "foo",
		Namespace: config.DefaultNamespace,
	}
	store.components["bar-baz"] = &model.ApplicationComponent{
		Name:          "bar-baz",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Ports: []spec.Ports{{Port: 8080}},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "foo-bar",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "baz",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Properties: apisv1.Properties{
				Ports: []spec.Ports{{Port: 8081}},
			},
		}},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate deployment")
	require.Len(t, store.apps, 1)
}

func TestCreateApplicationsAllowsDuplicateSafeShareResourceNameCollisionAcrossApps(t *testing.T) {
	for _, strategy := range []string{string(spec.ShareStrategyDefault), string(spec.ShareStrategyIgnore), "future-default"} {
		t.Run(strategy, func(t *testing.T) {
			store := newInMemoryAppStore()
			store.apps["app-1"] = &model.Applications{
				ID:        "app-1",
				Name:      "alpha",
				Namespace: config.DefaultNamespace,
			}
			store.components["backend"] = &model.ApplicationComponent{
				Name:          "backend",
				AppID:         "app-1",
				Namespace:     config.DefaultNamespace,
				ComponentType: config.ServerJob,
				Image:         "nginx:latest",
				Properties: mustJSONStruct(&apisv1.Properties{
					Ports: []spec.Ports{{Port: 8080}},
				}),
				Traits: mustJSONStruct(&apisv1.Traits{
					Share: &spec.ShareTraitSpec{Strategy: strategy},
				}),
			}

			svc := newMockServiceWithStore(store)
			svc.KubeClient = fake.NewSimpleClientset()
			resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
				Name: "beta",
				Component: []apisv1.CreateComponentRequest{{
					Name:          "backend",
					ComponentType: config.ServerJob,
					Image:         "nginx:latest",
					Properties: apisv1.Properties{
						Ports: []spec.Ports{{Port: 8081}},
					},
					Traits: apisv1.Traits{
						Share: &spec.ShareTraitSpec{Strategy: strategy},
					},
				}},
			})

			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, store.apps, 2)
		})
	}
}

func TestCreateApplicationsRejectsForceShareResourceNameCollisionAcrossApps(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "alpha",
		Namespace: config.DefaultNamespace,
	}
	store.components["backend"] = &model.ApplicationComponent{
		Name:          "backend",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Properties: mustJSONStruct(&apisv1.Properties{
			Ports: []spec.Ports{{Port: 8080}},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{
			Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)},
		}),
	}

	svc := newMockServiceWithStore(store)
	_, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "beta",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "backend",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Properties: apisv1.Properties{
				Ports: []spec.Ports{{Port: 8081}},
			},
			Traits: apisv1.Traits{
				Share: &spec.ShareTraitSpec{Strategy: string(spec.ShareStrategyForce)},
			},
		}},
	})

	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "duplicate deployment")
	require.Len(t, store.apps, 1)
}

func TestCreateApplicationsAllowsStandalonePVCReuseAcrossApps(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "alpha",
		Namespace: config.DefaultNamespace,
	}
	store.components["api"] = &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-1",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.ServerJob,
		Image:         "nginx:latest",
		Traits: mustJSONStruct(&apisv1.Traits{
			Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
		}),
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name: "beta",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "worker",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Traits: apisv1.Traits{
				Storage: []spec.StorageTraitSpec{testPersistentStorageTrait("cache", "shared-cache", false)},
			},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsAllowsGeneratedResourceNameCollisionAcrossNamespaces(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "foo",
		Namespace: "team-a",
	}
	store.components["bar-baz"] = &model.ApplicationComponent{
		Name:          "bar-baz",
		AppID:         "app-1",
		Namespace:     "team-a",
		ComponentType: config.ServerJob,
		Properties: mustJSONStruct(&apisv1.Properties{
			Ports: []spec.Ports{{Port: 8080}},
		}),
		Traits: mustJSONStruct(&apisv1.Traits{}),
	}

	svc := newMockServiceWithStore(store)
	svc.KubeClient = fake.NewSimpleClientset()
	resp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "foo-bar",
		Namespace: "team-b",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "baz",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Properties: apisv1.Properties{
				Ports: []spec.Ports{{Port: 8081}},
			},
		}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, store.apps, 2)
}

func TestCreateApplicationsRejectsRenamingExistingApplication(t *testing.T) {
	store := newInMemoryAppStore()
	app := &model.Applications{
		ID:        "app-1",
		Name:      "m2605081521cctqpk",
		Namespace: config.DefaultNamespace,
	}
	store.apps[app.ID] = app

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		ID:   app.ID,
		Name: "m2605081521renamed",
	}

	_, err := svc.CreateApplications(context.Background(), req)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	require.Contains(t, err.Error(), "application name is immutable")
	require.Equal(t, "m2605081521cctqpk", store.apps[app.ID].Name)
}

func TestCreateApplications_RejectsReservedPropertiesLabels(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name: "new-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "backend",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Properties: apisv1.Properties{
				Labels: map[string]string{
					config.LabelComponentName: "custom-backend",
				},
			},
		}},
	}

	_, err := svc.CreateApplications(context.Background(), req)
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrInvalidProperties)
}

func TestCreateApplicationsAllowsEmptySecretValues(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name: "new-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "app-secret",
			ComponentType: config.SecretJob,
			Properties: apisv1.Properties{
				Secret: map[string]string{
					"password": "",
					"username": "root",
				},
			},
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var storedSecret model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, store.components["app-secret"].Properties)), &storedSecret))
	require.Equal(t, "", storedSecret.Secret["password"])
	require.Equal(t, "root", storedSecret.Secret["username"])
}

func TestCreateApplicationsKeepsBase64LookingSecretTextFromRequest(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name: "imported-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "app-secret",
			ComponentType: config.SecretJob,
			Properties: apisv1.Properties{
				Secret: map[string]string{
					"password": "c2VjcmV0LXB3ZA==",
				},
			},
		}},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var storedSecret model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, store.components["app-secret"].Properties)), &storedSecret))
	require.Equal(t, "c2VjcmV0LXB3ZA==", storedSecret.Secret["password"])
}

func TestCreateApplicationsStoresConvertedSecretAsPlainText(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	createResp, err := svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		Name:      "converted-app",
		Namespace: config.DefaultNamespace,
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "app-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"PASSWORD": "test"},
				},
			},
			{
				Name:          "api",
				ComponentType: config.ServerJob,
				Image:         "nginx:1.27",
				Replicas:      1,
				Traits: apisv1.Traits{
					Envs: []spec.SimplifiedEnvSpec{{
						Name: "PASSWORD",
						ValueFrom: spec.ValueSource{
							Secret: &spec.SecretSelectorSpec{Name: "app-secret", Key: "PASSWORD"},
						},
					}},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createResp)

	var storedSecret model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, store.components["app-secret"].Properties)), &storedSecret))
	require.Equal(t, "test", storedSecret.Secret["PASSWORD"])

	components, err := svc.ListApplicationComponents(context.Background(), createResp.ID)
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
		{Source: "component.envs", EnvName: "PASSWORD", SecretName: "app-secret", Key: "PASSWORD", Value: "test", Resolved: true},
	}, apiComponent.Credentials)
}

func TestCreateApplicationsRoundTripsSecretTextFromComponentResponse(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Version:   "1.0.0",
		Project:   "proj",
	}
	store.components["imported-secret"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "imported-secret",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{
				"password": "c2VjcmV0LXB3ZA==",
				"cert":     "//4=",
			},
		}),
	}

	svc := newMockServiceWithStore(store)
	components, err := svc.ListApplicationComponents(context.Background(), "app-1")
	require.NoError(t, err)

	var secretComponent *model.ApplicationComponent
	for _, component := range components {
		if component != nil && component.Name == "imported-secret" {
			secretComponent = component
			break
		}
	}
	require.NotNil(t, secretComponent)

	secretDTO, err := assembler.ConvertComponentModelToDTO(secretComponent)
	require.NoError(t, err)
	require.Equal(t, "c2VjcmV0LXB3ZA==", secretDTO.Properties.Secret["password"])
	require.Equal(t, "//4=", secretDTO.Properties.Secret["cert"])

	_, err = svc.CreateApplications(context.Background(), apisv1.CreateApplicationsRequest{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Version:   "2.0.0",
		Project:   "proj",
		Component: []apisv1.CreateComponentRequest{{
			Name:          secretDTO.Name,
			ComponentType: secretDTO.ComponentType,
			Namespace:     secretDTO.Namespace,
			Properties:    secretDTO.Properties,
			Traits:        secretDTO.Traits,
		}},
	})
	require.NoError(t, err)

	var storedSecret model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, store.components["imported-secret"].Properties)), &storedSecret))
	require.Equal(t, "c2VjcmV0LXB3ZA==", storedSecret.Secret["password"])
	require.Equal(t, "//4=", storedSecret.Secret["cert"])
}

func TestPrepareComponentsPreservesEmptySecretMapForURLBackedSecret(t *testing.T) {
	components, err := prepareComponents("app-1", config.DefaultNamespace, []apisv1.CreateComponentRequest{
		{
			Name:          "remote-secret",
			ComponentType: config.SecretJob,
			Properties: apisv1.Properties{
				Secret: map[string]string{},
				Conf: map[string]string{
					"config.url":      "http://example.com/secret",
					"config.fileName": "remote.txt",
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, components, 1)

	var properties model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, components[0].Properties)), &properties))
	require.NotNil(t, properties.Secret)
	require.Empty(t, properties.Secret)

	require.Equal(t, &model.SecretInput{
		Name:      "remote-secret",
		Namespace: config.DefaultNamespace,
		Labels: map[string]string{
			config.LabelManagedBy:     config.ManagedByEruun,
			config.LabelAppID:         "app-1",
			config.LabelComponentID:   "0",
			config.LabelComponentName: "remote-secret",
		},
		URL:      "http://example.com/secret",
		FileName: "remote.txt",
	}, job.GenerateSecret(components[0], &properties))
}

func TestCreateApplicationsPreservesBase64LookingSecretTextDuringRefresh(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Version:   "1.0.0",
		Project:   "proj",
	}
	store.components["imported-secret"] = &model.ApplicationComponent{
		ID:            1,
		AppID:         "app-1",
		Name:          "imported-secret",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{"password": "c2VjcmV0LXB3ZA=="},
		}),
	}
	store.components["manual-secret"] = &model.ApplicationComponent{
		ID:            2,
		AppID:         "app-1",
		Name:          "manual-secret",
		Namespace:     config.DefaultNamespace,
		ComponentType: config.SecretJob,
		Properties: mustJSONStruct(&model.Properties{
			Secret: map[string]string{"password": "dGVzdA=="},
		}),
	}

	svc := newMockServiceWithStore(store)
	req := apisv1.CreateApplicationsRequest{
		ID:        "app-1",
		Name:      "demo",
		Namespace: config.DefaultNamespace,
		Version:   "2.0.0",
		Project:   "proj",
		Component: []apisv1.CreateComponentRequest{
			{
				Name:          "imported-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"password": "c2VjcmV0LXB3ZA=="},
				},
			},
			{
				Name:          "manual-secret",
				ComponentType: config.SecretJob,
				Properties: apisv1.Properties{
					Secret: map[string]string{"password": "dGVzdA=="},
				},
			},
		},
	}

	resp, err := svc.CreateApplications(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	var importedSecret model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, store.components["imported-secret"].Properties)), &importedSecret))
	require.Equal(t, "c2VjcmV0LXB3ZA==", importedSecret.Secret["password"])
	var manualSecret model.Properties
	require.NoError(t, json.Unmarshal([]byte(mustJSON(t, store.components["manual-secret"].Properties)), &manualSecret))
	require.Equal(t, "dGVzdA==", manualSecret.Secret["password"])
}

func TestCreateApplicationsRejectsInvalidExplicitServiceTraitName(t *testing.T) {
	store := newInMemoryAppStore()
	svc := newMockServiceWithStore(store)

	req := apisv1.CreateApplicationsRequest{
		Name: "new-app",
		Component: []apisv1.CreateComponentRequest{{
			Name:          "backend",
			ComponentType: config.ServerJob,
			Image:         "nginx:latest",
			Traits: apisv1.Traits{
				Service: []spec.ServiceTraitSpec{
					{
						Name: "Backend_Service",
						Type: string(spec.ServiceAccessInternal),
						Ports: []spec.ServicePortTraitSpec{
							{Port: 80, TargetPort: 80, Protocol: "TCP"},
						},
					},
				},
			},
		}},
	}

	_, err := svc.CreateApplications(context.Background(), req)
	require.Error(t, err)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestCreateApplicationsRejectsInvalidIngressNameAndHost(t *testing.T) {
	testCases := []struct {
		name        string
		ingressName string
		hosts       []string
		errContains string
	}{
		{
			name:        "invalid ingress name",
			ingressName: "Backend_Ingress",
			hosts:       []string{"api.example.com"},
			errContains: "component[0].traits.ingress[0].name",
		},
		{
			name:        "invalid ingress host",
			ingressName: "backend-ingress",
			hosts:       []string{"api.example.com."},
			errContains: "component[0].traits.ingress[0].hosts[0]",
		},
		{
			name:        "whitespace-padded ingress host",
			ingressName: "backend-ingress",
			hosts:       []string{" api.example.com "},
			errContains: "component[0].traits.ingress[0].hosts[0]",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			svc := newMockServiceWithStore(store)

			req := apisv1.CreateApplicationsRequest{
				Name: "new-app",
				Component: []apisv1.CreateComponentRequest{{
					Name:          "backend",
					ComponentType: config.ServerJob,
					Image:         "nginx:latest",
					Traits: apisv1.Traits{
						Ingress: []spec.IngressTraitsSpec{
							{
								Name:  tc.ingressName,
								Hosts: tc.hosts,
								Routes: []spec.IngressRoutes{
									{
										Path: "/",
										Backend: spec.IngressRoute{
											ServiceName: "backend",
											ServicePort: 80,
										},
									},
								},
							},
						},
					},
				}},
			}

			_, err := svc.CreateApplications(context.Background(), req)
			require.Error(t, err)
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Contains(t, err.Error(), tc.errContains)
			require.Empty(t, store.apps)
			require.Empty(t, store.components)
			require.Empty(t, store.workflows)
		})
	}
}
