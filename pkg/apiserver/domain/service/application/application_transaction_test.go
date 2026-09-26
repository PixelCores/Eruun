package application

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	sqlstore "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore/sql"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

// Inject a write error inside the real SQL transaction, after earlier writes
// have reached the database. Both SQLite and MySQL run these same assertions.
type transactionFaultStore struct {
	datastore.DataStore
	beforeWrite func(datastore.Entity) error
}

func (s *transactionFaultStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.DataStore.(datastore.Transactional).WithTransaction(ctx, func(tx datastore.DataStore) error {
		return fn(&transactionFaultStore{DataStore: tx, beforeWrite: s.beforeWrite})
	})
}

func (s *transactionFaultStore) Add(ctx context.Context, entity datastore.Entity) error {
	if err := s.beforeWrite(entity); err != nil {
		return err
	}
	return s.DataStore.Add(ctx, entity)
}

func (s *transactionFaultStore) Put(ctx context.Context, entity datastore.Entity) error {
	if err := s.beforeWrite(entity); err != nil {
		return err
	}
	return s.DataStore.Put(ctx, entity)
}

func TestDirectVersionUpdateAtomicWrites(t *testing.T) {
	testDirectVersionUpdateAtomicWrites(t, func(t *testing.T) *sqlstore.Driver { return newApplicationCallbackStore(t) })
}

func testDirectVersionUpdateAtomicWrites(t *testing.T, newStore func(*testing.T) *sqlstore.Driver) {
	for _, failure := range []string{"second component", "added component", "application", "workflow", "task", "second job", "none"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			store := newStore(t)
			app, components, workflow := seedTransactionApp(t, store)
			writeFailure := errors.New("injected write failure")
			jobsWritten := 0
			faultStore := &transactionFaultStore{DataStore: store, beforeWrite: func(entity datastore.Entity) error {
				fail := false
				switch value := entity.(type) {
				case *model.ApplicationComponent:
					fail = failure == "second component" && value.Name == "worker" || failure == "added component" && value.Name == "extra"
				case *model.Applications:
					fail = failure == "application"
				case *model.Workflow:
					fail = failure == "workflow"
				case *model.WorkflowQueue:
					fail = failure == "task"
				case *model.JobInfo:
					jobsWritten++
					fail = failure == "second job" && jobsWritten == 2
				}
				if fail {
					return writeFailure
				}
				return nil
			}}
			req := apisv1.UpdateVersionRequest{Version: "2.0.0", Description: "new description", AutoExec: boolPtr(false), Components: []apisv1.ComponentUpdateSpec{
				{Name: "api", Image: "api:v2"},
				{Name: "worker", Image: "worker:v2"},
				{Name: "extra", Action: "add", Image: "extra:v1", ComponentType: config.ServerJob},
			}}
			run := &versionUpdateRun{app: app, req: req, normalReq: req, componentMap: components, newVersion: req.Version}
			service := &applicationsServiceImpl{Store: faultStore}
			err := service.commitVersionUpdateRun(ctx, run)
			if failure != "none" {
				require.ErrorIs(t, err, writeFailure)
				if failure == "second job" {
					require.Equal(t, 2, jobsWritten)
				}
			} else {
				require.NoError(t, err)
			}

			persistedApp := &model.Applications{ID: app.ID}
			require.NoError(t, store.Get(ctx, persistedApp))
			persistedComponents, err := repository.FindComponentsByAppID(ctx, store, app.ID)
			require.NoError(t, err)
			persistedWorkflow, err := repository.WorkflowByID(ctx, store, workflow.ID)
			require.NoError(t, err)
			tasks, err := store.List(ctx, &model.WorkflowQueue{AppID: app.ID}, nil)
			require.NoError(t, err)
			jobs, err := store.List(ctx, &model.JobInfo{AppID: app.ID}, nil)
			require.NoError(t, err)
			if failure != "none" {
				require.Equal(t, "1.0.0", persistedApp.Version)
				require.Equal(t, "original description", persistedApp.Description)
				require.Len(t, persistedComponents, 2)
				for _, component := range persistedComponents {
					require.Equal(t, component.Name+":v1", component.Image)
				}
				require.Len(t, decodeWorkflowSteps(t, persistedWorkflow.Steps).Steps, 2)
				require.Empty(t, tasks)
				require.Empty(t, jobs)
				return
			}
			require.Equal(t, "2.0.0", persistedApp.Version)
			require.Equal(t, "new description", persistedApp.Description)
			require.Len(t, persistedComponents, 3)
			for _, component := range persistedComponents {
				wantImage := component.Name + ":v2"
				if component.Name == "extra" {
					wantImage = "extra:v1"
				}
				require.Equal(t, wantImage, component.Image)
			}
			require.Len(t, decodeWorkflowSteps(t, persistedWorkflow.Steps).Steps, 3)
			require.Len(t, jobs, 3)
			require.Len(t, tasks, 1)
			task := tasks[0].(*model.WorkflowQueue)
			require.Equal(t, config.WorkflowTaskTypeUpdate, task.Type)
			require.Equal(t, config.StatusCompleted, task.Status)
			require.Empty(t, task.WorkflowID, "autoExec=false must not enqueue an execution workflow")
		})
	}
}

func TestOperationTaskAtomicWrites(t *testing.T) {
	testOperationTaskAtomicWrites(t, func(t *testing.T) *sqlstore.Driver { return newApplicationCallbackStore(t) })
}

func testOperationTaskAtomicWrites(t *testing.T, newStore func(*testing.T) *sqlstore.Driver) {
	for _, callback := range []bool{false, true} {
		for _, failure := range []string{"task", "second job", "none"} {
			t.Run(fmt.Sprintf("callback=%t/%s", callback, failure), func(t *testing.T) {
				ctx := context.Background()
				store := newStore(t)
				app, _, _ := seedTransactionApp(t, store)
				var callbackJSON *model.JSONStruct
				if callback {
					callbackJSON = mustJSONStruct(&model.WorkflowCallback{Success: "https://example.com/callback"})
				}
				writeFailure := errors.New("injected operation record failure")
				jobsWritten := 0
				faultStore := &transactionFaultStore{DataStore: store, beforeWrite: func(entity datastore.Entity) error {
					if _, ok := entity.(*model.WorkflowQueue); ok && failure == "task" {
						return writeFailure
					}
					if _, ok := entity.(*model.JobInfo); ok {
						jobsWritten++
						if failure == "second job" && jobsWritten == 2 {
							return writeFailure
						}
					}
					return nil
				}}
				service := &applicationsServiceImpl{Store: faultStore}
				task, err := service.recordAppOperationTask(ctx, app, config.WorkflowTaskTypeRestart, operationTaskNameRestart, "", config.StatusCompleted, 1, 2,
					[]operationJobRecord{{name: "api", status: config.StatusCompleted}, {name: "worker", status: config.StatusCompleted}}, callbackJSON)
				if failure != "none" {
					require.ErrorIs(t, err, writeFailure)
					require.Nil(t, task)
				} else {
					require.NoError(t, err)
					require.NotNil(t, task)
				}
				tasks, err := store.List(ctx, &model.WorkflowQueue{AppID: app.ID}, nil)
				require.NoError(t, err)
				jobs, err := store.List(ctx, &model.JobInfo{AppID: app.ID}, nil)
				require.NoError(t, err)
				if failure != "none" {
					require.Empty(t, tasks)
					require.Empty(t, jobs)
					return
				}
				require.Len(t, tasks, 1)
				require.Len(t, jobs, 2)
				if callback {
					requireWorkflowCallbackSuccess(t, tasks[0].(*model.WorkflowQueue).Callback, "https://example.com/callback")
				}
			})
		}
	}
}

func seedTransactionApp(t *testing.T, store *sqlstore.Driver) (*model.Applications, map[string]*model.ApplicationComponent, *model.Workflow) {
	t.Helper()
	app := &model.Applications{ID: uuid.NewString(), Name: "transaction-app", Version: "1.0.0", Description: "original description", Namespace: "default"}
	t.Cleanup(func() {
		for _, entity := range []interface{}{&model.JobInfo{}, &model.WorkflowQueue{}, &model.ApplicationComponent{}, &model.Workflow{}} {
			require.NoError(t, store.Client.Where("app_id = ?", app.ID).Delete(entity).Error)
		}
		require.NoError(t, store.Client.Where("id = ?", app.ID).Delete(&model.Applications{}).Error)
	})
	require.NoError(t, store.Add(context.Background(), app))
	components := map[string]*model.ApplicationComponent{}
	for _, name := range []string{"api", "worker"} {
		component := &model.ApplicationComponent{AppID: app.ID, Name: name, Image: name + ":v1", Namespace: "default", ComponentType: config.ServerJob, Replicas: 1}
		require.NoError(t, store.Add(context.Background(), component))
		components[name] = component
	}
	workflow := &model.Workflow{ID: uuid.NewString(), AppID: app.ID, Name: "deploy", Steps: mustJSONStruct(&model.WorkflowSteps{Steps: []*model.WorkflowStep{{Name: "api", WorkflowType: config.JobDeploy}, {Name: "worker", WorkflowType: config.JobDeploy}}})}
	require.NoError(t, store.Add(context.Background(), workflow))
	return app, components, workflow
}

func TestLifecycleRecordFailureKeepsKubernetesEffectsExplicit(t *testing.T) {
	for _, operation := range []string{"restart", "stop", "start", "cleanup"} {
		for _, callback := range []bool{false, true} {
			if operation == "cleanup" && callback {
				continue
			}
			for _, failure := range []string{"task", "second job"} {
				t.Run(fmt.Sprintf("%s/callback=%t/%s", operation, callback, failure), func(t *testing.T) {
					ctx := context.Background()
					store := newInMemoryAppStore()
					app := &model.Applications{ID: "app-1", Name: "demo", Namespace: "default"}
					store.apps[app.ID] = app
					client := fake.NewSimpleClientset()
					for i, name := range []string{"api", "worker"} {
						component := &model.ApplicationComponent{ID: i + 1, AppID: app.ID, Name: name, Namespace: "default", ComponentType: config.ServerJob, Replicas: 1, Status: string(config.ComponentStatusRunning)}
						replicas := int32(1)
						if operation == "start" {
							component.Status = string(config.ComponentStatusStopped)
							replicas = 0
						}
						store.components[name] = component
						_, err := client.AppsV1().Deployments("default").Create(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: naming.WebServiceName(name, app.Name), Namespace: "default"}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}}, metav1.CreateOptions{})
						require.NoError(t, err)
					}
					writeFailure := errors.New("operation records unavailable")
					jobsWritten := 0
					store.beforeAdd = func(entity datastore.Entity) error {
						if _, ok := entity.(*model.WorkflowQueue); ok && failure == "task" {
							return writeFailure
						}
						if _, ok := entity.(*model.JobInfo); ok {
							jobsWritten++
							if failure == "second job" && jobsWritten == 2 {
								return writeFailure
							}
						}
						return nil
					}
					service := newMockServiceWithStore(store)
					service.KubeClient = client
					req := apisv1.ApplicationLifecycleRequest{}
					if callback {
						server, received := newLifecycleCallbackServer(t)
						req.Callback = &apisv1.WorkflowCallback{Success: server.URL}
						setTestURLSecurityPolicy(t, store, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
						t.Cleanup(func() { requireNoCallbackReceived(t, received) })
					}
					var err error
					switch operation {
					case "restart":
						_, err = service.RestartApplicationWorkloads(ctx, app.ID, req)
					case "stop":
						_, err = service.StopApplicationDeployments(ctx, app.ID, req)
					case "start":
						_, err = service.StartApplicationDeployments(ctx, app.ID, req)
					case "cleanup":
						_, err = service.CleanupApplicationResources(ctx, app.ID)
					}
					require.ErrorIs(t, err, writeFailure)
					require.ErrorContains(t, err, "Kubernetes "+operation+" may already have taken effect")
					require.Empty(t, store.tasks)
					require.Empty(t, store.jobs)
					if failure == "second job" {
						require.Equal(t, 2, jobsWritten)
					}
					for _, name := range []string{"api", "worker"} {
						deployment, err := client.AppsV1().Deployments("default").Get(ctx, naming.WebServiceName(name, app.Name), metav1.GetOptions{})
						if operation == "cleanup" {
							require.True(t, k8serrors.IsNotFound(err))
							continue
						}
						require.NoError(t, err)
						switch operation {
						case "restart":
							require.NotEmpty(t, deployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
						case "stop":
							require.Zero(t, *deployment.Spec.Replicas)
						case "start":
							require.Equal(t, int32(1), *deployment.Spec.Replicas)
						}
					}
				})
			}
		}
	}
}

func TestDirectVersionUpdateRollbackDoesNotRestoreKubernetesRemoval(t *testing.T) {
	ctx := context.Background()
	store := newInMemoryAppStore()
	app := &model.Applications{ID: "app-1", Name: "demo", Version: "1.0.0", Namespace: "default"}
	store.apps[app.ID] = app
	store.components["api"] = &model.ApplicationComponent{ID: 1, AppID: app.ID, Name: "api", Namespace: "default", ComponentType: config.ServerJob, Image: "api:v1", Replicas: 1}
	store.addWorkflowQueueErr = errors.New("operation record unavailable")
	name := naming.WebServiceName("api", app.Name)
	client := fake.NewSimpleClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
	service := newMockServiceWithStore(store)
	service.KubeClient = client
	resp, err := service.UpdateVersion(ctx, app.ID, apisv1.UpdateVersionRequest{Version: "2.0.0", AutoExec: boolPtr(false), Components: []apisv1.ComponentUpdateSpec{{Name: "api", Action: "remove"}}})
	require.ErrorContains(t, err, "Kubernetes cleanup may already have taken effect")
	require.Nil(t, resp)
	require.Equal(t, "1.0.0", store.apps[app.ID].Version)
	require.Contains(t, store.components, "api")
	require.Empty(t, store.tasks)
	_, err = client.AppsV1().Deployments("default").Get(ctx, name, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err), "database rollback cannot recreate a removed Kubernetes workload")
}
