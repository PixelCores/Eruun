package application

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestCleanupApplicationResourcesRejectsPendingStatefulSetCleanupWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name           string
		cleanupVersion int
		taskStatus     config.Status
		jobStatus      config.Status
		wantErr        error
	}{
		{
			name:           "queued v2 migration",
			cleanupVersion: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
			taskStatus:     config.StatusWaiting,
			jobStatus:      config.StatusWaiting,
			wantErr:        bcode.ErrWorkflowTaskRunning,
		},
		{
			name:           "failed v2 migration",
			cleanupVersion: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
			taskStatus:     config.StatusFailed,
			jobStatus:      config.StatusFailed,
			wantErr:        bcode.ErrApplicationConfig,
		},
		{
			name:           "cancelled v3 migration",
			cleanupVersion: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
			taskStatus:     config.StatusCancelled,
			jobStatus:      config.StatusCancelled,
			wantErr:        bcode.ErrApplicationConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, svc, component, kubeClient := newCleanupApplicationFenceFixture(
				t,
				tt.cleanupVersion,
				tt.taskStatus,
				tt.jobStatus,
			)
			queueRepo := svc.WorkflowQueueRepo.(*mockWorkflowQueueRepo)
			beforeStore := store.snapshot()
			beforeStatus := component.Status
			beforeReadyReplicas := component.ReadyReplicas
			beforeLastAbnormal := component.LastAbnormal

			resp, err := svc.CleanupApplicationResources(context.Background(), component.AppID)

			require.Nil(t, resp)
			require.ErrorIs(t, err, tt.wantErr)
			require.Empty(t, kubeClient.Actions(), "the fence must run before any Kubernetes request")
			require.Empty(t, queueRepo.queues, "a rejected cleanup must not create an operation task")
			require.Equal(t, beforeStore.apps, store.apps)
			require.Equal(t, beforeStore.components, store.components)
			require.Equal(t, beforeStore.tasks, store.tasks)
			require.Equal(t, beforeStore.jobs, store.jobs)
			require.Equal(t, beforeStatus, component.Status)
			require.Equal(t, beforeReadyReplicas, component.ReadyReplicas)
			require.Equal(t, beforeLastAbnormal, component.LastAbnormal)
		})
	}
}

func TestCleanupApplicationResourcesUsesCanonicalCaseInsensitiveLockKey(t *testing.T) {
	const canonicalAppID = "App-1"
	store := newInMemoryAppStore()
	store.apps[canonicalAppID] = &model.Applications{
		ID: canonicalAppID, Name: "shop", Namespace: config.DefaultNamespace,
	}
	component := statefulSetDeletionV2Component("mysql-headless")
	component.AppID = canonicalAppID
	store.components[component.Name] = component

	kubeClient := fake.NewSimpleClientset(cleanupFenceStatefulSet(component))
	svc := newMockServiceWithStore(store)
	svc.KubeClient = kubeClient
	svc.AppRepo = &caseInsensitiveCleanupApplicationRepository{
		ApplicationRepository: svc.AppRepo,
		canonicalID:           canonicalAppID,
	}
	lockProvider := locker.NewMemoryLocker("test-app-schedule")
	svc.ScheduleLocker = lockProvider
	held := lockProvider.NewMutex("app-schedule:app-1")
	require.NoError(t, held.TryLock(context.Background()))
	t.Cleanup(func() { _ = held.Unlock(context.Background()) })

	resp, err := svc.CleanupApplicationResources(context.Background(), " APP-1 ")

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Empty(t, kubeClient.Actions())
	require.Equal(t, string(config.ComponentStatusRunning), component.Status)
	require.Empty(t, svc.WorkflowQueueRepo.(*mockWorkflowQueueRepo).queues)
}

func TestCleanupApplicationResourcesReReadsApplicationInsideLock(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "shop", Namespace: config.DefaultNamespace,
	}
	component := statefulSetDeletionV2Component("mysql-headless")
	store.components[component.Name] = component
	kubeClient := fake.NewSimpleClientset(cleanupFenceStatefulSet(component))
	svc := newMockServiceWithStore(store)
	svc.KubeClient = kubeClient
	repo := &cleanupApplicationDisappearsAfterFirstReadRepository{ApplicationRepository: svc.AppRepo}
	svc.AppRepo = repo

	resp, err := svc.CleanupApplicationResources(context.Background(), "app-1")

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationNotExist)
	require.Equal(t, 2, repo.reads)
	require.Empty(t, kubeClient.Actions())
	require.Equal(t, string(config.ComponentStatusRunning), component.Status)
	require.Empty(t, svc.WorkflowQueueRepo.(*mockWorkflowQueueRepo).queues)
}

func TestDeleteApplicationCascadeBypassesPendingCleanupWithoutNestedLock(t *testing.T) {
	store := newCascadeDeleteStore()
	seedCascadeStoreData(store)
	component := statefulSetDeletionV2Component("mysql-headless")
	task, cleanupJob := cleanupLifecyclePendingStatefulSetRecords(
		t,
		component,
		model.VersionUpdateCleanupInfoVersionStatefulSetDeletion,
		config.StatusFailed,
		config.StatusFailed,
	)
	store.tasks[task.TaskID] = task
	store.nextJobID++
	cleanupJob.ID = store.nextJobID
	store.jobs[cleanupJob.ID] = cleanupJob

	svc := &applicationsServiceImpl{
		KubeClient:        fake.NewSimpleClientset(),
		Store:             store,
		AppRepo:           &cascadeAppRepo{store: store},
		ComponentRepo:     &cascadeComponentRepo{store: store},
		WorkflowQueueRepo: &mockWorkflowQueueRepo{},
		ScheduleLocker:    locker.NewMemoryLocker("test-app-schedule"),
		Cache:             newTestApplicationDeleteCancelSignalCache(t),
	}

	resp, err := svc.DeleteApplicationCascade(context.Background(), "app-1", apisv1.DeleteApplicationRequest{
		WaitSeconds: int64Ptr(0),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.Warnings)
	require.NotContains(t, store.apps, "app-1")
	require.Empty(t, store.tasks)
	require.Empty(t, store.jobs)
}

func newCleanupApplicationFenceFixture(
	t *testing.T,
	cleanupVersion int,
	taskStatus config.Status,
	jobStatus config.Status,
) (*inMemoryAppStore, *applicationsServiceImpl, *model.ApplicationComponent, *fake.Clientset) {
	t.Helper()
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "shop", Namespace: config.DefaultNamespace,
	}
	component := statefulSetDeletionV2Component("mysql-headless")
	store.components[component.Name] = component
	task, cleanupJob := cleanupLifecyclePendingStatefulSetRecords(
		t,
		component,
		cleanupVersion,
		taskStatus,
		jobStatus,
	)
	store.tasks[task.TaskID] = task
	store.jobs = append(store.jobs, cleanupJob)

	kubeClient := fake.NewSimpleClientset(cleanupFenceStatefulSet(component))
	svc := newMockServiceWithStore(store)
	svc.KubeClient = kubeClient
	return store, svc, component, kubeClient
}

func cleanupLifecyclePendingStatefulSetRecords(
	t *testing.T,
	component *model.ApplicationComponent,
	cleanupVersion int,
	taskStatus config.Status,
	jobStatus config.Status,
) (*model.WorkflowQueue, *model.JobInfo) {
	t.Helper()
	templates := []string(nil)
	if cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
		templates = []string{"data"}
	}
	cleanupInfo := model.VersionUpdateCleanupInfo{
		Source:  config.JobInfoSourceVersionUpdateRemove,
		Version: cleanupVersion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component:                       component,
			ResourceAppName:                 component.ResourceAppName,
			RequireStatefulSetDeletion:      true,
			StatefulSetPVCTemplatesToDelete: templates,
		}},
	}
	payload, err := json.Marshal(cleanupInfo)
	require.NoError(t, err)
	marker, err := versionUpdateCleanupJobInfoMarker(true, templates)
	require.NoError(t, err)
	task := &model.WorkflowQueue{
		TaskID: "pending-statefulset-cleanup", AppID: component.AppID,
		Status: taskStatus, CleanupInfo: string(payload),
	}
	cleanupJob := &model.JobInfo{
		AppID: component.AppID, TaskID: task.TaskID,
		Type: string(config.JobCleanupResources), ServiceName: component.Name,
		Status: string(jobStatus), InternalInfo: marker,
	}
	return task, cleanupJob
}

func cleanupFenceStatefulSet(component *model.ApplicationComponent) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      naming.StoreServerName(component.Name, component.ResourceNameKey()),
			Namespace: component.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
}

type caseInsensitiveCleanupApplicationRepository struct {
	repository.ApplicationRepository
	canonicalID string
}

func (r *caseInsensitiveCleanupApplicationRepository) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	if strings.EqualFold(strings.TrimSpace(id), r.canonicalID) {
		id = r.canonicalID
	}
	return r.ApplicationRepository.FindByID(ctx, id)
}

type cleanupApplicationDisappearsAfterFirstReadRepository struct {
	repository.ApplicationRepository
	reads int
}

func (r *cleanupApplicationDisappearsAfterFirstReadRepository) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	r.reads++
	if r.reads > 1 {
		return nil, datastore.ErrRecordNotExist
	}
	return r.ApplicationRepository.FindByID(ctx, id)
}

var _ repository.ApplicationRepository = (*caseInsensitiveCleanupApplicationRepository)(nil)
var _ repository.ApplicationRepository = (*cleanupApplicationDisappearsAfterFirstReadRepository)(nil)
