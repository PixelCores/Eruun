package application

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestCreateApplicationsRefreshRejectsPendingStatefulSetCleanupWithoutWrites(t *testing.T) {
	tests := []struct {
		name           string
		cleanupVersion int
	}{
		{name: "failed v2 cleanup", cleanupVersion: model.VersionUpdateCleanupInfoVersionStatefulSetDeletion},
		{name: "failed v3 cleanup", cleanupVersion: model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newCreateRefreshFenceStore()
			addCreateRefreshPendingCleanup(t, store, tt.cleanupVersion)
			before := store.snapshot()
			kubeClient := fake.NewSimpleClientset()
			svc := newMockServiceWithStore(store)
			svc.KubeClient = kubeClient
			req := createRefreshRequest("app-1", "2.0.0")
			req.Namespace = "must-not-be-created"

			resp, err := svc.CreateApplications(context.Background(), req)

			require.Nil(t, resp)
			require.ErrorIs(t, err, bcode.ErrApplicationConfig)
			require.Contains(t, bcode.SafeClientMessage(err), "unfinished StatefulSet migration")
			requireCreateRefreshStoreEqual(t, before, store)
			require.Empty(t, kubeClient.Actions(), "pending cleanup must fail before namespace reads or writes")
		})
	}
}

func TestCreateApplicationsTemplateRefreshRejectsPendingStatefulSetCleanupWithoutWrites(t *testing.T) {
	store := newCreateRefreshFenceStore()
	store.apps["app-1"].TemplateEnabled = true
	addCreateRefreshPendingCleanup(t, store, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion)
	before := store.snapshot()
	svc := newMockServiceWithStore(store)
	templateEnabled := true
	req := createRefreshRequest("", "1.0.0")
	req.TemplateEnabled = &templateEnabled
	req.Description = "must-not-be-written"

	resp, err := svc.CreateApplications(context.Background(), req)

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
	requireCreateRefreshStoreEqual(t, before, store)
}

func TestCreateApplicationsRefreshRechecksFencesInsideTransaction(t *testing.T) {
	tests := []struct {
		name        string
		inject      func(*testing.T, *inMemoryAppStore)
		expectedErr error
	}{
		{
			name: "workflow becomes active",
			inject: func(_ *testing.T, store *inMemoryAppStore) {
				store.tasks["active-task"] = &model.WorkflowQueue{
					TaskID: "active-task", AppID: "app-1", Status: config.StatusRunning,
				}
			},
			expectedErr: bcode.ErrWorkflowTaskRunning,
		},
		{
			name: "cleanup becomes pending",
			inject: func(t *testing.T, store *inMemoryAppStore) {
				addCreateRefreshPendingCleanup(t, store, model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion)
			},
			expectedErr: bcode.ErrApplicationConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newCreateRefreshFenceStore()
			transactionEntered := false
			store.beforeTransaction = func(store *inMemoryAppStore) {
				transactionEntered = true
				tt.inject(t, store)
			}
			svc := newMockServiceWithStore(store)

			resp, err := svc.CreateApplications(context.Background(), createRefreshRequest("app-1", "2.0.0"))

			require.Nil(t, resp)
			require.ErrorIs(t, err, tt.expectedErr)
			require.True(t, transactionEntered)
			require.Equal(t, "1.0.0", store.apps["app-1"].Version)
			require.Equal(t, 101, store.components["mysql"].ID)
			require.Equal(t, "mysql:8", store.components["mysql"].Image)
			require.Empty(t, store.workflows)
			require.Empty(t, store.tasks, "transaction-local fence state must roll back with the rejected refresh")
		})
	}
}

func TestCreateApplicationsRefreshRequiresTransactionBeforeKubernetesSideEffects(t *testing.T) {
	store := newCreateRefreshFenceStore()
	kubeClient := fake.NewSimpleClientset()
	svc := newMockServiceWithStore(store)
	svc.Store = &createRefreshNonTransactionalStore{DataStore: store}
	svc.KubeClient = kubeClient
	req := createRefreshRequest("app-1", "2.0.0")
	req.Namespace = "must-not-be-created"

	resp, err := svc.CreateApplications(context.Background(), req)

	require.Nil(t, resp)
	require.EqualError(t, err, "application refresh requires transactional datastore")
	require.Empty(t, kubeClient.Actions())
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, 101, store.components["mysql"].ID)
}

func TestCreateApplicationsNewApplicationDoesNotRequireScheduleLocker(t *testing.T) {
	tests := []struct {
		name            string
		templateEnabled bool
	}{
		{name: "standard application"},
		{name: "template key miss", templateEnabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newInMemoryAppStore()
			svc := newMockServiceWithStore(store)
			svc.ScheduleLocker = nil
			svc.Cache = nil
			req := apisv1.CreateApplicationsRequest{
				Name:      "new-app",
				Namespace: config.DefaultNamespace,
			}
			if tt.templateEnabled {
				req.TemplateEnabled = &tt.templateEnabled
			}

			resp, err := svc.CreateApplications(context.Background(), req)

			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.ID)
			require.Contains(t, store.apps, resp.ID)
			require.Equal(t, tt.templateEnabled, store.apps[resp.ID].TemplateEnabled)
		})
	}
}

func TestCreateApplicationsRefreshNormalizesLegacyEmptyNamespaceInsideTransaction(t *testing.T) {
	store := newCreateRefreshFenceStore()
	store.apps["app-1"].Namespace = ""
	store.components["mysql"].Namespace = ""
	svc := newMockServiceWithStore(store)
	req := createRefreshRequest("app-1", "2.0.0")
	req.Namespace = ""

	resp, err := svc.CreateApplications(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, config.DefaultNamespace, store.apps["app-1"].Namespace)
	require.Equal(t, config.DefaultNamespace, store.components["mysql"].Namespace)
}

func TestCreateApplicationsRefreshUsesCanonicalApplicationScheduleLock(t *testing.T) {
	store := newCreateRefreshFenceStore()
	svc := newMockServiceWithStore(store)
	svc.AppRepo = &caseInsensitiveRefreshApplicationRepository{
		ApplicationRepository: svc.AppRepo,
		canonicalID:           "app-1",
	}
	lockProvider := locker.NewMemoryLocker("test-app-schedule")
	svc.ScheduleLocker = lockProvider
	held := lockProvider.NewMutex("app-schedule:app-1")
	require.NoError(t, held.TryLock(context.Background()))
	t.Cleanup(func() { _ = held.Unlock(context.Background()) })

	resp, err := svc.CreateApplications(context.Background(), createRefreshRequest("APP-1", "2.0.0"))

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	require.Equal(t, "1.0.0", store.apps["app-1"].Version)
	require.Equal(t, 101, store.components["mysql"].ID)
	require.Empty(t, store.workflows)
}

func TestCreateApplicationsTemplateRefreshUsesCanonicalApplicationScheduleLock(t *testing.T) {
	store := newCreateRefreshFenceStore()
	store.apps["app-1"].TemplateEnabled = true
	before := store.snapshot()
	svc := newMockServiceWithStore(store)
	lockProvider := locker.NewMemoryLocker("test-app-schedule")
	svc.ScheduleLocker = lockProvider
	held := lockProvider.NewMutex("app-schedule:app-1")
	require.NoError(t, held.TryLock(context.Background()))
	t.Cleanup(func() { _ = held.Unlock(context.Background()) })
	templateEnabled := true
	req := createRefreshRequest("", "1.0.0")
	req.TemplateEnabled = &templateEnabled

	resp, err := svc.CreateApplications(context.Background(), req)

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrApplicationOperationLocked)
	requireCreateRefreshStoreEqual(t, before, store)
}

func TestCreateApplicationsTemplateRefreshPreservesExistingIDCallbackOverwriteSemantics(t *testing.T) {
	store := newCreateRefreshFenceStore()
	setTestURLSecurityPolicy(t, store, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
	store.apps["app-1"].TemplateEnabled = true
	oldCallback := mustJSONStruct(&model.WorkflowCallback{Success: "https://example.com/old"})
	store.apps["app-1"].Callback = oldCallback
	steps := mustJSONStruct(&model.WorkflowSteps{})
	store.workflows["wf-default"] = &model.Workflow{
		ID: "wf-default", AppID: "app-1", Name: "shop-workflow", Alias: "shop-workflow",
		Namespace: config.DefaultNamespace, WorkflowType: config.WorkflowTaskTypeWorkflow,
		Steps: steps, Callback: oldCallback,
	}
	store.workflows["wf-custom"] = &model.Workflow{
		ID: "wf-custom", AppID: "app-1", Name: "custom-workflow",
		Namespace: config.DefaultNamespace, WorkflowType: config.WorkflowTaskTypeTesting,
		Steps: steps, Callback: oldCallback,
	}
	svc := newMockServiceWithStore(store)
	templateEnabled := true
	req := createRefreshRequest("", "1.0.0")
	req.TemplateEnabled = &templateEnabled
	req.Callback = &apisv1.WorkflowCallback{Success: "http://127.0.0.1/callback"}

	resp, err := svc.CreateApplications(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "app-1", resp.ID)
	requireWorkflowCallbackSuccess(t, store.apps["app-1"].Callback, "http://127.0.0.1/callback")
	for _, workflow := range store.workflows {
		requireWorkflowCallbackSuccess(t, workflow.Callback, "http://127.0.0.1/callback")
	}
}

func TestCreateApplicationsExistingRefreshRequiresAvailableScheduleLockerWithoutWrites(t *testing.T) {
	store := newCreateRefreshFenceStore()
	before := store.snapshot()
	kubeClient := fake.NewSimpleClientset()
	svc := newMockServiceWithStore(store)
	svc.ScheduleLocker = nil
	svc.Cache = nil
	svc.KubeClient = kubeClient
	req := createRefreshRequest("app-1", "2.0.0")
	req.Namespace = "must-not-be-created"

	resp, err := svc.CreateApplications(context.Background(), req)

	require.Nil(t, resp)
	require.ErrorIs(t, err, bcode.ErrDistributedLockUnavailable)
	requireCreateRefreshStoreEqual(t, before, store)
	require.Empty(t, kubeClient.Actions())
}

func TestCreateApplicationsRefreshSerializesWithUpdateAndReset(t *testing.T) {
	store := newCreateRefreshFenceStore()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.beforeTransaction = func(*inMemoryAppStore) {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	svc := newMockServiceWithStore(store)
	svc.ScheduleLocker = locker.NewMemoryLocker("test-app-schedule")
	refreshDone := make(chan error, 1)
	go func() {
		_, err := svc.CreateApplications(context.Background(), createRefreshRequest("app-1", "2.0.0"))
		refreshDone <- err
	}()
	<-entered

	updateResp, updateErr := svc.UpdateVersion(context.Background(), "APP-1", apisv1.UpdateVersionRequest{
		Version: "3.0.0", AutoExec: boolPtr(false),
	})
	require.Nil(t, updateResp)
	require.ErrorIs(t, updateErr, bcode.ErrApplicationOperationLocked)

	resetResp, resetErr := svc.ResetApplicationDatabases(context.Background(), "APP-1", apisv1.DatabaseResetRequest{
		Components: []string{"mysql"},
	})
	require.Nil(t, resetResp)
	require.ErrorIs(t, resetErr, bcode.ErrApplicationOperationLocked)

	close(release)
	require.NoError(t, <-refreshDone)
	require.Equal(t, "2.0.0", store.apps["app-1"].Version)
}

func newCreateRefreshFenceStore() *inMemoryAppStore {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{
		ID: "app-1", Name: "shop", Version: "1.0.0", Namespace: config.DefaultNamespace,
		Description: "original",
	}
	store.components["mysql"] = statefulSetDeletionV2Component("mysql-headless")
	return store
}

func createRefreshRequest(appID, version string) apisv1.CreateApplicationsRequest {
	return apisv1.CreateApplicationsRequest{
		ID: appID, Name: "shop", Namespace: config.DefaultNamespace, Version: version,
		Description: "refreshed",
		Component: []apisv1.CreateComponentRequest{{
			Name: "mysql", ComponentType: config.StoreJob, Image: "mysql:9", Replicas: 1,
			Traits: statefulSetDeletionV2Traits("mysql-headless-v2"),
		}},
	}
}

func addCreateRefreshPendingCleanup(t *testing.T, store *inMemoryAppStore, cleanupVersion int) {
	t.Helper()
	component := store.components["mysql"]
	require.NotNil(t, component)
	templates := []string(nil)
	if cleanupVersion == model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion {
		templates = []string{"data"}
	}
	cleanupInfo := model.VersionUpdateCleanupInfo{
		Source: config.JobInfoSourceVersionUpdateRemove, Version: cleanupVersion,
		Components: []model.VersionUpdateCleanupComponent{{
			Component: component, ResourceAppName: "shop", RequireStatefulSetDeletion: true,
			StatefulSetPVCTemplatesToDelete: templates,
		}},
	}
	payload, err := json.Marshal(cleanupInfo)
	require.NoError(t, err)
	marker, err := versionUpdateCleanupJobInfoMarker(true, templates)
	require.NoError(t, err)
	store.tasks["pending-cleanup"] = &model.WorkflowQueue{
		TaskID: "pending-cleanup", AppID: "app-1", Status: config.StatusFailed, CleanupInfo: string(payload),
	}
	store.jobs = append(store.jobs, &model.JobInfo{
		TaskID: "pending-cleanup", Type: string(config.JobCleanupResources), ServiceName: "mysql",
		Status: string(config.StatusFailed), InternalInfo: marker,
	})
}

func requireCreateRefreshStoreEqual(t *testing.T, expected, actual *inMemoryAppStore) {
	t.Helper()
	require.Equal(t, expected.apps, actual.apps)
	require.Equal(t, expected.components, actual.components)
	require.Equal(t, expected.workflows, actual.workflows)
	require.Equal(t, expected.tasks, actual.tasks)
	require.ElementsMatch(t, expected.jobs, actual.jobs)
}

type createRefreshNonTransactionalStore struct {
	datastore.DataStore
}

type caseInsensitiveRefreshApplicationRepository struct {
	repository.ApplicationRepository
	canonicalID string
}

func (r *caseInsensitiveRefreshApplicationRepository) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	if strings.EqualFold(strings.TrimSpace(id), r.canonicalID) {
		id = r.canonicalID
	}
	return r.ApplicationRepository.FindByID(ctx, id)
}
