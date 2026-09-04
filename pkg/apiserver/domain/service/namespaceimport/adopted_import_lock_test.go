package namespaceimport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type notifyingLocker struct {
	locker.Locker
	attempted chan<- struct{}
}

func (l *notifyingLocker) NewMutex(key string, opts ...locker.Option) locker.Mutex {
	return &notifyingMutex{
		Mutex:     l.Locker.NewMutex(key, opts...),
		attempted: l.attempted,
	}
}

type notifyingMutex struct {
	locker.Mutex
	attempted chan<- struct{}
}

func (m *notifyingMutex) Lock(ctx context.Context) error {
	select {
	case m.attempted <- struct{}{}:
	default:
	}
	return m.Mutex.Lock(ctx)
}

func TestImportNamespaceResources_AdoptedApplySerializesOwnershipScanAndCommit(t *testing.T) {
	const namespace = "prod"
	deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
	deployment.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "shared-config"},
		},
	}}
	configMap := &corev1.ConfigMap{ObjectMeta: adoptedTestObjectMeta("shared-config", namespace)}
	client := fake.NewSimpleClientset(deployment, configMap)
	store := newInMemoryAppStore()
	appService := &namespaceImportAppServiceStub{persistStore: store}
	lockProvider := locker.NewMemoryLocker("test-adopted-import")
	svc := &namespaceImportServiceImpl{
		Cfg:                 adoptedImportTestConfig(),
		KubeClient:          client,
		AdoptedImportLocker: lockProvider,
		ApplicationService:  appService,
		ValidationService:   NewValidationService(),
		AppRepo:             &mockAppRepo{store: store},
		WorkflowRepo:        &mockWorkflowRepo{store: store},
		ComponentRepo:       &mockComponentRepo{store: store},
	}
	request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
	dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
	require.NoError(t, err)
	require.NotEmpty(t, dryRun.PlanFingerprint)
	client.ClearActions()

	held := lockProvider.NewMutex("namespace:"+namespace, locker.WithTTL(adoptedImportLockTTL))
	require.NoError(t, held.Lock(context.Background()))
	attempted := make(chan struct{}, 1)
	svc.AdoptedImportLocker = &notifyingLocker{Locker: lockProvider, attempted: attempted}
	request.Mode = importModeApply
	request.PlanFingerprint = dryRun.PlanFingerprint
	result := make(chan error, 1)
	go func() {
		_, applyErr := svc.ImportNamespaceResources(context.Background(), request)
		result <- applyErr
	}()

	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "adopted apply did not attempt the namespace lock")
	}
	assert.Empty(t, client.Actions(), "the ownership scan must not run before the namespace lock is acquired")

	snapshot := adoption.NewSnapshot(namespace, []adoption.ResourceSnapshot{
		{
			Source: adoption.ResourceIdentity{
				APIVersion:      appsv1.SchemeGroupVersion.String(),
				Kind:            "Deployment",
				Namespace:       namespace,
				Name:            "missing-owner-root",
				UID:             "missing-owner-root-uid",
				ResourceVersion: "1",
				SpecDigest:      "persisted-root-digest",
			},
			ComponentName:  "owner",
			DependencyRole: adoptedDependencyRoleWorkload,
			Ownership:      adoption.OwnershipExclusive,
			Disposition:    adoption.DispositionManaged,
		},
		{
			Source: adoption.ResourceIdentity{
				APIVersion:      corev1.SchemeGroupVersion.String(),
				Kind:            "ConfigMap",
				Namespace:       namespace,
				Name:            configMap.Name,
				UID:             "old-config-uid",
				ResourceVersion: "1",
				SpecDigest:      "persisted-config-digest",
			},
			DependencyRole: adoptedDependencyRoleConfigMap,
			Ownership:      adoption.OwnershipExclusive,
			Disposition:    adoption.DispositionManaged,
		},
	})
	snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
	require.NoError(t, err)
	store.apps["other-app"] = &model.Applications{
		ID:               "other-app",
		Name:             "other-app",
		Namespace:        namespace,
		ManagementMode:   config.ManagementModeAdopted,
		AdoptionSnapshot: snapshotJSON,
	}
	require.NoError(t, held.Unlock(context.Background()))

	select {
	case err = <-result:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "adopted apply did not finish after the namespace lock was released")
	}
	require.ErrorIs(t, err, bcode.ErrNamespaceImportPlanDrift)
	assert.Empty(t, appService.createReqs)
	assertKubeActionsReadOnly(t, client.Actions())
}

func TestImportNamespaceResources_AdoptedApplyRequiresAvailableNamespaceLocker(t *testing.T) {
	for _, test := range []struct {
		name string
		lock locker.Locker
	}{
		{name: "missing locker"},
		{name: "closed locker", lock: func() locker.Locker {
			lockProvider := locker.NewMemoryLocker("test-adopted-import")
			require.NoError(t, lockProvider.Close())
			return lockProvider
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			const namespace = "prod"
			deployment := adoptedTestDeployment("legacy-api", namespace, map[string]string{"app": "api"})
			client := fake.NewSimpleClientset(deployment)
			store := newInMemoryAppStore()
			appService := &namespaceImportAppServiceStub{}
			svc := &namespaceImportServiceImpl{
				Cfg:                adoptedImportTestConfig(),
				KubeClient:         client,
				ApplicationService: appService,
				ValidationService:  NewValidationService(),
				AppRepo:            &mockAppRepo{store: store},
				WorkflowRepo:       &mockWorkflowRepo{store: store},
				ComponentRepo:      &mockComponentRepo{store: store},
			}
			request := adoptedSingleWorkloadRequest(namespace, "api-app", "api", "Deployment", deployment.Name)
			dryRun, err := svc.ImportNamespaceResources(context.Background(), request)
			require.NoError(t, err)
			client.ClearActions()

			svc.AdoptedImportLocker = test.lock
			request.Mode = importModeApply
			request.PlanFingerprint = dryRun.PlanFingerprint
			_, err = svc.ImportNamespaceResources(context.Background(), request)

			require.ErrorIs(t, err, bcode.ErrDistributedLockUnavailable)
			assert.Empty(t, client.Actions())
			assert.Empty(t, appService.createReqs)
		})
	}
}

func TestWithAdoptedNamespaceApplyLock_ReleasesAfterPanic(t *testing.T) {
	const namespace = "prod"
	lockProvider := locker.NewMemoryLocker("test-adopted-import")
	svc := &namespaceImportServiceImpl{AdoptedImportLocker: lockProvider}

	assert.Panics(t, func() {
		_, _ = svc.withAdoptedNamespaceApplyLock(
			context.Background(),
			namespace,
			func(context.Context) (*apisv1.ImportNamespaceApplicationsResponse, error) {
				panic("boom")
			},
		)
	})

	probe := lockProvider.NewMutex(
		"namespace:"+namespace,
		locker.WithTTL(adoptedImportLockTTL),
		locker.WithRetryCount(0),
	)
	require.NoError(t, probe.TryLock(context.Background()))
	require.NoError(t, probe.Unlock(context.Background()))
}
