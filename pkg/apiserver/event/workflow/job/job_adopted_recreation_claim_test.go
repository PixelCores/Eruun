package job

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainadoption "github.com/PixelCores/Eruun/pkg/apiserver/domain/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type recreationConfirmationFailStore struct {
	*adoptedSourceStore
	applicationGetCount int
	failApplicationGet  int
	panicApplicationGet int
	applicationGetPanic any
	err                 error
}

type observedAcquireLocker struct {
	locker.Locker
	mu                sync.Mutex
	lockCalls         int
	secondLockStarted chan struct{}
	firstLockAcquired chan struct{}
	allowFirstReturn  chan struct{}
}

func (l *observedAcquireLocker) NewMutex(key string, opts ...locker.Option) locker.Mutex {
	return &observedAcquireMutex{
		Mutex: l.Locker.NewMutex(key, opts...),
		owner: l,
	}
}

type observedAcquireMutex struct {
	locker.Mutex
	owner *observedAcquireLocker
}

func (m *observedAcquireMutex) Lock(ctx context.Context) error {
	m.owner.mu.Lock()
	m.owner.lockCalls++
	call := m.owner.lockCalls
	if call == 2 {
		close(m.owner.secondLockStarted)
	}
	m.owner.mu.Unlock()
	if err := m.Mutex.Lock(ctx); err != nil {
		return err
	}
	if call != 1 || m.owner.firstLockAcquired == nil {
		return nil
	}
	close(m.owner.firstLockAcquired)
	select {
	case <-m.owner.allowFirstReturn:
		return nil
	case <-ctx.Done():
		_ = m.Mutex.Unlock(context.Background())
		return ctx.Err()
	}
}

func (s *recreationConfirmationFailStore) Get(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.Applications); ok {
		s.applicationGetCount++
		if s.applicationGetCount == s.panicApplicationGet {
			panic(s.applicationGetPanic)
		}
		if s.applicationGetCount == s.failApplicationGet {
			return s.err
		}
	}
	return s.adoptedSourceStore.Get(ctx, entity)
}

func TestPrepareRecreationCandidateReleasesGuardOnPanic(t *testing.T) {
	ctx := context.Background()
	source := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "backend-config", Namespace: "ops", UID: types.UID("configmap-old"),
		},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	baseStore := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	binding, adopted, err := adoptedResourceForJob(
		ctx,
		baseStore,
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops"},
		"ConfigMap",
		"ops",
		source.Name,
	)
	require.NoError(t, err)
	require.True(t, adopted)
	recreation, err := prepareAdoptedDependencyRecreation(baseStore, binding)
	require.NoError(t, err)

	const panicValue = "reload application panic"
	store := &recreationConfirmationFailStore{
		adoptedSourceStore:  baseStore,
		panicApplicationGet: 1,
		applicationGetPanic: panicValue,
	}
	lockProvider := locker.NewMemoryLocker(shareLockerPrefix)
	candidate := source.DeepCopy()
	candidate.UID = ""
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = recreation.adoptedResourceBinding.prepareRecreationCandidate(
			ctx,
			store,
			candidate,
			&jobRuntime{},
			lockProvider,
		)
	}()
	require.Equal(t, panicValue, recovered)

	reacquireCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	guard, err := acquireAdoptedRecreationGuard(
		reacquireCtx,
		lockProvider,
		&recreation.adoptedResourceBinding,
	)
	require.NoError(t, err)
	guard.release()
	require.Empty(t, candidate.Annotations[config.AnnotationAdoptedRecreationToken])
	require.Nil(t, decodeTestAdoptionSnapshot(t, baseStore.app).Resources[0].PendingRecreation)
}

func TestPrepareRecreatedSnapshotStateFallsBackToSnapshotNamespace(t *testing.T) {
	const token = "persisted-token"
	source := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "backend-config", Namespace: "ops", UID: types.UID("configmap-old"), ResourceVersion: "1",
		},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	saved.Source.Namespace = ""
	saved.PendingRecreation = &domainadoption.RecreationClaim{Token: token}
	savedApp := adoptedApplication(t, "app-1", "ops", saved)
	savedSnapshot := decodeTestAdoptionSnapshot(t, savedApp)
	created := source.DeepCopy()
	created.UID = types.UID("configmap-new")
	created.ResourceVersion = "2"
	created.Annotations = map[string]string{config.AnnotationAdoptedRecreationToken: token}

	newUID, _, updatedApp, err := prepareRecreatedSnapshotState(
		&savedSnapshot.Resources[0],
		&savedSnapshot,
		savedApp,
		created,
		created,
	)
	require.NoError(t, err)
	require.Equal(t, string(created.UID), newUID)
	persisted := decodeTestAdoptionSnapshot(t, updatedApp)
	require.Empty(t, persisted.Resources[0].Source.Namespace)
	require.Equal(t, string(created.UID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
}

func TestAdoptedConfigMapLegacyNamespaceRecreationAlreadyExistsWithClaimConverges(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("configmap-old")
	newUID := types.UID("configmap-new")
	source := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backend-config",
			Namespace: "ops",
			UID:       oldUID,
		},
		Data: map[string]string{"application.yaml": "source"},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	saved.Source.Namespace = ""
	app := adoptedApplication(t, "app-1", "ops", saved)
	legacySnapshot := decodeTestAdoptionSnapshot(t, app)
	legacySnapshot.Version = 1
	legacySnapshotJSON, err := model.NewJSONStructByStruct(legacySnapshot)
	require.NoError(t, err)
	app.AdoptionSnapshot = legacySnapshotJSON
	store := &adoptedSourceStore{app: app}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		token := candidate.Annotations[config.AnnotationAdoptedRecreationToken]
		require.NotEmpty(t, token)
		replacement := candidate.DeepCopy()
		replacement.UID = newUID
		replacement.ResourceVersion = "22"
		replacement.Data["application.yaml"] = "stale"
		require.NoError(t, client.Tracker().Add(replacement))
		return true, nil, k8serrors.NewAlreadyExists(
			schema.GroupResource{Resource: "configmaps"},
			candidate.Name,
		)
	})
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace},
		Data:       map[string]string{"application.yaml": "updated"},
	}
	controller := NewDeployConfigMapJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   string(config.JobDeployConfigMap),
			JobInfo:   desired,
		},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
		nil,
	)

	require.NoError(t, controller.run(ctx))
	replacement, err := client.CoreV1().ConfigMaps("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newUID, replacement.UID)
	require.Equal(t, "updated", replacement.Data["application.yaml"])
	require.Equal(t, 1, countClientActions(client, "update", "configmaps"))

	// A later delivery sees the finalized v2 binding and must converge without
	// attempting another Create with the already-consumed recreation claim.
	require.NoError(t, controller.run(ctx))
	replacement, err = client.CoreV1().ConfigMaps("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newUID, replacement.UID)
	require.Equal(t, "updated", replacement.Data["application.yaml"])
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, domainadoption.SnapshotVersion, persisted.Version)
	require.Empty(t, persisted.Resources[0].Source.Namespace)
	require.Equal(t, string(newUID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
	require.Equal(t, 2, store.applicationCASCount)
	require.Equal(t, 1, countClientActions(client, "create", "configmaps"))
	require.Equal(t, 1, countClientActions(client, "update", "configmaps"))
	require.Equal(t, 0, countClientActions(client, "delete", "configmaps"))
}

func TestAdoptedRecreationLockRejectsStaleClaimantAfterFinalization(t *testing.T) {
	ctx := context.Background()
	source := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "backend-config", Namespace: "ops", UID: types.UID("configmap-old"), ResourceVersion: "1",
		},
		Data: map[string]string{"mode": "source"},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	job := &model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops"}
	firstBinding, adopted, err := adoptedResourceForJob(ctx, store, job, "ConfigMap", "ops", source.Name)
	require.NoError(t, err)
	require.True(t, adopted)
	secondBinding, adopted, err := adoptedResourceForJob(ctx, store, job, "ConfigMap", "ops", source.Name)
	require.NoError(t, err)
	require.True(t, adopted)
	firstRecreation, err := prepareAdoptedDependencyRecreation(store, firstBinding)
	require.NoError(t, err)
	secondRecreation, err := prepareAdoptedDependencyRecreation(store, secondBinding)
	require.NoError(t, err)

	lockProvider := &observedAcquireLocker{
		Locker:            locker.NewMemoryLocker(shareLockerPrefix),
		secondLockStarted: make(chan struct{}),
	}
	runtime := &jobRuntime{}
	firstCandidate := source.DeepCopy()
	firstCandidate.UID = ""
	firstCandidate.ResourceVersion = ""
	firstGuard, err := firstRecreation.adoptedResourceBinding.prepareRecreationCandidate(
		ctx,
		store,
		firstCandidate,
		runtime,
		lockProvider,
	)
	require.NoError(t, err)
	defer firstGuard.release()

	type prepareResult struct {
		guard       *adoptedRecreationGuard
		err         error
		wouldCreate bool
	}
	secondResult := make(chan prepareResult, 1)
	secondCandidate := source.DeepCopy()
	secondCandidate.UID = ""
	secondCandidate.ResourceVersion = ""
	go func() {
		guard, prepareErr := secondRecreation.adoptedResourceBinding.prepareRecreationCandidate(
			ctx,
			store,
			secondCandidate,
			runtime,
			lockProvider,
		)
		secondResult <- prepareResult{
			guard:       guard,
			err:         prepareErr,
			wouldCreate: prepareErr == nil,
		}
	}()
	<-lockProvider.secondLockStarted

	// Finalize the first replacement while the second stale claimant is
	// blocked. Even if the live replacement is deleted immediately afterwards,
	// the second claimant must reload the new canonical UID before any Create.
	firstCreated := firstCandidate.DeepCopy()
	firstCreated.UID = types.UID("configmap-first")
	firstCreated.ResourceVersion = "2"
	require.NoError(t, firstRecreation.persistCreated(firstGuard.Context(), firstCreated, firstCreated, runtime))
	firstGuard.release()

	result := <-secondResult
	if result.guard != nil {
		result.guard.release()
	}
	require.Error(t, result.err)
	require.ErrorContains(t, result.err, "changed concurrently")
	require.False(t, result.wouldCreate)
	require.Empty(t, secondCandidate.Annotations[config.AnnotationAdoptedRecreationToken])
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(firstCreated.UID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
}

func TestAdoptedRecreationClaimFailsClosedWithoutLocker(t *testing.T) {
	ctx := context.Background()
	source := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "backend-config", Namespace: "ops", UID: types.UID("configmap-old")},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	binding, adopted, err := adoptedResourceForJob(
		ctx,
		store,
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops"},
		"ConfigMap",
		"ops",
		source.Name,
	)
	require.NoError(t, err)
	require.True(t, adopted)
	recreation, err := prepareAdoptedDependencyRecreation(store, binding)
	require.NoError(t, err)
	candidate := source.DeepCopy()
	candidate.UID = ""

	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(
		ctx,
		store,
		candidate,
		&jobRuntime{},
		nil,
	)
	require.ErrorContains(t, err, "locker is unavailable")
	require.Nil(t, guard)
	require.Empty(t, candidate.Annotations[config.AnnotationAdoptedRecreationToken])
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
}

func TestAdoptedRecreationRecoverySerializesStaleCreator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const token = "persisted-token"
	source := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "backend-config", Namespace: "ops", UID: types.UID("configmap-old"), ResourceVersion: "1",
		},
		Data: map[string]string{"mode": "source"},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	saved.PendingRecreation = &domainadoption.RecreationClaim{Token: token}
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	job := &model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops"}
	recoveryBinding, adopted, err := adoptedResourceForJob(ctx, store, job, "ConfigMap", "ops", source.Name)
	require.NoError(t, err)
	require.True(t, adopted)
	staleBinding, adopted, err := adoptedResourceForJob(ctx, store, job, "ConfigMap", "ops", source.Name)
	require.NoError(t, err)
	require.True(t, adopted)
	staleRecreation, err := prepareAdoptedDependencyRecreation(store, staleBinding)
	require.NoError(t, err)

	replacement := source.DeepCopy()
	replacement.UID = types.UID("configmap-recovered")
	replacement.ResourceVersion = "2"
	replacement.Annotations = map[string]string{config.AnnotationAdoptedRecreationToken: token}
	lockProvider := &observedAcquireLocker{
		Locker:            locker.NewMemoryLocker(shareLockerPrefix),
		secondLockStarted: make(chan struct{}),
		firstLockAcquired: make(chan struct{}),
		allowFirstReturn:  make(chan struct{}),
	}
	jobRuntime := &jobRuntime{}
	type recoveryResult struct {
		recovered bool
		err       error
	}
	recoveryDone := make(chan recoveryResult, 1)
	go func() {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx,
			store,
			recoveryBinding,
			replacement,
			replacement,
			jobRuntime,
			lockProvider,
		)
		recoveryDone <- recoveryResult{recovered: recovered, err: recoverErr}
	}()
	<-lockProvider.firstLockAcquired

	candidate := source.DeepCopy()
	candidate.UID = ""
	candidate.ResourceVersion = ""
	type creatorResult struct {
		err         error
		createCalls int
	}
	creatorDone := make(chan creatorResult, 1)
	go func() {
		guard, prepareErr := staleRecreation.adoptedResourceBinding.prepareRecreationCandidate(
			ctx,
			store,
			candidate,
			jobRuntime,
			lockProvider,
		)
		result := creatorResult{err: prepareErr}
		if prepareErr == nil {
			result.createCalls++
			guard.release()
		}
		creatorDone <- result
	}()
	<-lockProvider.secondLockStarted
	select {
	case result := <-creatorDone:
		require.Failf(t, "stale creator bypassed recovery lock", "result: %+v", result)
	default:
	}

	close(lockProvider.allowFirstReturn)
	recoveryOutcome := <-recoveryDone
	require.NoError(t, recoveryOutcome.err)
	require.True(t, recoveryOutcome.recovered)
	creatorOutcome := <-creatorDone
	require.ErrorContains(t, creatorOutcome.err, "changed concurrently")
	require.Zero(t, creatorOutcome.createCalls)
	require.Empty(t, candidate.Annotations[config.AnnotationAdoptedRecreationToken])
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(replacement.UID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
}

func TestRecoverPendingAdoptedDependencyReloadsStaleBinding(t *testing.T) {
	ctx := context.Background()
	const token = "persisted-token"
	oldUID := types.UID("configmap-old")
	newUID := types.UID("configmap-new")
	source := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "backend-config", Namespace: "ops", UID: oldUID},
		Data:       map[string]string{"mode": "source"},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	job := &model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops"}
	binding, adopted, err := adoptedResourceForJob(ctx, store, job, "ConfigMap", "ops", source.Name)
	require.NoError(t, err)
	require.True(t, adopted)
	require.Nil(t, binding.resource.PendingRecreation)

	canonical := decodeTestAdoptionSnapshot(t, store.app)
	canonical.Resources[0].PendingRecreation = &domainadoption.RecreationClaim{Token: token}
	canonicalJSON, err := model.NewJSONStructByStruct(canonical)
	require.NoError(t, err)
	store.app.AdoptionSnapshot = canonicalJSON
	replacement := source.DeepCopy()
	replacement.UID = newUID
	replacement.ResourceVersion = "2"
	replacement.Annotations = map[string]string{config.AnnotationAdoptedRecreationToken: token}

	recovered, err := recoverPendingAdoptedDependency(
		ctx,
		store,
		binding,
		replacement,
		replacement,
		&jobRuntime{},
		locker.NewMemoryLocker(shareLockerPrefix),
	)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Equal(t, string(newUID), binding.resource.Source.UID)
	require.Nil(t, binding.resource.PendingRecreation)
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(newUID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
}

func TestRecoverPendingAdoptedWorkloadAcceptsConcurrentFinalization(t *testing.T) {
	ctx := context.Background()
	oldUID := types.UID("deployment-old")
	newUID := types.UID("deployment-new")
	source := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-backend", Namespace: "ops", UID: oldUID},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"workload",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	saved.Source.UID = string(newUID)
	saved.Source.ResourceVersion = "2"
	canonicalComponent := sourceComponent("app-1", "backend", "Deployment", source.Name, newUID)
	staleComponent := sourceComponent("app-1", "backend", "Deployment", source.Name, oldUID)
	store := &adoptedSourceStore{
		app:       adoptedApplication(t, "app-1", "ops", saved),
		component: canonicalComponent,
	}
	replacement := source.DeepCopy()
	replacement.UID = newUID
	replacement.ResourceVersion = "2"

	recovered, err := recoverPendingAdoptedWorkload(
		ctx,
		store,
		&model.JobTask{Name: "backend", AppID: "app-1", Namespace: "ops"},
		staleComponent,
		"Deployment",
		"ops",
		source.Name,
		replacement,
		replacement,
		&jobRuntime{},
		locker.NewMemoryLocker(shareLockerPrefix),
	)
	require.NoError(t, err)
	require.True(t, recovered)
	require.NotNil(t, staleComponent.SourceWorkloadUID)
	require.Equal(t, string(newUID), *staleComponent.SourceWorkloadUID)
	require.Equal(t, 0, store.applicationCASCount)
}

func TestAdoptedConfigMapConcurrentFinalizationPreventsRollback(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("configmap-old")
	newUID := types.UID("configmap-new")
	source := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "backend-config", Namespace: "ops", UID: oldUID},
		Data:       map[string]string{"mode": "source"},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	store := &adoptedSourceStore{app: adoptedApplication(t, "app-1", "ops", saved)}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		candidate.UID = newUID
		candidate.ResourceVersion = "2"
		canonical := decodeTestAdoptionSnapshot(t, store.app)
		require.NotNil(t, canonical.Resources[0].PendingRecreation)
		require.Equal(
			t,
			canonical.Resources[0].PendingRecreation.Token,
			candidate.Annotations[config.AnnotationAdoptedRecreationToken],
		)
		canonical.Resources[0].Source.UID = string(newUID)
		canonical.Resources[0].Source.ResourceVersion = candidate.ResourceVersion
		canonical.Resources[0].PendingRecreation = nil
		canonicalJSON, encodeErr := model.NewJSONStructByStruct(canonical)
		require.NoError(t, encodeErr)
		store.app.AdoptionSnapshot = canonicalJSON
		return false, nil, nil
	})
	controller := NewDeployConfigMapJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   string(config.JobDeployConfigMap),
			JobInfo:   source.DeepCopy(),
		},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
		nil,
	)

	require.NoError(t, controller.run(ctx))
	require.Equal(t, 1, countClientActions(client, "create", "configmaps"))
	require.Equal(t, 0, countClientActions(client, "delete", "configmaps"))
	replacement, err := client.CoreV1().ConfigMaps("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, newUID, replacement.UID)
	persisted := decodeTestAdoptionSnapshot(t, store.app)
	require.Equal(t, string(newUID), persisted.Resources[0].Source.UID)
	require.Nil(t, persisted.Resources[0].PendingRecreation)
}

func TestAdoptedConfigMapUnconfirmedPersistenceSkipsRollback(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	oldUID := types.UID("configmap-old")
	newUID := types.UID("configmap-new")
	source := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "backend-config", Namespace: "ops", UID: oldUID},
		Data:       map[string]string{"mode": "source"},
	}
	saved := adoptedSnapshotResource(
		t,
		source,
		"backend",
		"configmap",
		domainadoption.OwnershipExclusive,
		domainadoption.DispositionManaged,
	)
	confirmationErr := errors.New("confirmation database unavailable")
	baseStore := &adoptedSourceStore{
		app:                        adoptedApplication(t, "app-1", "ops", saved),
		applicationCASErrOnAttempt: 2,
		applicationCASErr:          errors.New("commit result unavailable"),
	}
	store := &recreationConfirmationFailStore{
		adoptedSourceStore: baseStore,
		failApplicationGet: 4,
		err:                confirmationErr,
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		candidate := action.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		candidate.UID = newUID
		candidate.ResourceVersion = "2"
		return false, nil, nil
	})
	controller := NewDeployConfigMapJobCtl(
		&model.JobTask{
			Name:      "backend",
			AppID:     "app-1",
			Namespace: "ops",
			JobType:   string(config.JobDeployConfigMap),
			JobInfo:   source.DeepCopy(),
		},
		client,
		store,
		func() {},
		locker.NewNoopLocker(shareLockerPrefix),
		nil,
	)

	err := controller.run(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, errAdoptedRecreationPersistenceUnconfirmed)
	require.ErrorIs(t, err, confirmationErr)
	require.ErrorContains(t, err, "pending claim retained")
	require.Equal(t, 1, countClientActions(client, "create", "configmaps"))
	require.Equal(t, 0, countClientActions(client, "delete", "configmaps"))
	replacement, getErr := client.CoreV1().ConfigMaps("ops").Get(ctx, source.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, newUID, replacement.UID)
	persisted := decodeTestAdoptionSnapshot(t, baseStore.app)
	require.Equal(t, string(oldUID), persisted.Resources[0].Source.UID)
	require.NotNil(t, persisted.Resources[0].PendingRecreation)
}
