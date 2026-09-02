package job

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type databaseResetComponentStore struct {
	noopStore
	jobInfoMu       sync.Mutex
	components      map[string]*model.ApplicationComponent
	jobInfos        map[int]*model.JobInfo
	nextJobInfoID   int
	jobInfoWriteErr error
}

func newDatabaseResetComponentStore(components ...*model.ApplicationComponent) *databaseResetComponentStore {
	store := &databaseResetComponentStore{
		components: make(map[string]*model.ApplicationComponent, len(components)),
		jobInfos:   make(map[int]*model.JobInfo),
	}
	for _, component := range components {
		if component == nil {
			continue
		}
		store.components[component.Name] = component
	}
	return store
}

func (s *databaseResetComponentStore) Add(ctx context.Context, entity datastore.Entity) error {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return s.noopStore.Add(ctx, entity)
	}
	s.jobInfoMu.Lock()
	defer s.jobInfoMu.Unlock()
	if s.jobInfoWriteErr != nil {
		return s.jobInfoWriteErr
	}
	copy := cloneDatabaseResetJobInfo(jobInfo)
	if copy.ID == 0 {
		s.nextJobInfoID++
		copy.ID = s.nextJobInfoID
	}
	if copy.ID > s.nextJobInfoID {
		s.nextJobInfoID = copy.ID
	}
	s.jobInfos[copy.ID] = copy
	jobInfo.ID = copy.ID
	return nil
}

func (s *databaseResetComponentStore) Put(ctx context.Context, entity datastore.Entity) error {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return s.noopStore.Put(ctx, entity)
	}
	s.jobInfoMu.Lock()
	defer s.jobInfoMu.Unlock()
	if s.jobInfoWriteErr != nil {
		return s.jobInfoWriteErr
	}
	s.jobInfos[jobInfo.ID] = cloneDatabaseResetJobInfo(jobInfo)
	return nil
}

func (s *databaseResetComponentStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	jobInfoQuery, ok := query.(*model.JobInfo)
	if !ok || jobInfoQuery == nil {
		return s.noopStore.List(ctx, query, opts)
	}
	s.jobInfoMu.Lock()
	defer s.jobInfoMu.Unlock()
	ids := make([]int, 0, len(s.jobInfos))
	for id := range s.jobInfos {
		ids = append(ids, id)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(ids)))
	result := make([]datastore.Entity, 0, len(ids))
	for _, id := range ids {
		jobInfo := s.jobInfos[id]
		if jobInfoQuery.TaskID != "" && jobInfo.TaskID != jobInfoQuery.TaskID {
			continue
		}
		if !databaseResetMatchesInFilter(opts, "type", jobInfo.Type) ||
			!databaseResetMatchesInFilter(opts, "service_name", jobInfo.ServiceName) {
			continue
		}
		result = append(result, cloneDatabaseResetJobInfo(jobInfo))
	}
	return result, nil
}

func (s *databaseResetComponentStore) seedJobInfo(jobInfo *model.JobInfo) {
	if jobInfo == nil {
		return
	}
	s.jobInfoMu.Lock()
	defer s.jobInfoMu.Unlock()
	copy := cloneDatabaseResetJobInfo(jobInfo)
	if copy.ID == 0 {
		s.nextJobInfoID++
		copy.ID = s.nextJobInfoID
	}
	if copy.ID > s.nextJobInfoID {
		s.nextJobInfoID = copy.ID
	}
	s.jobInfos[copy.ID] = copy
}

func cloneDatabaseResetJobInfo(jobInfo *model.JobInfo) *model.JobInfo {
	if jobInfo == nil {
		return nil
	}
	copy := *jobInfo
	return &copy
}

func databaseResetMatchesInFilter(opts *datastore.ListOptions, key, value string) bool {
	if opts == nil {
		return true
	}
	for _, filter := range opts.FilterOptions.In {
		if filter.Key != key {
			continue
		}
		for _, candidate := range filter.Values {
			if candidate == value {
				return true
			}
		}
		return false
	}
	return true
}

func (s *databaseResetComponentStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	component, ok := entity.(*model.ApplicationComponent)
	if !ok || component == nil {
		return false, nil
	}
	for _, stored := range s.components {
		if componentMatchesConditions(stored, conditions) {
			applyComponentRuntimeUpdateMap(stored, updates)
			return true, nil
		}
	}
	return false, nil
}

func (s *databaseResetComponentStore) IsExistByCondition(_ context.Context, _ string, conditions map[string]interface{}, _ interface{}) (bool, error) {
	for _, component := range s.components {
		if componentMatchesConditions(component, conditions) {
			return true, nil
		}
	}
	return false, nil
}

func TestDatabaseResetJobCtlResetsStandalonePVCWithoutRestartingServer(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		ClaimName: "mysql-data",
		TmpCreate: false,
		Size:      "1Gi",
	}})
	api := databaseResetServerComponent(t, "api")
	api.Status = string(config.ComponentStatusRunning)
	api.ReadyReplicas = 1
	api.LastAbnormal = "unchanged"
	store := newDatabaseResetComponentStore(db, api)

	result, statefulSet := databaseResetStatefulSet(t, db)
	pvc := firstAdditionalPVC(t, result)
	pvc.Spec.VolumeName = "pv-old"
	deployment := databaseResetDeployment(t, api)

	client := fake.NewSimpleClientset(statefulSet, pvc, deployment)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db, api), client, store, nil)

	require.NoError(t, ctl.run(context.Background()))

	updatedPVC, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "mysql-data", metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, updatedPVC.Spec.VolumeName)

	updatedStatefulSet, err := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, updatedStatefulSet.Spec.Replicas)
	require.Equal(t, int32(1), *updatedStatefulSet.Spec.Replicas)

	updatedDeployment, err := client.AppsV1().Deployments("default").Get(context.Background(), deployment.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, updatedDeployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" && action.GetResource().Resource == "deployments" {
			t.Fatalf("database reset must not patch server deployments: %#v", action)
		}
	}
	require.Equal(t, string(config.ComponentStatusRunning), db.Status)
	require.Equal(t, string(config.ComponentStatusRunning), api.Status)
	require.Equal(t, int32(1), api.ReadyReplicas)
	require.Equal(t, "unchanged", api.LastAbnormal)
}

func TestDatabaseResetJobCtlIgnoresLegacyRestartComponentsAcrossShareStrategies(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		ClaimName: "mysql-data",
		TmpCreate: false,
		Size:      "1Gi",
	}})
	shareCases := []struct {
		name     string
		strategy string
		shared   bool
	}{
		{name: "ordinary"},
		{name: "default", strategy: string(config.ShareStrategyDefault), shared: true},
		{name: "ignore", strategy: string(config.ShareStrategyIgnore), shared: true},
		{name: "unknown", strategy: "future-default", shared: true},
		{name: "force", strategy: string(config.ShareStrategyForce), shared: true},
	}

	components := []*model.ApplicationComponent{db}
	servers := make([]*model.ApplicationComponent, 0, len(shareCases))
	result, statefulSet := databaseResetStatefulSet(t, db)
	objects := []runtime.Object{statefulSet, firstAdditionalPVC(t, result)}
	for _, testCase := range shareCases {
		server := databaseResetServerComponent(t, testCase.name+"-web")
		server.Status = string(config.ComponentStatusRunning)
		server.ReadyReplicas = 1
		server.LastAbnormal = "unchanged-" + testCase.name
		if testCase.shared {
			server.Traits = mustDatabaseResetJSON(t, &spec.Traits{
				Share: &spec.ShareTraitSpec{Strategy: testCase.strategy},
			})
		}
		servers = append(servers, server)
		components = append(components, server)
		objects = append(objects, databaseResetDeployment(t, server))
	}

	store := newDatabaseResetComponentStore(components...)
	client := fake.NewSimpleClientset(objects...)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(components...), client, store, nil)

	require.NoError(t, ctl.run(context.Background()))
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" && action.GetResource().Resource == "deployments" {
			t.Fatalf("database reset must ignore legacy restart targets: %#v", action)
		}
	}
	for i, server := range servers {
		require.Equal(t, string(config.ComponentStatusRunning), server.Status)
		require.Equal(t, int32(1), server.ReadyReplicas)
		require.Equal(t, "unchanged-"+shareCases[i].name, server.LastAbnormal)
		deployment := databaseResetDeployment(t, server)
		current, err := client.AppsV1().Deployments("default").Get(context.Background(), deployment.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Empty(t, current.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	}
}

func TestDatabaseResetJobCtlUpdatesMySQLInitSQLURLBeforePVCReset(t *testing.T) {
	db := databaseResetStoreComponentWithInitSQLURL(t, "mysql", "https://files.example/game-1.0.0.sql", []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		ClaimName: "mysql-data",
		Size:      "1Gi",
	}})
	db.Replicas = 3
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	replicas := int32(3)
	statefulSet.Spec.Replicas = &replicas
	statefulSet.Status.Replicas = 3
	statefulSet.Status.ReadyReplicas = 3
	pvc := firstAdditionalPVC(t, result)
	client := fake.NewSimpleClientset(statefulSet, pvc)

	ctl := NewDatabaseResetJobCtl(databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", db), client, store, nil)
	require.NoError(t, ctl.run(context.Background()))

	updated, err := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "https://files.example/game-1.0.8.sql", databaseResetStatefulSetInitSQLURL(t, updated))
	require.Equal(t, int32(3), *updated.Spec.Replicas)
	require.Equal(t, string(config.ComponentStatusRunning), db.Status)
	require.Equal(t, int32(3), db.ReadyReplicas)
	initSQLUpdateIndex := databaseResetStatefulSetSQLURLUpdateActionIndex(client.Actions(), "https://files.example/game-1.0.8.sql")
	pvcDeleteIndex := databaseResetPVCDeleteActionIndex(client.Actions())
	require.NotEqual(t, -1, initSQLUpdateIndex, "expected StatefulSet update containing the new SQL_URL")
	require.NotEqual(t, -1, pvcDeleteIndex, "expected PVC deletion")
	require.Less(t, initSQLUpdateIndex, pvcDeleteIndex, "SQL_URL must be updated before PVC deletion begins")
}

func TestDatabaseResetJobCtlRestoresReplicasWhenInitSQLURLUpdateFails(t *testing.T) {
	db := databaseResetStoreComponentWithInitSQLURL(t, "mysql", "https://files.example/game-1.0.0.sql", []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		ClaimName: "mysql-data",
		Size:      "1Gi",
	}})
	db.Replicas = 3
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	liveReplicas := int32(2)
	statefulSet.Spec.Replicas = &liveReplicas
	statefulSet.Status.Replicas = liveReplicas
	statefulSet.Status.ReadyReplicas = liveReplicas
	pvc := firstAdditionalPVC(t, result)
	client := fake.NewSimpleClientset(statefulSet, pvc)
	client.PrependReactor("update", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateAction, ok := action.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		updated, ok := updateAction.GetObject().(*appsv1.StatefulSet)
		if !ok || !statefulSetHasInitSQLURL(updated) {
			return false, nil, nil
		}
		for _, container := range updated.Spec.Template.Spec.InitContainers {
			for _, env := range container.Env {
				if env.Name == "SQL_URL" && env.Value == "https://files.example/game-1.0.8.sql" {
					return true, nil, errors.New("init SQL update denied")
				}
			}
		}
		return false, nil, nil
	})

	ctl := NewDatabaseResetJobCtl(databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", db), client, store, nil)
	err := ctl.run(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "init SQL update denied")
	updated, getErr := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, liveReplicas, *updated.Spec.Replicas)
	require.Equal(t, "https://files.example/game-1.0.0.sql", databaseResetStatefulSetInitSQLURL(t, updated))
	requireNoDatabaseResetPVCDelete(t, client.Actions())
}

func TestDatabaseResetJobCtlRestoresCheckpointedReplicasAfterRecoveredPreflightFailure(t *testing.T) {
	db := databaseResetStoreComponentWithInitSQLURL(t, "mysql", "https://files.example/game-1.0.0.sql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	db.Replicas = 3
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	originalReplicas := int32(2)
	statefulSet.Spec.Replicas = &originalReplicas
	statefulSet.Status.Replicas = originalReplicas
	statefulSet.Status.ReadyReplicas = originalReplicas
	pvc := firstAdditionalPVC(t, result)
	client := fake.NewSimpleClientset(statefulSet, pvc)

	firstTask := databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", db)
	firstCtl := NewDatabaseResetJobCtl(firstTask, client, store, nil)
	plans, err := firstCtl.prepareDatabaseResetPlans(context.Background(), []*model.ApplicationComponent{db}, "https://files.example/game-1.0.8.sql")
	require.NoError(t, err)
	require.NoError(t, firstCtl.checkpointDatabaseResetReplicas(context.Background(), plans))
	require.NoError(t, firstCtl.scaleStatefulSet(context.Background(), "default", statefulSet.Name, 0))

	checkpoint := databaseResetStoredReplicaCheckpoint(t, store)
	require.Equal(t, originalReplicas, checkpoint.OriginalReplicas[databaseResetReplicaCheckpointKey("default", statefulSet.Name)])
	require.NotContains(t, firstTask.InternalInfo, "game-1.0.8.sql")
	client.ClearActions()
	preflightFailed := false
	client.PrependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		if preflightFailed {
			return false, nil, nil
		}
		preflightFailed = true
		return true, nil, errors.New("temporary statefulset read failure")
	})

	failedRecoveryTask := databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", db)
	failedRecoveryCtl := NewDatabaseResetJobCtl(failedRecoveryTask, client, store, nil)
	err = failedRecoveryCtl.run(context.Background())
	require.ErrorContains(t, err, "temporary statefulset read failure")
	failedRecoveryTask.Status = config.StatusFailed
	failedRecoveryTask.Error = err.Error()
	require.NoError(t, failedRecoveryCtl.SaveInfo(context.Background()))
	requireNoDatabaseResetKubernetesMutations(t, client.Actions())

	storedAfterFailure := databaseResetStoredJobInfoByExecutionKey(t, store, "step:0/component:0")
	require.Equal(t, string(config.StatusFailed), storedAfterFailure.Status)
	require.Equal(t, err.Error(), storedAfterFailure.Error)
	require.Equal(t, storedAfterFailure.InternalInfo, failedRecoveryTask.InternalInfo)
	checkpoint = databaseResetStoredReplicaCheckpoint(t, store)
	require.True(t, checkpoint.Prepared)
	require.Equal(t, originalReplicas, checkpoint.OriginalReplicas[databaseResetReplicaCheckpointKey("default", statefulSet.Name)])
	require.Equal(t, 1, databaseResetStoredJobInfoCount(store))

	client.ClearActions()
	denyDatabaseResetInitSQLURLUpdate(client, "https://files.example/game-1.0.8.sql")

	retryTask := databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", db)
	retryCtl := NewDatabaseResetJobCtl(retryTask, client, store, nil)
	err = retryCtl.run(context.Background())

	require.ErrorContains(t, err, "init SQL update denied")
	updated, getErr := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, originalReplicas, *updated.Spec.Replicas)
	require.Equal(t, "https://files.example/game-1.0.0.sql", databaseResetStatefulSetInitSQLURL(t, updated))
	requireNoDatabaseResetPVCDelete(t, client.Actions())
}

func TestDatabaseResetJobCtlPreservesCheckpointedZeroReplicasOnUpdateFailure(t *testing.T) {
	db := databaseResetStoreComponentWithInitSQLURL(t, "mysql", "https://files.example/game-1.0.0.sql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	db.Replicas = 3
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	zero := int32(0)
	statefulSet.Spec.Replicas = &zero
	statefulSet.Status.Replicas = 0
	statefulSet.Status.ReadyReplicas = 0
	client := fake.NewSimpleClientset(statefulSet, firstAdditionalPVC(t, result))
	denyDatabaseResetInitSQLURLUpdate(client, "https://files.example/game-1.0.8.sql")

	ctl := NewDatabaseResetJobCtl(databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", db), client, store, nil)
	err := ctl.run(context.Background())

	require.ErrorContains(t, err, "init SQL update denied")
	updated, getErr := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, zero, *updated.Spec.Replicas)
	require.Equal(t, zero, databaseResetStoredReplicaCheckpoint(t, store).OriginalReplicas[databaseResetReplicaCheckpointKey("default", statefulSet.Name)])
	requireNoDatabaseResetPVCDelete(t, client.Actions())
}

func TestDatabaseResetJobCtlIsolatesReplicaCheckpointsByExecutionKey(t *testing.T) {
	mysql := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	redis := databaseResetStoreComponent(t, "redis", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/data", ClaimName: "redis-data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(mysql, redis)
	mysqlResult, mysqlStatefulSet := databaseResetStatefulSet(t, mysql)
	redisResult, redisStatefulSet := databaseResetStatefulSet(t, redis)
	mysqlReplicas := int32(2)
	redisReplicas := int32(4)
	mysqlStatefulSet.Spec.Replicas = &mysqlReplicas
	redisStatefulSet.Spec.Replicas = &redisReplicas
	client := fake.NewSimpleClientset(
		mysqlStatefulSet,
		redisStatefulSet,
		firstAdditionalPVC(t, mysqlResult),
		firstAdditionalPVC(t, redisResult),
	)

	firstTask := databaseResetTask(mysql)
	firstTask.JobInfo.(*DatabaseResetJobInfo).ExecutionKey = "step:0/component:0"
	firstCtl := NewDatabaseResetJobCtl(firstTask, client, store, nil)
	firstPlans, err := firstCtl.prepareDatabaseResetPlans(context.Background(), []*model.ApplicationComponent{mysql}, "")
	require.NoError(t, err)
	require.NoError(t, firstCtl.checkpointDatabaseResetReplicas(context.Background(), firstPlans))

	secondTask := databaseResetTask(redis)
	secondTask.JobInfo.(*DatabaseResetJobInfo).ExecutionKey = "step:1/component:0"
	secondCtl := NewDatabaseResetJobCtl(secondTask, client, store, nil)
	secondPlans, err := secondCtl.prepareDatabaseResetPlans(context.Background(), []*model.ApplicationComponent{redis}, "")
	require.NoError(t, err)
	require.NoError(t, secondCtl.checkpointDatabaseResetReplicas(context.Background(), secondPlans))

	require.Equal(t, 2, databaseResetStoredJobInfoCount(store))
	firstCheckpoint := databaseResetStoredReplicaCheckpointByExecutionKey(t, store, "step:0/component:0")
	secondCheckpoint := databaseResetStoredReplicaCheckpointByExecutionKey(t, store, "step:1/component:0")
	require.Equal(t, mysqlReplicas, firstCheckpoint.OriginalReplicas[databaseResetReplicaCheckpointKey("default", mysqlStatefulSet.Name)])
	require.NotContains(t, firstCheckpoint.OriginalReplicas, databaseResetReplicaCheckpointKey("default", redisStatefulSet.Name))
	require.Equal(t, redisReplicas, secondCheckpoint.OriginalReplicas[databaseResetReplicaCheckpointKey("default", redisStatefulSet.Name)])
	require.NotContains(t, secondCheckpoint.OriginalReplicas, databaseResetReplicaCheckpointKey("default", mysqlStatefulSet.Name))

	secondRaw := secondTask.InternalInfo
	firstTask.Status = config.StatusCompleted
	require.NoError(t, firstCtl.SaveInfo(context.Background()))
	require.Equal(t, 2, databaseResetStoredJobInfoCount(store))
	require.Equal(t, secondRaw, databaseResetStoredJobInfoByExecutionKey(t, store, "step:1/component:0").InternalInfo)
}

func TestDatabaseResetJobCtlPersistsParallelReplicaCheckpointsIndependently(t *testing.T) {
	mysql := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	redis := databaseResetStoreComponent(t, "redis", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/data", ClaimName: "redis-data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(mysql, redis)
	mysqlResult, mysqlStatefulSet := databaseResetStatefulSet(t, mysql)
	redisResult, redisStatefulSet := databaseResetStatefulSet(t, redis)
	client := fake.NewSimpleClientset(
		mysqlStatefulSet,
		redisStatefulSet,
		firstAdditionalPVC(t, mysqlResult),
		firstAdditionalPVC(t, redisResult),
	)

	tasks := []*model.JobTask{databaseResetTask(mysql), databaseResetTask(redis)}
	tasks[0].JobInfo.(*DatabaseResetJobInfo).ExecutionKey = "step:0/substep:0/component:0"
	tasks[1].JobInfo.(*DatabaseResetJobInfo).ExecutionKey = "step:0/substep:1/component:0"
	components := [][]*model.ApplicationComponent{{mysql}, {redis}}
	errorsByTask := make([]error, len(tasks))
	var waitGroup sync.WaitGroup
	for index := range tasks {
		index := index
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			ctl := NewDatabaseResetJobCtl(tasks[index], client, store, nil)
			plans, err := ctl.prepareDatabaseResetPlans(context.Background(), components[index], "")
			if err == nil {
				err = ctl.checkpointDatabaseResetReplicas(context.Background(), plans)
			}
			errorsByTask[index] = err
		}()
	}
	waitGroup.Wait()

	require.NoError(t, errorsByTask[0])
	require.NoError(t, errorsByTask[1])
	require.Equal(t, 2, databaseResetStoredJobInfoCount(store))
	databaseResetStoredReplicaCheckpointByExecutionKey(t, store, "step:0/substep:0/component:0")
	databaseResetStoredReplicaCheckpointByExecutionKey(t, store, "step:0/substep:1/component:0")
}

func TestDatabaseResetJobCtlRetriesFromUnpreparedCheckpointIdentity(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	client := fake.NewSimpleClientset()
	task := databaseResetTask(db)
	ctl := NewDatabaseResetJobCtl(task, client, store, nil)

	err := ctl.run(context.Background())
	require.ErrorContains(t, err, "get statefulset")
	requireNoDatabaseResetKubernetesMutations(t, client.Actions())
	require.NoError(t, ctl.SaveInfo(context.Background()))
	identity := databaseResetStoredReplicaCheckpoint(t, store)
	require.False(t, identity.Prepared)
	require.Nil(t, identity.OriginalReplicas)

	result, statefulSet := databaseResetStatefulSet(t, db)
	require.NoError(t, client.Tracker().Add(statefulSet))
	require.NoError(t, client.Tracker().Add(firstAdditionalPVC(t, result)))
	recoveredTask := databaseResetTask(db)
	recoveredCtl := NewDatabaseResetJobCtl(recoveredTask, client, store, nil)
	plans, err := recoveredCtl.prepareDatabaseResetPlans(context.Background(), []*model.ApplicationComponent{db}, "")
	require.NoError(t, err)
	require.NoError(t, recoveredCtl.checkpointDatabaseResetReplicas(context.Background(), plans))

	require.Equal(t, 1, databaseResetStoredJobInfoCount(store))
	checkpoint := databaseResetStoredReplicaCheckpoint(t, store)
	require.True(t, checkpoint.Prepared)
	require.Equal(t, int32(1), checkpoint.OriginalReplicas[databaseResetReplicaCheckpointKey("default", statefulSet.Name)])
}

func TestDatabaseResetJobCtlFailsBeforeMutationWhenReplicaCheckpointCannotPersist(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	store.jobInfoWriteErr = errors.New("checkpoint database unavailable")
	result, statefulSet := databaseResetStatefulSet(t, db)
	client := fake.NewSimpleClientset(statefulSet, firstAdditionalPVC(t, result))

	ctl := NewDatabaseResetJobCtl(databaseResetTask(db), client, store, nil)
	err := ctl.run(context.Background())

	require.ErrorContains(t, err, "persist database reset replica checkpoint")
	require.Empty(t, db.Status)
	requireNoDatabaseResetKubernetesMutations(t, client.Actions())
}

func TestDatabaseResetJobCtlRejectsInvalidReplicaCheckpointBeforeMutation(t *testing.T) {
	tests := []struct {
		name         string
		internalInfo string
		errorText    string
	}{
		{name: "empty", internalInfo: ``, errorText: "checkpoint is empty"},
		{name: "corrupted", internalInfo: `{`, errorText: "decode database reset replica checkpoint"},
		{name: "legacy unscoped", internalInfo: `{"version":1,"originalReplicas":{"default/demo-mysql":1}}`, errorText: "version 1 is unsupported"},
		{name: "unsupported version", internalInfo: `{"version":3,"executionKey":"step:0/component:0","prepared":true,"originalReplicas":{"default/mysql":1}}`, errorText: "version 3 is unsupported"},
		{name: "missing execution key", internalInfo: `{"version":2,"prepared":true,"originalReplicas":{"default/demo-mysql":1}}`, errorText: "execution key is missing"},
		{name: "prepared replicas missing", internalInfo: `{"version":2,"executionKey":"step:0/component:0","prepared":true}`, errorText: "original replicas are missing"},
		{name: "unprepared replicas present", internalInfo: `{"version":2,"executionKey":"step:0/component:0","prepared":false,"originalReplicas":{"default/demo-mysql":1}}`, errorText: "is not prepared but contains original replicas"},
		{name: "missing target", internalInfo: `{"version":2,"executionKey":"step:0/component:0","prepared":true,"originalReplicas":{"default/other":1}}`, errorText: "is missing statefulset"},
		{name: "negative replicas", internalInfo: `{"version":2,"executionKey":"step:0/component:0","prepared":true,"originalReplicas":{"default/demo-mysql":-1}}`, errorText: "has invalid replicas"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
				Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
			}})
			store := newDatabaseResetComponentStore(db)
			result, statefulSet := databaseResetStatefulSet(t, db)
			client := fake.NewSimpleClientset(statefulSet, firstAdditionalPVC(t, result))
			task := databaseResetTask(db)
			store.seedJobInfo(&model.JobInfo{
				Type:         task.JobType,
				TaskID:       task.TaskID,
				ServiceName:  resolveJobServiceName(task),
				InternalInfo: test.internalInfo,
			})

			ctl := NewDatabaseResetJobCtl(task, client, store, nil)
			err := ctl.run(context.Background())

			require.ErrorContains(t, err, test.errorText)
			require.Empty(t, db.Status)
			requireNoDatabaseResetKubernetesMutations(t, client.Actions())
		})
	}
}

func TestDatabaseResetJobCtlRejectsMissingExecutionKeyBeforeMutation(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	client := fake.NewSimpleClientset(statefulSet, firstAdditionalPVC(t, result))
	task := databaseResetTask(db)
	task.JobInfo.(*DatabaseResetJobInfo).ExecutionKey = ""

	ctl := NewDatabaseResetJobCtl(task, client, store, nil)
	err := ctl.run(context.Background())

	require.ErrorContains(t, err, "database reset execution key is missing")
	requireNoDatabaseResetKubernetesMutations(t, client.Actions())
}

func TestDatabaseResetJobCtlRejectsDuplicateExecutionCheckpointsBeforeMutation(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	client := fake.NewSimpleClientset(statefulSet, firstAdditionalPVC(t, result))
	task := databaseResetTask(db)
	raw := `{"version":2,"executionKey":"step:0/component:0","prepared":true,"originalReplicas":{"default/demo-mysql":1}}`
	for range 2 {
		store.seedJobInfo(&model.JobInfo{
			Type:         task.JobType,
			TaskID:       task.TaskID,
			ServiceName:  resolveJobServiceName(task),
			InternalInfo: raw,
		})
	}

	ctl := NewDatabaseResetJobCtl(task, client, store, nil)
	err := ctl.run(context.Background())

	require.ErrorContains(t, err, "multiple database reset replica checkpoints")
	requireNoDatabaseResetKubernetesMutations(t, client.Actions())
}

func TestDatabaseResetJobCtlSaveInfoUpsertsReplicaCheckpoint(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name: "data", Type: "persistent", MountPath: "/var/lib/mysql", ClaimName: "mysql-data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	statefulSet.Spec.Replicas = nil
	client := fake.NewSimpleClientset(statefulSet, firstAdditionalPVC(t, result))
	task := databaseResetTask(db)
	ctl := NewDatabaseResetJobCtl(task, client, store, nil)

	require.NoError(t, ctl.run(context.Background()))
	checkpointInfo := task.InternalInfo
	require.Equal(t, int32(1), databaseResetStoredReplicaCheckpoint(t, store).OriginalReplicas[databaseResetReplicaCheckpointKey("default", statefulSet.Name)])
	task.Status = config.StatusCompleted
	require.NoError(t, ctl.SaveInfo(context.Background()))

	require.Len(t, store.jobInfos, 1)
	for _, jobInfo := range store.jobInfos {
		require.Equal(t, string(config.StatusCompleted), jobInfo.Status)
		require.Equal(t, checkpointInfo, jobInfo.InternalInfo)
	}
}

func TestDatabaseResetJobCtlSkipsInitSQLURLUpdateWhenUnchanged(t *testing.T) {
	const currentURL = "https://files.example/game-1.0.8.sql"
	db := databaseResetStoreComponentWithInitSQLURL(t, "mysql", currentURL, []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		ClaimName: "mysql-data",
		Size:      "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	pvc := firstAdditionalPVC(t, result)
	client := fake.NewSimpleClientset(statefulSet, pvc)

	ctl := NewDatabaseResetJobCtl(databaseResetTaskWithInitSQLURL("  "+currentURL+"  ", db), client, store, nil)
	require.NoError(t, ctl.run(context.Background()))

	statefulSetUpdates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" && action.GetResource().Resource == "statefulsets" {
			statefulSetUpdates++
		}
	}
	require.Equal(t, 2, statefulSetUpdates, "only scale-to-zero and replica restoration should update the StatefulSet")
}

func TestDatabaseResetJobCtlWithoutInitSQLURLKeepsExistingValue(t *testing.T) {
	const currentURL = "https://files.example/game-1.0.0.sql"
	db := databaseResetStoreComponentWithInitSQLURL(t, "mysql", currentURL, []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		ClaimName: "mysql-data",
		Size:      "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	result, statefulSet := databaseResetStatefulSet(t, db)
	pvc := firstAdditionalPVC(t, result)
	client := fake.NewSimpleClientset(statefulSet, pvc)

	ctl := NewDatabaseResetJobCtl(databaseResetTask(db), client, store, nil)
	require.NoError(t, ctl.run(context.Background()))

	updated, err := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, currentURL, databaseResetStatefulSetInitSQLURL(t, updated))
}

func TestDatabaseResetJobCtlInitSQLURLDoesNotApplyToRedis(t *testing.T) {
	mysql := databaseResetStoreComponentWithInitSQLURL(t, "mysql", "https://files.example/game-1.0.0.sql", []spec.StorageTraitSpec{{
		Name: "mysql-data", Type: "persistent", MountPath: "/var/lib/mysql", Size: "1Gi",
	}})
	redis := databaseResetStoreComponent(t, "redis", []spec.StorageTraitSpec{{
		Name: "redis-data", Type: "persistent", MountPath: "/data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(mysql, redis)
	mysqlResult, mysqlStatefulSet := databaseResetStatefulSet(t, mysql)
	redisResult, redisStatefulSet := databaseResetStatefulSet(t, redis)
	client := fake.NewSimpleClientset(mysqlStatefulSet, firstAdditionalPVC(t, mysqlResult), redisStatefulSet, firstAdditionalPVC(t, redisResult))

	ctl := NewDatabaseResetJobCtl(databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", mysql, redis), client, store, nil)
	require.NoError(t, ctl.run(context.Background()))

	updatedMySQL, err := client.AppsV1().StatefulSets("default").Get(context.Background(), mysqlStatefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "https://files.example/game-1.0.8.sql", databaseResetStatefulSetInitSQLURL(t, updatedMySQL))
	updatedRedis, err := client.AppsV1().StatefulSets("default").Get(context.Background(), redisStatefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.False(t, statefulSetHasInitSQLURL(updatedRedis))
}

func TestDatabaseResetJobCtlRejectsInitSQLURLWhenNoStatefulSetMatches(t *testing.T) {
	redis := databaseResetStoreComponent(t, "redis", []spec.StorageTraitSpec{{
		Name: "redis-data", Type: "persistent", MountPath: "/data", Size: "1Gi",
	}})
	store := newDatabaseResetComponentStore(redis)
	result, statefulSet := databaseResetStatefulSet(t, redis)
	client := fake.NewSimpleClientset(statefulSet, firstAdditionalPVC(t, result))

	ctl := NewDatabaseResetJobCtl(databaseResetTaskWithInitSQLURL("https://files.example/game-1.0.8.sql", redis), client, store, nil)
	err := ctl.run(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "no matching init container SQL_URL target")
	require.Empty(t, redis.Status)
	for _, action := range client.Actions() {
		require.NotContains(t, []string{"create", "update", "patch", "delete"}, action.GetVerb(), "preflight failure must not mutate Kubernetes resources")
	}
}

func TestDatabaseResetJobCtlPreservesStoppedServer(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		ClaimName: "mysql-data",
		TmpCreate: false,
		Size:      "1Gi",
	}})
	api := databaseResetServerComponent(t, "api")
	api.Status = string(config.ComponentStatusStopped)
	api.ReadyReplicas = 0
	api.LastAbnormal = "stopped by user"
	store := newDatabaseResetComponentStore(db, api)

	result, statefulSet := databaseResetStatefulSet(t, db)
	pvc := firstAdditionalPVC(t, result)
	deployment := databaseResetDeployment(t, api)
	zero := int32(0)
	deployment.Spec.Replicas = &zero

	client := fake.NewSimpleClientset(statefulSet, pvc, deployment)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db, api), client, store, nil)

	require.NoError(t, ctl.run(context.Background()))

	updatedDeployment, err := client.AppsV1().Deployments("default").Get(context.Background(), deployment.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, updatedDeployment.Spec.Template.Annotations[config.AnnotationWorkloadRestartAt])
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" && action.GetResource().Resource == "deployments" {
			t.Fatalf("stopped server deployment should not be patched: %#v", action)
		}
	}
	require.Equal(t, string(config.ComponentStatusRunning), db.Status)
	require.Equal(t, string(config.ComponentStatusStopped), api.Status)
	require.Equal(t, int32(0), api.ReadyReplicas)
	require.Equal(t, "stopped by user", api.LastAbnormal)
}

func TestDatabaseResetJobCtlDeletesVolumeClaimTemplatePVCWithoutRecreate(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "mysql-data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		TmpCreate: true,
		Size:      "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	require.Len(t, statefulSet.Spec.VolumeClaimTemplates, 1)
	templatePVCName := statefulSet.Spec.VolumeClaimTemplates[0].Name + "-" + statefulSet.Name + "-0"
	templatePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: "default"},
		Spec:       statefulSet.Spec.VolumeClaimTemplates[0].Spec,
	}

	client := fake.NewSimpleClientset(statefulSet, templatePVC)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db), client, store, nil)

	require.NoError(t, ctl.run(context.Background()))
	_, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), templatePVCName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err), "volumeClaimTemplate PVC should be deleted and recreated by StatefulSet later")
}

func TestDatabaseResetJobCtlKeepsSimilarPrefixTemplatePVC(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "mysql-data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		TmpCreate: true,
		Size:      "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	require.Len(t, statefulSet.Spec.VolumeClaimTemplates, 1)
	templateName := statefulSet.Spec.VolumeClaimTemplates[0].Name
	templatePVCName := templateName + "-" + statefulSet.Name + "-0"
	templatePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: "default"},
		Spec:       statefulSet.Spec.VolumeClaimTemplates[0].Spec,
	}
	unrelatedPVCName := templateName + "-" + statefulSet.Name + "-backup-0"
	unrelatedPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: unrelatedPVCName, Namespace: "default"},
		Spec:       statefulSet.Spec.VolumeClaimTemplates[0].Spec,
	}

	client := fake.NewSimpleClientset(statefulSet, templatePVC, unrelatedPVC)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db), client, store, nil)

	require.NoError(t, ctl.run(context.Background()))
	_, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), templatePVCName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err), "owned volumeClaimTemplate PVC should be deleted")
	keptPVC, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), unrelatedPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, unrelatedPVCName, keptPVC.Name)
}

func TestDatabaseResetJobCtlSkipsMissingVolumeClaimTemplatePVC(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "mysql-data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		TmpCreate: true,
		Size:      "1Gi",
	}})
	db.Replicas = 3
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	statefulSet.Status.Replicas = 3
	statefulSet.Status.ReadyReplicas = 3
	require.Len(t, statefulSet.Spec.VolumeClaimTemplates, 1)
	templatePVCName := statefulSet.Spec.VolumeClaimTemplates[0].Name + "-" + statefulSet.Name + "-0"
	templatePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: "default"},
		Spec:       statefulSet.Spec.VolumeClaimTemplates[0].Spec,
	}

	client := fake.NewSimpleClientset(statefulSet, templatePVC)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db), client, store, nil)

	require.NoError(t, ctl.run(context.Background()))
	_, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), templatePVCName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err), "existing template PVC should be deleted")
	updatedStatefulSet, err := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSet.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, updatedStatefulSet.Spec.Replicas)
	require.Equal(t, int32(3), *updatedStatefulSet.Spec.Replicas)
}

func TestDatabaseResetJobCtlFailsWhenStandalonePVCMissing(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "mysql-data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		TmpCreate: false,
		Size:      "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)

	client := fake.NewSimpleClientset(statefulSet)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db), client, store, nil)

	err := ctl.run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "get pvc default/mysql-data before reset")
	require.Equal(t, string(config.ComponentStatusFailed), db.Status)
}

func TestDatabaseResetJobCtlSkipsMissingServerDeployment(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "mysql-data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		TmpCreate: false,
		Size:      "1Gi",
	}})
	api := databaseResetServerComponent(t, "api")
	store := newDatabaseResetComponentStore(db, api)
	result, statefulSet := databaseResetStatefulSet(t, db)
	pvc := firstAdditionalPVC(t, result)

	client := fake.NewSimpleClientset(statefulSet, pvc)
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db, api), client, store, nil)

	require.NoError(t, ctl.run(context.Background()))
	require.Equal(t, string(config.ComponentStatusRunning), db.Status)
	require.Empty(t, api.Status)
}

func TestDatabaseResetJobCtlReturnsErrorWhenStatefulSetMissing(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "mysql-data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		TmpCreate: true,
		Size:      "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	client := fake.NewSimpleClientset()
	ctl := NewDatabaseResetJobCtl(databaseResetTask(db), client, store, nil)

	err := ctl.run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "get statefulset")
	require.Empty(t, db.Status, "StatefulSet preflight must fail before runtime status changes")
}

func TestDatabaseResetJobCtlTimesOutWhenPodsRemain(t *testing.T) {
	db := databaseResetStoreComponent(t, "mysql", []spec.StorageTraitSpec{{
		Name:      "mysql-data",
		Type:      "persistent",
		MountPath: "/var/lib/mysql",
		TmpCreate: true,
		Size:      "1Gi",
	}})
	store := newDatabaseResetComponentStore(db)
	_, statefulSet := databaseResetStatefulSet(t, db)
	templatePVCName := statefulSet.Spec.VolumeClaimTemplates[0].Name + "-" + statefulSet.Name + "-0"
	templatePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: templatePVCName, Namespace: "default"},
		Spec:       statefulSet.Spec.VolumeClaimTemplates[0].Spec,
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSet.Name + "-0",
			Namespace: "default",
			Labels:    statefulSet.Spec.Selector.MatchLabels,
		},
	}
	client := fake.NewSimpleClientset(statefulSet, templatePVC, pod)
	task := databaseResetTask(db)
	task.Timeout = 1
	ctl := NewDatabaseResetJobCtl(task, client, store, nil)

	err := ctl.run(context.Background())
	require.Error(t, err)
	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok, "expected timeout status error, got %v", err)
	require.Equal(t, config.StatusTimeout, statusErr.Status)
}

func databaseResetTask(databaseComponents ...*model.ApplicationComponent) *model.JobTask {
	return databaseResetTaskWithInitSQLURL("", databaseComponents...)
}

func databaseResetTaskWithInitSQLURL(initSQLURL string, databaseComponents ...*model.ApplicationComponent) *model.JobTask {
	var restartComponents []*model.ApplicationComponent
	filteredDatabases := make([]*model.ApplicationComponent, 0, len(databaseComponents))
	for _, component := range databaseComponents {
		if component == nil {
			continue
		}
		if component.ComponentType == config.ServerJob {
			restartComponents = append(restartComponents, component)
			continue
		}
		filteredDatabases = append(filteredDatabases, component)
	}
	return &model.JobTask{
		Name:       "database-reset",
		Namespace:  "default",
		WorkflowID: "workflow-1",
		ProjectID:  "project-1",
		AppID:      "app-1",
		TaskID:     "task-1",
		JobType:    string(config.JobDatabaseReset),
		Timeout:    5,
		JobInfo: &DatabaseResetJobInfo{
			DatabaseComponents: filteredDatabases,
			RestartComponents:  restartComponents,
			InitSQLURL:         initSQLURL,
			ExecutionKey:       "step:0/component:0",
		},
	}
}

func databaseResetStoreComponentWithInitSQLURL(t *testing.T, name, initSQLURL string, storage []spec.StorageTraitSpec) *model.ApplicationComponent {
	t.Helper()
	component := databaseResetStoreComponent(t, name, storage)
	component.Traits = mustDatabaseResetJSON(t, &spec.Traits{
		Init: []spec.InitTraitSpec{{
			Name:  "init-sql",
			Image: "curlimages/curl:latest",
			Properties: spec.Properties{Env: map[string]string{
				"SQL_URL": initSQLURL,
			}},
		}},
		Storage: storage,
	})
	return component
}

func databaseResetStatefulSetInitSQLURL(t *testing.T, statefulSet *appsv1.StatefulSet) string {
	t.Helper()
	for _, container := range statefulSet.Spec.Template.Spec.InitContainers {
		for _, env := range container.Env {
			if env.Name == "SQL_URL" {
				return env.Value
			}
		}
	}
	t.Fatal("StatefulSet does not contain init container SQL_URL")
	return ""
}

func databaseResetStoreComponent(t *testing.T, name string, storage []spec.StorageTraitSpec) *model.ApplicationComponent {
	t.Helper()
	return &model.ApplicationComponent{
		ID:              len(name),
		AppID:           "app-1",
		Name:            name,
		Namespace:       "default",
		Image:           "mysql:8.0",
		Replicas:        1,
		ComponentType:   config.StoreJob,
		ResourceAppName: "demo",
		Properties:      mustDatabaseResetJSON(t, &model.Properties{}),
		Traits: mustDatabaseResetJSON(t, &spec.Traits{
			Storage: storage,
		}),
	}
}

func databaseResetServerComponent(t *testing.T, name string) *model.ApplicationComponent {
	t.Helper()
	return &model.ApplicationComponent{
		ID:              len(name) + 100,
		AppID:           "app-1",
		Name:            name,
		Namespace:       "default",
		Image:           "nginx:latest",
		Replicas:        1,
		ComponentType:   config.ServerJob,
		ResourceAppName: "demo",
		Properties: mustDatabaseResetJSON(t, &model.Properties{
			Ports: []model.Ports{{Port: 8080}},
		}),
		Traits: mustDatabaseResetJSON(t, &spec.Traits{}),
	}
}

func databaseResetStatefulSet(t *testing.T, component *model.ApplicationComponent) (*GenerateServiceResult, *appsv1.StatefulSet) {
	t.Helper()
	registerDatabaseResetTraitProcessors(t)
	result := GenerateStoreService(component)
	require.NotNil(t, result)
	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	require.True(t, ok)
	statefulSet.Status.Replicas = 1
	statefulSet.Status.ReadyReplicas = 1
	return result, statefulSet
}

func registerDatabaseResetTraitProcessors(t *testing.T) {
	t.Helper()
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)
}

func firstAdditionalPVC(t *testing.T, result *GenerateServiceResult) *corev1.PersistentVolumeClaim {
	t.Helper()
	for _, obj := range result.AdditionalObjects {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if ok {
			return pvc
		}
	}
	t.Fatalf("no additional pvc found")
	return nil
}

func databaseResetDeployment(t *testing.T, component *model.ApplicationComponent) *appsv1.Deployment {
	t.Helper()
	properties := ParseProperties(component.Properties)
	result := GenerateWebService(component, &properties)
	require.NotNil(t, result)
	deployment, ok := result.Service.(*appsv1.Deployment)
	require.True(t, ok)
	return deployment
}

func mustDatabaseResetJSON(t *testing.T, value interface{}) *model.JSONStruct {
	t.Helper()
	result, err := model.NewJSONStructByStruct(value)
	require.NoError(t, err)
	return result
}

func denyDatabaseResetInitSQLURLUpdate(client *fake.Clientset, deniedURL string) {
	client.PrependReactor("update", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateAction, ok := action.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		statefulSet, ok := updateAction.GetObject().(*appsv1.StatefulSet)
		if !ok {
			return false, nil, nil
		}
		for _, container := range statefulSet.Spec.Template.Spec.InitContainers {
			for _, env := range container.Env {
				if env.Name == "SQL_URL" && env.Value == deniedURL {
					return true, nil, errors.New("init SQL update denied")
				}
			}
		}
		return false, nil, nil
	})
}

func databaseResetStoredReplicaCheckpoint(t *testing.T, store *databaseResetComponentStore) databaseResetReplicaCheckpoint {
	t.Helper()
	store.jobInfoMu.Lock()
	defer store.jobInfoMu.Unlock()
	require.Len(t, store.jobInfos, 1)
	for _, jobInfo := range store.jobInfos {
		var checkpoint databaseResetReplicaCheckpoint
		require.NoError(t, json.Unmarshal([]byte(jobInfo.InternalInfo), &checkpoint))
		return checkpoint
	}
	t.Fatal("database reset replica checkpoint not found")
	return databaseResetReplicaCheckpoint{}
}

func databaseResetStoredReplicaCheckpointByExecutionKey(t *testing.T, store *databaseResetComponentStore, executionKey string) databaseResetReplicaCheckpoint {
	t.Helper()
	jobInfo := databaseResetStoredJobInfoByExecutionKey(t, store, executionKey)
	var checkpoint databaseResetReplicaCheckpoint
	require.NoError(t, json.Unmarshal([]byte(jobInfo.InternalInfo), &checkpoint))
	require.Equal(t, executionKey, checkpoint.ExecutionKey)
	return checkpoint
}

func databaseResetStoredJobInfoByExecutionKey(t *testing.T, store *databaseResetComponentStore, executionKey string) *model.JobInfo {
	t.Helper()
	store.jobInfoMu.Lock()
	defer store.jobInfoMu.Unlock()
	for _, jobInfo := range store.jobInfos {
		var checkpoint databaseResetReplicaCheckpoint
		if json.Unmarshal([]byte(jobInfo.InternalInfo), &checkpoint) == nil && checkpoint.ExecutionKey == executionKey {
			return cloneDatabaseResetJobInfo(jobInfo)
		}
	}
	t.Fatalf("database reset replica checkpoint for execution key %q not found", executionKey)
	return nil
}

func databaseResetStoredJobInfoCount(store *databaseResetComponentStore) int {
	store.jobInfoMu.Lock()
	defer store.jobInfoMu.Unlock()
	return len(store.jobInfos)
}

func databaseResetStatefulSetSQLURLUpdateActionIndex(actions []k8stesting.Action, expectedURL string) int {
	for index, action := range actions {
		if action.GetVerb() != "update" || action.GetResource().Resource != "statefulsets" {
			continue
		}
		updateAction, ok := action.(k8stesting.UpdateAction)
		if !ok {
			continue
		}
		statefulSet, ok := updateAction.GetObject().(*appsv1.StatefulSet)
		if !ok {
			continue
		}
		for _, container := range statefulSet.Spec.Template.Spec.InitContainers {
			for _, env := range container.Env {
				if env.Name == "SQL_URL" && env.Value == expectedURL {
					return index
				}
			}
		}
	}
	return -1
}

func databaseResetPVCDeleteActionIndex(actions []k8stesting.Action) int {
	for index, action := range actions {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "persistentvolumeclaims" {
			return index
		}
	}
	return -1
}

func requireNoDatabaseResetPVCDelete(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	require.Equal(t, -1, databaseResetPVCDeleteActionIndex(actions), "PVC deletion must not begin")
}

func requireNoDatabaseResetKubernetesMutations(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		require.NotContains(t, []string{"create", "update", "patch", "delete"}, action.GetVerb(), "preflight failure must not mutate Kubernetes resources")
	}
}
