package job

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdeploy"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type databaseResetComponentStore struct {
	jobtest.NoopStore
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
		return s.NoopStore.Add(ctx, entity)
	}
	s.jobInfoMu.Lock()
	defer s.jobInfoMu.Unlock()
	if s.jobInfoWriteErr != nil {
		return s.jobInfoWriteErr
	}
	copy := jobtest.CloneDatabaseResetJobInfo(jobInfo)
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
		return s.NoopStore.Put(ctx, entity)
	}
	s.jobInfoMu.Lock()
	defer s.jobInfoMu.Unlock()
	if s.jobInfoWriteErr != nil {
		return s.jobInfoWriteErr
	}
	s.jobInfos[jobInfo.ID] = jobtest.CloneDatabaseResetJobInfo(jobInfo)
	return nil
}

func (s *databaseResetComponentStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	jobInfoQuery, ok := query.(*model.JobInfo)
	if !ok || jobInfoQuery == nil {
		return s.NoopStore.List(ctx, query, opts)
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
		if !jobtest.DatabaseResetMatchesInFilter(opts, "type", jobInfo.Type) ||
			!jobtest.DatabaseResetMatchesInFilter(opts, "service_name", jobInfo.ServiceName) {
			continue
		}
		result = append(result, jobtest.CloneDatabaseResetJobInfo(jobInfo))
	}
	return result, nil
}

func (s *databaseResetComponentStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	component, ok := entity.(*model.ApplicationComponent)
	if !ok || component == nil {
		return false, nil
	}
	for _, stored := range s.components {
		if jobtest.ComponentMatchesConditions(stored, conditions) {
			jobtest.ApplyComponentRuntimeUpdateMap(stored, updates)
			return true, nil
		}
	}
	return false, nil
}

func (s *databaseResetComponentStore) IsExistByCondition(_ context.Context, _ string, conditions map[string]interface{}, _ interface{}) (bool, error) {
	for _, component := range s.components {
		if jobtest.ComponentMatchesConditions(component, conditions) {
			return true, nil
		}
	}
	return false, nil
}

func databaseResetStatefulSet(t *testing.T, component *model.ApplicationComponent) (*jobexec.GenerateServiceResult, *appsv1.StatefulSet) {
	t.Helper()
	result, err := jobdeploy.GenerateStoreService(component)
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, result)
	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	require.True(t, ok)
	statefulSet.Status.Replicas = 1
	statefulSet.Status.ReadyReplicas = 1
	return result, statefulSet
}

func databaseResetDeployment(t *testing.T, component *model.ApplicationComponent) *appsv1.Deployment {
	t.Helper()
	properties, err := jobexec.ParseProperties(component.Properties)
	if err != nil {
		t.Fatal(err)
	}
	result, err := jobdeploy.GenerateWebService(component, &properties)
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, result)
	deployment, ok := result.Service.(*appsv1.Deployment)
	require.True(t, ok)
	return deployment
}
