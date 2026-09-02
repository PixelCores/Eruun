package repository

import (
	"context"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type repositoryTestStore struct {
	addErr    error
	putErr    error
	deleteErr error
	getErr    error
	listErr   error
	countErr  error

	addEntity    datastore.Entity
	putEntity    datastore.Entity
	deleteEntity datastore.Entity
	getEntity    datastore.Entity

	listEntities []datastore.Entity
	lastQuery    datastore.Entity
	lastListOpts *datastore.ListOptions

	lastDeleteByFilterEntity datastore.Entity
	lastDeleteByFilterOpts   *datastore.FilterOptions

	isExistValue bool
	isExistErr   error

	casSwapped    bool
	casErr        error
	casField      string
	casValue      interface{}
	casUpdates    map[string]interface{}
	casUpdateTime time.Time

	casWithConditionsSwapped    bool
	casWithConditionsErr        error
	casConditions               map[string]interface{}
	casWithConditionsUpdateTime time.Time

	isExistByCondValue bool
	isExistByCondErr   error
	isExistByCondTable string
	isExistByCondCond  map[string]interface{}
	isExistByCondDest  interface{}
}

func (s *repositoryTestStore) Add(_ context.Context, entity datastore.Entity) error {
	s.addEntity = entity
	return s.addErr
}

func (s *repositoryTestStore) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	for _, entity := range entities {
		if err := s.Add(ctx, entity); err != nil {
			return err
		}
	}
	return nil
}

func (s *repositoryTestStore) Put(_ context.Context, entity datastore.Entity) error {
	s.putEntity = entity
	return s.putErr
}

func (s *repositoryTestStore) Delete(_ context.Context, entity datastore.Entity) error {
	s.deleteEntity = entity
	return s.deleteErr
}

func (s *repositoryTestStore) DeleteByFilter(_ context.Context, entity datastore.Entity, options *datastore.FilterOptions) error {
	s.lastDeleteByFilterEntity = entity
	s.lastDeleteByFilterOpts = options
	return s.deleteErr
}

func (s *repositoryTestStore) Get(_ context.Context, entity datastore.Entity) error {
	s.getEntity = entity
	return s.getErr
}

func (s *repositoryTestStore) List(_ context.Context, query datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	s.lastQuery = query
	s.lastListOpts = options
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listEntities, nil
}

func (s *repositoryTestStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, s.countErr
}

func (s *repositoryTestStore) IsExist(_ context.Context, entity datastore.Entity) (bool, error) {
	s.getEntity = entity
	return s.isExistValue, s.isExistErr
}

func (s *repositoryTestStore) IsExistByCondition(_ context.Context, table string, cond map[string]interface{}, dest interface{}) (bool, error) {
	s.isExistByCondTable = table
	s.isExistByCondCond = cond
	s.isExistByCondDest = dest
	return s.isExistByCondValue, s.isExistByCondErr
}

func (s *repositoryTestStore) CompareAndSwap(_ context.Context, entity datastore.Entity, field string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	s.casField = field
	s.casValue = conditionValue
	s.casUpdates = updates
	if s.casSwapped && !s.casUpdateTime.IsZero() {
		entity.SetUpdateTime(s.casUpdateTime)
	}
	return s.casSwapped, s.casErr
}

func (s *repositoryTestStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	s.casConditions = conditions
	s.casUpdates = updates
	if s.casWithConditionsSwapped && !s.casWithConditionsUpdateTime.IsZero() {
		entity.SetUpdateTime(s.casWithConditionsUpdateTime)
	}
	return s.casWithConditionsSwapped, s.casWithConditionsErr
}

var _ datastore.DataStore = (*repositoryTestStore)(nil)
var _ datastore.ConditionalCompareAndSwap = (*repositoryTestStore)(nil)
