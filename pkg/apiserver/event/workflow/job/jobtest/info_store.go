package jobtest

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type JobInfoStore struct {
	AddCount  int
	AddErr    error
	LastAdded datastore.Entity
}

func (s *JobInfoStore) Add(_ context.Context, entity datastore.Entity) error {
	s.AddCount++
	s.LastAdded = entity
	return s.AddErr
}

func (s *JobInfoStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *JobInfoStore) Put(context.Context, datastore.Entity) error { return nil }

func (s *JobInfoStore) Delete(context.Context, datastore.Entity) error { return nil }

func (s *JobInfoStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *JobInfoStore) Get(context.Context, datastore.Entity) error { return nil }

func (s *JobInfoStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, nil
}

func (s *JobInfoStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *JobInfoStore) IsExist(context.Context, datastore.Entity) (bool, error) { return false, nil }

func (s *JobInfoStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *JobInfoStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

var _ datastore.DataStore = (*JobInfoStore)(nil)
