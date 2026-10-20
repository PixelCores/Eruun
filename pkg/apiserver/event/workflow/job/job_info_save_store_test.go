package job

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type jobInfoSaveStore struct {
	jobtest.NoopStore
	existing  []*model.JobInfo
	added     *model.JobInfo
	updated   *model.JobInfo
	addErr    error
	beforeCAS func()
}

func (s *jobInfoSaveStore) Add(_ context.Context, entity datastore.Entity) error {
	if s.addErr != nil {
		return s.addErr
	}
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return datastore.ErrEntityInvalid
	}
	copy := *jobInfo
	s.added = &copy
	s.existing = append(s.existing, &copy)
	return nil
}

func (s *jobInfoSaveStore) Put(_ context.Context, entity datastore.Entity) error {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return datastore.ErrEntityInvalid
	}
	copy := *jobInfo
	s.updated = &copy
	return nil
}

func (s *jobInfoSaveStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	entities := make([]datastore.Entity, 0, len(s.existing))
	for _, jobInfo := range s.existing {
		copy := *jobInfo
		entities = append(entities, &copy)
	}
	return entities, nil
}

func (s *jobInfoSaveStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return false, datastore.ErrEntityInvalid
	}
	if s.beforeCAS != nil {
		hook := s.beforeCAS
		s.beforeCAS = nil
		hook()
	}
	for _, current := range s.existing {
		if current == nil || current.ID != jobInfo.ID {
			continue
		}
		if current.Status != conditions["status"] ||
			current.InternalInfo != conditions["internal_info"] ||
			jobinfo.JobInfoExecutionKey(*current) != conditions["execution_key"] ||
			current.RunGeneration != conditions["run_generation"] ||
			current.Attempt != conditions["attempt"] {
			return false, nil
		}
		current.Status, _ = updates["status"].(string)
		current.Error, _ = updates["error"].(string)
		current.EndTime, _ = updates["end_time"].(int64)
		current.InternalInfo, _ = updates["internal_info"].(string)
		copy := *current
		s.updated = &copy
		return true, nil
	}
	return false, nil
}
