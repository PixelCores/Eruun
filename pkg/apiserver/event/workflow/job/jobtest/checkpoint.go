package jobtest

import (
	"context"
	"sort"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type CloudJobCheckpointStore struct {
	records map[int]*model.JobInfo
	nextID  int
}

func NewCloudJobCheckpointStore() *CloudJobCheckpointStore {
	return &CloudJobCheckpointStore{
		records: map[int]*model.JobInfo{},
	}
}

func CloneJobInfo(info *model.JobInfo) *model.JobInfo {
	if info == nil {
		return nil
	}
	clone := *info
	return &clone
}

func matchesInFilters(opts *datastore.ListOptions, key, value string) bool {
	if opts == nil {
		return true
	}
	for _, item := range opts.FilterOptions.In {
		if item.Key != key {
			continue
		}
		for _, candidate := range item.Values {
			if candidate == value {
				return true
			}
		}
		return false
	}
	return true
}

func (s *CloudJobCheckpointStore) Add(_ context.Context, entity datastore.Entity) error {
	info, ok := entity.(*model.JobInfo)
	if !ok {
		return datastore.ErrEntityInvalid
	}
	s.nextID++
	clone := CloneJobInfo(info)
	clone.ID = s.nextID
	if clone.CreateTime.IsZero() {
		clone.CreateTime = time.Now()
	}
	s.records[clone.ID] = clone
	return nil
}

func (s *CloudJobCheckpointStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *CloudJobCheckpointStore) Put(_ context.Context, entity datastore.Entity) error {
	info, ok := entity.(*model.JobInfo)
	if !ok {
		return datastore.ErrEntityInvalid
	}
	clone := CloneJobInfo(info)
	if clone.ID == 0 {
		s.nextID++
		clone.ID = s.nextID
	}
	if clone.CreateTime.IsZero() {
		clone.CreateTime = time.Now()
	}
	clone.UpdateTime = time.Now()
	s.records[clone.ID] = clone
	return nil
}

func (s *CloudJobCheckpointStore) Delete(context.Context, datastore.Entity) error { return nil }
func (s *CloudJobCheckpointStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}
func (s *CloudJobCheckpointStore) Get(context.Context, datastore.Entity) error { return nil }

func (s *CloudJobCheckpointStore) List(_ context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	jobInfo, ok := query.(*model.JobInfo)
	if !ok {
		return nil, datastore.ErrEntityInvalid
	}
	out := make([]datastore.Entity, 0, len(s.records))
	for _, item := range s.records {
		if jobInfo.TaskID != "" && item.TaskID != jobInfo.TaskID {
			continue
		}
		if !matchesInFilters(opts, "type", item.Type) {
			continue
		}
		if !matchesInFilters(opts, "service_name", item.ServiceName) {
			continue
		}
		out = append(out, CloneJobInfo(item))
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i].(*model.JobInfo)
		right := out[j].(*model.JobInfo)
		return left.CreateTime.After(right.CreateTime)
	})
	if opts != nil && opts.PageSize > 0 && len(out) > opts.PageSize {
		out = out[:opts.PageSize]
	}
	return out, nil
}

func (s *CloudJobCheckpointStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return int64(len(s.records)), nil
}

func (s *CloudJobCheckpointStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}
func (s *CloudJobCheckpointStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}
func (s *CloudJobCheckpointStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func (s *CloudJobCheckpointStore) Records() map[int]*model.JobInfo {
	return s.records
}
