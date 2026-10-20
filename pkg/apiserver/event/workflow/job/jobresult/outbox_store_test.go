package jobresult

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type resultOutboxTestStore struct {
	mu                sync.Mutex
	outboxes          map[string]*model.JobResultOutbox
	jobInfos          map[int]*model.JobInfo
	rejectTransitions map[string]int
}

// These result/dispatcher tests assume available scheduler capacity. Admission
// policy and transaction isolation are exercised against SQL in repository tests.
func (s *resultOutboxTestStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(s)
}

func (s *resultOutboxTestStore) CurrentDatabaseTime(ctx context.Context) (time.Time, error) {
	return time.Now().UTC(), ctx.Err()
}

func newResultOutboxTestStore() *resultOutboxTestStore {
	return &resultOutboxTestStore{
		outboxes:          make(map[string]*model.JobResultOutbox),
		jobInfos:          make(map[int]*model.JobInfo),
		rejectTransitions: make(map[string]int),
	}
}

func (s *resultOutboxTestStore) rejectNextTransition(from, to config.JobResultOutboxState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejectTransitions[jobtest.OutboxTransitionKey(from, to)]++
}

func (s *resultOutboxTestStore) Add(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch v := entity.(type) {
	case *model.JobResultOutbox:
		if _, exists := s.outboxes[v.ID]; exists {
			return datastore.ErrRecordExist
		}
		copy := *v
		now := time.Now()
		if copy.CreateTime.IsZero() {
			copy.CreateTime = now
		}
		if copy.UpdateTime.IsZero() {
			copy.UpdateTime = copy.CreateTime
		}
		s.outboxes[v.ID] = &copy
	case *model.JobInfo:
		if _, exists := s.jobInfos[v.ID]; exists {
			return datastore.ErrRecordExist
		}
		copy := *v
		now := time.Now()
		if copy.CreateTime.IsZero() {
			copy.CreateTime = now
		}
		if copy.UpdateTime.IsZero() {
			copy.UpdateTime = copy.CreateTime
		}
		s.jobInfos[v.ID] = &copy
	default:
		return nil
	}
	return nil
}

func (s *resultOutboxTestStore) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	for _, entity := range entities {
		if err := s.Add(ctx, entity); err != nil {
			return err
		}
	}
	return nil
}

func (s *resultOutboxTestStore) Put(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch v := entity.(type) {
	case *model.JobResultOutbox:
		if _, exists := s.outboxes[v.ID]; !exists {
			return datastore.ErrRecordNotExist
		}
		copy := *v
		copy.UpdateTime = time.Now()
		s.outboxes[v.ID] = &copy
	case *model.JobInfo:
		if _, exists := s.jobInfos[v.ID]; !exists {
			return datastore.ErrRecordNotExist
		}
		copy := *v
		copy.UpdateTime = time.Now()
		s.jobInfos[v.ID] = &copy
	default:
		return nil
	}
	return nil
}

func (s *resultOutboxTestStore) Delete(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch v := entity.(type) {
	case *model.JobResultOutbox:
		if _, exists := s.outboxes[v.ID]; !exists {
			return datastore.ErrRecordNotExist
		}
		delete(s.outboxes, v.ID)
	case *model.JobInfo:
		if _, exists := s.jobInfos[v.ID]; !exists {
			return datastore.ErrRecordNotExist
		}
		delete(s.jobInfos, v.ID)
	default:
		return nil
	}
	return nil
}

func (*resultOutboxTestStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *resultOutboxTestStore) Get(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch v := entity.(type) {
	case *model.JobResultOutbox:
		stored, exists := s.outboxes[v.ID]
		if !exists {
			return datastore.ErrRecordNotExist
		}
		*v = *stored
		return nil
	case *model.JobInfo:
		stored, exists := s.jobInfos[v.ID]
		if !exists {
			return datastore.ErrRecordNotExist
		}
		*v = *stored
		return nil
	default:
		return datastore.ErrEntityInvalid
	}
}

func (s *resultOutboxTestStore) List(_ context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch q := query.(type) {
	case *model.JobResultOutbox:
		outboxes := make([]*model.JobResultOutbox, 0, len(s.outboxes))
		for _, outbox := range s.outboxes {
			if q.ID != "" && outbox.ID != q.ID {
				continue
			}
			if q.TaskID != "" && outbox.TaskID != q.TaskID {
				continue
			}
			if q.State != "" && outbox.State != q.State {
				continue
			}
			if !jobtest.MatchOutboxFilters(outbox, opts) {
				continue
			}
			copy := *outbox
			outboxes = append(outboxes, &copy)
		}
		sort.Slice(outboxes, func(i, j int) bool {
			if opts != nil && len(opts.SortBy) > 0 && opts.SortBy[0].Key == "lease_expires_at" {
				a, b := outboxes[i].LeaseExpiresAt, outboxes[j].LeaseExpiresAt
				if a == nil && b != nil {
					return true
				}
				if a != nil && b == nil {
					return false
				}
				if a != nil && b != nil && !a.Equal(*b) {
					return a.Before(*b)
				}
			}
			if !outboxes[i].UpdateTime.Equal(outboxes[j].UpdateTime) {
				return outboxes[i].UpdateTime.Before(outboxes[j].UpdateTime)
			}
			return outboxes[i].CreateTime.Before(outboxes[j].CreateTime)
		})
		return jobtest.PaginateOutboxes(outboxes, opts), nil
	case *model.JobInfo:
		jobInfos := make([]*model.JobInfo, 0, len(s.jobInfos))
		for _, jobInfo := range s.jobInfos {
			if q.TaskID != "" && jobInfo.TaskID != q.TaskID {
				continue
			}
			if !jobtest.MatchJobInfoFilters(jobInfo, opts) {
				continue
			}
			copy := *jobInfo
			jobInfos = append(jobInfos, &copy)
		}
		sort.Slice(jobInfos, func(i, j int) bool {
			if opts != nil && len(opts.SortBy) > 0 {
				switch opts.SortBy[0].Key {
				case "id":
					return jobInfos[i].ID > jobInfos[j].ID
				case "delay_execute_at":
					if jobInfos[i].DelayExecuteAt != jobInfos[j].DelayExecuteAt {
						return jobInfos[i].DelayExecuteAt < jobInfos[j].DelayExecuteAt
					}
				}
			}
			return jobInfos[i].CreateTime.After(jobInfos[j].CreateTime)
		})
		return jobtest.PaginateJobInfos(jobInfos, opts), nil
	default:
		return nil, datastore.ErrEntityInvalid
	}
}

func (*resultOutboxTestStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (*resultOutboxTestStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (*resultOutboxTestStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *resultOutboxTestStore) CompareAndSwap(ctx context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	return s.CompareAndSwapWithConditions(ctx, entity, map[string]interface{}{
		conditionField: conditionValue,
	}, updates)
}

func (s *resultOutboxTestStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if jobInfo, ok := entity.(*model.JobInfo); ok {
		current, exists := s.jobInfos[jobInfo.ID]
		if !exists {
			return false, nil
		}
		if matched, err := resultOutboxJobInfoMatchesConditions(current, conditions); !matched || err != nil {
			return matched, err
		}
		if state, ok := updates["delay_state"].(string); ok {
			current.DelayState = config.JobDelayState(state)
		} else if state, ok := updates["delay_state"].(config.JobDelayState); ok {
			current.DelayState = state
		}
		if state, ok := updates["scheduling_state"].(string); ok {
			current.SchedulingState = state
			if state == workflowconfig.JobSchedulingQueued {
				current.SchedulingState = workflowconfig.JobSchedulingAdmitted
			}
		}
		if value, ok := updates["scheduling_class"].(string); ok {
			current.SchedulingClass = value
		}
		if value, ok := updates["scheduling_priority"].(int); ok {
			current.SchedulingPriority = value
		}
		if value, ok := updates["scheduling_generation"].(uint64); ok {
			current.SchedulingGeneration = value
		}
		if value, ok := updates["scheduling_owner_status"].(config.Status); ok {
			current.SchedulingOwnerStatus = value
		}
		if value, ok := updates["scheduling_queued_at"].(*time.Time); ok {
			current.SchedulingQueuedAt = value
		}
		if value, ok := updates["scheduling_expires_at"].(*time.Time); ok {
			current.SchedulingExpiresAt = value
		}
		if value, ok := updates["scheduling_reason"].(string); ok {
			current.SchedulingReason = value
		}
		if status, ok := updates["status"].(string); ok {
			current.Status = status
		}
		if message, ok := updates["error"].(string); ok {
			current.Error = message
		}
		if endTime, ok := updates["end_time"].(int64); ok {
			current.EndTime = endTime
		}
		if startTime, ok := updates["start_time"].(int64); ok {
			current.StartTime = startTime
		}
		if info, ok := updates["info"].(string); ok {
			current.Info = info
		}
		current.UpdateTime = time.Now()
		return true, nil
	}

	outbox, ok := entity.(*model.JobResultOutbox)
	if !ok {
		return false, datastore.ErrEntityInvalid
	}
	current, exists := s.outboxes[outbox.ID]
	if !exists {
		return false, nil
	}
	if strings.TrimSpace(fmt.Sprint(conditions["id"])) != current.ID {
		return false, nil
	}
	for field, value := range conditions {
		switch field {
		case "id":
			if strings.TrimSpace(current.ID) != strings.TrimSpace(fmt.Sprint(value)) {
				return false, nil
			}
		case "state":
			if string(current.State) != strings.TrimSpace(fmt.Sprint(value)) {
				return false, nil
			}
		case "message_id":
			if strings.TrimSpace(current.MessageID) != strings.TrimSpace(fmt.Sprint(value)) {
				return false, nil
			}
		case "lease_expires_at":
			expected, _ := value.(*time.Time)
			if (current.LeaseExpiresAt == nil) != (expected == nil) || (expected != nil && !current.LeaseExpiresAt.Equal(*expected)) {
				return false, nil
			}
		default:
			return false, datastore.ErrEntityInvalid
		}
	}
	toState, _ := updates["state"].(config.JobResultOutboxState)
	if count := s.rejectTransitions[jobtest.OutboxTransitionKey(current.State, toState)]; count > 0 {
		s.rejectTransitions[jobtest.OutboxTransitionKey(current.State, toState)] = count - 1
		return false, nil
	}
	jobtest.ApplyOutboxUpdates(current, updates)
	current.UpdateTime = time.Now()
	return true, nil
}

func resultOutboxJobInfoMatchesConditions(current *model.JobInfo, conditions map[string]interface{}) (bool, error) {
	for field, value := range conditions {
		switch field {
		case "attempt":
			if fmt.Sprint(current.Attempt) != fmt.Sprint(value) {
				return false, nil
			}
		case "status":
			if current.Status != fmt.Sprint(value) {
				return false, nil
			}
		case "app_id":
			if current.AppID != fmt.Sprint(value) {
				return false, nil
			}
		case "execution_key":
			key := fmt.Sprint(value)
			if ptr, ok := value.(*string); ok && ptr != nil {
				key = *ptr
			}
			if jobinfo.JobInfoExecutionKey(*current) != key {
				return false, nil
			}
		case "run_generation":
			if fmt.Sprint(current.RunGeneration) != fmt.Sprint(value) {
				return false, nil
			}
		case "delay_state":
			if string(current.DelayState) != strings.TrimSpace(fmt.Sprint(value)) {
				return false, nil
			}
		case "scheduling_state":
			if current.SchedulingState != fmt.Sprint(value) {
				return false, nil
			}
		case "scheduling_generation":
			if fmt.Sprint(current.SchedulingGeneration) != fmt.Sprint(value) {
				return false, nil
			}
		case "scheduling_owner_status":
			if string(current.SchedulingOwnerStatus) != fmt.Sprint(value) {
				return false, nil
			}
		case "scheduling_expires_at":
			if value == nil {
				if current.SchedulingExpiresAt != nil {
					return false, nil
				}
			} else {
				deadline, ok := value.(time.Time)
				if !ok || current.SchedulingExpiresAt == nil || !current.SchedulingExpiresAt.Equal(deadline) {
					return false, nil
				}
			}
		default:
			return false, datastore.ErrEntityInvalid
		}
	}
	return true, nil
}

func (s *resultOutboxTestStore) jobInfoByTaskID(taskID string) *model.JobInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, jobInfo := range s.jobInfos {
		if jobInfo.TaskID != taskID {
			continue
		}
		copy := *jobInfo
		return &copy
	}
	return nil
}
