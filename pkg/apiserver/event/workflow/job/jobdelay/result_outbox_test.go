package jobdelay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type resultOutboxTestStore struct {
	mu       sync.Mutex
	outboxes map[string]*model.JobResultOutbox
	jobInfos map[int]*model.JobInfo
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

func seedCommittedDelayTestCheckpoint(t *testing.T, store *resultOutboxTestStore, payload *jobinfo.DelayJobPayload) {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	store.mu.Lock()
	defer store.mu.Unlock()
	var record *model.JobInfo
	for _, existing := range store.jobInfos {
		if jobinfo.JobInfoExecutionKey(*existing) == payload.ExecutionKey {
			record = existing
			break
		}
	}
	if record == nil {
		id := len(store.jobInfos) + 1
		for store.jobInfos[id] != nil {
			id++
		}
		key := payload.ExecutionKey
		record = &model.JobInfo{ID: id, TaskID: payload.TaskID, ExecutionKey: &key, RunGeneration: payload.RunGeneration, Type: payload.JobType, ServiceName: payload.ServiceName, Status: string(config.StatusDistributed)}
		store.jobInfos[id] = record
	}
	if record.WorkspaceID == "" {
		record.WorkspaceID = "test-workspace"
	}
	record.DelayState = config.JobDelayStatePending
	record.DelayExecuteAt = payload.ExecuteAt
	record.DelayPayload = string(raw)
}

type delayFencingStore struct {
	*resultOutboxTestStore
	task   *model.WorkflowQueue
	getErr error
}

type settlingDelayStore struct {
	*delayFencingStore
	settleOnce sync.Once
	jobInfoID  int
	outboxID   string
}

func (s *settlingDelayStore) Get(ctx context.Context, entity datastore.Entity) error {
	err := s.delayFencingStore.Get(ctx, entity)
	if err == nil {
		if _, ok := entity.(*model.JobResultOutbox); ok {
			s.settleResult()
		}
	}
	return err
}

func (s *settlingDelayStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	entities, err := s.delayFencingStore.List(ctx, query, opts)
	if err == nil && len(entities) > 0 {
		if _, ok := query.(*model.JobInfo); ok {
			s.settleResult()
		}
	}
	return entities, err
}

func (s *settlingDelayStore) settleResult() {
	s.settleOnce.Do(func() {
		s.resultOutboxTestStore.mu.Lock()
		defer s.resultOutboxTestStore.mu.Unlock()
		if jobInfo := s.resultOutboxTestStore.jobInfos[s.jobInfoID]; jobInfo != nil {
			jobInfo.Status = string(config.StatusCompleted)
		}
		delete(s.resultOutboxTestStore.outboxes, s.outboxID)
	})
}

func (s *delayFencingStore) Get(ctx context.Context, entity datastore.Entity) error {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return s.resultOutboxTestStore.Get(ctx, entity)
	}
	if s.getErr != nil {
		return s.getErr
	}
	if s.task == nil || task.TaskID != s.task.TaskID {
		return datastore.ErrRecordNotExist
	}
	*task = *s.task
	return nil
}

func newResultOutboxTestStore() *resultOutboxTestStore {
	return &resultOutboxTestStore{
		outboxes: make(map[string]*model.JobResultOutbox),
		jobInfos: make(map[int]*model.JobInfo),
	}
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

func TestDelayDispatcherDropsStaleWorkflowOwnership(t *testing.T) {
	tests := []struct {
		name          string
		task          *model.WorkflowQueue
		runGeneration uint64
		runToken      string
	}{
		{
			name:          "generation changed",
			task:          &model.WorkflowQueue{TaskID: "task-delay-fenced", RunGeneration: 2, RunToken: "run-2"},
			runGeneration: 1,
			runToken:      "run-1",
		},
		{
			name:          "token cleared by reaper",
			task:          &model.WorkflowQueue{TaskID: "task-delay-fenced", RunGeneration: 2},
			runGeneration: 2,
			runToken:      "run-2",
		},
		{
			name:          "legacy tokenless message after reaper",
			task:          &model.WorkflowQueue{TaskID: "task-delay-fenced", RunGeneration: 2},
			runGeneration: 2,
		},
		{
			name:          "task deleted",
			runGeneration: 2,
			runToken:      "run-2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &delayFencingStore{resultOutboxTestStore: newResultOutboxTestStore(), task: tc.task}
			client := fake.NewSimpleClientset()
			dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
			payload := &jobinfo.DelayJobPayload{
				TaskID:        "task-delay-fenced",
				JobType:       string(config.JobDeployScheduled),
				Namespace:     "default",
				ExecutionKey:  "execution-stale",
				RunGeneration: tc.runGeneration,
				RunToken:      tc.runToken,
				ServiceName:   "svc-a",
				Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name:      "delay-job-fenced",
					Namespace: "default",
				}},
			}

			require.NoError(t, dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client))
			require.Empty(t, client.Actions(), "stale delayed ownership must be rejected before Kubernetes access")
			require.Empty(t, store.outboxes)
		})
	}
}

func TestDelayDispatcherPreservesCommittedDelayedJobAcrossWorkflowGeneration(t *testing.T) {
	executionKey := "execution-committed"
	store := &delayFencingStore{
		resultOutboxTestStore: newResultOutboxTestStore(),
		task:                  &model.WorkflowQueue{TaskID: "task-delay-committed", RunGeneration: 2, RunToken: "run-2"},
	}
	require.NoError(t, store.Add(context.Background(), &model.JobInfo{
		ID:            1,
		TaskID:        "task-delay-committed",
		Type:          string(config.JobDeployInstant),
		ServiceName:   "svc-a",
		Status:        string(config.StatusDistributed),
		ExecutionKey:  &executionKey,
		RunGeneration: 1,
	}))
	client := fake.NewSimpleClientset()
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
	payload := &jobinfo.DelayJobPayload{
		TaskID:        "task-delay-committed",
		JobType:       string(config.JobDeployInstant),
		Namespace:     "default",
		ExecutionKey:  executionKey,
		RunGeneration: 1,
		RunToken:      "run-1",
		ServiceName:   "svc-a",
		Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-committed",
			Namespace: "default",
			Annotations: map[string]string{
				config.AnnotationJobExecutionKey:  executionKey,
				config.AnnotationJobRunGeneration: "1",
			},
		}},
	}

	seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, payload)
	require.NoError(t, dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client))
	_, err := client.BatchV1().Jobs("default").Get(context.Background(), "delay-job-committed", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, store.outboxes, 1)
}

func TestDelayDispatcherDoesNotRecreateSettledDelayedExecution(t *testing.T) {
	for _, status := range []config.Status{
		config.StatusCompleted,
		config.StatusPassed,
		config.StatusSkipped,
		config.StatusFailed,
		config.StatusTimeout,
		config.StatusCancelled,
		config.StatusReject,
	} {
		t.Run(string(status), func(t *testing.T) {
			executionKey := "execution-settled"
			store := &delayFencingStore{
				resultOutboxTestStore: newResultOutboxTestStore(),
				task:                  &model.WorkflowQueue{TaskID: "task-delay-settled", RunGeneration: 2, RunToken: "run-2"},
			}
			require.NoError(t, store.Add(context.Background(), &model.JobInfo{
				ID:            1,
				TaskID:        "task-delay-settled",
				Type:          string(config.JobDeployScheduled),
				ServiceName:   "svc-a",
				Status:        string(status),
				ExecutionKey:  &executionKey,
				RunGeneration: 2,
			}))
			client := fake.NewSimpleClientset()
			dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
			payload := &jobinfo.DelayJobPayload{
				TaskID:        "task-delay-settled",
				JobType:       string(config.JobDeployScheduled),
				Namespace:     "default",
				ExecutionKey:  executionKey,
				RunGeneration: 2,
				RunToken:      "run-2",
				ServiceName:   "svc-a",
				Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name:      "delay-job-settled",
					Namespace: "default",
					Annotations: map[string]string{
						config.AnnotationJobExecutionKey:  executionKey,
						config.AnnotationJobRunGeneration: "2",
						config.AnnotationJobRunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
					},
				}},
			}

			require.NoError(t, dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client))
			require.Empty(t, client.Actions(), "a settled delayed execution must not access or recreate its Kubernetes Job")
			require.Empty(t, store.outboxes, "a settled delayed execution must not recreate its result outbox")
		})
	}
}

func TestDelayDispatcherAcceptsCurrentGenerationWithCompatibleToken(t *testing.T) {
	for _, runToken := range []string{"run-2", ""} {
		t.Run(fmt.Sprintf("token-%q", runToken), func(t *testing.T) {
			store := &delayFencingStore{
				resultOutboxTestStore: newResultOutboxTestStore(),
				task:                  &model.WorkflowQueue{TaskID: "task-delay-current", RunGeneration: 2, RunToken: "run-2"},
			}
			client := fake.NewSimpleClientset()
			dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
			payload := &jobinfo.DelayJobPayload{
				TaskID:        "task-delay-current",
				JobType:       string(config.JobDeployScheduled),
				Namespace:     "default",
				ExecutionKey:  "execution-current",
				RunGeneration: 2,
				RunToken:      runToken,
				ServiceName:   "svc-a",
				Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name:      "delay-job-current",
					Namespace: "default",
					Annotations: map[string]string{
						config.AnnotationJobExecutionKey:  "execution-current",
						config.AnnotationJobRunGeneration: "2",
					},
				}},
			}

			seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, payload)
			require.NoError(t, dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client))
			_, err := client.BatchV1().Jobs("default").Get(context.Background(), "delay-job-current", metav1.GetOptions{})
			require.NoError(t, err)
			require.Len(t, store.outboxes, 1)
		})
	}
}

func TestDelayDispatcherDoesNotBindOutboxToDifferentJobExecution(t *testing.T) {
	for _, policy := range []workflowconfig.JobRunPolicy{
		workflowconfig.JobRunPolicySkipIfCompleted,
		workflowconfig.JobRunPolicyRecreate,
	} {
		t.Run(string(policy), func(t *testing.T) {
			executionKey := "execution-current"
			resultStore := newResultOutboxTestStore()
			resultStore.jobInfos[1] = &model.JobInfo{
				ID:            1,
				TaskID:        "task-delay-current",
				Type:          string(config.JobDeployScheduled),
				ServiceName:   "svc-a",
				Status:        string(config.StatusDistributed),
				ExecutionKey:  &executionKey,
				RunGeneration: 2,
			}
			store := &delayFencingStore{
				resultOutboxTestStore: resultStore,
				task:                  &model.WorkflowQueue{TaskID: "task-delay-current", RunGeneration: 2, RunToken: "run-2"},
			}
			existing := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name:      "delay-job-shared",
				Namespace: "default",
				Annotations: map[string]string{
					config.AnnotationJobExecutionKey:  "execution-old",
					config.AnnotationJobRunGeneration: "1",
				},
			}}
			client := fake.NewSimpleClientset(existing)
			dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
			payload := &jobinfo.DelayJobPayload{
				TaskID:        "task-delay-current",
				JobType:       string(config.JobDeployScheduled),
				Namespace:     "default",
				ExecutionKey:  executionKey,
				RunGeneration: 2,
				RunToken:      "run-2",
				ServiceName:   "svc-a",
				Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name:      existing.Name,
					Namespace: existing.Namespace,
					Annotations: map[string]string{
						config.AnnotationJobExecutionKey:  executionKey,
						config.AnnotationJobRunGeneration: "2",
						config.AnnotationJobRunPolicy:     string(policy),
					},
				}},
			}

			seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, payload)
			err := dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client)
			require.ErrorContains(t, err, "belongs to another execution")
			require.ErrorIs(t, err, errDelayDispatchNoRetry)
			require.Empty(t, store.outboxes)
			require.Equal(t, string(config.StatusFailed), store.jobInfos[1].Status)

			for _, action := range client.Actions() {
				require.NotContains(t, []string{"create", "delete"}, action.GetVerb(), "a mismatched existing Job must not be mutated or rebound")
			}
		})
	}
}

func TestDelayDispatcherDoesNotRecreateWhenResultSettlesBetweenReads(t *testing.T) {
	executionKey := "execution-settles-between-reads"
	resultStore := newResultOutboxTestStore()
	resultStore.jobInfos[1] = &model.JobInfo{
		ID:            1,
		TaskID:        "task-delay-settles-between-reads",
		Type:          string(config.JobDeployScheduled),
		ServiceName:   "svc-a",
		Status:        string(config.StatusDistributed),
		ExecutionKey:  &executionKey,
		RunGeneration: 2,
	}
	payload := &jobinfo.DelayJobPayload{
		TaskID:        "task-delay-settles-between-reads",
		JobType:       string(config.JobDeployScheduled),
		Namespace:     "default",
		ExecutionKey:  executionKey,
		RunGeneration: 2,
		RunToken:      "run-2",
		ServiceName:   "svc-a",
		Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-settles-between-reads",
			Namespace: "default",
			Annotations: map[string]string{
				config.AnnotationJobExecutionKey:  executionKey,
				config.AnnotationJobRunGeneration: "2",
				config.AnnotationJobRunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
			},
		}},
	}
	resultPayload := jobresult.NewJobResultPayloadFromDelay(payload, payload.Job)
	outbox := jobresult.BuildJobResultOutbox(resultPayload, config.JobResultOutboxStateResultPending)
	require.NoError(t, resultStore.Add(context.Background(), outbox))
	store := &settlingDelayStore{
		delayFencingStore: &delayFencingStore{
			resultOutboxTestStore: resultStore,
			task:                  &model.WorkflowQueue{TaskID: payload.TaskID, RunGeneration: 2, RunToken: "run-2"},
		},
		jobInfoID: 1,
		outboxID:  outbox.ID,
	}
	client := fake.NewSimpleClientset(payload.Job.DeepCopy())
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")

	seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, payload)
	require.NoError(t, dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client))
	require.Empty(t, client.Actions(), "a result that settles concurrently must prevent Kubernetes recreation")
	require.Equal(t, string(config.StatusCompleted), store.jobInfos[1].Status)
	require.Empty(t, store.outboxes)
}

func TestDelayDispatcherRetriesOwnershipLookupFailure(t *testing.T) {
	store := &delayFencingStore{
		resultOutboxTestStore: newResultOutboxTestStore(),
		getErr:                errors.New("database unavailable"),
	}
	client := fake.NewSimpleClientset()
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")
	payload := &jobinfo.DelayJobPayload{
		TaskID:        "task-delay-current",
		JobType:       string(config.JobDeployScheduled),
		Namespace:     "default",
		ExecutionKey:  "execution-current",
		RunGeneration: 2,
		RunToken:      "run-2",
		Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-current",
			Namespace: "default",
		}},
	}

	err := dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client)
	require.ErrorContains(t, err, "load workflow task for delayed job")
	require.Empty(t, client.Actions(), "a failed ownership lookup must retry before Kubernetes access")
	require.Empty(t, store.outboxes)
}

func TestDelayDispatcherDispatchPersistsResultOutboxWithoutQueueDependency(t *testing.T) {
	store := &delayFencingStore{
		resultOutboxTestStore: newResultOutboxTestStore(),
		task:                  &model.WorkflowQueue{TaskID: "task-delay-1", RunGeneration: 1, RunToken: "run-1"},
	}
	client := fake.NewSimpleClientset()
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-1", Namespace: "default"}}
	item := &delayItem{
		payload: &jobinfo.DelayJobPayload{
			TaskID:         "task-delay-1",
			ExecutionKey:   "execution-delay-1",
			RunGeneration:  1,
			JobType:        string(config.JobDeployScheduled),
			Namespace:      "default",
			ServiceName:    "svc-a",
			TimeoutSeconds: 60,
			Job:            jobObj,
		},
	}

	seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, item.payload)
	require.NoError(t, dispatcher.dispatchJob(context.Background(), item, client))

	resultPayload := jobresult.NewJobResultPayloadFromDelay(item.payload, jobObj)
	outbox, err := jobresult.GetJobResultOutboxByPayload(context.Background(), store, resultPayload)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, outbox.State)

	createdJob, err := client.BatchV1().Jobs("default").Get(context.Background(), "delay-job-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "delay-job-1", createdJob.Name)
}

func TestDelayDispatcherRecoversDueCheckpointWithoutQueue(t *testing.T) {
	store := newResultOutboxTestStore()
	client := fake.NewSimpleClientset()
	executionKey := "execution-delay-recovery"
	payload := &jobinfo.DelayJobPayload{
		ExecuteAt:     time.Now().Add(-time.Minute).Unix(),
		Namespace:     "default",
		JobType:       string(config.JobDeployInstant),
		TaskID:        "task-delay-recovery",
		ExecutionKey:  executionKey,
		RunGeneration: 4,
		ServiceName:   "delay-job-recovery",
		Job: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name:      "delay-job-recovery",
			Namespace: "default",
		}},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	store.jobInfos[1] = &model.JobInfo{
		ID:             1,
		Type:           payload.JobType,
		TaskID:         payload.TaskID,
		ServiceName:    payload.ServiceName,
		Status:         string(config.StatusDistributed),
		ExecutionKey:   &executionKey,
		RunGeneration:  payload.RunGeneration,
		DelayState:     config.JobDelayStatePending,
		DelayExecuteAt: payload.ExecuteAt,
		DelayPayload:   string(raw),
	}
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")

	require.NoError(t, dispatcher.recoverDueCheckpoints(context.Background()))
	item, wait := dispatcher.nextItem()
	require.NotNil(t, item)
	require.Zero(t, wait)
	seedCommittedDelayTestCheckpoint(t, store, item.payload)
	require.NoError(t, dispatcher.dispatchJob(context.Background(), item, client))
	dispatcher.finish(context.Background(), item)

	created, err := client.BatchV1().Jobs("default").Get(context.Background(), payload.Job.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, executionKey, created.Annotations[config.AnnotationJobExecutionKey])
	require.Equal(t, config.JobDelayStateDispatched, store.jobInfos[1].DelayState)
	require.Len(t, store.outboxes, 1)
}

func TestDelayDispatcherDispatchDoesNotPersistOutboxBeforeJobExists(t *testing.T) {
	store := &delayFencingStore{
		resultOutboxTestStore: newResultOutboxTestStore(),
		task:                  &model.WorkflowQueue{TaskID: "task-delay-create-fail", RunGeneration: 1, RunToken: "run-1"},
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("create failed before job persisted")
	})
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-create-fail", Namespace: "default"}}
	item := &delayItem{
		payload: &jobinfo.DelayJobPayload{
			TaskID:         "task-delay-create-fail",
			ExecutionKey:   "execution-delay-create-fail",
			RunGeneration:  1,
			JobType:        string(config.JobDeployScheduled),
			Namespace:      "default",
			ServiceName:    "svc-a",
			TimeoutSeconds: 60,
			Job:            jobObj,
		},
	}

	seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, item.payload)
	err := dispatcher.dispatchJob(context.Background(), item, client)
	require.EqualError(t, err, "create failed before job persisted")

	resultPayload := jobresult.NewJobResultPayloadFromDelay(item.payload, jobObj)
	_, getErr := jobresult.GetJobResultOutboxByPayload(context.Background(), store, resultPayload)
	require.ErrorIs(t, getErr, datastore.ErrRecordNotExist)
}

func TestDelayDispatcherDispatchPersistsOutboxWhenCreateErrorLeavesJobPresent(t *testing.T) {
	store := &delayFencingStore{
		resultOutboxTestStore: newResultOutboxTestStore(),
		task:                  &model.WorkflowQueue{TaskID: "task-delay-create-visible", RunGeneration: 2, RunToken: "run-2"},
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		jobObj, ok := createAction.GetObject().(*batchv1.Job)
		if !ok {
			return false, nil, nil
		}
		_ = client.Tracker().Add(jobObj.DeepCopy())
		return true, jobObj, k8serrors.NewInternalError(fmt.Errorf("create returned transient error after persisting"))
	})
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "delay-job-create-visible",
		Namespace: "default",
		Annotations: map[string]string{
			config.AnnotationJobExecutionKey:  "execution-current",
			config.AnnotationJobRunGeneration: "2",
		},
	}}
	item := &delayItem{
		payload: &jobinfo.DelayJobPayload{
			TaskID:         "task-delay-create-visible",
			JobType:        string(config.JobDeployScheduled),
			ExecutionKey:   "execution-current",
			RunGeneration:  2,
			RunToken:       "run-2",
			Namespace:      "default",
			ServiceName:    "svc-a",
			TimeoutSeconds: 60,
			Job:            jobObj,
		},
	}

	seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, item.payload)
	require.NoError(t, dispatcher.dispatchJob(context.Background(), item, client))

	resultPayload := jobresult.NewJobResultPayloadFromDelay(item.payload, jobObj)
	outbox, getErr := jobresult.GetJobResultOutboxByPayload(context.Background(), store, resultPayload)
	require.NoError(t, getErr)
	require.Equal(t, config.JobResultOutboxStateResultPending, outbox.State)
}

func TestDelayDispatcherDispatchRejectsDifferentJobAfterCreateError(t *testing.T) {
	store := &delayFencingStore{
		resultOutboxTestStore: newResultOutboxTestStore(),
		task:                  &model.WorkflowQueue{TaskID: "task-delay-create-conflict", RunGeneration: 1, RunToken: "run-1"},
	}
	client := fake.NewSimpleClientset()
	client.Fake.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		desired, ok := createAction.GetObject().(*batchv1.Job)
		if !ok {
			return false, nil, nil
		}
		conflicting := desired.DeepCopy()
		conflicting.Annotations[config.AnnotationJobTaskID] = "foreign-task"
		conflicting.Annotations[config.AnnotationJobExecutionKey] = "foreign-execution"
		require.NoError(t, client.Tracker().Add(conflicting))
		return true, desired, k8serrors.NewInternalError(fmt.Errorf("create response lost"))
	})
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-create-conflict", Namespace: "default"}}
	payload := &jobinfo.DelayJobPayload{
		TaskID:         "task-delay-create-conflict",
		ExecutionKey:   "execution-delay-create-conflict",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
		Job:            jobObj,
	}
	resultPayload := jobresult.NewJobResultPayloadFromDelay(payload, jobObj)
	jobInfo := testResultJobInfo(4, resultPayload)
	jobInfo.Status = string(config.StatusDistributed)
	require.NoError(t, store.Add(context.Background(), jobInfo))

	seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, payload)
	err := dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client)
	require.ErrorIs(t, err, errDelayDispatchNoRetry)
	require.ErrorContains(t, err, "foreign-task")

	_, getErr := jobresult.GetJobResultOutboxByPayload(context.Background(), store, resultPayload)
	require.ErrorIs(t, getErr, datastore.ErrRecordNotExist)
	storedJobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, storedJobInfo)
	require.Equal(t, string(config.StatusFailed), storedJobInfo.Status)
	require.Contains(t, storedJobInfo.Error, "foreign-task")
}

func TestDelayDispatcherDispatchDoesNotRecreateWhenResultOutboxPendingAndJobMissing(t *testing.T) {
	store := &delayFencingStore{
		resultOutboxTestStore: newResultOutboxTestStore(),
		task:                  &model.WorkflowQueue{TaskID: "task-delay-2", RunGeneration: 1, RunToken: "run-1"},
	}
	client := fake.NewSimpleClientset()
	dispatcher := NewDelayDispatcher(nil, &workspace.Manager{Client: client, RESTConfig: &rest.Config{}}, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-2", Namespace: "default"}}
	payload := &jobinfo.DelayJobPayload{
		TaskID:         "task-delay-2",
		ExecutionKey:   "execution-delay-2",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
		Job:            jobObj,
	}
	resultPayload := jobresult.NewJobResultPayloadFromDelay(payload, jobObj)
	require.NoError(t, store.Add(context.Background(), jobresult.BuildJobResultOutbox(resultPayload, config.JobResultOutboxStateResultPending)))

	seedCommittedDelayTestCheckpoint(t, store.resultOutboxTestStore, payload)
	err := dispatcher.dispatchJob(context.Background(), &delayItem{payload: payload}, client)
	require.NoError(t, err)
	require.Empty(t, client.Actions())
}
