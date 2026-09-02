package job

import (
	"context"
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
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type resultOutboxTestStore struct {
	mu                sync.Mutex
	outboxes          map[string]*model.JobResultOutbox
	jobInfos          map[int]*model.JobInfo
	rejectTransitions map[string]int
}

type contextCheckingResultOutboxStore struct {
	*resultOutboxTestStore
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
		outboxes:          make(map[string]*model.JobResultOutbox),
		jobInfos:          make(map[int]*model.JobInfo),
		rejectTransitions: make(map[string]int),
	}
}

func ensureActiveTestContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func (s *contextCheckingResultOutboxStore) Add(ctx context.Context, entity datastore.Entity) error {
	if err := ensureActiveTestContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Add(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	if err := ensureActiveTestContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.BatchAdd(ctx, entities)
}

func (s *contextCheckingResultOutboxStore) Put(ctx context.Context, entity datastore.Entity) error {
	if err := ensureActiveTestContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Put(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) Delete(ctx context.Context, entity datastore.Entity) error {
	if err := ensureActiveTestContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Delete(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) Get(ctx context.Context, entity datastore.Entity) error {
	if err := ensureActiveTestContext(ctx); err != nil {
		return err
	}
	return s.resultOutboxTestStore.Get(ctx, entity)
}

func (s *contextCheckingResultOutboxStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	if err := ensureActiveTestContext(ctx); err != nil {
		return nil, err
	}
	return s.resultOutboxTestStore.List(ctx, query, opts)
}

func (s *contextCheckingResultOutboxStore) CompareAndSwap(ctx context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	if err := ensureActiveTestContext(ctx); err != nil {
		return false, err
	}
	return s.resultOutboxTestStore.CompareAndSwap(ctx, entity, conditionField, conditionValue, updates)
}

func (s *contextCheckingResultOutboxStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if err := ensureActiveTestContext(ctx); err != nil {
		return false, err
	}
	return s.resultOutboxTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func outboxTransitionKey(from, to config.JobResultOutboxState) string {
	return string(from) + "->" + string(to)
}

func (s *resultOutboxTestStore) rejectNextTransition(from, to config.JobResultOutboxState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejectTransitions[outboxTransitionKey(from, to)]++
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
			if !matchOutboxFilters(outbox, opts) {
				continue
			}
			copy := *outbox
			outboxes = append(outboxes, &copy)
		}
		sort.Slice(outboxes, func(i, j int) bool {
			if !outboxes[i].UpdateTime.Equal(outboxes[j].UpdateTime) {
				return outboxes[i].UpdateTime.Before(outboxes[j].UpdateTime)
			}
			return outboxes[i].CreateTime.Before(outboxes[j].CreateTime)
		})
		return paginateOutboxes(outboxes, opts), nil
	case *model.JobInfo:
		jobInfos := make([]*model.JobInfo, 0, len(s.jobInfos))
		for _, jobInfo := range s.jobInfos {
			if q.TaskID != "" && jobInfo.TaskID != q.TaskID {
				continue
			}
			if !matchJobInfoFilters(jobInfo, opts) {
				continue
			}
			copy := *jobInfo
			jobInfos = append(jobInfos, &copy)
		}
		sort.Slice(jobInfos, func(i, j int) bool {
			return jobInfos[i].CreateTime.After(jobInfos[j].CreateTime)
		})
		return paginateJobInfos(jobInfos, opts), nil
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
		default:
			return false, datastore.ErrEntityInvalid
		}
	}
	toState, _ := updates["state"].(config.JobResultOutboxState)
	if count := s.rejectTransitions[outboxTransitionKey(current.State, toState)]; count > 0 {
		s.rejectTransitions[outboxTransitionKey(current.State, toState)] = count - 1
		return false, nil
	}
	applyOutboxUpdates(current, updates)
	current.UpdateTime = time.Now()
	return true, nil
}

func matchOutboxFilters(outbox *model.JobResultOutbox, opts *datastore.ListOptions) bool {
	if opts == nil {
		return true
	}
	for _, filter := range opts.FilterOptions.In {
		if filter.Key != "state" {
			continue
		}
		matched := false
		for _, value := range filter.Values {
			if string(outbox.State) == value {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func matchJobInfoFilters(jobInfo *model.JobInfo, opts *datastore.ListOptions) bool {
	if opts == nil {
		return true
	}
	for _, filter := range opts.FilterOptions.In {
		switch filter.Key {
		case "type":
			if !containsString(filter.Values, jobInfo.Type) {
				return false
			}
		case "service_name":
			if !containsString(filter.Values, jobInfo.ServiceName) {
				return false
			}
		case "execution_key":
			if jobInfo.ExecutionKey == nil || !containsString(filter.Values, *jobInfo.ExecutionKey) {
				return false
			}
		case "run_generation":
			if !containsString(filter.Values, fmt.Sprint(jobInfo.RunGeneration)) {
				return false
			}
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func paginateOutboxes(outboxes []*model.JobResultOutbox, opts *datastore.ListOptions) []datastore.Entity {
	start, end := pageBounds(len(outboxes), opts)
	list := make([]datastore.Entity, 0, end-start)
	for _, outbox := range outboxes[start:end] {
		list = append(list, outbox)
	}
	return list
}

func paginateJobInfos(jobInfos []*model.JobInfo, opts *datastore.ListOptions) []datastore.Entity {
	start, end := pageBounds(len(jobInfos), opts)
	list := make([]datastore.Entity, 0, end-start)
	for _, jobInfo := range jobInfos[start:end] {
		list = append(list, jobInfo)
	}
	return list
}

func pageBounds(length int, opts *datastore.ListOptions) (int, int) {
	if opts == nil || opts.PageSize <= 0 || opts.Page <= 0 {
		return 0, length
	}
	start := (opts.Page - 1) * opts.PageSize
	if start >= length {
		return length, length
	}
	end := start + opts.PageSize
	if end > length {
		end = length
	}
	return start, end
}

func applyOutboxUpdates(outbox *model.JobResultOutbox, updates map[string]interface{}) {
	for key, value := range updates {
		switch key {
		case "state":
			outbox.State = value.(config.JobResultOutboxState)
		case "message_id":
			outbox.MessageID = value.(string)
		case "attempts":
			outbox.Attempts = value.(int)
		case "last_error":
			outbox.LastError = value.(string)
		}
	}
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
			dispatcher := NewDelayDispatcher(nil, client, store, "", "")
			payload := &DelayJobPayload{
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

			require.NoError(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}))
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")
	payload := &DelayJobPayload{
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

	require.NoError(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}))
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
			dispatcher := NewDelayDispatcher(nil, client, store, "", "")
			payload := &DelayJobPayload{
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
						config.AnnotationJobRunPolicy:     string(config.JobRunPolicyRecreate),
					},
				}},
			}

			require.NoError(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}))
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
			dispatcher := NewDelayDispatcher(nil, client, store, "", "")
			payload := &DelayJobPayload{
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

			require.NoError(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}))
			_, err := client.BatchV1().Jobs("default").Get(context.Background(), "delay-job-current", metav1.GetOptions{})
			require.NoError(t, err)
			require.Len(t, store.outboxes, 1)
		})
	}
}

func TestDelayDispatcherDoesNotBindOutboxToDifferentJobExecution(t *testing.T) {
	for _, policy := range []config.JobRunPolicy{
		config.JobRunPolicySkipIfCompleted,
		config.JobRunPolicyRecreate,
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
			dispatcher := NewDelayDispatcher(nil, client, store, "", "")
			payload := &DelayJobPayload{
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

			err := dispatcher.dispatch(context.Background(), &delayItem{payload: payload})
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
	payload := &DelayJobPayload{
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
				config.AnnotationJobRunPolicy:     string(config.JobRunPolicyRecreate),
			},
		}},
	}
	resultPayload := newJobResultPayloadFromDelay(payload, payload.Job)
	outbox := buildJobResultOutbox(resultPayload, config.JobResultOutboxStateResultPending)
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")

	require.NoError(t, dispatcher.dispatch(context.Background(), &delayItem{payload: payload}))
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")
	payload := &DelayJobPayload{
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

	err := dispatcher.dispatch(context.Background(), &delayItem{payload: payload})
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-1", Namespace: "default"}}
	item := &delayItem{
		payload: &DelayJobPayload{
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

	require.NoError(t, dispatcher.dispatch(context.Background(), item))

	resultPayload := newJobResultPayloadFromDelay(item.payload, jobObj)
	outbox, err := getJobResultOutboxByPayload(context.Background(), store, resultPayload)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, outbox.State)

	createdJob, err := client.BatchV1().Jobs("default").Get(context.Background(), "delay-job-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "delay-job-1", createdJob.Name)
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-create-fail", Namespace: "default"}}
	item := &delayItem{
		payload: &DelayJobPayload{
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

	err := dispatcher.dispatch(context.Background(), item)
	require.EqualError(t, err, "create failed before job persisted")

	resultPayload := newJobResultPayloadFromDelay(item.payload, jobObj)
	_, getErr := getJobResultOutboxByPayload(context.Background(), store, resultPayload)
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "delay-job-create-visible",
		Namespace: "default",
		Annotations: map[string]string{
			config.AnnotationJobExecutionKey:  "execution-current",
			config.AnnotationJobRunGeneration: "2",
		},
	}}
	item := &delayItem{
		payload: &DelayJobPayload{
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

	require.NoError(t, dispatcher.dispatch(context.Background(), item))

	resultPayload := newJobResultPayloadFromDelay(item.payload, jobObj)
	outbox, getErr := getJobResultOutboxByPayload(context.Background(), store, resultPayload)
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-create-conflict", Namespace: "default"}}
	payload := &DelayJobPayload{
		TaskID:         "task-delay-create-conflict",
		ExecutionKey:   "execution-delay-create-conflict",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
		Job:            jobObj,
	}
	resultPayload := newJobResultPayloadFromDelay(payload, jobObj)
	jobInfo := testResultJobInfo(4, resultPayload)
	jobInfo.Status = string(config.StatusWaiting)
	require.NoError(t, store.Add(context.Background(), jobInfo))

	err := dispatcher.dispatch(context.Background(), &delayItem{payload: payload})
	require.ErrorIs(t, err, errDelayDispatchNoRetry)
	require.ErrorContains(t, err, "foreign-task")

	_, getErr := getJobResultOutboxByPayload(context.Background(), store, resultPayload)
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
	dispatcher := NewDelayDispatcher(nil, client, store, "", "")

	jobObj := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "delay-job-2", Namespace: "default"}}
	payload := &DelayJobPayload{
		TaskID:         "task-delay-2",
		ExecutionKey:   "execution-delay-2",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
		Job:            jobObj,
	}
	resultPayload := newJobResultPayloadFromDelay(payload, jobObj)
	require.NoError(t, store.Add(context.Background(), buildJobResultOutbox(resultPayload, config.JobResultOutboxStateResultPending)))

	err := dispatcher.dispatch(context.Background(), &delayItem{payload: payload})
	require.NoError(t, err)
	require.Empty(t, client.Actions())
}

func TestResultOutboxDispatcherClaimsPendingBeforeEnqueue(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &enqueueCaptureQueue{enqueueID: "result-1"}
	dispatcher := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-claim",
		ExecutionKey:   "execution-delay-claim",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-claim",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	store.rejectNextTransition(config.JobResultOutboxStateResultPending, config.JobResultOutboxStateResultDispatching)

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))
	require.Empty(t, queue.enqueued)

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, "", refreshed.MessageID)
}

func TestResultOutboxDispatcherPersistsQueuedStateAfterSuccessfulEnqueue(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &enqueueCaptureQueue{enqueueID: "result-queued-1"}
	dispatcher := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-queued",
		ExecutionKey:   "execution-delay-queued",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-queued",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultQueued, refreshed.State)
	require.Equal(t, "result-queued-1", refreshed.MessageID)
	require.Equal(t, "", refreshed.LastError)
}

func TestResultOutboxDispatcherTransitionsOnlyTargetOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &enqueueCaptureQueue{enqueueID: "result-7"}
	dispatcher := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)

	targetPayload := &JobResultPayload{
		TaskID:         "task-delay-target",
		ExecutionKey:   "execution-delay-target",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-target",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	siblingPayload := &JobResultPayload{
		TaskID:         "task-delay-sibling",
		ExecutionKey:   "execution-delay-sibling",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-sibling",
		ServiceName:    "svc-b",
		TimeoutSeconds: 60,
	}
	targetOutbox := buildJobResultOutbox(targetPayload, config.JobResultOutboxStateResultPending)
	siblingOutbox := buildJobResultOutbox(siblingPayload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), targetOutbox))
	require.NoError(t, store.Add(context.Background(), siblingOutbox))

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), targetOutbox))

	refreshedTarget, err := getJobResultOutboxByID(context.Background(), store, targetOutbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultQueued, refreshedTarget.State)
	require.Equal(t, "result-7", refreshedTarget.MessageID)

	refreshedSibling, err := getJobResultOutboxByID(context.Background(), store, siblingOutbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshedSibling.State)
}

func TestResultOutboxDispatcherReturnsToPendingWhenEnqueueFails(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &enqueueCaptureQueue{enqueueErr: errors.New("queue down")}
	dispatcher := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-enqueue-fail",
		ExecutionKey:   "execution-delay-enqueue-fail",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-enqueue-fail",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	require.NoError(t, store.Add(context.Background(), &model.JobInfo{
		ID:          3,
		TaskID:      payload.TaskID,
		Type:        payload.JobType,
		ServiceName: payload.ServiceName,
		Status:      string(config.StatusWaiting),
	}))

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, 1, refreshed.Attempts)
	require.Contains(t, refreshed.LastError, "result enqueue failed")
	require.Contains(t, refreshed.LastError, "queue down")
	require.Equal(t, "", refreshed.MessageID)

	jobInfo := store.jobInfoByTaskID(payload.TaskID)
	require.NotNil(t, jobInfo)
	require.Equal(t, string(config.StatusWaiting), jobInfo.Status)
}

func TestResultOutboxDispatcherLeavesDispatchingWhenPendingRequeueClaimLost(t *testing.T) {
	store := newResultOutboxTestStore()
	queue := &enqueueCaptureQueue{enqueueErr: errors.New("queue down")}
	dispatcher := NewResultOutboxDispatcher(queue, fake.NewSimpleClientset(), store)

	payload := &JobResultPayload{
		TaskID:         "task-delay-requeue-cas-lost",
		ExecutionKey:   "execution-delay-requeue-cas-lost",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-requeue-cas-lost",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultPending)
	require.NoError(t, store.Add(context.Background(), outbox))
	store.rejectNextTransition(config.JobResultOutboxStateResultDispatching, config.JobResultOutboxStateResultPending)

	require.NoError(t, dispatcher.dispatchPendingOutbox(context.Background(), outbox))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultDispatching, refreshed.State)
	require.Equal(t, 0, refreshed.Attempts)
}

func TestResultOutboxDispatcherRecoverLocalProcessingRecoversAllPages(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
	dispatcher.batchSize = 2

	for i := 0; i < 3; i++ {
		payload := &JobResultPayload{
			TaskID:         fmt.Sprintf("task-recover-%d", i),
			ExecutionKey:   fmt.Sprintf("execution-recover-%d", i),
			RunGeneration:  1,
			JobType:        string(config.JobDeployScheduled),
			Namespace:      "default",
			Name:           fmt.Sprintf("delay-job-recover-%d", i),
			ServiceName:    "svc-a",
			TimeoutSeconds: 60,
		}
		outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultProcessingLocal)
		require.NoError(t, store.Add(context.Background(), outbox))
	}

	require.NoError(t, dispatcher.recoverLocalProcessing(context.Background()))

	pending, err := listJobResultOutboxesByStates(context.Background(), store, []config.JobResultOutboxState{config.JobResultOutboxStateResultPending}, 10)
	require.NoError(t, err)
	require.Len(t, pending, 3)

	processing, err := listJobResultOutboxesByStates(context.Background(), store, []config.JobResultOutboxState{config.JobResultOutboxStateResultProcessingLocal}, 10)
	require.NoError(t, err)
	require.Empty(t, processing)
}

func TestResultOutboxDispatcherRecoversStaleDispatchingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
	dispatcher.dispatchGrace = 10 * time.Millisecond

	payload := &JobResultPayload{
		TaskID:         "task-dispatching-stale",
		ExecutionKey:   "execution-dispatching-stale",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching-stale",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultDispatching)
	outbox.CreateTime = time.Now().Add(-2 * time.Minute)
	outbox.UpdateTime = time.Now().Add(-time.Minute)
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.processResultDispatching(context.Background()))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, 1, refreshed.Attempts)
	require.Contains(t, refreshed.LastError, "exceeded recovery grace")
}

func TestResultOutboxDispatcherKeepsFreshDispatchingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
	dispatcher.dispatchGrace = time.Minute

	payload := &JobResultPayload{
		TaskID:         "task-dispatching-fresh",
		ExecutionKey:   "execution-dispatching-fresh",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-dispatching-fresh",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultDispatching)
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.processResultDispatching(context.Background()))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultDispatching, refreshed.State)
	require.Equal(t, 0, refreshed.Attempts)
	require.Equal(t, "", refreshed.LastError)
}

func TestResultOutboxDispatcherRecoversStaleLocalProcessingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
	dispatcher.pollInterval = 5 * time.Millisecond

	payload := &JobResultPayload{
		TaskID:         "task-local-processing-stale",
		ExecutionKey:   "execution-local-processing-stale",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-local-processing-stale",
		ServiceName:    "svc-a",
		TimeoutSeconds: 1,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultProcessingLocal)
	outbox.CreateTime = time.Now().Add(-2 * time.Minute)
	outbox.UpdateTime = time.Now().Add(-2 * time.Minute)
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.processResultProcessingLocal(context.Background()))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultPending, refreshed.State)
	require.Equal(t, 1, refreshed.Attempts)
	require.Contains(t, refreshed.LastError, "exceeded recovery grace")
}

func TestResultOutboxDispatcherKeepsFreshLocalProcessingOutbox(t *testing.T) {
	store := newResultOutboxTestStore()
	dispatcher := NewResultOutboxDispatcher(&enqueueCaptureQueue{}, fake.NewSimpleClientset(), store)
	dispatcher.pollInterval = 5 * time.Millisecond

	payload := &JobResultPayload{
		TaskID:         "task-local-processing-fresh",
		ExecutionKey:   "execution-local-processing-fresh",
		RunGeneration:  1,
		JobType:        string(config.JobDeployScheduled),
		Namespace:      "default",
		Name:           "delay-job-local-processing-fresh",
		ServiceName:    "svc-a",
		TimeoutSeconds: 60,
	}
	outbox := buildJobResultOutbox(payload, config.JobResultOutboxStateResultProcessingLocal)
	require.NoError(t, store.Add(context.Background(), outbox))

	require.NoError(t, dispatcher.processResultProcessingLocal(context.Background()))

	refreshed, err := getJobResultOutboxByID(context.Background(), store, outbox.ID)
	require.NoError(t, err)
	require.Equal(t, config.JobResultOutboxStateResultProcessingLocal, refreshed.State)
	require.Equal(t, 0, refreshed.Attempts)
	require.Equal(t, "", refreshed.LastError)
}

func TestJobResultPayloadFromOutboxRequiresMandatoryFields(t *testing.T) {
	valid := &model.JobResultOutbox{
		ID:            "outbox-valid",
		TaskID:        "task-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 7,
		Namespace:     "default",
		Name:          "job-1",
	}
	payload := jobResultPayloadFromOutbox(valid)
	require.NotNil(t, payload)
	require.Equal(t, "execution-1", payload.ExecutionKey)
	require.Equal(t, uint64(7), payload.RunGeneration)

	missingTask := &model.JobResultOutbox{
		ID:        "outbox-missing-task",
		Namespace: "default",
		Name:      "job-1",
	}
	require.Nil(t, jobResultPayloadFromOutbox(missingTask))

	missingNamespace := &model.JobResultOutbox{
		ID:     "outbox-missing-namespace",
		TaskID: "task-1",
		Name:   "job-1",
	}
	require.Nil(t, jobResultPayloadFromOutbox(missingNamespace))

	missingName := &model.JobResultOutbox{
		ID:        "outbox-missing-name",
		TaskID:    "task-1",
		Namespace: "default",
	}
	require.Nil(t, jobResultPayloadFromOutbox(missingName))
}

func TestJobResultOutboxIDIsScopedByExecutionIdentity(t *testing.T) {
	first := &JobResultPayload{
		TaskID:        "task-1",
		Namespace:     "default",
		Name:          "job-1",
		ExecutionKey:  "execution-1",
		RunGeneration: 1,
	}
	second := *first
	second.ExecutionKey = "execution-2"
	nextGeneration := *first
	nextGeneration.RunGeneration = 2
	incomplete := *first
	incomplete.ExecutionKey = ""

	require.NotEqual(t, jobResultOutboxID(first), jobResultOutboxID(&second))
	require.NotEqual(t, jobResultOutboxID(first), jobResultOutboxID(&nextGeneration))
	require.Empty(t, jobResultOutboxID(&incomplete))
}
