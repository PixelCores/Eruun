package workflow

import (
	"context"

	"encoding/json"
	"errors"
	"fmt"

	"strings"
	"sync"

	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func newTestWorkflowController(t *testing.T, task *model.WorkflowQueue, client kubernetes.Interface, store datastore.DataStore) *WorkflowCtl {
	t.Helper()
	ensureTestWorkflowExecutionIdentity(task)
	switch typedStore := store.(type) {
	case *controllerTestStore:
		if typedStore.task == nil {
			typedStore.task = cloneWorkflowQueue(task)
		}
	case *controlledWorkflowCASStore:
		if typedStore.controllerTestStore != nil && typedStore.task == nil {
			typedStore.task = cloneWorkflowQueue(task)
		}
	}
	ctl, err := NewWorkflowController(
		task,
		client,
		nil,
		store,
		&config.Config{AllowPrivateURLTargets: true},
		nil,
		&spec.URLSecurityPolicySpec{AllowPrivateByDefault: true},
	)
	require.NoError(t, err)
	return ctl
}

type controllerTestStore struct {
	mu                              sync.Mutex
	application                     *model.Applications
	workflow                        *model.Workflow
	task                            *model.WorkflowQueue
	components                      []*model.ApplicationComponent
	jobs                            []*model.JobInfo
	jobInfoAddErr                   error
	compareAndSwapWithConditionsErr error
	beforeCompareHook               func(task *model.WorkflowQueue)
}

type controlledWorkflowCASStore struct {
	*controllerTestStore
	controlMu           sync.Mutex
	casCalls            int
	falseCalls          map[int]func(task *model.WorkflowQueue)
	failTaskGetAfterCAS int
	taskGetErr          error
}

func (s *controlledWorkflowCASStore) Get(ctx context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.controlMu.Lock()
		shouldFail := s.failTaskGetAfterCAS > 0 && s.casCalls >= s.failTaskGetAfterCAS
		err := s.taskGetErr
		s.controlMu.Unlock()
		if shouldFail {
			if err == nil {
				err = errors.New("injected workflow task get failure")
			}
			return err
		}
	}
	return s.controllerTestStore.Get(ctx, entity)
}

func (s *controlledWorkflowCASStore) CompareAndSwap(
	ctx context.Context,
	entity datastore.Entity,
	field string,
	condition interface{},
	updates map[string]interface{},
) (bool, error) {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.controlMu.Lock()
		s.casCalls++
		hook, forceFalse := s.falseCalls[s.casCalls]
		s.controlMu.Unlock()
		if forceFalse {
			if hook != nil {
				s.mu.Lock()
				hook(s.task)
				s.mu.Unlock()
			}
			return false, nil
		}
	}
	return s.controllerTestStore.CompareAndSwap(ctx, entity, field, condition, updates)
}

func (s *controlledWorkflowCASStore) CompareAndSwapWithConditions(
	ctx context.Context,
	entity datastore.Entity,
	conditions map[string]interface{},
	updates map[string]interface{},
) (bool, error) {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		s.controlMu.Lock()
		s.casCalls++
		hook, forceFalse := s.falseCalls[s.casCalls]
		s.controlMu.Unlock()
		if forceFalse {
			if hook != nil {
				s.mu.Lock()
				hook(s.task)
				s.mu.Unlock()
			}
			return false, nil
		}
	}
	return s.controllerTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
}

func (s *controllerTestStore) Add(_ context.Context, entity datastore.Entity) error {
	if _, ok := entity.(*model.JobInfo); ok && s.jobInfoAddErr != nil {
		return s.jobInfoAddErr
	}
	return nil
}

func (s *controllerTestStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *controllerTestStore) CurrentDatabaseTime(context.Context) (time.Time, error) {
	return time.Now().UTC(), nil
}

func (s *controllerTestStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *controllerTestStore) Put(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, ok := entity.(*model.WorkflowQueue); ok && task != nil {
		if s.task != nil && s.task.TaskID == task.TaskID {
			*s.task = *task
			return nil
		}
		s.task = task
	}
	if jobInfo, ok := entity.(*model.JobInfo); ok && jobInfo != nil {
		for i, existing := range s.jobs {
			if existing == nil {
				continue
			}
			if existing.ID != 0 && existing.ID == jobInfo.ID {
				cp := *jobInfo
				s.jobs[i] = &cp
				return nil
			}
			if existing.TaskID == jobInfo.TaskID && existing.Type == jobInfo.Type && existing.ServiceName == jobInfo.ServiceName {
				cp := *jobInfo
				s.jobs[i] = &cp
				return nil
			}
		}
		cp := *jobInfo
		s.jobs = append(s.jobs, &cp)
	}
	return nil
}

func (s *controllerTestStore) Delete(context.Context, datastore.Entity) error { return nil }

func (s *controllerTestStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *controllerTestStore) Get(_ context.Context, entity datastore.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch e := entity.(type) {
	case *model.Applications:
		if s.application != nil && e.ID == s.application.ID {
			*e = *s.application
			return nil
		}
	case *model.Workflow:
		if s.workflow != nil && e.ID == s.workflow.ID {
			*e = *s.workflow
			return nil
		}
	case *model.WorkflowQueue:
		if s.task != nil && e.TaskID == s.task.TaskID {
			*e = *s.task
			return nil
		}
	}
	return datastore.ErrRecordNotExist
}

func (s *controllerTestStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch q := query.(type) {
	case *model.ApplicationComponent:
		result := make([]datastore.Entity, 0, len(s.components))
		for _, component := range s.components {
			if component == nil {
				continue
			}
			if q.AppID != "" && component.AppID != q.AppID {
				continue
			}
			cp := *component
			result = append(result, &cp)
		}
		return result, nil
	case *model.JobInfo:
		result := make([]datastore.Entity, 0, len(s.jobs))
		for _, jobInfo := range s.jobs {
			if jobInfo == nil {
				continue
			}
			if q.TaskID != "" && jobInfo.TaskID != q.TaskID {
				continue
			}
			cp := *jobInfo
			result = append(result, &cp)
		}
		return result, nil
	}
	return nil, fmt.Errorf("unsupported query %T", query)
}

func (s *controllerTestStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *controllerTestStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *controllerTestStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *controllerTestStore) CompareAndSwap(_ context.Context, entity datastore.Entity, field string, condition interface{}, updates map[string]interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if taskEntity, ok := entity.(*model.WorkflowQueue); ok {
		if taskEntity == nil || s.task == nil || taskEntity.TaskID != s.task.TaskID {
			return false, nil
		}
		s.runBeforeCompareHookLocked()

		if !matchWorkflowQueueField(s.task, field, condition) {
			return false, nil
		}

		for key, value := range updates {
			applyWorkflowQueueUpdate(s.task, key, value)
		}
		return true, nil
	}
	if jobEntity, ok := entity.(*model.JobInfo); ok {
		jobInfo := s.findJobInfoLocked(jobEntity)
		if jobInfo == nil {
			return false, nil
		}
		s.runBeforeCompareHookLocked()
		if !matchJobInfoField(jobInfo, field, condition) {
			return false, nil
		}
		for key, value := range updates {
			applyJobInfoUpdate(jobInfo, key, value)
		}
		return true, nil
	}
	return false, nil
}

func (s *controllerTestStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.compareAndSwapWithConditionsErr != nil {
		return false, s.compareAndSwapWithConditionsErr
	}

	if componentEntity, ok := entity.(*model.ApplicationComponent); ok && componentEntity != nil {
		for _, component := range s.components {
			if component == nil || component.AppID != componentEntity.AppID {
				continue
			}
			if componentEntity.ID > 0 {
				if component.ID != componentEntity.ID {
					continue
				}
			} else if component.Name != componentEntity.Name {
				continue
			}
			for key, value := range updates {
				switch key {
				case "status":
					component.Status, _ = value.(string)
				case "ready_replicas":
					component.ReadyReplicas, _ = value.(int32)
				case "last_abnormal":
					component.LastAbnormal, _ = value.(string)
				}
			}
			return true, nil
		}
		return false, nil
	}

	taskEntity, ok := entity.(*model.WorkflowQueue)
	if !ok || taskEntity == nil || s.task == nil || taskEntity.TaskID != s.task.TaskID {
		return false, nil
	}
	s.runBeforeCompareHookLocked()
	for field, expected := range conditions {
		if !matchWorkflowQueueField(s.task, field, expected) {
			return false, nil
		}
	}
	for key, value := range updates {
		applyWorkflowQueueUpdate(s.task, key, value)
	}
	return true, nil
}

func (s *controllerTestStore) runBeforeCompareHookLocked() {
	if s.beforeCompareHook == nil || s.task == nil {
		return
	}
	hook := s.beforeCompareHook
	s.beforeCompareHook = nil
	hook(s.task)
}

func (s *controllerTestStore) findJobInfoLocked(candidate *model.JobInfo) *model.JobInfo {
	if candidate == nil {
		return nil
	}
	for _, jobInfo := range s.jobs {
		if jobInfo == nil {
			continue
		}
		if candidate.ID != 0 && jobInfo.ID == candidate.ID {
			return jobInfo
		}
		if candidate.ID == 0 && jobInfo.TaskID == candidate.TaskID && jobInfo.Type == candidate.Type && jobInfo.ServiceName == candidate.ServiceName {
			return jobInfo
		}
	}
	return nil
}

func matchWorkflowQueueField(task *model.WorkflowQueue, field string, condition interface{}) bool {
	if task == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "approval_pending":
		expected, ok := condition.(bool)
		return ok && task.ApprovalPending == expected
	case "pending_approval_step":
		expected, ok := condition.(string)
		return ok && task.PendingApprovalStep == expected
	case "status":
		switch expected := condition.(type) {
		case config.Status:
			return task.Status == expected
		case string:
			return string(task.Status) == expected
		default:
			return false
		}
	case "current_step":
		switch expected := condition.(type) {
		case int:
			return task.CurrentStep == expected
		case int32:
			return task.CurrentStep == int(expected)
		case int64:
			return task.CurrentStep == int(expected)
		default:
			return false
		}
	case "run_generation":
		switch expected := condition.(type) {
		case uint64:
			return task.RunGeneration == expected
		case int:
			return task.RunGeneration == uint64(expected)
		default:
			return false
		}
	case "run_token":
		expected, ok := condition.(string)
		return ok && task.RunToken == expected
	case "worker_id":
		expected, ok := condition.(string)
		return ok && task.WorkerID == expected
	case "lease_expires_at":
		expected, ok := condition.(time.Time)
		return ok && task.LeaseExpiresAt != nil && task.LeaseExpiresAt.Equal(expected)
	default:
		return false
	}
}

func matchJobInfoField(jobInfo *model.JobInfo, field string, condition interface{}) bool {
	if jobInfo == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "status":
		expected, ok := condition.(string)
		return ok && jobInfo.Status == expected
	case "id":
		expected, ok := condition.(int)
		return ok && jobInfo.ID == expected
	case "task_id":
		expected, ok := condition.(string)
		return ok && jobInfo.TaskID == expected
	case "type":
		expected, ok := condition.(string)
		return ok && jobInfo.Type == expected
	case "service_name":
		expected, ok := condition.(string)
		return ok && jobInfo.ServiceName == expected
	default:
		return false
	}
}

func applyWorkflowQueueUpdate(task *model.WorkflowQueue, key string, value interface{}) {
	if task == nil {
		return
	}
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "status":
		switch v := value.(type) {
		case config.Status:
			task.Status = v
		case string:
			task.Status = config.Status(v)
		}
	case "approval_pending":
		if v, ok := value.(bool); ok {
			task.ApprovalPending = v
		}
	case "pending_approval_step":
		if v, ok := value.(string); ok {
			task.PendingApprovalStep = v
		}
	case "current_step":
		switch v := value.(type) {
		case int:
			task.CurrentStep = v
		case int32:
			task.CurrentStep = int(v)
		case int64:
			task.CurrentStep = int(v)
		}
	case "run_generation":
		if v, ok := value.(uint64); ok {
			task.RunGeneration = v
		}
	case "run_token":
		if v, ok := value.(string); ok {
			task.RunToken = v
		}
	case "worker_id":
		if v, ok := value.(string); ok {
			task.WorkerID = v
		}
	case "heartbeat_at":
		if v, ok := value.(time.Time); ok {
			task.HeartbeatAt = &v
		} else if value == nil {
			task.HeartbeatAt = nil
		}
	case "lease_expires_at":
		if v, ok := value.(time.Time); ok {
			task.LeaseExpiresAt = &v
		} else if value == nil {
			task.LeaseExpiresAt = nil
		}
	case "dispatch_attempts":
		if v, ok := value.(uint); ok {
			task.DispatchAttempts = v
		}
	case "scheduling_reason":
		if v, ok := value.(string); ok {
			task.SchedulingReason = v
		}
	}
}

func applyJobInfoUpdate(jobInfo *model.JobInfo, key string, value interface{}) {
	if jobInfo == nil {
		return
	}
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "status":
		if v, ok := value.(string); ok {
			jobInfo.Status = v
		}
	case "end_time":
		if v, ok := value.(int64); ok {
			jobInfo.EndTime = v
		}
	case "error":
		if v, ok := value.(string); ok {
			jobInfo.Error = v
		}
	}
}

func ensureTestWorkflowExecutionIdentity(task *model.WorkflowQueue) {
	if task == nil {
		return
	}
	if task.RunGeneration == 0 {
		task.RunGeneration = 1
	}
	if task.RunToken == "" {
		task.RunToken = "test-run-" + task.TaskID
	}
	if task.WorkerID == "" {
		task.WorkerID = "test-worker"
	}
}

func cloneWorkflowQueue(task *model.WorkflowQueue) *model.WorkflowQueue {
	if task == nil {
		return nil
	}
	cloned := *task
	ensureTestWorkflowExecutionIdentity(&cloned)
	return &cloned
}

func mustVersionUpdateCleanupInternalInfo(t *testing.T, component *model.ApplicationComponent, insertBeforeStepIndex int) string {
	t.Helper()
	payload, err := json.Marshal(struct {
		Source string `json:"source"`
	}{Source: config.JobInfoSourceVersionUpdateRemove})
	require.NoError(t, err)
	return string(payload)
}
