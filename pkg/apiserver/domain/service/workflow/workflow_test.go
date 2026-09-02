package workflow

import (
	"context"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	miniredis "github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func newTestWorkflowCancelSignalCache(t *testing.T) cache.ICache {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(server.Close)

	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = redisClient.Close()
	})

	return cache.NewWithClient(false, cache.CacheTypeMem, redisClient)
}

func withAllowPrivateURLPolicy(t testing.TB, svc *workflowServiceImpl) *workflowServiceImpl {
	t.Helper()
	svc.URLSecurityPolicyProvider = newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true})
	return svc
}

type failingDataStore struct {
	err error
}

func (f *failingDataStore) Add(context.Context, datastore.Entity) error { return f.err }

func (f *failingDataStore) BatchAdd(context.Context, []datastore.Entity) error { return f.err }

func (f *failingDataStore) Put(context.Context, datastore.Entity) error { return f.err }

func (f *failingDataStore) Delete(context.Context, datastore.Entity) error { return f.err }

func (f *failingDataStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return f.err
}

func (f *failingDataStore) Get(context.Context, datastore.Entity) error { return f.err }

func (f *failingDataStore) List(context.Context, datastore.Entity, *datastore.ListOptions) ([]datastore.Entity, error) {
	return nil, f.err
}

func (f *failingDataStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, f.err
}

func (f *failingDataStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, f.err
}

func (f *failingDataStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, f.err
}

func (f *failingDataStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, f.err
}

func holdWorkflowTestAppScheduleLock(t *testing.T, lockProvider locker.Locker, appID string) func() {
	t.Helper()
	acquired := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- schedulelock.WithAppScheduleLock(context.Background(), lockProvider, appID, "test-hold-app-schedule-lock", true, func(context.Context) error {
			close(acquired)
			<-release
			return nil
		})
	}()
	select {
	case <-acquired:
	case err := <-result:
		t.Fatalf("hold app schedule lock: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out acquiring app schedule lock")
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			close(release)
			require.NoError(t, <-result)
		})
	}
}

type enqueueWorkflowDataStore struct {
	scheduleDataStore
	components []*model.ApplicationComponent
}

func (s *enqueueWorkflowDataStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *enqueueWorkflowDataStore) List(ctx context.Context, query datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	if compQuery, ok := query.(*model.ApplicationComponent); ok {
		out := make([]datastore.Entity, 0, len(s.components))
		for _, component := range s.components {
			if component == nil {
				continue
			}
			if compQuery.AppID != "" && component.AppID != compQuery.AppID {
				continue
			}
			out = append(out, component)
		}
		return out, nil
	}
	return s.scheduleDataStore.List(ctx, query, options)
}

type statusDataStore struct {
	task               *model.WorkflowQueue
	tasks              []*model.WorkflowQueue
	workflow           *model.Workflow
	app                *model.Applications
	jobs               []*model.JobInfo
	components         []*model.ApplicationComponent
	beforeCAS          func(task *model.WorkflowQueue)
	onGet              func(context.Context, datastore.Entity)
	respectContextErr  bool
	putDropsZeroValues bool
}

type manualExecTestStore struct {
	*statusDataStore
	taskAddStarted chan struct{}
	releaseTaskAdd chan struct{}
}

func (s *manualExecTestStore) Add(ctx context.Context, entity datastore.Entity) error {
	if task, ok := entity.(*model.WorkflowQueue); ok && task != nil {
		if s.taskAddStarted != nil {
			select {
			case s.taskAddStarted <- struct{}{}:
			default:
			}
		}
		if s.releaseTaskAdd != nil {
			select {
			case <-s.releaseTaskAdd:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		cp := *task
		s.tasks = append(s.tasks, &cp)
		return nil
	}
	return s.statusDataStore.Add(ctx, entity)
}

func (s *manualExecTestStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *statusDataStore) Add(_ context.Context, entity datastore.Entity) error {
	if job, ok := entity.(*model.JobInfo); ok && job != nil {
		cp := *job
		s.jobs = append(s.jobs, &cp)
	}
	return nil
}

func (s *statusDataStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *statusDataStore) Put(_ context.Context, entity datastore.Entity) error {
	if task, ok := entity.(*model.WorkflowQueue); ok && task != nil {
		if s.task != nil && s.task.TaskID == task.TaskID {
			if s.putDropsZeroValues {
				s.task.TaskRevoker = task.TaskRevoker
				s.task.Status = task.Status
				s.task.CancelSource = task.CancelSource
				s.task.CurrentStep = task.CurrentStep
				if task.ApprovalPending {
					s.task.ApprovalPending = task.ApprovalPending
				}
				if task.PendingApprovalStep != "" {
					s.task.PendingApprovalStep = task.PendingApprovalStep
				}
				return nil
			}
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

func (s *statusDataStore) Delete(context.Context, datastore.Entity) error { return nil }

func (s *statusDataStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *statusDataStore) Get(ctx context.Context, entity datastore.Entity) error {
	if s.onGet != nil {
		s.onGet(ctx, entity)
	}
	if s.respectContextErr && ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	switch v := entity.(type) {
	case *model.WorkflowQueue:
		if task := s.findTask(v.TaskID); task != nil {
			*v = *task
			return nil
		}
	case *model.Workflow:
		if s.workflow != nil && v.ID == s.workflow.ID {
			*v = *s.workflow
			return nil
		}
	case *model.Applications:
		if s.app != nil && v.ID == s.app.ID {
			*v = *s.app
			return nil
		}
	}
	return datastore.ErrRecordNotExist
}

func (s *statusDataStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	if taskQuery, ok := query.(*model.WorkflowQueue); ok {
		var out []datastore.Entity
		if len(s.tasks) > 0 {
			for _, task := range s.tasks {
				if task == nil {
					continue
				}
				if taskQuery.AppID != "" && task.AppID != taskQuery.AppID {
					continue
				}
				out = append(out, task)
			}
			return out, nil
		}
		if s.task == nil {
			return nil, nil
		}
		if taskQuery.AppID != "" && s.task.AppID != taskQuery.AppID {
			return nil, nil
		}
		return []datastore.Entity{s.task}, nil
	}
	if jobQuery, ok := query.(*model.JobInfo); ok {
		var out []datastore.Entity
		for _, job := range s.jobs {
			if jobQuery.TaskID != "" && job.TaskID != jobQuery.TaskID {
				continue
			}
			out = append(out, job)
		}
		if len(out) == 0 {
			return nil, datastore.ErrRecordNotExist
		}
		return out, nil
	}
	if compQuery, ok := query.(*model.ApplicationComponent); ok {
		var out []datastore.Entity
		for _, component := range s.components {
			if component == nil {
				continue
			}
			if compQuery.AppID != "" && component.AppID != compQuery.AppID {
				continue
			}
			out = append(out, component)
		}
		return out, nil
	}
	return nil, datastore.ErrRecordNotExist
}

func (s *statusDataStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *statusDataStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *statusDataStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *statusDataStore) runBeforeCASHook(task *model.WorkflowQueue) {
	if s.beforeCAS == nil {
		return
	}
	hook := s.beforeCAS
	s.beforeCAS = nil
	hook(task)
}

func (s *statusDataStore) findTask(taskID string) *model.WorkflowQueue {
	if s.task != nil && s.task.TaskID == taskID {
		return s.task
	}
	for _, task := range s.tasks {
		if task != nil && task.TaskID == taskID {
			return task
		}
	}
	return nil
}

func (s *statusDataStore) findJobInfo(candidate *model.JobInfo) *model.JobInfo {
	if candidate == nil {
		return nil
	}
	for _, job := range s.jobs {
		if job == nil {
			continue
		}
		if candidate.ID != 0 && job.ID == candidate.ID {
			return job
		}
		if candidate.ID == 0 && job.TaskID == candidate.TaskID && job.Type == candidate.Type && job.ServiceName == candidate.ServiceName {
			return job
		}
	}
	return nil
}

func (s *statusDataStore) matchTaskConditionFor(task *model.WorkflowQueue, field string, value interface{}) bool {
	if task == nil {
		return false
	}
	switch field {
	case "task_id":
		expected, ok := value.(string)
		return ok && task.TaskID == expected
	case "approval_pending":
		expected, ok := value.(bool)
		return ok && task.ApprovalPending == expected
	case "status":
		expected, ok := value.(config.Status)
		return ok && task.Status == expected
	case "current_step":
		expected, ok := value.(int)
		return ok && task.CurrentStep == expected
	case "pending_approval_step":
		expected, ok := value.(string)
		return ok && task.PendingApprovalStep == expected
	default:
		return false
	}
}

func (s *statusDataStore) matchTaskCondition(field string, value interface{}) bool {
	return s.matchTaskConditionFor(s.task, field, value)
}

func matchJobInfoCondition(job *model.JobInfo, field string, value interface{}) bool {
	if job == nil {
		return false
	}
	switch field {
	case "status":
		expected, ok := value.(string)
		return ok && job.Status == expected
	case "id":
		expected, ok := value.(int)
		return ok && job.ID == expected
	case "task_id":
		expected, ok := value.(string)
		return ok && job.TaskID == expected
	case "type":
		expected, ok := value.(string)
		return ok && job.Type == expected
	case "service_name":
		expected, ok := value.(string)
		return ok && job.ServiceName == expected
	default:
		return false
	}
}

func (s *statusDataStore) applyTaskUpdatesTo(task *model.WorkflowQueue, updates map[string]interface{}) {
	if task == nil {
		return
	}
	for k, v := range updates {
		switch k {
		case "approval_pending":
			task.ApprovalPending, _ = v.(bool)
		case "pending_approval_step":
			task.PendingApprovalStep, _ = v.(string)
		case "status":
			task.Status, _ = v.(config.Status)
		case "current_step":
			task.CurrentStep, _ = v.(int)
		case "task_revoker":
			task.TaskRevoker, _ = v.(string)
		case "cancel_source":
			task.CancelSource, _ = v.(string)
		}
	}
}

func applyJobInfoUpdates(job *model.JobInfo, updates map[string]interface{}) {
	if job == nil {
		return
	}
	for k, v := range updates {
		switch k {
		case "status":
			if status, ok := v.(string); ok {
				job.Status = status
			}
		case "end_time":
			if endTime, ok := v.(int64); ok {
				job.EndTime = endTime
			}
		case "error":
			if message, ok := v.(string); ok {
				job.Error = message
			}
		}
	}
}

func (s *statusDataStore) applyTaskUpdates(updates map[string]interface{}) {
	s.applyTaskUpdatesTo(s.task, updates)
}

func (s *statusDataStore) CompareAndSwap(_ context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	if wq, ok := entity.(*model.WorkflowQueue); ok {
		task := s.findTask(wq.TaskID)
		if task == nil {
			return false, nil
		}
		s.runBeforeCASHook(task)
		if !s.matchTaskConditionFor(task, conditionField, conditionValue) {
			return false, nil
		}
		s.applyTaskUpdatesTo(task, updates)
		return true, nil
	}
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		job := s.findJobInfo(jobInfo)
		if job == nil {
			return false, nil
		}
		s.runBeforeCASHook(s.task)
		if !matchJobInfoCondition(job, conditionField, conditionValue) {
			return false, nil
		}
		applyJobInfoUpdates(job, updates)
		return true, nil
	}
	return false, nil
}

func (s *statusDataStore) CompareAndSwapWithConditions(
	_ context.Context,
	entity datastore.Entity,
	conditions map[string]interface{},
	updates map[string]interface{},
) (bool, error) {
	wq, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return false, nil
	}
	task := s.findTask(wq.TaskID)
	if task == nil {
		return false, nil
	}
	s.runBeforeCASHook(task)
	for field, value := range conditions {
		if !s.matchTaskConditionFor(task, field, value) {
			return false, nil
		}
	}
	s.applyTaskUpdatesTo(task, updates)
	return true, nil
}

type scheduleDataStore struct {
	app         *model.Applications
	workflows   []*model.Workflow
	schedules   []*model.WorkflowSchedule
	queues      []*model.WorkflowQueue
	tasks       []*model.WorkflowQueue
	jobs        []*model.JobInfo
	queueAddErr error
}

type transactionalScheduleDataStore struct {
	*scheduleDataStore
}

func (s *transactionalScheduleDataStore) WithTransaction(_ context.Context, fn func(tx datastore.DataStore) error) error {
	schedules := cloneWorkflowSchedules(s.schedules)
	queues := cloneWorkflowQueues(s.queues)
	tasks := cloneWorkflowQueues(s.tasks)
	if err := fn(s.scheduleDataStore); err != nil {
		s.schedules = schedules
		s.queues = queues
		s.tasks = tasks
		return err
	}
	return nil
}

func cloneWorkflowSchedules(in []*model.WorkflowSchedule) []*model.WorkflowSchedule {
	if in == nil {
		return nil
	}
	out := make([]*model.WorkflowSchedule, len(in))
	for i, item := range in {
		if item == nil {
			continue
		}
		clone := *item
		out[i] = &clone
	}
	return out
}

func cloneWorkflowQueues(in []*model.WorkflowQueue) []*model.WorkflowQueue {
	if in == nil {
		return nil
	}
	out := make([]*model.WorkflowQueue, len(in))
	for i, item := range in {
		if item == nil {
			continue
		}
		clone := *item
		out[i] = &clone
	}
	return out
}

func (s *scheduleDataStore) Add(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.WorkflowSchedule:
		s.schedules = append(s.schedules, v)
	case *model.WorkflowQueue:
		if s.queueAddErr != nil {
			return s.queueAddErr
		}
		if key := workflowQueueIdempotencyKey(v); key != "" {
			for _, task := range s.tasks {
				if workflowQueueIdempotencyKey(task) == key {
					return datastore.ErrRecordExist
				}
			}
		}
		s.queues = append(s.queues, v)
		s.tasks = append(s.tasks, v)
	case *model.Applications:
		s.app = v
	case *model.Workflow:
		s.workflows = append(s.workflows, v)
	}
	return nil
}

func (s *scheduleDataStore) BatchAdd(context.Context, []datastore.Entity) error { return nil }

func (s *scheduleDataStore) Put(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.WorkflowSchedule:
		for i, schedule := range s.schedules {
			if schedule != nil && schedule.ID == v.ID {
				s.schedules[i] = v
				return nil
			}
		}
		s.schedules = append(s.schedules, v)
	case *model.WorkflowQueue:
		for i, queue := range s.queues {
			if queue != nil && queue.TaskID == v.TaskID {
				s.queues[i] = v
				return nil
			}
		}
		s.queues = append(s.queues, v)
		s.tasks = append(s.tasks, v)
	}
	return nil
}

func (s *scheduleDataStore) Delete(_ context.Context, entity datastore.Entity) error {
	if schedule, ok := entity.(*model.WorkflowSchedule); ok {
		for i, item := range s.schedules {
			if item != nil && item.ID == schedule.ID {
				s.schedules = append(s.schedules[:i], s.schedules[i+1:]...)
				return nil
			}
		}
		return datastore.ErrRecordNotExist
	}
	return nil
}

func (s *scheduleDataStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *scheduleDataStore) Get(_ context.Context, entity datastore.Entity) error {
	switch v := entity.(type) {
	case *model.Applications:
		if s.app != nil && v.ID == s.app.ID {
			*v = *s.app
			return nil
		}
	case *model.Workflow:
		for _, wf := range s.workflows {
			if wf != nil && v.ID == wf.ID {
				*v = *wf
				return nil
			}
		}
	case *model.WorkflowSchedule:
		for _, schedule := range s.schedules {
			if schedule != nil && v.ID == schedule.ID {
				*v = *schedule
				return nil
			}
		}
	case *model.WorkflowQueue:
		for _, task := range s.tasks {
			if task != nil && v.TaskID == task.TaskID {
				*v = *task
				return nil
			}
		}
	}
	return datastore.ErrRecordNotExist
}

func (s *scheduleDataStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	switch q := query.(type) {
	case *model.WorkflowSchedule:
		var out []datastore.Entity
		for _, schedule := range s.schedules {
			if schedule == nil {
				continue
			}
			if q.AppID != "" && schedule.AppID != q.AppID {
				continue
			}
			if q.WorkflowID != "" && schedule.WorkflowID != q.WorkflowID {
				continue
			}
			if q.Enabled && !schedule.Enabled {
				continue
			}
			out = append(out, schedule)
		}
		return out, nil
	case *model.WorkflowQueue:
		var out []datastore.Entity
		for _, task := range s.tasks {
			if task == nil {
				continue
			}
			if q.AppID != "" && task.AppID != q.AppID {
				continue
			}
			if q.Status != "" && task.Status != q.Status {
				continue
			}
			if key := workflowQueueIdempotencyKey(q); key != "" && workflowQueueIdempotencyKey(task) != key {
				continue
			}
			out = append(out, task)
		}
		return out, nil
	case *model.JobInfo:
		var out []datastore.Entity
		for _, job := range s.jobs {
			if job == nil {
				continue
			}
			if q.TaskID != "" && job.TaskID != q.TaskID {
				continue
			}
			out = append(out, job)
		}
		if len(out) == 0 {
			return nil, datastore.ErrRecordNotExist
		}
		return out, nil
	case *model.Workflow:
		var out []datastore.Entity
		for _, wf := range s.workflows {
			if wf == nil {
				continue
			}
			if q.AppID != "" && wf.AppID != q.AppID {
				continue
			}
			out = append(out, wf)
		}
		return out, nil
	case *model.ApplicationComponent:
		return []datastore.Entity{&model.ApplicationComponent{Name: "dummy", AppID: q.AppID}}, nil
	}
	return nil, nil
}

func (s *scheduleDataStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *scheduleDataStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *scheduleDataStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, nil
}

func (s *scheduleDataStore) CompareAndSwap(_ context.Context, entity datastore.Entity, field string, condition interface{}, updates map[string]interface{}) (bool, error) {
	if task, ok := entity.(*model.WorkflowQueue); ok {
		if field != "status" {
			return false, nil
		}
		expected, ok := condition.(config.Status)
		if !ok {
			return false, nil
		}
		for _, current := range s.tasks {
			if current == nil || current.TaskID != task.TaskID || current.Status != expected {
				continue
			}
			if status, ok := updates["status"].(config.Status); ok {
				current.Status = status
			}
			return true, nil
		}
		return false, nil
	}
	schedule, ok := entity.(*model.WorkflowSchedule)
	if !ok {
		return false, nil
	}
	switch field {
	case "next_run":
		for _, item := range s.schedules {
			if item == nil || item.ID != schedule.ID {
				continue
			}
			expected, ok := condition.(int64)
			if !ok || item.NextRun != expected {
				return false, nil
			}
			if next, ok := updates["next_run"].(int64); ok {
				item.NextRun = next
			}
			return true, nil
		}
	case "id":
		cond, ok := condition.(string)
		if !ok {
			return false, nil
		}
		for _, item := range s.schedules {
			if item == nil || item.ID != cond {
				continue
			}
			if value, ok := updates["app_id"].(string); ok {
				item.AppID = value
			}
			if value, ok := updates["workflow_id"].(string); ok {
				item.WorkflowID = value
			}
			if value, ok := updates["cron"].(string); ok {
				item.Cron = value
			}
			if value, ok := updates["enabled"].(bool); ok {
				item.Enabled = value
			}
			if value, ok := updates["next_run"].(int64); ok {
				item.NextRun = value
			}
			if value, ok := updates["last_run"].(int64); ok {
				item.LastRun = value
			}
			return true, nil
		}
	}
	return false, nil
}

func workflowQueueIdempotencyKey(task *model.WorkflowQueue) string {
	if task == nil || task.IdempotencyKey == nil {
		return ""
	}
	return *task.IdempotencyKey
}

var _ datastore.DataStore = (*failingDataStore)(nil)

var _ datastore.DataStore = (*statusDataStore)(nil)

var _ datastore.DataStore = (*scheduleDataStore)(nil)

var _ WorkflowService = (*workflowServiceImpl)(nil)
