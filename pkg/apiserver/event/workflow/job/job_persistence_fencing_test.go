package job

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type workflowOwnedJobInfoStore struct {
	noopStore
	workflowTask     *model.WorkflowQueue
	addedJobInfo     *model.JobInfo
	jobInfos         []datastore.Entity
	putJobInfo       *model.JobInfo
	components       []*model.ApplicationComponent
	updatedComponent *model.ApplicationComponent
	transactionCalls int
}

func (s *workflowOwnedJobInfoStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	s.transactionCalls++
	return fn(s)
}

func (s *workflowOwnedJobInfoStore) Add(_ context.Context, entity datastore.Entity) error {
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		copied := *jobInfo
		s.addedJobInfo = &copied
	}
	return nil
}

func (s *workflowOwnedJobInfoStore) List(_ context.Context, entity datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	if _, ok := entity.(*model.ApplicationComponent); ok {
		components := make([]datastore.Entity, 0, len(s.components))
		for _, component := range s.components {
			components = append(components, component)
		}
		return components, nil
	}
	query, ok := entity.(*model.JobInfo)
	if !ok {
		return s.jobInfos, nil
	}
	filtered := make([]datastore.Entity, 0, len(s.jobInfos))
	for _, candidate := range s.jobInfos {
		jobInfo, ok := candidate.(*model.JobInfo)
		if !ok || jobInfo == nil || (query.TaskID != "" && jobInfo.TaskID != query.TaskID) {
			continue
		}
		matches := true
		if options != nil {
			for _, filter := range options.In {
				var value string
				switch filter.Key {
				case "type":
					value = jobInfo.Type
				case "service_name":
					value = jobInfo.ServiceName
				case "execution_key":
					value = jobInfoExecutionKey(*jobInfo)
				case "run_generation":
					value = strconv.FormatUint(jobInfo.RunGeneration, 10)
				default:
					continue
				}
				if !containsJobInfoFilterValue(filter.Values, value) {
					matches = false
					break
				}
			}
		}
		if matches {
			filtered = append(filtered, jobInfo)
		}
	}
	return filtered, nil
}

func containsJobInfoFilterValue(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (s *workflowOwnedJobInfoStore) Put(_ context.Context, entity datastore.Entity) error {
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil {
		return datastore.ErrEntityInvalid
	}
	copied := *jobInfo
	s.putJobInfo = &copied
	return nil
}

func (s *workflowOwnedJobInfoStore) CompareAndSwapWithConditions(
	_ context.Context,
	entity datastore.Entity,
	conditions map[string]interface{},
	updates map[string]interface{},
) (bool, error) {
	switch typed := entity.(type) {
	case *model.WorkflowQueue:
		if typed == nil || s.workflowTask == nil || typed.TaskID != s.workflowTask.TaskID {
			return false, nil
		}
		if conditions["run_generation"] != s.workflowTask.RunGeneration ||
			conditions["run_token"] != s.workflowTask.RunToken ||
			conditions["worker_id"] != s.workflowTask.WorkerID {
			return false, nil
		}
		return true, nil
	case *model.JobInfo:
		if typed == nil {
			return false, nil
		}
		for _, entity := range s.jobInfos {
			current, ok := entity.(*model.JobInfo)
			if !ok || current == nil || current.ID != typed.ID {
				continue
			}
			if current.Status != conditions["status"] ||
				jobInfoExecutionKey(*current) != conditions["execution_key"] ||
				current.RunGeneration != conditions["run_generation"] ||
				current.Attempt != conditions["attempt"] {
				return false, nil
			}
			if status, ok := updates["status"].(string); ok {
				current.Status = status
			}
			if jobError, ok := updates["error"].(string); ok {
				current.Error = jobError
			}
			if startTime, ok := updates["start_time"].(int64); ok {
				current.StartTime = startTime
			}
			if endTime, ok := updates["end_time"].(int64); ok {
				current.EndTime = endTime
			}
			if info, ok := updates["info"].(string); ok {
				current.Info = info
			}
			copied := *current
			s.putJobInfo = &copied
			return true, nil
		}
	case *model.ApplicationComponent:
		for _, component := range s.components {
			if !componentMatchesConditions(component, conditions) {
				continue
			}
			applyComponentRuntimeUpdateMap(component, updates)
			copied := *component
			s.updatedComponent = &copied
			return true, nil
		}
	}
	return false, nil
}

var _ datastore.Transactional = (*workflowOwnedJobInfoStore)(nil)
var _ datastore.ConditionalCompareAndSwap = (*workflowOwnedJobInfoStore)(nil)

func TestSaveJobInfoUsesWorkflowOwnershipFence(t *testing.T) {
	current := &model.WorkflowQueue{
		TaskID:        "task-1",
		RunGeneration: 2,
		RunToken:      "token-2",
		WorkerID:      "worker-b",
	}

	t.Run("stale worker cannot persist", func(t *testing.T) {
		store := &workflowOwnedJobInfoStore{workflowTask: current}
		job := &model.JobTask{
			TaskID:        current.TaskID,
			Name:          "api",
			JobType:       string(config.JobDeploy),
			RunGeneration: 1,
			RunToken:      "token-1",
			WorkerID:      "worker-a",
		}

		err := saveJobInfo(context.Background(), store, job)

		require.ErrorIs(t, err, repository.ErrWorkflowOwnershipLost)
		require.Nil(t, store.addedJobInfo)
		require.Equal(t, 1, store.transactionCalls)
	})

	t.Run("current owner persists", func(t *testing.T) {
		store := &workflowOwnedJobInfoStore{workflowTask: current}
		job := &model.JobTask{
			TaskID:        current.TaskID,
			Name:          "api",
			JobType:       string(config.JobDeploy),
			RunGeneration: current.RunGeneration,
			RunToken:      current.RunToken,
			WorkerID:      current.WorkerID,
		}

		err := saveJobInfo(context.Background(), store, job)

		require.NoError(t, err)
		require.NotNil(t, store.addedJobInfo)
		require.Equal(t, current.RunGeneration, store.addedJobInfo.RunGeneration)
		require.Equal(t, 1, store.transactionCalls)
	})
}

func TestUpdateJobInfoStatusUsesExecutionOwnershipFence(t *testing.T) {
	current := &model.WorkflowQueue{
		TaskID:        "task-result",
		RunGeneration: 2,
		RunToken:      "token-2",
		WorkerID:      "worker-b",
	}
	oldExecutionKey := "execution-1"
	currentExecutionKey := "execution-2"
	jobInfos := func() []datastore.Entity {
		return []datastore.Entity{
			&model.JobInfo{
				ID:            1,
				TaskID:        current.TaskID,
				Type:          string(config.JobDeployScheduled),
				ServiceName:   "svc-a",
				Status:        string(config.StatusWaiting),
				ExecutionKey:  &oldExecutionKey,
				RunGeneration: 1,
			},
			&model.JobInfo{
				ID:            2,
				TaskID:        current.TaskID,
				Type:          string(config.JobDeployScheduled),
				ServiceName:   "svc-a",
				Status:        string(config.StatusWaiting),
				ExecutionKey:  &currentExecutionKey,
				RunGeneration: current.RunGeneration,
			},
		}
	}

	t.Run("stale generation cannot update", func(t *testing.T) {
		store := &workflowOwnedJobInfoStore{workflowTask: current, jobInfos: jobInfos()}
		payload := &JobResultPayload{
			TaskID:        current.TaskID,
			Name:          "job-svc-a",
			Namespace:     "default",
			JobType:       string(config.JobDeployScheduled),
			ServiceName:   "svc-a",
			ExecutionKey:  oldExecutionKey,
			RunGeneration: 1,
			RunToken:      "token-1",
			WorkerID:      "worker-a",
		}

		err := updateJobInfoStatus(context.Background(), store, payload, config.StatusCompleted, "", 0, 1, "")

		require.ErrorIs(t, err, errResultDispatchNoRetry)
		require.ErrorIs(t, err, repository.ErrWorkflowOwnershipLost)
		require.Nil(t, store.putJobInfo)
		require.Equal(t, 1, store.transactionCalls)
	})

	t.Run("current generation updates exact job info", func(t *testing.T) {
		store := &workflowOwnedJobInfoStore{workflowTask: current, jobInfos: jobInfos()}
		payload := &JobResultPayload{
			TaskID:        current.TaskID,
			Name:          "job-svc-a",
			Namespace:     "default",
			JobType:       string(config.JobDeployScheduled),
			ServiceName:   "svc-a",
			ExecutionKey:  currentExecutionKey,
			RunGeneration: current.RunGeneration,
			RunToken:      current.RunToken,
			WorkerID:      current.WorkerID,
		}

		err := updateJobInfoStatus(context.Background(), store, payload, config.StatusCompleted, "", 0, 1, "")

		require.NoError(t, err)
		require.NotNil(t, store.putJobInfo)
		require.Equal(t, 2, store.putJobInfo.ID)
		require.Equal(t, current.RunGeneration, store.putJobInfo.RunGeneration)
		require.Equal(t, string(config.StatusCompleted), store.putJobInfo.Status)
		require.Equal(t, 1, store.transactionCalls)
	})
}

func TestUpdateJobInfoStatusUsesLegacyExecutionKeyWithoutFencing(t *testing.T) {
	otherKey := "execution-other"
	expectedKey := "execution-legacy"
	store := &workflowOwnedJobInfoStore{jobInfos: []datastore.Entity{
		&model.JobInfo{
			ID:            1,
			TaskID:        "task-legacy",
			Type:          string(config.JobDeployScheduled),
			ServiceName:   "svc-a",
			Status:        string(config.StatusDistributed),
			ExecutionKey:  &otherKey,
			RunGeneration: 1,
		},
		&model.JobInfo{
			ID:            2,
			TaskID:        "task-legacy",
			Type:          string(config.JobDeployScheduled),
			ServiceName:   "svc-a",
			Status:        string(config.StatusDistributed),
			ExecutionKey:  &expectedKey,
			RunGeneration: 1,
		},
	}}
	payload := &JobResultPayload{
		TaskID:        "task-legacy",
		Name:          "job-svc-a",
		Namespace:     "default",
		JobType:       string(config.JobDeployScheduled),
		ServiceName:   "svc-a",
		ExecutionKey:  expectedKey,
		RunGeneration: 1,
	}

	err := updateJobInfoStatus(context.Background(), store, payload, config.StatusCompleted, "", 0, 1, "logs")

	require.NoError(t, err)
	require.NotNil(t, store.putJobInfo)
	require.Equal(t, 2, store.putJobInfo.ID)
	require.Equal(t, string(config.StatusCompleted), store.putJobInfo.Status)
	require.Equal(t, "logs", store.putJobInfo.Info)
	require.Zero(t, store.transactionCalls)
}

type recordingTerminalJobCtl struct {
	saveCalls int
	saveErr   error
	onSave    func()
}

func (*recordingTerminalJobCtl) Run(context.Context) error { return nil }
func (*recordingTerminalJobCtl) Clean(context.Context)     {}
func (c *recordingTerminalJobCtl) SaveInfo(context.Context) error {
	c.saveCalls++
	if c.onSave != nil {
		c.onSave()
	}
	return c.saveErr
}

func TestPersistTerminalJobStateSkipsInfrastructureCancellation(t *testing.T) {
	job := &model.JobTask{JobType: "unknown", Status: config.StatusCancelled}
	store := &noopStore{}

	cancelledCtx, cancel := context.WithCancelCause(context.Background())
	cancel(repository.ErrWorkflowLeaseRenewalFailed)
	cancelledCtl := &recordingTerminalJobCtl{}
	persistTerminalJobState(cancelledCtx, cancelledCtl, job, store, nil)
	require.Zero(t, cancelledCtl.saveCalls)

	regularCancelledCtx, regularCancel := context.WithCancel(context.Background())
	regularCancel()
	regularCancelledCtl := &recordingTerminalJobCtl{}
	persistTerminalJobState(regularCancelledCtx, regularCancelledCtl, job, store, nil)
	require.Equal(t, 1, regularCancelledCtl.saveCalls)

	nilContextCtl := &recordingTerminalJobCtl{}
	persistTerminalJobState(nil, nilContextCtl, job, store, nil)
	require.Equal(t, 1, nilContextCtl.saveCalls)

	activeCtl := &recordingTerminalJobCtl{}
	persistTerminalJobState(context.Background(), activeCtl, job, store, nil)
	require.Equal(t, 1, activeCtl.saveCalls)
}

func TestPersistTerminalJobStateDoesNotProjectComponentAfterSaveFailure(t *testing.T) {
	store := &componentStatusStore{
		components: []*model.ApplicationComponent{{
			AppID:         "app-1",
			Name:          "app-config",
			ComponentType: config.ConfJob,
		}},
	}
	jobTask := &model.JobTask{
		Name:    "app-config",
		AppID:   "app-1",
		JobType: string(config.JobDeployConfigMap),
		Status:  config.StatusCompleted,
	}
	ctl := &recordingTerminalJobCtl{saveErr: errors.New("workflow ownership changed")}

	persistTerminalJobState(context.Background(), ctl, jobTask, store, nil)

	require.Equal(t, 1, ctl.saveCalls)
	require.Nil(t, store.updated)
}

func TestPersistTerminalJobStateFencesComponentProjectionAfterOwnershipTransfer(t *testing.T) {
	store := &workflowOwnedJobInfoStore{
		workflowTask: &model.WorkflowQueue{
			TaskID: "task-1", RunGeneration: 1, RunToken: "token-1", WorkerID: "worker-a",
		},
		components: []*model.ApplicationComponent{{
			AppID: "app-1", Name: "app-config", ComponentType: config.ConfJob,
		}},
	}
	jobTask := &model.JobTask{
		TaskID: "task-1", AppID: "app-1", Name: "app-config",
		JobType: string(config.JobDeployConfigMap), Status: config.StatusCompleted,
		RunGeneration: 1, RunToken: "token-1", WorkerID: "worker-a",
	}
	ctl := &recordingTerminalJobCtl{onSave: func() {
		store.workflowTask = &model.WorkflowQueue{
			TaskID: "task-1", RunGeneration: 2, RunToken: "token-2", WorkerID: "worker-b",
		}
	}}

	persistTerminalJobState(context.Background(), ctl, jobTask, store, nil)

	require.Equal(t, 1, ctl.saveCalls)
	require.Equal(t, 1, store.transactionCalls)
	require.Nil(t, store.updatedComponent)
	require.Empty(t, store.components[0].Status)
}
