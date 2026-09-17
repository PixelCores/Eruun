package job

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type jobInfoSaveStore struct {
	noopStore
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
			jobInfoExecutionKey(*current) != conditions["execution_key"] ||
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

func evaluationCheckpoint(t *testing.T, task *model.JobTask, sequence uint64) string {
	t.Helper()
	job := task.JobInfo.(*batchv1.Job).DeepCopy()
	attempt := task.Attempt
	if attempt == 0 {
		attempt = 1
	}
	job.Annotations[workflowconfig.AnnotationJobAttempt] = fmt.Sprintf("%d", attempt)
	runner := json.RawMessage(nil)
	if sequence > 0 {
		runner = json.RawMessage(fmt.Sprintf(`{"protocolVersion":"v1","lastSequence":%d}`, sequence))
	}
	checkpoint := instantJobRetryCheckpoint{
		Kind: "instant_job_retry", Version: 1, Attempt: attempt, Deadline: time.Now().Add(time.Hour).UnixNano(), Job: job, Runner: runner,
	}
	if attempt > 1 {
		checkpoint.PreviousUID, checkpoint.RetryAt = "previous-job", time.Now().UnixNano()
	}
	raw, err := json.Marshal(checkpoint)
	require.NoError(t, err)
	return string(raw)
}

func TestSaveJobInfoPreservesConcurrentEvaluationRunnerCheckpoint(t *testing.T) {
	task := retryTestTask(t, &workflowconfig.JobRetryPolicy{OnOOM: "stop"})
	task.JobType, task.WorkspaceID, task.Status = string(config.JobEval), "space", config.StatusCompleted
	task.InternalInfo = evaluationCheckpoint(t, task, 0)
	executionKey := task.ExecutionKey
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID: 7, Type: task.JobType, TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, ServiceName: task.Name,
		Status: string(config.StatusRunning), ExecutionKey: &executionKey, RunGeneration: task.RunGeneration,
		Attempt: 1, InternalInfo: evaluationCheckpoint(t, task, 4),
	}}}
	store.beforeCAS = func() {
		store.existing[0].InternalInfo = evaluationCheckpoint(t, task, 5)
	}

	require.NoError(t, saveJobInfo(context.Background(), store, task))
	require.NotNil(t, store.updated)
	state, _, err := EvaluationRunnerCheckpoint(store.updated)
	require.NoError(t, err)
	require.JSONEq(t, `{"protocolVersion":"v1","lastSequence":5}`, string(state))
}

func TestNewEvaluationAttemptDoesNotInheritRunnerCheckpoint(t *testing.T) {
	task := retryTestTask(t, retryTestPolicy())
	task.JobType, task.WorkspaceID = string(config.JobEval), "space"
	executionKey := task.ExecutionKey
	existing := &model.JobInfo{Type: task.JobType, TaskID: task.TaskID, WorkspaceID: task.WorkspaceID, ExecutionKey: &executionKey,
		RunGeneration: task.RunGeneration, Attempt: 1, InternalInfo: evaluationCheckpoint(t, task, 3)}
	desired := *existing
	desired.Attempt = 2
	task.Attempt = 2
	desired.InternalInfo = evaluationCheckpoint(t, task, 0)

	require.NoError(t, preserveEvaluationRunnerCheckpoint(existing, &desired))
	state, _, err := EvaluationRunnerCheckpoint(&desired)
	require.NoError(t, err)
	require.Empty(t, state)
}

func TestSaveJobInfoUpdatesRecoveredExecutionTerminalState(t *testing.T) {
	executionKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusFailed),
		Error:         "first execution failed",
		ExecutionKey:  &executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusCompleted,
		ExecutionKey:  executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.added)
	require.NotNil(t, store.updated)
	require.Equal(t, 7, store.updated.ID)
	require.Equal(t, string(config.StatusCompleted), store.updated.Status)
	require.Empty(t, store.updated.Error)
}

func TestSaveJobInfoKeepsDistinctExecutionsSeparate(t *testing.T) {
	existingKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusCompleted),
		ExecutionKey:  &existingKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusCompleted,
		ExecutionKey:  "execution-2",
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.updated)
	require.NotNil(t, store.added)
	require.NotNil(t, store.added.ExecutionKey)
	require.Equal(t, "execution-2", *store.added.ExecutionKey)
}

func TestSaveJobInfoPreservesSuccessfulTerminalState(t *testing.T) {
	executionKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusCompleted),
		ExecutionKey:  &executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusDistributed,
		ExecutionKey:  executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.added)
	require.Nil(t, store.updated)
	require.Equal(t, string(config.StatusCompleted), store.existing[0].Status)
}

func TestSaveJobInfoReloadsAfterConcurrentTerminalUpdate(t *testing.T) {
	executionKey := "execution-1"
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:            7,
		Type:          string(config.JobDeployCallback),
		TaskID:        "task-1",
		ServiceName:   "callback",
		Status:        string(config.StatusRunning),
		ExecutionKey:  &executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}}}
	store.beforeCAS = func() {
		store.existing[0].Status = string(config.StatusCompleted)
	}
	job := &model.JobTask{
		Name:          "callback",
		TaskID:        "task-1",
		JobType:       string(config.JobDeployCallback),
		Status:        config.StatusDistributed,
		ExecutionKey:  executionKey,
		RunGeneration: 3,
		Attempt:       1,
	}

	require.NoError(t, saveJobInfo(context.Background(), store, job))
	require.Nil(t, store.added)
	require.Nil(t, store.updated)
	require.Equal(t, string(config.StatusCompleted), store.existing[0].Status)
}
