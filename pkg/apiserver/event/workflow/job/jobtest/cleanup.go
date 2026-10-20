package jobtest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type CleanupComponentStore struct {
	NoopStore
	Component             *model.ApplicationComponent
	WorkflowTask          *model.WorkflowQueue
	BeforeWorkflowTaskGet func()
	WorkflowTaskGetErr    error
	PutComponent          *model.ApplicationComponent
	JobInfo               *model.JobInfo
	AddedJobInfo          *model.JobInfo
	PutJobInfo            *model.JobInfo
	CasJobInfo            *model.JobInfo
	CasErr                error
	BeforeConditionalCAS  func()
}

func (s *CleanupComponentStore) Get(ctx context.Context, entity datastore.Entity) error {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return s.NoopStore.Get(ctx, entity)
	}
	if s.BeforeWorkflowTaskGet != nil {
		hook := s.BeforeWorkflowTaskGet
		s.BeforeWorkflowTaskGet = nil
		hook()
	}
	if s.WorkflowTaskGetErr != nil {
		return s.WorkflowTaskGetErr
	}
	if s.WorkflowTask == nil {
		// Existing cleanup tests model an actively executing workflow unless they
		// explicitly install a persisted task state.
		task.Status = config.StatusRunning
		return nil
	}
	taskCopy := *s.WorkflowTask
	*task = taskCopy
	return nil
}

func (s *CleanupComponentStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	switch query.(type) {
	case *model.ApplicationComponent:
		if s.Component == nil {
			return nil, nil
		}
		return []datastore.Entity{s.Component}, nil
	case *model.JobInfo:
		if s.JobInfo == nil {
			return nil, nil
		}
		return []datastore.Entity{s.JobInfo}, nil
	default:
		return nil, nil
	}
}

func (s *CleanupComponentStore) Add(_ context.Context, entity datastore.Entity) error {
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		jobInfoCopy := *jobInfo
		s.AddedJobInfo = &jobInfoCopy
	}
	return nil
}

func (s *CleanupComponentStore) Put(_ context.Context, entity datastore.Entity) error {
	switch value := entity.(type) {
	case *model.ApplicationComponent:
		component := value
		componentCopy := *component
		s.PutComponent = &componentCopy
	case *model.JobInfo:
		jobInfoCopy := *value
		s.PutJobInfo = &jobInfoCopy
		s.JobInfo = &jobInfoCopy
	}
	return nil
}

func (s *CleanupComponentStore) CompareAndSwap(_ context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	if s.CasErr != nil {
		return false, s.CasErr
	}
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil || s.JobInfo == nil {
		return false, nil
	}
	if jobInfo.ID != 0 && s.JobInfo.ID != jobInfo.ID {
		return false, nil
	}
	if jobInfo.ID == 0 && (s.JobInfo.TaskID != jobInfo.TaskID || s.JobInfo.Type != jobInfo.Type || s.JobInfo.ServiceName != jobInfo.ServiceName) {
		return false, nil
	}
	if conditionField != "status" || s.JobInfo.Status != conditionValue {
		return false, nil
	}
	for key, value := range updates {
		switch key {
		case "status":
			if status, ok := value.(string); ok {
				s.JobInfo.Status = status
			}
		case "start_time":
			if startTime, ok := value.(int64); ok {
				s.JobInfo.StartTime = startTime
			}
		case "end_time":
			if endTime, ok := value.(int64); ok {
				s.JobInfo.EndTime = endTime
			}
		case "error":
			if message, ok := value.(string); ok {
				s.JobInfo.Error = message
			}
		case "internal_info":
			if internalInfo, ok := value.(string); ok {
				s.JobInfo.InternalInfo = internalInfo
			}
		}
	}
	jobInfoCopy := *s.JobInfo
	s.CasJobInfo = &jobInfoCopy
	return true, nil
}

func (s *CleanupComponentStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if s.CasErr != nil {
		return false, s.CasErr
	}
	if s.BeforeConditionalCAS != nil {
		hook := s.BeforeConditionalCAS
		s.BeforeConditionalCAS = nil
		hook()
	}
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		if jobInfo == nil || s.JobInfo == nil {
			return false, nil
		}
		if expectedStatus, found := conditions["status"]; found && s.JobInfo.Status != expectedStatus {
			return false, nil
		}
		if expectedInternalInfo, found := conditions["internal_info"]; found && s.JobInfo.InternalInfo != expectedInternalInfo {
			return false, nil
		}
		if expectedGeneration, found := conditions["run_generation"]; found && s.JobInfo.RunGeneration != expectedGeneration {
			return false, nil
		}
		if expectedAttempt, found := conditions["attempt"]; found && s.JobInfo.Attempt != expectedAttempt {
			return false, nil
		}
		if expectedExecutionKey, found := conditions["execution_key"]; found {
			switch value := expectedExecutionKey.(type) {
			case nil:
				if s.JobInfo.ExecutionKey != nil {
					return false, nil
				}
			case string:
				if s.JobInfo.ExecutionKey == nil || *s.JobInfo.ExecutionKey != value {
					return false, nil
				}
			default:
				return false, nil
			}
		}
		s.ApplyJobInfoUpdates(updates)
		jobInfoCopy := *s.JobInfo
		s.CasJobInfo = &jobInfoCopy
		return true, nil
	}
	component, ok := entity.(*model.ApplicationComponent)
	if !ok || component == nil || s.Component == nil {
		return false, nil
	}
	if !ComponentMatchesConditions(s.Component, conditions) {
		return false, nil
	}
	ApplyComponentRuntimeUpdateMap(s.Component, updates)
	componentCopy := *s.Component
	s.PutComponent = &componentCopy
	return true, nil
}

func (s *CleanupComponentStore) ApplyJobInfoUpdates(updates map[string]interface{}) {
	for key, value := range updates {
		switch key {
		case "type":
			s.JobInfo.Type, _ = value.(string)
		case "workflow_id":
			s.JobInfo.WorkflowID, _ = value.(string)
		case "product_id":
			s.JobInfo.ProductID, _ = value.(string)
		case "app_id":
			s.JobInfo.AppID, _ = value.(string)
		case "task_id":
			s.JobInfo.TaskID, _ = value.(string)
		case "status":
			s.JobInfo.Status, _ = value.(string)
		case "start_time":
			s.JobInfo.StartTime, _ = value.(int64)
		case "end_time":
			s.JobInfo.EndTime, _ = value.(int64)
		case "info":
			s.JobInfo.Info, _ = value.(string)
		case "internal_info":
			s.JobInfo.InternalInfo, _ = value.(string)
		case "service_name":
			s.JobInfo.ServiceName, _ = value.(string)
		case "error":
			s.JobInfo.Error, _ = value.(string)
		case "production":
			s.JobInfo.Production, _ = value.(bool)
		case "target_env":
			s.JobInfo.TargetEnv, _ = value.(string)
		case "execution_key":
			switch executionKey := value.(type) {
			case nil:
				s.JobInfo.ExecutionKey = nil
			case string:
				executionKeyCopy := executionKey
				s.JobInfo.ExecutionKey = &executionKeyCopy
			}
		case "run_generation":
			if generation, ok := value.(uint64); ok {
				s.JobInfo.RunGeneration = generation
			}
		case "attempt":
			if attempt, ok := value.(uint); ok {
				s.JobInfo.Attempt = attempt
			}
		}
	}
}

func VersionUpdateRemoveCleanupInternalInfo() string {
	return `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `"}`
}

func VersionUpdateRequireStatefulSetDeletionInternalInfo() string {
	return `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `","requireStatefulSetDeletion":true}`
}

func VersionUpdateRequireStatefulSetPVCDeletionInternalInfo(t *testing.T, templates ...string) string {
	t.Helper()
	payload, err := json.Marshal(struct {
		Source                          string   `json:"source"`
		Version                         int      `json:"version"`
		RequireStatefulSetDeletion      bool     `json:"requireStatefulSetDeletion"`
		StatefulSetPVCTemplatesToDelete []string `json:"statefulSetPVCTemplatesToDelete"`
	}{
		Source:                          config.JobInfoSourceVersionUpdateRemove,
		Version:                         model.VersionUpdateCleanupInfoVersionStatefulSetPVCDeletion,
		RequireStatefulSetDeletion:      true,
		StatefulSetPVCTemplatesToDelete: templates,
	})
	require.NoError(t, err)
	return string(payload)
}

func MapKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
