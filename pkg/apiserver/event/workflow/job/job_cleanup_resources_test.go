package job

import (
	"context"
	"encoding/json"

	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type cleanupComponentStore struct {
	noopStore
	component             *model.ApplicationComponent
	workflowTask          *model.WorkflowQueue
	beforeWorkflowTaskGet func()
	workflowTaskGetErr    error
	putComponent          *model.ApplicationComponent
	jobInfo               *model.JobInfo
	addedJobInfo          *model.JobInfo
	putJobInfo            *model.JobInfo
	casJobInfo            *model.JobInfo
	casErr                error
	beforeConditionalCAS  func()
}

type cleanupMutableErrorContext struct {
	context.Context
	err error
}

func (c *cleanupMutableErrorContext) Err() error { return c.err }

func (s *cleanupComponentStore) Get(ctx context.Context, entity datastore.Entity) error {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return s.noopStore.Get(ctx, entity)
	}
	if s.beforeWorkflowTaskGet != nil {
		hook := s.beforeWorkflowTaskGet
		s.beforeWorkflowTaskGet = nil
		hook()
	}
	if s.workflowTaskGetErr != nil {
		return s.workflowTaskGetErr
	}
	if s.workflowTask == nil {
		// Existing cleanup tests model an actively executing workflow unless they
		// explicitly install a persisted task state.
		task.Status = config.StatusRunning
		return nil
	}
	taskCopy := *s.workflowTask
	*task = taskCopy
	return nil
}

func (s *cleanupComponentStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	switch query.(type) {
	case *model.ApplicationComponent:
		if s.component == nil {
			return nil, nil
		}
		return []datastore.Entity{s.component}, nil
	case *model.JobInfo:
		if s.jobInfo == nil {
			return nil, nil
		}
		return []datastore.Entity{s.jobInfo}, nil
	default:
		return nil, nil
	}
}

func (s *cleanupComponentStore) Add(_ context.Context, entity datastore.Entity) error {
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		jobInfoCopy := *jobInfo
		s.addedJobInfo = &jobInfoCopy
	}
	return nil
}

func (s *cleanupComponentStore) Put(_ context.Context, entity datastore.Entity) error {
	switch value := entity.(type) {
	case *model.ApplicationComponent:
		component := value
		componentCopy := *component
		s.putComponent = &componentCopy
	case *model.JobInfo:
		jobInfoCopy := *value
		s.putJobInfo = &jobInfoCopy
		s.jobInfo = &jobInfoCopy
	}
	return nil
}

func (s *cleanupComponentStore) CompareAndSwap(_ context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	if s.casErr != nil {
		return false, s.casErr
	}
	jobInfo, ok := entity.(*model.JobInfo)
	if !ok || jobInfo == nil || s.jobInfo == nil {
		return false, nil
	}
	if jobInfo.ID != 0 && s.jobInfo.ID != jobInfo.ID {
		return false, nil
	}
	if jobInfo.ID == 0 && (s.jobInfo.TaskID != jobInfo.TaskID || s.jobInfo.Type != jobInfo.Type || s.jobInfo.ServiceName != jobInfo.ServiceName) {
		return false, nil
	}
	if conditionField != "status" || s.jobInfo.Status != conditionValue {
		return false, nil
	}
	for key, value := range updates {
		switch key {
		case "status":
			if status, ok := value.(string); ok {
				s.jobInfo.Status = status
			}
		case "start_time":
			if startTime, ok := value.(int64); ok {
				s.jobInfo.StartTime = startTime
			}
		case "end_time":
			if endTime, ok := value.(int64); ok {
				s.jobInfo.EndTime = endTime
			}
		case "error":
			if message, ok := value.(string); ok {
				s.jobInfo.Error = message
			}
		case "internal_info":
			if internalInfo, ok := value.(string); ok {
				s.jobInfo.InternalInfo = internalInfo
			}
		}
	}
	jobInfoCopy := *s.jobInfo
	s.casJobInfo = &jobInfoCopy
	return true, nil
}

func (s *cleanupComponentStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if s.casErr != nil {
		return false, s.casErr
	}
	if s.beforeConditionalCAS != nil {
		hook := s.beforeConditionalCAS
		s.beforeConditionalCAS = nil
		hook()
	}
	if jobInfo, ok := entity.(*model.JobInfo); ok {
		if jobInfo == nil || s.jobInfo == nil {
			return false, nil
		}
		if expectedStatus, found := conditions["status"]; found && s.jobInfo.Status != expectedStatus {
			return false, nil
		}
		if expectedInternalInfo, found := conditions["internal_info"]; found && s.jobInfo.InternalInfo != expectedInternalInfo {
			return false, nil
		}
		if expectedGeneration, found := conditions["run_generation"]; found && s.jobInfo.RunGeneration != expectedGeneration {
			return false, nil
		}
		if expectedAttempt, found := conditions["attempt"]; found && s.jobInfo.Attempt != expectedAttempt {
			return false, nil
		}
		if expectedExecutionKey, found := conditions["execution_key"]; found {
			switch value := expectedExecutionKey.(type) {
			case nil:
				if s.jobInfo.ExecutionKey != nil {
					return false, nil
				}
			case string:
				if s.jobInfo.ExecutionKey == nil || *s.jobInfo.ExecutionKey != value {
					return false, nil
				}
			default:
				return false, nil
			}
		}
		for key, value := range updates {
			switch key {
			case "type":
				s.jobInfo.Type, _ = value.(string)
			case "workflow_id":
				s.jobInfo.WorkflowID, _ = value.(string)
			case "product_id":
				s.jobInfo.ProductID, _ = value.(string)
			case "app_id":
				s.jobInfo.AppID, _ = value.(string)
			case "task_id":
				s.jobInfo.TaskID, _ = value.(string)
			case "status":
				s.jobInfo.Status, _ = value.(string)
			case "start_time":
				s.jobInfo.StartTime, _ = value.(int64)
			case "end_time":
				s.jobInfo.EndTime, _ = value.(int64)
			case "info":
				s.jobInfo.Info, _ = value.(string)
			case "internal_info":
				s.jobInfo.InternalInfo, _ = value.(string)
			case "service_name":
				s.jobInfo.ServiceName, _ = value.(string)
			case "error":
				s.jobInfo.Error, _ = value.(string)
			case "production":
				s.jobInfo.Production, _ = value.(bool)
			case "target_env":
				s.jobInfo.TargetEnv, _ = value.(string)
			case "execution_key":
				switch executionKey := value.(type) {
				case nil:
					s.jobInfo.ExecutionKey = nil
				case string:
					executionKeyCopy := executionKey
					s.jobInfo.ExecutionKey = &executionKeyCopy
				}
			case "run_generation":
				if generation, ok := value.(uint64); ok {
					s.jobInfo.RunGeneration = generation
				}
			case "attempt":
				if attempt, ok := value.(uint); ok {
					s.jobInfo.Attempt = attempt
				}
			}
		}
		jobInfoCopy := *s.jobInfo
		s.casJobInfo = &jobInfoCopy
		return true, nil
	}
	component, ok := entity.(*model.ApplicationComponent)
	if !ok || component == nil || s.component == nil {
		return false, nil
	}
	if !componentMatchesConditions(s.component, conditions) {
		return false, nil
	}
	applyComponentRuntimeUpdateMap(s.component, updates)
	componentCopy := *s.component
	s.putComponent = &componentCopy
	return true, nil
}

func versionUpdateRemoveCleanupInternalInfo() string {
	return `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `"}`
}

func versionUpdateRequireStatefulSetDeletionInternalInfo() string {
	return `{"source":"` + config.JobInfoSourceVersionUpdateRemove + `","requireStatefulSetDeletion":true}`
}

func versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t *testing.T, templates ...string) string {
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

func mapKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
