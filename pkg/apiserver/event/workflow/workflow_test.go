package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type fakeDataStore struct {
	workflow       *model.Workflow
	application    *model.Applications
	components     []*model.ApplicationComponent
	jobInfos       []*model.JobInfo
	jobInfoListErr error
}

func (f *fakeDataStore) Add(context.Context, datastore.Entity) error {
	return fmt.Errorf("not implemented")
}

func (f *fakeDataStore) BatchAdd(context.Context, []datastore.Entity) error {
	return fmt.Errorf("not implemented")
}

func (f *fakeDataStore) Put(context.Context, datastore.Entity) error {
	return fmt.Errorf("not implemented")
}

func (f *fakeDataStore) Delete(context.Context, datastore.Entity) error {
	return fmt.Errorf("not implemented")
}

func (f *fakeDataStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return fmt.Errorf("not implemented")
}

func (f *fakeDataStore) Get(ctx context.Context, entity datastore.Entity) error {
	switch e := entity.(type) {
	case *model.Workflow:
		*e = *f.workflow
		return nil
	case *model.Applications:
		if f.application != nil {
			*e = *f.application
			return nil
		}
		e.Name = e.ID
		return nil
	default:
		return fmt.Errorf("unsupported entity type %T", entity)
	}
}

func (f *fakeDataStore) List(ctx context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	switch query.(type) {
	case *model.ApplicationComponent:
		result := make([]datastore.Entity, len(f.components))
		for i, c := range f.components {
			result[i] = c
		}
		return result, nil
	case *model.JobInfo:
		if f.jobInfoListErr != nil {
			return nil, f.jobInfoListErr
		}
		var result []datastore.Entity
		for _, jobInfo := range f.jobInfos {
			if jobInfo == nil {
				continue
			}
			if q, ok := query.(*model.JobInfo); ok {
				if q.TaskID != "" && jobInfo.TaskID != q.TaskID {
					continue
				}
			}
			result = append(result, jobInfo)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported list query %T", query)
	}
}

func (f *fakeDataStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (f *fakeDataStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (f *fakeDataStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (f *fakeDataStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return true, nil
}

func (f *fakeDataStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, _ map[string]interface{}, updates map[string]interface{}) (bool, error) {
	component, ok := entity.(*model.ApplicationComponent)
	if !ok {
		return true, nil
	}
	for _, current := range f.components {
		if current == nil || current.AppID != component.AppID || !strings.EqualFold(current.Name, component.Name) {
			continue
		}
		if status, ok := updates["status"].(string); ok {
			current.Status = status
		}
		if readyReplicas, ok := updates["ready_replicas"].(int32); ok {
			current.ReadyReplicas = readyReplicas
		}
		if lastAbnormal, ok := updates["last_abnormal"].(string); ok {
			current.LastAbnormal = lastAbnormal
		}
		return true, nil
	}
	return true, nil
}

func mustGenerateJobTasks(t *testing.T, ctx context.Context, task *model.WorkflowQueue, store datastore.DataStore, defaultJobTimeoutSeconds int64) []StepExecution {
	t.Helper()
	executions, err := GenerateJobTasks(ctx, task, store, defaultJobTimeoutSeconds)
	require.NoError(t, err)
	return executions
}

func mustVersionUpdateCleanupInfo(t *testing.T, components ...model.VersionUpdateCleanupComponent) string {
	t.Helper()
	payload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:     config.JobInfoSourceVersionUpdateRemove,
		Version:    1,
		Components: components,
	})
	require.NoError(t, err)
	return string(payload)
}

func mustVersionUpdateCleanupOnlyInfo(t *testing.T, components ...model.VersionUpdateCleanupComponent) string {
	t.Helper()
	payload, err := json.Marshal(model.VersionUpdateCleanupInfo{
		Source:      config.JobInfoSourceVersionUpdateRemove,
		Version:     1,
		CleanupOnly: true,
		Components:  components,
	})
	require.NoError(t, err)
	return string(payload)
}

func mustVersionUpdateResourceActionInfo(t *testing.T, restartOnly bool, components ...string) string {
	t.Helper()
	payload, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source:                   config.JobInfoSourceVersionUpdateAction,
		Version:                  1,
		RestartOnly:              restartOnly,
		RestartComponents:        components,
		ImageReadyTimeoutSeconds: int64(config.DefaultVersionUpdateImageReadyTimeout),
	})
	require.NoError(t, err)
	return string(payload)
}

func mustVersionUpdateMarkerInfo(t *testing.T) string {
	t.Helper()
	payload, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source:  config.JobInfoSourceVersionUpdateAction,
		Version: 1,
	})
	require.NoError(t, err)
	return string(payload)
}

func mustVersionUpdateImageReadyActionInfo(t *testing.T, timeoutSeconds int64, components ...string) string {
	t.Helper()
	payload, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source:                   config.JobInfoSourceVersionUpdateAction,
		Version:                  1,
		ImageReadyComponents:     components,
		ImageReadyTimeoutSeconds: timeoutSeconds,
	})
	require.NoError(t, err)
	return string(payload)
}

func mustVersionUpdateExecutionScopeActionInfo(t *testing.T, components ...string) string {
	t.Helper()
	payload, err := json.Marshal(model.VersionUpdateResourceActionInfo{
		Source:              config.JobInfoSourceVersionUpdateAction,
		Version:             1,
		ExecutionScope:      config.VersionUpdateExecutionScopeChangedComponents,
		ExecutionComponents: components,
	})
	require.NoError(t, err)
	return string(payload)
}

func versionUpdateCleanupMarker() string {
	return `{"source":"version_update_remove"}`
}

func requireWorkflowGenerationFailed(t *testing.T, executions []StepExecution, message string) {
	t.Helper()
	require.Len(t, executions, 1)
	require.Equal(t, "workflow-generation", executions[0].Name)
	jobs := executions[0].Jobs[config.JobPriorityLow]
	require.Len(t, jobs, 1)
	require.Equal(t, string(config.JobCleanupResources), jobs[0].JobType)
	require.Contains(t, fmt.Sprint(jobs[0].JobInfo), message)
}

func requireWorkflowJob(t *testing.T, executions []StepExecution, name string, jobType config.JobType) *model.JobTask {
	t.Helper()
	for _, execution := range executions {
		for _, jobs := range execution.Jobs {
			for _, job := range jobs {
				if job != nil && job.Name == name && job.JobType == string(jobType) {
					return job
				}
			}
		}
	}
	t.Fatalf("job %s/%s not found", name, jobType)
	return nil
}

func workflowJobNames(executions []StepExecution) map[string]struct{} {
	names := make(map[string]struct{})
	for _, execution := range executions {
		for _, jobs := range execution.Jobs {
			for _, job := range jobs {
				if job != nil {
					names[job.Name] = struct{}{}
				}
			}
		}
	}
	return names
}
