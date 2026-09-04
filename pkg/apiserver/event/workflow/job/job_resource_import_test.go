package job

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
)

type fakeResourceImportExecutor struct {
	taskType          config.WorkflowTaskType
	request           json.RawMessage
	checkpoint        json.RawMessage
	prepared          json.RawMessage
	prepareErr        error
	prepareCalls      int
	executeCalls      int
	result            json.RawMessage
	err               error
	beforeExecuteCall func()
}

func (f *fakeResourceImportExecutor) PrepareResourceImportJob(
	_ context.Context,
	_ config.WorkflowTaskType,
	_ json.RawMessage,
) (json.RawMessage, error) {
	f.prepareCalls++
	return f.prepared, f.prepareErr
}

func (f *fakeResourceImportExecutor) ExecuteResourceImportJob(
	_ context.Context,
	taskType config.WorkflowTaskType,
	request json.RawMessage,
	checkpoint json.RawMessage,
) (json.RawMessage, error) {
	f.executeCalls++
	if f.beforeExecuteCall != nil {
		f.beforeExecuteCall()
	}
	f.taskType = taskType
	f.request = append(json.RawMessage(nil), request...)
	f.checkpoint = append(json.RawMessage(nil), checkpoint...)
	return f.result, f.err
}

func TestResourceImportJobControllerDelegatesToModule(t *testing.T) {
	executor := &fakeResourceImportExecutor{result: json.RawMessage(`{"namespace":"team-production","resources":[]}`)}
	jobTask := &model.JobTask{
		JobType: string(config.JobResourceImportScan),
		JobInfo: &importcontract.TaskEnvelope{
			Version:   importcontract.TaskEnvelopeVersion,
			Namespace: "team-production",
			Request:   json.RawMessage(`{"namespace":"team-production","rules":[{"kinds":["Deployment"]}]}`),
		},
	}
	controller := NewResourceImportJobCtl(jobTask, nil, executor)

	require.NoError(t, controller.Run(context.Background()))
	assert.Equal(t, config.WorkflowTaskTypeResourceImportScan, executor.taskType)
	assert.Zero(t, executor.prepareCalls)
	assert.JSONEq(t, string(executor.result), jobTask.Info)
}

func TestResourceImportManagePersistsCheckpointBeforeApply(t *testing.T) {
	checkpoint := json.RawMessage(`{"version":1,"scanTaskId":"scan-1","applyRequest":{"mode":"apply"}}`)
	store := &jobInfoSaveStore{}
	executor := &fakeResourceImportExecutor{
		prepared: checkpoint,
		result:   json.RawMessage(`{"apps":[{"name":"payments"}],"summary":{"appsApplied":1}}`),
	}
	executor.beforeExecuteCall = func() {
		require.NotNil(t, store.added)
		assert.JSONEq(t, string(checkpoint), store.added.InternalInfo)
	}
	jobTask := &model.JobTask{
		TaskID:      "manage-1",
		WorkspaceID: "workspace-1",
		JobType:     string(config.JobResourceImportManage),
		Status:      config.StatusRunning,
		JobInfo: &importcontract.TaskEnvelope{
			Version:   importcontract.TaskEnvelopeVersion,
			Namespace: "team-production",
			Request:   json.RawMessage(`{"scanTaskId":"scan-1","applications":[{"name":"payments"}]}`),
		},
	}
	controller := NewResourceImportJobCtl(jobTask, store, executor)

	require.NoError(t, controller.Run(context.Background()))
	require.Equal(t, 1, executor.prepareCalls)
	assert.JSONEq(t, string(checkpoint), string(executor.checkpoint))
}

func TestResourceImportManageRecoveryReusesPersistedCheckpoint(t *testing.T) {
	checkpoint := `{"version":1,"scanTaskId":"scan-1","applyRequest":{"mode":"apply"}}`
	store := &jobInfoSaveStore{existing: []*model.JobInfo{{
		ID:           7,
		TaskID:       "manage-1",
		WorkspaceID:  "workspace-1",
		Type:         string(config.JobResourceImportManage),
		Status:       string(config.StatusRunning),
		InternalInfo: checkpoint,
	}}}
	executor := &fakeResourceImportExecutor{
		prepared: json.RawMessage(`{"unexpected":true}`),
		result:   json.RawMessage(`{"apps":[{"name":"payments"}],"summary":{"appsApplied":1}}`),
	}
	jobTask := &model.JobTask{
		TaskID:      "manage-1",
		WorkspaceID: "workspace-1",
		JobType:     string(config.JobResourceImportManage),
		Status:      config.StatusRunning,
		JobInfo: &importcontract.TaskEnvelope{
			Version:   importcontract.TaskEnvelopeVersion,
			Namespace: "team-production",
			Request:   json.RawMessage(`{"scanTaskId":"scan-1","applications":[{"name":"payments"}]}`),
		},
	}
	controller := NewResourceImportJobCtl(jobTask, store, executor)

	require.NoError(t, controller.Run(context.Background()))
	assert.Zero(t, executor.prepareCalls)
	assert.JSONEq(t, checkpoint, string(executor.checkpoint))
}

func TestResourceImportManageDoesNotApplyWhenCheckpointPersistenceFails(t *testing.T) {
	store := &jobInfoSaveStore{addErr: errors.New("database unavailable")}
	executor := &fakeResourceImportExecutor{
		prepared: json.RawMessage(`{"version":1,"scanTaskId":"scan-1","applyRequest":{"mode":"apply"}}`),
	}
	jobTask := &model.JobTask{
		TaskID:      "manage-1",
		WorkspaceID: "workspace-1",
		JobType:     string(config.JobResourceImportManage),
		Status:      config.StatusRunning,
		JobInfo: &importcontract.TaskEnvelope{
			Version:   importcontract.TaskEnvelopeVersion,
			Namespace: "team-production",
			Request:   json.RawMessage(`{"scanTaskId":"scan-1","applications":[{"name":"payments"}]}`),
		},
	}
	controller := NewResourceImportJobCtl(jobTask, store, executor)

	require.ErrorContains(t, controller.Run(context.Background()), "persist resource import management checkpoint")
	assert.Zero(t, executor.executeCalls)
	assert.Empty(t, jobTask.InternalInfo)
}

func TestResourceImportJobControllerFailsWithoutExecutor(t *testing.T) {
	controller := NewResourceImportJobCtl(&model.JobTask{}, nil, nil)
	require.Error(t, controller.Run(context.Background()))
}
