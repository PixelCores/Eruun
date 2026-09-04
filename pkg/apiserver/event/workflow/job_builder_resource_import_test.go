package workflow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
)

func TestBuildResourceImportJobExecutionCreatesApplicationlessJob(t *testing.T) {
	request := json.RawMessage(`{"namespace":"team-production","rules":[{"kinds":["Deployment"]}]}`)
	envelope, err := json.Marshal(importcontract.TaskEnvelope{
		Version:   importcontract.TaskEnvelopeVersion,
		Namespace: "team-production",
		Request:   request,
	})
	require.NoError(t, err)
	task := &model.WorkflowQueue{
		TaskID:             "scan-task-1",
		WorkspaceID:        "workspace-1",
		Type:               config.WorkflowTaskTypeResourceImportScan,
		ResourceActionInfo: string(envelope),
	}

	execution, matched, err := buildResourceImportJobExecution(task, 60)
	require.NoError(t, err)
	require.True(t, matched)
	require.NotNil(t, execution)
	require.Len(t, execution.Jobs[config.JobPriorityNormal], 1)
	jobTask := execution.Jobs[config.JobPriorityNormal][0]
	assert.Equal(t, string(config.JobResourceImportScan), jobTask.JobType)
	assert.Equal(t, "workspace-1", jobTask.WorkspaceID)
	assert.Empty(t, jobTask.ProjectID)
	assert.Equal(t, "team-production", jobTask.Namespace)
	assert.Empty(t, jobTask.AppID)
	assert.Empty(t, jobTask.WorkflowID)
}

func TestBuildResourceImportJobExecutionRejectsInvalidEnvelope(t *testing.T) {
	task := &model.WorkflowQueue{
		TaskID:             "scan-task-1",
		WorkspaceID:        "workspace-1",
		Type:               config.WorkflowTaskTypeResourceImportScan,
		ResourceActionInfo: `{"version":1,"namespace":"team-production"}`,
	}

	_, matched, err := buildResourceImportJobExecution(task, 60)
	require.True(t, matched)
	require.Error(t, err)
}

func TestResourceImportTaskTypesAreInternal(t *testing.T) {
	assert.False(t, config.IsSupportedWorkflowTaskType(config.WorkflowTaskTypeResourceImportScan))
	assert.False(t, config.IsSupportedWorkflowTaskType(config.WorkflowTaskTypeResourceImportManage))
	assert.False(t, config.IsSupportedWorkflowJobType(config.JobResourceImportScan))
	assert.False(t, config.IsSupportedWorkflowJobType(config.JobResourceImportManage))
}
