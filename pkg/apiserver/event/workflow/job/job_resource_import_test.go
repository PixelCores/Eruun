package job

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
)

type fakeResourceImportExecutor struct {
	taskType config.WorkflowTaskType
	request  json.RawMessage
	result   json.RawMessage
	err      error
}

func (f *fakeResourceImportExecutor) ExecuteResourceImportJob(
	_ context.Context,
	taskType config.WorkflowTaskType,
	request json.RawMessage,
) (json.RawMessage, error) {
	f.taskType = taskType
	f.request = append(json.RawMessage(nil), request...)
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
	assert.JSONEq(t, string(executor.result), jobTask.Info)
}

func TestResourceImportJobControllerFailsWithoutExecutor(t *testing.T) {
	controller := NewResourceImportJobCtl(&model.JobTask{}, nil, nil)
	require.Error(t, controller.Run(context.Background()))
}
