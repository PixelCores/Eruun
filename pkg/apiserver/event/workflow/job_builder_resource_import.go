package workflow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func buildResourceImportJobExecution(
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
) (*StepExecution, bool, error) {
	if task == nil || !isResourceImportWorkflowTask(task.Type) {
		return nil, false, nil
	}
	var info importcontract.TaskEnvelope
	if err := json.Unmarshal([]byte(task.ResourceActionInfo), &info); err != nil {
		return nil, true, fmt.Errorf("decode resource import task info: %w", err)
	}
	info.Namespace = strings.TrimSpace(info.Namespace)
	if info.Version != importcontract.TaskEnvelopeVersion || info.Namespace == "" || len(info.Request) == 0 || !json.Valid(info.Request) {
		return nil, true, fmt.Errorf("resource import task info is invalid")
	}
	jobType := config.JobResourceImportScan
	name := "scan-resources"
	if task.Type == config.WorkflowTaskTypeResourceImportManage {
		jobType = config.JobResourceImportManage
		name = "manage-resources"
	}
	jobTask := NewJobTask(name, info.Namespace, "", "", "", task.TaskID, defaultJobTimeoutSeconds)
	jobTask.WorkspaceID = task.WorkspaceID
	jobTask.JobType = string(jobType)
	jobTask.JobInfo = &info
	return &StepExecution{
		Name:          name,
		Mode:          config.WorkflowModeStepByStep,
		StepType:      config.WorkflowStepTypeComponent,
		FailurePolicy: workflowconfig.WorkflowFailurePolicyCleanupFailed,
		Jobs:          map[int][]*model.JobTask{config.JobPriorityNormal: {jobTask}},
	}, true, nil
}

func isResourceImportWorkflowTask(taskType config.WorkflowTaskType) bool {
	return taskType == config.WorkflowTaskTypeResourceImportScan ||
		taskType == config.WorkflowTaskTypeResourceImportManage
}
