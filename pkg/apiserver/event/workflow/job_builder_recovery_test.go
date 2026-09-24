package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	evaluationjobs "github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestRestoreEvaluationChoosesReservedSuccessorAcrossLeaseTakeover(t *testing.T) {
	const priority = config.JobPriorityNormal
	parent := &model.WorkflowQueue{TaskID: "recovery-task", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob,
		Status: config.StatusRunning, RunGeneration: 4, RunToken: "new-lease", WorkerID: "new-worker"}
	root := workflowExecutionKey(parent.TaskID, 2, 0, priority, 0, "work", string(config.JobEval))
	task := &model.JobTask{Name: "work", Namespace: "space-ns", WorkspaceID: "space", TaskID: parent.TaskID, JobType: string(config.JobEval)}
	require.NoError(t, evaluationjobs.SetEvaluationTraits(task, spec.JobTraits{Evaluation: &spec.EvaluationTraitSpec{
		Env: "ack", Agent: "codex", Model: "openai/model", TaskPackageID: "11111111-1111-1111-1111-111111111111",
		Recovery: &spec.EvaluationRecoverySpec{AgentVersion: spec.CodexRecoveryVersion, ReplaySafe: true},
	}}))
	var metadata map[string]any
	require.NoError(t, json.Unmarshal([]byte(task.EvaluationInfo), &metadata))
	rows := []*model.JobInfo{{Type: task.JobType, TaskID: parent.TaskID, WorkspaceID: "space", ExecutionKey: ptr.To(root), RunGeneration: 2,
		Status: string(config.StatusFailed), EvaluationInfo: task.EvaluationInfo}}
	wantKey, wantInfo := "", ""
	for index := 1; index <= 2; index++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s/recovery/%d", root, index)))
		key := hex.EncodeToString(digest[:])
		metadata["rootExecutionKey"], metadata["recoveryIndex"], metadata["resumeCheckpointId"] = root, index, "complete-point"
		encoded, err := json.Marshal(metadata)
		require.NoError(t, err)
		status := config.StatusFailed
		if index == 2 {
			status = config.StatusQueued
		}
		rows = append(rows, &model.JobInfo{Type: task.JobType, TaskID: parent.TaskID, WorkspaceID: "space", ExecutionKey: ptr.To(key), RunGeneration: 2,
			Status: string(status), Attempt: 1, EvaluationInfo: string(encoded)})
		wantKey, wantInfo = key, string(encoded)
	}
	for _, status := range []config.Status{config.StatusQueued, config.StatusPrepare, config.StatusRunning} {
		rows[2].Status = string(status)
		for _, order := range []string{"oldest first", "newest first"} {
			t.Run(string(status)+"/"+order, func(t *testing.T) {
				candidate := *task
				executions := []StepExecution{{Jobs: map[int][]*model.JobTask{priority: {&candidate}}}}
				applyWorkflowExecutionIdentity(executions, parent)
				ordered := append([]*model.JobInfo(nil), rows...)
				if order == "newest first" {
					ordered[0], ordered[2] = ordered[2], ordered[0]
				}
				require.NoError(t, restoreCommittedJobExecutions(context.Background(), executions, parent, &controllerTestStore{jobs: ordered}))
				restored := executions[0].Jobs[priority][0]
				require.Equal(t, wantKey, restored.ExecutionKey)
				require.Equal(t, wantInfo, restored.EvaluationInfo)
				require.Equal(t, status, restored.Status)
				require.EqualValues(t, 2, restored.RunGeneration)
				require.EqualValues(t, 4, restored.OwnerRunGeneration)
				require.Equal(t, "new-lease", restored.RunToken)
				require.Equal(t, "new-worker", restored.WorkerID)
			})
		}
	}
}

func TestRestoreDoesNotReuseUnstartedOriginalExecutions(t *testing.T) {
	const priority = config.JobPriorityNormal
	for _, kind := range []config.JobType{config.JobEval, config.JobCommand} {
		for _, status := range []config.Status{config.StatusQueued, config.StatusPrepare, config.StatusRunning} {
			t.Run(string(kind)+"/"+string(status), func(t *testing.T) {
				parent := &model.WorkflowQueue{TaskID: "parent", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob,
					RunGeneration: 2, Status: config.StatusRunning, RunToken: "new-token", WorkerID: "new-worker"}
				task := &model.JobTask{Name: "work", WorkspaceID: "space", TaskID: parent.TaskID, JobType: string(kind)}
				executions := []StepExecution{{Jobs: map[int][]*model.JobTask{priority: {task}}}}
				applyWorkflowExecutionIdentity(executions, parent)
				newKey := task.ExecutionKey
				oldKey := workflowExecutionKey(parent.TaskID, 1, 0, priority, 0, task.Name, task.JobType)
				row := &model.JobInfo{TaskID: parent.TaskID, WorkspaceID: "space", Type: task.JobType, ExecutionKey: &oldKey,
					RunGeneration: 1, Status: string(status)}
				require.NoError(t, restoreCommittedJobExecutions(context.Background(), executions, parent, &controllerTestStore{jobs: []*model.JobInfo{row}}))
				require.Equal(t, newKey, task.ExecutionKey, "only a committed recovery lineage can resume before the retry checkpoint")
				require.EqualValues(t, 2, task.RunGeneration)
			})
		}
	}
}
