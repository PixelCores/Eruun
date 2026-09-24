package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	evaluationjobs "github.com/PixelCores/Eruun/pkg/apiserver/jobs"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
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

func TestGenerateEvaluationTakeoverDuringCollectionPreservesAttempt(t *testing.T) {
	for _, remaining := range []time.Duration{480 * time.Second, -time.Second} {
		t.Run(remaining.String(), func(t *testing.T) {
			ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "member"})
			declaration := spec.JobSpec{Name: "evaluation", Type: "job", Traits: spec.JobTraits{Evaluation: &spec.EvaluationTraitSpec{
				Env: "ack", Agent: "codex", Model: "openai/model", TaskPackageID: "11111111-1111-1111-1111-111111111111",
				Recovery: &spec.EvaluationRecoverySpec{AgentVersion: spec.CodexRecoveryVersion, ReplaySafe: true},
			}}}
			raw, err := json.Marshal(declaration)
			require.NoError(t, err)
			parent := &model.WorkflowQueue{TaskID: "takeover", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob,
				JobSpec: string(raw), Status: config.StatusRunning, RunGeneration: 1, RunToken: "first-owner", WorkerID: "worker-1"}
			store := &evaluationWorkflowStore{}
			cfg := &config.Config{Jobs: &spec.JobsRuntimeConfig{RunnerImage: "example.com/runner:0.22.0", APIURL: "https://api.example.com"}}
			executions, err := GenerateJobTasks(ctx, parent, store, 3600, cfg)
			require.NoError(t, err)
			original := executions[0].Jobs[config.JobPriorityNormal][0]
			root := original.ExecutionKey
			digest := sha256.Sum256([]byte(root + "/recovery/1"))
			key := hex.EncodeToString(digest[:])
			var metadata map[string]any
			require.NoError(t, json.Unmarshal([]byte(original.EvaluationInfo), &metadata))
			metadata["rootExecutionKey"], metadata["recoveryIndex"], metadata["resumeCheckpointId"] = root, 1, "complete-point"
			metadata["recoveryName"], metadata["recoveryIsolated"] = "eruun-recovery-"+key[:32], true
			deadline := time.Now().Add(remaining).UnixNano()
			metadata["executionDeadline"] = deadline
			raw, err = json.Marshal(metadata)
			require.NoError(t, err)
			original.Name, original.ExecutionKey = metadata["recoveryName"].(string), key
			workload := original.JobInfo.(*batchv1.Job)
			workload.Name = original.Name
			workflowjob.ApplyTaskIDAnnotation(original)
			workflowjob.ApplyExecutionIdentity(original)
			workload.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			checkpoint, err := json.Marshal(map[string]any{"kind": "instant_job_retry", "version": 1, "attempt": 1,
				"job": workload, "currentUID": "running-successor", "deadline": deadline})
			require.NoError(t, err)
			store.jobInfos = []*model.JobInfo{{Type: original.JobType, WorkspaceID: "space", TaskID: parent.TaskID,
				Status: string(config.StatusRunning), ExecutionKey: &key, RunGeneration: 1, Attempt: 1,
				InternalInfo: string(checkpoint), EvaluationInfo: string(raw)}}
			parent.RunGeneration, parent.RunToken, parent.WorkerID = 2, "replacement-owner", "worker-2"
			executions, err = GenerateJobTasks(ctx, parent, store, 3600, cfg)
			require.NoError(t, err)
			require.Len(t, executions, 1)
			restored := executions[0].Jobs[config.JobPriorityNormal][0]
			require.Equal(t, config.StatusRunning, restored.Status, "takeover must not generate a synthetic failure")
			require.Equal(t, key, restored.ExecutionKey)
			require.EqualValues(t, 2, restored.OwnerRunGeneration)
			require.Equal(t, "replacement-owner", restored.RunToken)
			wantWorkload, err := json.Marshal(workload)
			require.NoError(t, err)
			gotWorkload, err := json.Marshal(restored.JobInfo)
			require.NoError(t, err)
			require.JSONEq(t, string(wantWorkload), string(gotWorkload))
			require.Equal(t, string(checkpoint), restored.InternalInfo)
		})
	}
}
