package workflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestRestoreRunningRetryCheckpointKeepsExecutionAndLeaseGenerationsSeparate(t *testing.T) {
	const priority = 10
	task := &model.WorkflowQueue{TaskID: "retry-task", Status: config.StatusRunning, RunGeneration: 4, RunToken: "token-4", WorkerID: "worker-4"}
	oldKey := workflowExecutionKey(task.TaskID, 2, 0, priority, 0, "work", string(config.JobDeployInstant))
	policyJSON, err := json.Marshal(workflowconfig.JobRetryPolicy{OnOOM: "retry", MaxRetries: 1, BackoffSeconds: 1})
	require.NoError(t, err)
	zero := int32(0)
	desired := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "ns", Annotations: map[string]string{
		config.AnnotationJobTaskID: task.TaskID, config.AnnotationJobExecutionKey: oldKey, config.AnnotationJobRunGeneration: "2",
		workflowconfig.AnnotationJobAttempt: "2", workflowconfig.AnnotationJobRetryPolicy: string(policyJSON),
	}}, Spec: batchv1.JobSpec{BackoffLimit: &zero, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever}}}}
	checkpoint, err := json.Marshal(map[string]interface{}{
		"kind": "instant_job_retry", "version": 1, "attempt": 2, "job": desired,
		"previousUID": "attempt-1", "currentUID": "attempt-2", "retryAt": time.Now().Add(-time.Minute).UnixNano(), "deadline": time.Now().Add(time.Hour).UnixNano(),
	})
	require.NoError(t, err)
	for _, tt := range []struct {
		name, checkpoint        string
		wantRestored, wantError bool
	}{
		{"running checkpoint", string(checkpoint), true, false},
		{"ordinary running job is not a retry checkpoint", "", false, false},
		{"malformed checkpoint fails closed", "invalid json", false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executions := []StepExecution{{Jobs: map[int][]*model.JobTask{priority: {{Name: "work", Namespace: "ns", TaskID: task.TaskID,
				JobType: string(config.JobDeployInstant), JobInfo: desired.DeepCopy()}}}}}
			applyWorkflowExecutionIdentity(executions, task)
			executions[0].Jobs[priority][0].SchedulingClass = "background"
			store := &controllerTestStore{jobs: []*model.JobInfo{{Type: string(config.JobDeployInstant), TaskID: task.TaskID,
				Status: string(config.StatusRunning), ExecutionKey: &oldKey, RunGeneration: 2, Attempt: 2, InternalInfo: tt.checkpoint, SchedulingClass: "high"}}}
			err := restoreCommittedJobExecutions(context.Background(), executions, task, store)
			if tt.wantError {
				require.ErrorContains(t, err, "restore instant job retry")
				return
			}
			require.NoError(t, err)
			restored := executions[0].Jobs[priority][0]
			require.Equal(t, uint64(4), restored.OwnerRunGeneration)
			require.Equal(t, "token-4", restored.RunToken)
			require.Equal(t, "worker-4", restored.WorkerID)
			if tt.wantRestored {
				require.Equal(t, "high", restored.SchedulingClass)
				require.Equal(t, oldKey, restored.ExecutionKey)
				require.Equal(t, uint64(2), restored.RunGeneration)
				require.Equal(t, uint(2), restored.Attempt)
				require.Equal(t, config.StatusRunning, restored.Status)
				require.Equal(t, "2", restored.JobInfo.(*batchv1.Job).Annotations[config.AnnotationJobRunGeneration])
			} else {
				require.Equal(t, uint64(4), restored.RunGeneration)
				require.Equal(t, "background", restored.SchedulingClass)
			}
		})
	}
}

func TestWorkspaceJobRecoveryPreservesExecutionIdentityAcrossLeaseReplacement(t *testing.T) {
	for _, jobType := range []config.JobType{config.JobCommand, config.JobAgentEvaluation} {
		t.Run(string(jobType), func(t *testing.T) {
			const priority = config.JobPriorityNormal
			task := &model.WorkflowQueue{TaskID: "workspace-task", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob,
				Status: config.StatusRunning, RunGeneration: 4, RunToken: "new-token", WorkerID: "new-worker"}
			oldKey := workflowExecutionKey(task.TaskID, 2, 0, priority, 0, "work", string(jobType))
			zero := int32(0)
			workload := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "space-ns", Annotations: map[string]string{
				config.AnnotationJobTaskID: task.TaskID, config.AnnotationJobExecutionKey: oldKey, config.AnnotationJobRunGeneration: "2",
				workflowconfig.AnnotationJobAttempt: "1", workflowconfig.AnnotationJobRetryPolicy: `{"onOOM":"stop"}`,
			}}, Spec: batchv1.JobSpec{BackoffLimit: &zero, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever}}}}
			checkpoint, err := json.Marshal(map[string]interface{}{
				"kind": "instant_job_retry", "version": 1, "attempt": 1, "job": workload,
				"currentUID": "same-kubernetes-job", "deadline": time.Now().Add(time.Hour).UnixNano(),
			})
			require.NoError(t, err)
			executions := []StepExecution{{Jobs: map[int][]*model.JobTask{priority: {{Name: "work", Namespace: "space-ns", WorkspaceID: "space",
				TaskID: task.TaskID, JobType: string(jobType), JobInfo: workload.DeepCopy()}}}}}
			applyWorkflowExecutionIdentity(executions, task)
			store := &controllerTestStore{jobs: []*model.JobInfo{{Type: string(jobType), TaskID: task.TaskID, WorkspaceID: "space",
				Status: string(config.StatusRunning), ExecutionKey: &oldKey, RunGeneration: 2, Attempt: 1, InternalInfo: string(checkpoint)}}}
			require.NoError(t, restoreCommittedJobExecutions(context.Background(), executions, task, store))
			restored := executions[0].Jobs[priority][0]
			require.Empty(t, restored.AppID)
			require.Equal(t, uint64(4), restored.OwnerRunGeneration)
			require.Equal(t, "new-token", restored.RunToken)
			require.Equal(t, "new-worker", restored.WorkerID)
			require.Equal(t, oldKey, restored.ExecutionKey)
			require.Equal(t, uint64(2), restored.RunGeneration)
			require.Equal(t, "2", restored.JobInfo.(*batchv1.Job).Annotations[config.AnnotationJobRunGeneration])
			require.JSONEq(t, string(checkpoint), restored.InternalInfo)
		})
	}
}
