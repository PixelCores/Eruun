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
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestTaskStagesAndJobJSONKeepRetryCheckpointPrivate(t *testing.T) {
	const envSecret = "private-retry-env-sentinel"
	const argumentSecret = "private-retry-argument-sentinel"
	executionKey := "task-retry/instant/1"
	backoffLimit := int32(0)
	desired := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "batch", Namespace: "workspace-ns",
			Annotations: map[string]string{
				config.AnnotationJobTaskID:              "task-retry",
				config.AnnotationJobExecutionKey:        executionKey,
				config.AnnotationJobRunGeneration:       "1",
				workflowconfig.AnnotationJobAttempt:     "1",
				workflowconfig.AnnotationJobRetryPolicy: `{"onOOM":"stop"}`,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{Name: "main", Image: "example.com/batch:1",
					Env: []corev1.EnvVar{{Name: "API_TOKEN", Value: envSecret}}, Args: []string{argumentSecret},
				}},
			}},
		},
	}
	checkpoint, err := json.Marshal(map[string]interface{}{
		"kind": "instant_job_retry", "version": 1, "attempt": 1,
		"deadline": time.Now().Add(time.Hour).UnixNano(), "job": desired,
	})
	require.NoError(t, err)
	// Exercise a real recovery snapshot, including the full private Pod spec.
	recovered := &model.JobTask{TaskID: "task-retry", ExecutionKey: executionKey, RunGeneration: 1, InternalInfo: string(checkpoint)}
	require.NoError(t, workflowjob.RestoreInstantJobRetryCheckpoint(recovered))
	require.Equal(t, envSecret, recovered.JobInfo.(*batchv1.Job).Spec.Template.Spec.Containers[0].Env[0].Value)

	queuedAt := time.Date(2026, 9, 7, 9, 0, 0, 0, time.UTC)
	for _, state := range []string{workflowconfig.JobSchedulingQueued, workflowconfig.JobSchedulingAdmitted, workflowconfig.JobSchedulingReleased} {
		t.Run(state, func(t *testing.T) {
			record := &model.JobInfo{
				ID: 1, TaskID: "task-retry", AppID: "app-retry", ServiceName: "batch",
				Type: string(config.JobDeployInstant), Status: string(config.StatusRunning),
				Info: "instant: workspace-ns/batch", InternalInfo: string(checkpoint), Attempt: 1,
				SchedulingState: state, SchedulingClass: "high", SchedulingPriority: 100,
				SchedulingQueuedAt: &queuedAt, SchedulingReason: "global job admission",
			}
			service := &workflowServiceImpl{Store: &statusDataStore{
				task: &model.WorkflowQueue{TaskID: record.TaskID, AppID: record.AppID, Status: config.StatusRunning, Type: config.WorkflowTaskTypeWorkflow},
				jobs: []*model.JobInfo{record},
			}}
			stages, err := service.GetTaskStages(context.Background(), record.TaskID)
			require.NoError(t, err)
			require.Len(t, stages.Stages, 1)
			require.Len(t, stages.Stages[0].Info, 2)
			require.Equal(t, record.Info, stages.Stages[0].Info[0].Message)
			require.Contains(t, stages.Stages[0].Info[1].Message, "scheduling: "+state+"; class: high; queuedAt: 2026-09-07T09:00:00Z")
			for _, response := range []interface{}{record, stages} {
				encoded, err := json.Marshal(response)
				require.NoError(t, err)
				for _, private := range []string{envSecret, argumentSecret, "API_TOKEN", "instant_job_retry", "internal_info", "InternalInfo"} {
					require.NotContains(t, string(encoded), private)
				}
			}
		})
	}
}
