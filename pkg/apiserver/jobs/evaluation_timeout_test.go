package jobs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

func TestOnlineEvaluationTimeoutLimitPreservesRunningExecution(t *testing.T) {
	s, store, ctx := testJobService(t)
	dataset := "11111111-1111-1111-1111-111111111111"
	require.NoError(t, store.Add(ctx, &model.JobArtifact{ID: dataset, WorkspaceID: "space", Kind: artifacts.KindDataset, Digest: strings.Repeat("a", 64)}))
	request := SubmitRequest{JobSpec: evaluationDeclaration("long evaluation", dataset, "oracle", "")}
	request.Traits.Evaluation.TimeoutSeconds = workflowconfig.MaxEvaluationTimeoutSeconds
	accepted, err := s.Submit(ctx, request)
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, store.Get(ctx, parent))
	task, err := BuildTask(ctx, s.Store, s.Config, parent, "space-ns")
	require.NoError(t, err)
	require.NoError(t, BuildEvaluationTask(ctx, s.Store, s.Config, task, spec.JobTraits{}))
	workload := task.JobInfo.(*batchv1.Job)
	require.EqualValues(t, workflowconfig.MaxEvaluationTimeoutSeconds+spec.EvaluationCollectionGraceSeconds, *workload.Spec.ActiveDeadlineSeconds)
	var payload map[string]any
	for _, env := range workload.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "ERUUN_JOB_CONFIG" {
			require.NoError(t, json.Unmarshal([]byte(env.Value), &payload))
		}
	}
	require.EqualValues(t, workflowconfig.MaxEvaluationTimeoutSeconds, payload["timeoutSeconds"])
	require.EqualValues(t, spec.EvaluationCollectionGraceSeconds, payload["finalizationTimeoutSeconds"])
	require.Equal(t, s.Config.Jobs.APIURL+"/api/v1/job-runners/"+accepted.TaskID+"/sandboxes", payload["sandboxURL"])
	require.Equal(t, "20Gi", workload.Spec.Template.Spec.Volumes[0].EmptyDir.SizeLimit.String())
	storage := workload.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceEphemeralStorage]
	require.Equal(t, "20Gi", storage.String())

	policy := workflowconfig.DefaultJobSchedulerPolicy()
	policy.MaxEvaluationTimeoutSeconds = 3600
	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler, Value: encoded}))
	_, err = s.Submit(ctx, request)
	require.ErrorIs(t, err, bcode.ErrJobInput)
	require.ErrorContains(t, BuildEvaluationTask(ctx, s.Store, s.Config, task, spec.JobTraits{}), "current maximum of 3600")

	// Recovery uses the committed execution's original request, even after an
	// administrator lowers the cap. It must neither fail nor shorten its lease.
	for _, state := range []config.Status{config.StatusRunning, config.StatusDistributed} {
		task.Status = state
		require.NoError(t, BuildEvaluationTask(ctx, s.Store, s.Config, task, spec.JobTraits{}))
		require.EqualValues(t, workflowconfig.MaxEvaluationTimeoutSeconds+spec.EvaluationCollectionGraceSeconds, task.Timeout)
	}
}

func TestEvaluationTimeoutPolicyUnavailableFailsClosed(t *testing.T) {
	s, store, ctx := testJobService(t)
	require.NoError(t, store.Delete(ctx, &model.SystemSetting{Type: model.SystemSettingTypeWorkflowScheduler}))
	request := SubmitRequest{JobSpec: evaluationDeclaration("evaluation", "11111111-1111-1111-1111-111111111111", "oracle", "")}
	_, err := s.Submit(ctx, request)
	require.ErrorIs(t, err, bcode.ErrServiceUnavailable)
}

func TestRecoveryRenderingPreservesCommittedDeadline(t *testing.T) {
	for _, tt := range []struct {
		name       string
		status     config.Status
		remaining  time.Duration
		checkpoint string
		wantError  string
	}{
		{"running before finalization", config.StatusRunning, time.Hour, "valid", ""},
		{"running during finalization", config.StatusRunning, 480 * time.Second, "valid", ""},
		{"expired running reaches admission timeout", config.StatusRunning, -time.Second, "valid", ""},
		{"queued cannot start during finalization", config.StatusQueued, 480 * time.Second, "", "deadline elapsed"},
		{"preparing cannot start during finalization", config.StatusPrepare, 480 * time.Second, "", "deadline elapsed"},
		{"running reservation without checkpoint", config.StatusRunning, 480 * time.Second, "", "deadline elapsed"},
		{"corrupt checkpoint fails closed", config.StatusRunning, 480 * time.Second, "corrupt", "decode instant Job retry checkpoint"},
		{"foreign checkpoint fails closed", config.StatusRunning, 480 * time.Second, "foreign", "execution identity"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, task, _ := newRecoverableEvaluation(t)
			ctx := context.Background()
			again, err := f.service.RecoverEvaluation(ctx, task)
			require.NoError(t, err)
			require.True(t, again)
			info, err := decodeEvaluationInfo(task.EvaluationInfo)
			require.NoError(t, err)
			info.ExecutionDeadline = time.Now().Add(tt.remaining).UnixNano()
			encoded, err := json.Marshal(info)
			require.NoError(t, err)
			task.EvaluationInfo, task.Status = string(encoded), tt.status
			workflowjob.ApplyTaskIDAnnotation(task)
			workload := task.JobInfo.(*batchv1.Job)
			workload.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
			workload.Spec.Template.Spec.Containers[0].Image = "example.com/committed-runner:1"
			if tt.checkpoint == "foreign" {
				workload.Annotations[config.AnnotationJobExecutionKey] = "another-execution"
			}
			checkpoint, err := json.Marshal(map[string]any{"kind": "instant_job_retry", "version": 1,
				"attempt": 1, "job": workload, "currentUID": "committed-job", "deadline": info.ExecutionDeadline})
			require.NoError(t, err)
			switch tt.checkpoint {
			case "valid", "foreign":
				task.InternalInfo = string(checkpoint)
			case "corrupt":
				task.InternalInfo = "invalid json"
			}
			before := task.InternalInfo
			err = BuildEvaluationTask(ctx, f.service.Store, f.service.Config, task, spec.JobTraits{})
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			wantWorkload, err := json.Marshal(workload)
			require.NoError(t, err)
			gotWorkload, err := json.Marshal(task.JobInfo)
			require.NoError(t, err)
			require.JSONEq(t, string(wantWorkload), string(gotWorkload), "takeover must retain the committed workload, including its original image and budget")
			require.Equal(t, before, task.InternalInfo)
			require.Equal(t, string(encoded), task.EvaluationInfo)
			_, deadline, err := workflowjob.EvaluationRunnerCheckpoint(&model.JobInfo{Type: task.JobType,
				TaskID: task.TaskID, ExecutionKey: &task.ExecutionKey, RunGeneration: task.RunGeneration,
				Attempt: task.Attempt, InternalInfo: task.InternalInfo})
			require.NoError(t, err)
			require.Equal(t, info.ExecutionDeadline, deadline)
		})
	}
}
