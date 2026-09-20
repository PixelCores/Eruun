package jobs

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
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
