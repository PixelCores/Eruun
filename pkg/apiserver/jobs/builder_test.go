package jobs

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
)

func TestCommandAndEvaluationRenderIntoTheSameWorkspaceNamespace(t *testing.T) {
	f := newRunnerFixture(t)
	ctx := account.WithScope(context.Background(), account.Scope{WorkspaceID: "space", Namespace: "space-ns", Role: "member"})
	request := commandRequest()
	request.Spec = json.RawMessage(`{"image":"busybox:1.37.0","command":["sleep"],"args":["120"],"timeoutSeconds":60}`)
	command, err := f.service.Submit(ctx, request)
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: command.TaskID}
	require.NoError(t, f.raw.Get(ctx, parent))
	task, err := BuildTask(ctx, f.service.Store, f.service.Config, parent, "space-ns")
	require.NoError(t, err)
	workload := task.JobInfo.(*batchv1.Job)
	require.Equal(t, f.workload.Namespace, workload.Namespace)
	require.Empty(t, task.AppID)
	require.Equal(t, string(config.JobCommand), task.JobType)
	require.EqualValues(t, 60, task.Timeout)
	require.EqualValues(t, 60, *workload.Spec.ActiveDeadlineSeconds)
	require.Equal(t, "eruun-job-"+parent.TaskID, workload.Name)
	require.Empty(t, workload.Spec.Template.Spec.ServiceAccountName)
	require.False(t, *workload.Spec.Template.Spec.AutomountServiceAccountToken)
	require.Empty(t, workload.Spec.Template.Spec.Containers[0].Env)
	require.Equal(t, "1", workload.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String())
	require.Equal(t, "2Gi", workload.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().String())
	require.Equal(t, "2", workload.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().String())
	require.Equal(t, "4Gi", workload.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
}

func TestEvaluationBuilderUsesBoundedRunnerIdentityAndDownwardAPI(t *testing.T) {
	f := newRunnerFixture(t)
	pod := f.workload.Spec.Template.Spec
	require.Equal(t, workspace.EvaluationRunnerName, pod.ServiceAccountName)
	require.True(t, *pod.AutomountServiceAccountToken)
	require.EqualValues(t, spec.EvaluationCollectionGraceSeconds, *pod.TerminationGracePeriodSeconds)
	require.EqualValues(t, 90*86400, *f.workload.Spec.TTLSecondsAfterFinished)
	require.EqualValues(t, 3600+spec.EvaluationCollectionGraceSeconds, *f.workload.Spec.ActiveDeadlineSeconds)
	require.EqualValues(t, 0, *f.workload.Spec.BackoffLimit)
	require.Equal(t, corev1.RestartPolicyNever, pod.RestartPolicy)
	require.Len(t, pod.Containers, 1)
	runner := pod.Containers[0]
	require.Equal(t, "runner", runner.Name)
	require.Equal(t, []string{"python", "/opt/eruun/runner.py"}, runner.Command)
	require.Equal(t, f.service.Config.Jobs.RunnerImage, runner.Image)
	require.EqualValues(t, 1000, *runner.SecurityContext.RunAsUser)
	require.True(t, *runner.SecurityContext.ReadOnlyRootFilesystem)
	require.Equal(t, []corev1.Capability{"ALL"}, runner.SecurityContext.Capabilities.Drop)
	fields := map[string]string{}
	for _, env := range runner.Env {
		require.NotEqual(t, "APP_ID", env.Name)
		if env.Name == "ERUUN_JOB_CONFIG" {
			var runtimeConfig map[string]any
			require.NoError(t, json.Unmarshal([]byte(env.Value), &runtimeConfig))
			require.Equal(t, f.parent.JobToken, runtimeConfig["token"])
			require.Equal(t, "default", runtimeConfig["sandboxServiceAccount"])
		} else if env.ValueFrom != nil && env.ValueFrom.FieldRef != nil {
			require.Empty(t, env.Value)
			fields[env.Name] = env.ValueFrom.FieldRef.FieldPath
		}
	}
	require.Equal(t, map[string]string{"POD_NAME": "metadata.name", "POD_UID": "metadata.uid", "POD_NAMESPACE": "metadata.namespace"}, fields)
	task := &model.JobTask{Name: f.workload.Name, Namespace: "space-ns", WorkspaceID: "space", TaskID: f.parent.TaskID, JobType: string(config.JobAgentEvaluation), JobInfo: f.workload}
	require.NoError(t, workspace.PrepareEvaluationTask(task, &model.Workspace{ID: "space", Namespace: "space-ns"}, spec.WorkspaceConfig{}, runner.Image))
	require.Equal(t, f.parent.TaskID, f.workload.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, "original-execution", f.workload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
	require.Equal(t, "2", f.workload.Spec.Template.Annotations[config.AnnotationJobRunGeneration])
}
