package jobs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
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

// Credentials now arrive as traits.envs, which BuildCommandJob renders before
// the evaluation branch appends its own values. Guards against the platform
// envs replacing that slice and silently dropping the model credential.
func TestEvaluationCredentialEnvsSurviveRunnerPlatformEnvs(t *testing.T) {
	service, raw, ctx := testJobService(t)
	dataset := &model.JobArtifact{ID: "11111111-1111-1111-1111-111111111111", WorkspaceID: "space", Kind: artifacts.KindDataset, Digest: strings.Repeat("a", 64)}
	require.NoError(t, raw.Add(ctx, dataset))
	_, err := service.Kube.CoreV1().Secrets("space-ns").Create(ctx,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "evaluation-model", Namespace: "space-ns"}, Data: map[string][]byte{"api-key": []byte("secret")}},
		metav1.CreateOptions{})
	require.NoError(t, err)

	accepted, err := service.Submit(ctx, SubmitRequest{WorkspaceID: "space", JobSpec: spec.JobSpec{
		Name: "evaluate", Type: "eval",
		Spec: json.RawMessage(`{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"11111111-1111-1111-1111-111111111111","agent":{"name":"terminus-2","model":"openai/gpt-4"}}`),
		Traits: spec.JobTraits{Envs: []spec.SimplifiedEnvSpec{
			{Name: "OPENAI_API_KEY", ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{Name: "evaluation-model", Key: "api-key"}}},
		}},
	}})
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, raw.Get(ctx, parent))
	task, err := BuildTask(ctx, service.Store, service.Config, parent, "space-ns")
	require.NoError(t, err)

	envs := map[string]corev1.EnvVar{}
	for _, env := range task.JobInfo.(*batchv1.Job).Spec.Template.Spec.Containers[0].Env {
		envs[env.Name] = env
	}
	credential, ok := envs["OPENAI_API_KEY"]
	require.True(t, ok, "declared credential must reach the Runner container")
	require.Equal(t, "evaluation-model", credential.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "api-key", credential.ValueFrom.SecretKeyRef.Key)
	require.Empty(t, credential.Value, "credential must stay a Secret reference")
	require.Contains(t, envs, "ERUUN_JOB_CONFIG")
	require.Contains(t, envs, "POD_NAME")
}

// Submission must fail when the referenced Secret or key is absent.
func TestEvaluationCredentialPreflightRejectsMissingSecretKey(t *testing.T) {
	service, raw, ctx := testJobService(t)
	dataset := &model.JobArtifact{ID: "11111111-1111-1111-1111-111111111111", WorkspaceID: "space", Kind: artifacts.KindDataset, Digest: strings.Repeat("a", 64)}
	require.NoError(t, raw.Add(ctx, dataset))
	_, err := service.Kube.CoreV1().Secrets("space-ns").Create(ctx,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "evaluation-model", Namespace: "space-ns"}, Data: map[string][]byte{"other": []byte("secret")}},
		metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = service.Submit(ctx, SubmitRequest{WorkspaceID: "space", JobSpec: spec.JobSpec{
		Name: "evaluate", Type: "eval",
		Spec: json.RawMessage(`{"framework":"harbor","frameworkVersion":"0.22.0","datasetId":"11111111-1111-1111-1111-111111111111","agent":{"name":"terminus-2","model":"openai/gpt-4"}}`),
		Traits: spec.JobTraits{Envs: []spec.SimplifiedEnvSpec{
			{Name: "OPENAI_API_KEY", ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{Name: "evaluation-model", Key: "api-key"}}},
		}},
	}})
	require.Error(t, err)
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
			require.Equal(t, f.service.Config.Jobs.APIURL+"/api/v1/job-runners/"+f.parent.TaskID+"/events", runtimeConfig["eventURL"])
		} else if env.ValueFrom != nil && env.ValueFrom.FieldRef != nil {
			require.Empty(t, env.Value)
			fields[env.Name] = env.ValueFrom.FieldRef.FieldPath
		}
	}
	require.Equal(t, map[string]string{"POD_NAME": "metadata.name", "POD_UID": "metadata.uid", "POD_NAMESPACE": "metadata.namespace"}, fields)
	task := &model.JobTask{Name: f.workload.Name, Namespace: "space-ns", WorkspaceID: "space", TaskID: f.parent.TaskID, JobType: string(config.JobEval), JobInfo: f.workload}
	require.NoError(t, workspace.PrepareEvaluationTask(task, &model.Workspace{ID: "space", Namespace: "space-ns"}, spec.WorkspaceConfig{}, runner.Image))
	require.Equal(t, f.parent.TaskID, f.workload.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, "original-execution", f.workload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
	require.Equal(t, "2", f.workload.Spec.Template.Annotations[config.AnnotationJobRunGeneration])
}
