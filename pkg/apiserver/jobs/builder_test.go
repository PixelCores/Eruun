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
	request.Stages[0].Steps[0].Spec = json.RawMessage(`{"image":"busybox:1.37.0","command":["sleep"],"args":["120"],"timeoutSeconds":60}`)
	command, err := f.service.Submit(ctx, request)
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: command.TaskID}
	require.NoError(t, f.raw.Get(ctx, parent))
	stages, err := BuildTask(parent, "space-ns")
	require.NoError(t, err)
	require.Len(t, stages, 1)
	require.Len(t, stages[0], 1)
	task := stages[0][0]
	workload := task.JobInfo.(*batchv1.Job)
	require.Equal(t, f.workload.Namespace, workload.Namespace)
	require.Empty(t, task.AppID)
	require.Equal(t, string(config.JobCommand), task.JobType)
	require.EqualValues(t, 60, task.Timeout)
	require.EqualValues(t, 60, *workload.Spec.ActiveDeadlineSeconds)
	require.Equal(t, "eruun-job-"+parent.TaskID+"-"+request.Stages[0].Steps[0].Name, workload.Name)
	require.Empty(t, workload.Spec.Template.Spec.ServiceAccountName)
	require.False(t, *workload.Spec.Template.Spec.AutomountServiceAccountToken)
	require.Empty(t, workload.Spec.Template.Spec.Containers[0].Env)
	require.Equal(t, "1", workload.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String())
	require.Equal(t, "2Gi", workload.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().String())
	require.Equal(t, "2", workload.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().String())
	require.Equal(t, "4Gi", workload.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String())
}

// Credentials now arrive as traits.envs, which buildCommandJob renders before
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
		Name: "evaluate", Stages: []spec.JobStage{{Name: "run", Steps: []spec.JobStep{{Name: "evaluate", Type: "job",
			Traits: spec.Traits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", TaskPackageID: dataset.ID, Agent: "terminus-2", Model: "openai/gpt-4"}, Envs: []spec.SimplifiedEnvSpec{
				{Name: "OPENAI_API_KEY", ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{Name: "evaluation-model", Key: "api-key"}}},
			}},
		}}}},
	}})
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: accepted.TaskID}
	require.NoError(t, raw.Get(ctx, parent))
	stages, err := BuildTask(parent, "space-ns")
	require.NoError(t, err)
	require.Len(t, stages, 1)
	require.Len(t, stages[0], 1)
	task := stages[0][0]
	require.NoError(t, BuildEvaluationTask(ctx, service.Store, service.Config, task, spec.JobTraits{}))

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
		Name: "evaluate", Stages: []spec.JobStage{{Name: "run", Steps: []spec.JobStep{{Name: "evaluate", Type: "job",
			Traits: spec.Traits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", TaskPackageID: dataset.ID, Agent: "terminus-2", Model: "openai/gpt-4"}, Envs: []spec.SimplifiedEnvSpec{
				{Name: "OPENAI_API_KEY", ValueFrom: spec.ValueSource{Secret: &spec.SecretSelectorSpec{Name: "evaluation-model", Key: "api-key"}}},
			}},
		}}}},
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
			require.Equal(t, f.identity.Token, runtimeConfig["token"])
			require.NotContains(t, runtimeConfig["agent"].(map[string]any), "model", "oracle does not submit an empty model to the Runner")
			require.Equal(t, "default", runtimeConfig["sandboxServiceAccount"])
			require.Equal(t, f.service.Config.Jobs.APIURL+"/api/v1/job-runners/"+f.parent.TaskID+"/events", runtimeConfig["eventURL"])
		} else if env.ValueFrom != nil && env.ValueFrom.FieldRef != nil {
			require.Empty(t, env.Value)
			fields[env.Name] = env.ValueFrom.FieldRef.FieldPath
		}
	}
	require.Equal(t, map[string]string{"POD_NAME": "metadata.name", "POD_UID": "metadata.uid", "POD_NAMESPACE": "metadata.namespace"}, fields)
	task := &model.JobTask{Name: f.workload.Name, Namespace: "space-ns", WorkspaceID: "space", TaskID: f.parent.TaskID, JobType: string(config.JobEval), JobInfo: f.workload, EvaluationInfo: f.record.EvaluationInfo}
	require.NoError(t, PrepareEvaluationTask(task, &model.Workspace{ID: "space", Namespace: "space-ns"}, spec.WorkspaceConfig{}, runner.Image))
	require.Equal(t, f.parent.TaskID, f.workload.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, "original-execution", f.workload.Spec.Template.Annotations[config.AnnotationJobExecutionKey])
	require.Equal(t, "2", f.workload.Spec.Template.Annotations[config.AnnotationJobRunGeneration])
}

func TestEvaluationBuilderSeparatesResourcesAndPreservesRecoveredSnapshot(t *testing.T) {
	service, raw, ctx := testJobService(t)
	dataset := &model.JobArtifact{ID: "11111111-1111-1111-1111-111111111111", WorkspaceID: "space", Kind: artifacts.KindDataset, Digest: strings.Repeat("a", 64)}
	require.NoError(t, raw.Add(ctx, dataset))
	traits := spec.JobTraits{Evaluation: &spec.EvaluationTraitSpec{Env: "ack", TaskPackageID: dataset.ID, Agent: "terminus-2", Model: "openai/model-a"}}
	traits.Resources = &spec.ResourceTraitsSpec{CPU: "1", Memory: "1Gi"}
	traits.Evaluation.SandboxResources = &spec.ResourceTraitsSpec{CPU: "4", Memory: "8Gi"}
	task := &model.JobTask{Name: "evaluation-component", Namespace: "space-ns", WorkspaceID: "space", AppID: "app", TaskID: "workflow-task", ExecutionKey: "step-execution", JobInfo: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{config.LabelAppID: "app", config.LabelComponentName: "evaluation-component", config.LabelComponentID: "23"}, Annotations: map[string]string{config.AnnotationComponentName: "evaluation.component"}}}}
	require.NoError(t, BuildEvaluationTask(ctx, service.Store, service.Config, task, traits))
	snapshot := task.EvaluationInfo
	workload := task.JobInfo.(*batchv1.Job)
	require.Equal(t, "app", workload.Labels[config.LabelAppID])
	require.Equal(t, "evaluation-component", workload.Spec.Template.Labels[config.LabelComponentName])
	require.Equal(t, "23", workload.Labels[config.LabelComponentID])
	require.Equal(t, "23", workload.Spec.Template.Labels[config.LabelComponentID])
	require.Equal(t, "evaluation.component", workload.Annotations[config.AnnotationComponentName])
	require.Equal(t, "evaluation.component", workload.Spec.Template.Annotations[config.AnnotationComponentName])
	require.Equal(t, "1", workload.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String())
	for _, env := range workload.Spec.Template.Spec.Containers[0].Env {
		if env.Name != "ERUUN_JOB_CONFIG" {
			continue
		}
		var rendered struct {
			Resources    spec.ResourceTraitsSpec `json:"resources"`
			ExecutionKey string                  `json:"executionKey"`
		}
		require.NoError(t, json.Unmarshal([]byte(env.Value), &rendered))
		require.Equal(t, "4", rendered.Resources.CPU)
		require.Equal(t, "8Gi", rendered.Resources.Memory)
		require.Equal(t, task.ExecutionKey, rendered.ExecutionKey)
	}
	traits.Evaluation.Model = "openai/model-b"
	backoff := int32(2)
	workload.Spec.BackoffLimit = &backoff
	require.NoError(t, BuildEvaluationTask(ctx, service.Store, service.Config, task, traits))
	require.Equal(t, snapshot, task.EvaluationInfo, "recovery retains the committed declaration and capability")
	require.EqualValues(t, 0, *task.JobInfo.(*batchv1.Job).Spec.BackoffLimit, "recovery must not enable Kubernetes retries of evaluation trials")
	require.Equal(t, "evaluation-component", task.JobInfo.(*batchv1.Job).Labels[config.LabelComponentName])
	require.Equal(t, "evaluation.component", task.JobInfo.(*batchv1.Job).Annotations[config.AnnotationComponentName])
	info, err := decodeEvaluationInfo(task.EvaluationInfo)
	require.NoError(t, err)
	require.Equal(t, "openai/model-a", info.Traits.Evaluation.Model)
}

func TestBuildTaskCompilesStagesWithOwnedVolumeAndExistingReferences(t *testing.T) {
	command := json.RawMessage(`{"image":"busybox:1.37.0","command":["true"],"timeoutSeconds":90}`)
	declaration := spec.JobSpec{Name: "pipeline", Volume: &spec.JobVolume{Size: "2Gi", StorageClass: "shared", AccessMode: "ReadWriteMany"},
		Stages: []spec.JobStage{
			{Name: "parallel", Steps: []spec.JobStep{
				{Name: "fetch", Type: "command", Spec: command, Retry: &spec.JobStepRetry{BackoffLimit: 2},
					Traits: spec.Traits{Storage: []spec.StorageTraitSpec{{Name: "input", Type: "persistent", ClaimName: "existing-input", MountPath: "/input"}},
						Resources: &spec.ResourceTraitsSpec{CPU: "250m", Memory: "64Mi"},
						Init: []spec.InitTraitSpec{{Name: "prepare", Image: "busybox:1.37.0", Properties: spec.Properties{Command: []string{"true"}},
							Traits: spec.Traits{Storage: []spec.StorageTraitSpec{{Name: "seed", Type: "persistent", ClaimName: "existing-seed", MountPath: "/seed"}}}}},
						Sidecar: []spec.SidecarTraitsSpec{{Name: "observe", Image: "busybox:1.37.0", Command: []string{"true"}}}}},
				{Name: "inspect", Type: "command", Spec: command},
			}},
			{Name: "final", Steps: []spec.JobStep{{Name: "publish", Type: "command", Spec: command}}},
		}}
	raw, err := json.Marshal(declaration)
	require.NoError(t, err)
	parent := &model.WorkflowQueue{TaskID: "1234567890abcdef12345678", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob, JobSpec: string(raw)}
	stages, err := BuildTask(parent, "space-ns")
	require.NoError(t, err)
	require.Len(t, stages, 2)
	require.Len(t, stages[0], 3, "only the owned volume gets a PVC task; existing claims stay references")
	require.Len(t, stages[1], 1)
	volumeTask := stages[0][0]
	require.Equal(t, string(config.JobDeployPVC), volumeTask.JobType)
	require.Equal(t, spec.JobVolumeName(parent.TaskID), volumeTask.Name)
	pvc := volumeTask.JobInfo.(*corev1.PersistentVolumeClaim)
	require.Equal(t, "space-ns", pvc.Namespace)
	require.Equal(t, parent.TaskID, pvc.Labels[spec.JobTaskLabel])
	require.Equal(t, "2Gi", pvc.Spec.Resources.Requests.Storage().String())
	require.Equal(t, "shared", *pvc.Spec.StorageClassName)
	require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, pvc.Spec.AccessModes)
	for _, task := range []*model.JobTask{stages[0][1], stages[0][2], stages[1][0]} {
		workload := task.JobInfo.(*batchv1.Job)
		require.EqualValues(t, 90, task.Timeout)
		require.EqualValues(t, 90, *workload.Spec.ActiveDeadlineSeconds)
		pod := workload.Spec.Template.Spec
		require.Equal(t, volumeTask.Name, pod.Volumes[len(pod.Volumes)-1].PersistentVolumeClaim.ClaimName)
		for _, containers := range [][]corev1.Container{pod.Containers, pod.InitContainers} {
			for _, container := range containers {
				require.Contains(t, container.VolumeMounts, corev1.VolumeMount{Name: "workspace", MountPath: "/workspace"})
			}
		}
	}
	fetch := stages[0][1].JobInfo.(*batchv1.Job)
	require.EqualValues(t, 2, *fetch.Spec.BackoffLimit)
	require.EqualValues(t, 0, *stages[0][2].JobInfo.(*batchv1.Job).Spec.BackoffLimit)
	require.EqualValues(t, 0, *stages[1][0].JobInfo.(*batchv1.Job).Spec.BackoffLimit)
	require.Equal(t, "250m", fetch.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String())
	require.Equal(t, "existing-input", fetch.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
	require.Equal(t, "existing-seed", fetch.Spec.Template.Spec.Volumes[1].PersistentVolumeClaim.ClaimName)
}

func TestBuildTaskRejectsInvalidGeneratedNameForPersistedTask(t *testing.T) {
	request := commandRequest()
	raw, err := json.Marshal(request.JobSpec)
	require.NoError(t, err)
	_, err = BuildTask(&model.WorkflowQueue{TaskID: strings.Repeat("a", 60), WorkspaceID: "space", Type: config.WorkflowTaskTypeJob, JobSpec: string(raw)}, "space-ns")
	require.ErrorContains(t, err, "generates invalid Kubernetes Job name")
}
