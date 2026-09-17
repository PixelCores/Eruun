package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func BuildTask(ctx context.Context, store datastore.DataStore, cfg *config.Config, task *model.WorkflowQueue, namespace string) (*model.JobTask, error) {
	if task == nil || task.Type != config.WorkflowTaskTypeJob || task.AppID != "" || task.WorkspaceID == "" || task.TaskID == "" {
		return nil, fmt.Errorf("invalid standalone Job identity")
	}
	var declaration spec.JobSpec
	if err := spec.DecodeJobJSON([]byte(task.JobSpec), &declaration); err != nil {
		return nil, err
	}
	if err := declaration.Normalize(); err != nil {
		return nil, err
	}
	name := "eruun-job-" + task.TaskID
	var workload *batchv1.Job
	var timeout int64
	var err error
	if declaration.Type == string(config.JobCommand) {
		var command spec.CommandJobSpec
		if err = spec.DecodeJobJSON(declaration.Spec, &command); err != nil {
			return nil, err
		}
		workload, err = workflowjob.BuildCommandJob(name, namespace, command, declaration.Traits)
		timeout = command.TimeoutSeconds
	} else {
		if cfg == nil || cfg.Jobs == nil || len(task.JobToken) != 64 || declaration.ResultPolicy == nil {
			return nil, fmt.Errorf("evaluation requires configured Runner, capability and result policy")
		}
		var evaluation spec.AgentEvaluationSpec
		if err = spec.DecodeJobJSON(declaration.Spec, &evaluation); err != nil {
			return nil, err
		}
		dataset := &model.JobArtifact{ID: evaluation.DatasetID}
		if err = store.Get(ctx, dataset); err != nil {
			return nil, err
		}
		if dataset.Kind != artifacts.KindDataset || dataset.WorkspaceID != task.WorkspaceID || dataset.Expired {
			return nil, fmt.Errorf("dataset is unavailable in this workspace")
		}
		timeout = evaluation.TimeoutSeconds
		workload, err = workflowjob.BuildCommandJob(name, namespace, spec.CommandJobSpec{Image: cfg.Jobs.RunnerImage, Command: []string{"python", "/opt/eruun/runner.py"}, TimeoutSeconds: timeout}, declaration.Traits)
		if err != nil {
			return nil, err
		}
		runner := &workload.Spec.Template.Spec.Containers[0]
		runner.Name = "runner"
		runner.WorkingDir = "/work"
		runner.SecurityContext = &corev1.SecurityContext{RunAsUser: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(true), AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
		configJSON, marshalErr := json.Marshal(map[string]any{
			"taskId": task.TaskID, "namespace": namespace, "datasetURL": cfg.Jobs.APIURL + "/api/v1/job-runners/" + task.TaskID + "/dataset", "resultURL": cfg.Jobs.APIURL + "/api/v1/job-runners/" + task.TaskID + "/results", "eventURL": cfg.Jobs.APIURL + "/api/v1/job-runners/" + task.TaskID + "/events", "token": task.JobToken, "datasetDigest": dataset.Digest,
			"agent": spec.EvaluationAgent{Name: evaluation.Agent.Name, Model: evaluation.Agent.Model}, "options": evaluation.Options, "resources": declaration.Traits.Resources, "sandboxServiceAccount": "default", "timeoutSeconds": timeout,
			"transferTimeoutSeconds":     spec.JobArchiveTimeoutSeconds,
			"finalizationTimeoutSeconds": spec.EvaluationCollectionGraceSeconds,
		})
		if marshalErr != nil {
			return nil, marshalErr
		}
		runner.Env = []corev1.EnvVar{{Name: "ERUUN_JOB_CONFIG", Value: string(configJSON)}}
		for _, field := range [][2]string{{"POD_NAME", "metadata.name"}, {"POD_UID", "metadata.uid"}, {"POD_NAMESPACE", "metadata.namespace"}} {
			runner.Env = append(runner.Env, corev1.EnvVar{Name: field[0], ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: field[1]}}})
		}
		for _, credential := range evaluation.Agent.Credentials {
			runner.Env = append(runner.Env, corev1.EnvVar{Name: credential.Name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: credential.SecretKeyRef.Name}, Key: credential.SecretKeyRef.Key}}})
		}
		runner.VolumeMounts = []corev1.VolumeMount{{Name: "work", MountPath: "/work"}}
		workload.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "work", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: nil}}}}
		workload.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: ptr.To(int64(1000))}
		workload.Spec.Template.Spec.ServiceAccountName = workspace.EvaluationRunnerName
		workload.Spec.Template.Spec.AutomountServiceAccountToken = ptr.To(true)
		workload.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(spec.EvaluationCollectionGraceSeconds))
		workload.Spec.Template.Labels = map[string]string{workspace.EvaluationRunnerLabel: "true", "eruun.io/task-id": task.TaskID}
		workload.Spec.TTLSecondsAfterFinished = ptr.To(int32(declaration.ResultPolicy.RetentionDays * 86400))
	}
	if err != nil {
		return nil, err
	}
	if declaration.Type == string(config.JobEval) {
		timeout += spec.EvaluationCollectionGraceSeconds // Allow the trusted runner to collect and upload its outputs.
	}
	workload.Spec.ActiveDeadlineSeconds = ptr.To(timeout)
	return &model.JobTask{Name: name, Namespace: namespace, WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, JobType: declaration.Type, Timeout: timeout, Status: config.StatusQueued, JobInfo: workload}, nil
}
