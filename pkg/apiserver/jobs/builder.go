package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/workspace"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

var errEvaluationPolicyUnavailable = errors.New("evaluation timeout policy unavailable")

// evaluationInfo is private execution metadata; it never appears in API JobInfo.
// Each Job owns its declaration and capability even when a workflow has several evaluations.
type evaluationInfo struct {
	Traits                 spec.JobTraits `json:"traits"`
	RunnerToken            string         `json:"runnerToken"`
	FrameworkVersion       string         `json:"frameworkVersion"`
	ResumeCheckpointID     string         `json:"resumeCheckpointId,omitempty"`
	RootExecutionKey       string         `json:"rootExecutionKey,omitempty"`
	RecoveryOfExecutionKey string         `json:"recoveryOfExecutionKey,omitempty"`
	RecoveryIndex          int            `json:"recoveryIndex,omitempty"`
	ExecutionDeadline      int64          `json:"executionDeadline,omitempty"`
	RecoveryRunnerStopped  bool           `json:"recoveryRunnerStopped,omitempty"`
	RecoveryIsolated       bool           `json:"recoveryIsolated,omitempty"`
	RecoveryName           string         `json:"recoveryName,omitempty"`
}

func decodeEvaluationInfo(raw string) (*evaluationInfo, error) {
	var info evaluationInfo
	if err := spec.DecodeJobJSON([]byte(raw), &info); err != nil {
		return nil, fmt.Errorf("decode evaluation execution: %w", err)
	}
	if len(info.RunnerToken) != 64 || info.FrameworkVersion != spec.HarborVersion {
		return nil, fmt.Errorf("invalid evaluation execution metadata")
	}
	if err := spec.NormalizeEvaluationTraits(&info.Traits); err != nil {
		return nil, err
	}
	return &info, nil
}

// SetEvaluationTraits initializes a new execution declaration. Recovery must
// restore the committed EvaluationInfo before rendering its Kubernetes Job.
func SetEvaluationTraits(job *model.JobTask, traits spec.JobTraits) error {
	if job == nil {
		return fmt.Errorf("evaluation Job is required")
	}
	if err := spec.NormalizeEvaluationTraits(&traits); err != nil {
		return err
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return fmt.Errorf("create evaluation capability: %w", err)
	}
	raw, err := json.Marshal(evaluationInfo{Traits: traits, RunnerToken: hex.EncodeToString(token), FrameworkVersion: spec.HarborVersion})
	if err != nil {
		return err
	}
	job.EvaluationInfo = string(raw)
	job.JobType = string(config.JobEval)
	return nil
}

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
	job := &model.JobTask{Name: "eruun-job-" + task.TaskID, Namespace: namespace, WorkspaceID: task.WorkspaceID, TaskID: task.TaskID, JobType: declaration.Type, Status: config.StatusQueued}
	if declaration.Traits.Evaluation != nil {
		if err := SetEvaluationTraits(job, declaration.Traits); err != nil {
			return nil, err
		}
		return job, nil
	}
	var command spec.CommandJobSpec
	if err := spec.DecodeJobJSON(declaration.Spec, &command); err != nil {
		return nil, err
	}
	workload, err := buildCommandJob(job.Name, namespace, command, declaration.Traits)
	if err != nil {
		return nil, err
	}
	job.Timeout = command.TimeoutSeconds
	job.JobInfo = workload
	workload.Spec.ActiveDeadlineSeconds = ptr.To(job.Timeout)
	return job, nil
}

// BuildEvaluationTask renders both standalone and Application evaluation Jobs.
// A committed snapshot wins over the supplied traits when recovering an execution.
func BuildEvaluationTask(ctx context.Context, store datastore.DataStore, cfg *config.Config, job *model.JobTask, traits spec.JobTraits) error {
	if job == nil || job.Name == "" || job.Namespace == "" || job.WorkspaceID == "" || job.TaskID == "" {
		return fmt.Errorf("invalid evaluation Job identity")
	}
	if cfg == nil || cfg.Jobs == nil {
		return fmt.Errorf("evaluation Runner is not configured")
	}
	if job.EvaluationInfo == "" {
		if err := SetEvaluationTraits(job, traits); err != nil {
			return err
		}
	}
	info, err := decodeEvaluationInfo(job.EvaluationInfo)
	if err != nil {
		return err
	}
	if info.RecoveryName != "" {
		job.Name = info.RecoveryName
	}
	evaluation := info.Traits.Evaluation
	// An online limit change governs newly rendered executions. A committed
	// running/distributed execution or recovery reservation retains its deadline;
	// lowering the policy must not terminate healthy long-running work.
	if info.ResumeCheckpointID == "" && job.Status != config.StatusRunning && job.Status != config.StatusDistributed {
		policy, err := repository.LoadJobSchedulerPolicy(ctx, store)
		if err != nil {
			return fmt.Errorf("%w: %w", errEvaluationPolicyUnavailable, err)
		}
		if evaluation.TimeoutSeconds > policy.MaxEvaluationTimeoutSeconds {
			return fmt.Errorf("evaluation timeoutSeconds exceeds the current maximum of %d", policy.MaxEvaluationTimeoutSeconds)
		}
	}
	if evaluation.ResultPolicy == nil {
		space := &model.Workspace{ID: job.WorkspaceID}
		if err := store.Get(ctx, space); err != nil {
			return fmt.Errorf("resolve evaluation workspace: %w", err)
		}
		if space.Namespace != job.Namespace {
			return fmt.Errorf("evaluation namespace does not belong to its workspace")
		}
		policy := spec.DefaultJobResultPolicy()
		if len(space.JobResultPolicy) != 0 {
			if err := spec.DecodeJobJSON(space.JobResultPolicy, &policy); err != nil {
				return fmt.Errorf("read workspace evaluation policy: %w", err)
			}
		}
		if err := policy.Validate(); err != nil {
			return fmt.Errorf("workspace evaluation policy: %w", err)
		}
		evaluation.ResultPolicy = &policy
	}
	for _, target := range evaluation.ResultPolicy.Targets {
		if target.Type == "minio" && cfg.Jobs.MinIO == nil {
			return fmt.Errorf("MinIO has not been configured by the administrator")
		}
	}
	dataset := &model.JobArtifact{ID: evaluation.TaskPackageID}
	if err := store.Get(ctx, dataset); err != nil {
		return fmt.Errorf("resolve evaluation task package: %w", err)
	}
	if dataset.Kind != artifacts.KindDataset || dataset.WorkspaceID != job.WorkspaceID || dataset.Expired {
		return fmt.Errorf("task package is unavailable in this workspace")
	}
	workload, err := buildCommandJob(job.Name, job.Namespace, spec.CommandJobSpec{Image: cfg.Jobs.RunnerImage, Command: []string{"python", "/opt/eruun/runner.py"}, TimeoutSeconds: evaluation.TimeoutSeconds}, info.Traits)
	if err != nil {
		return err
	}
	runner := &workload.Spec.Template.Spec.Containers[0]
	runner.Name = "runner"
	runner.WorkingDir = "/work"
	runner.SecurityContext = &corev1.SecurityContext{RunAsUser: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(true), AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
	agent := map[string]string{"name": evaluation.Agent}
	if evaluation.Model != "" {
		agent["model"] = evaluation.Model
	}
	runnerConfig := map[string]any{
		"taskId": job.TaskID, "executionKey": job.ExecutionKey, "namespace": job.Namespace,
		"datasetURL": cfg.Jobs.APIURL + "/api/v1/job-runners/" + job.TaskID + "/dataset",
		"resultURL":  cfg.Jobs.APIURL + "/api/v1/job-runners/" + job.TaskID + "/results",
		"eventURL":   cfg.Jobs.APIURL + "/api/v1/job-runners/" + job.TaskID + "/events",
		"sandboxURL": cfg.Jobs.APIURL + "/api/v1/job-runners/" + job.TaskID + "/sandboxes",
		"token":      info.RunnerToken, "datasetDigest": dataset.Digest,
		"agent":     agent,
		"options":   map[string]int{"attempts": evaluation.Attempts, "concurrency": evaluation.Concurrency},
		"resources": evaluation.SandboxResources, "sandboxServiceAccount": "default", "timeoutSeconds": evaluation.TimeoutSeconds,
		"transferTimeoutSeconds": spec.JobArchiveTimeoutSeconds, "finalizationTimeoutSeconds": spec.EvaluationCollectionGraceSeconds,
	}
	if evaluation.Recovery != nil {
		runnerConfig["recovery"] = evaluation.Recovery
		runnerConfig["checkpointURL"] = cfg.Jobs.APIURL + "/api/v1/job-runners/" + job.TaskID + "/checkpoints"
	}
	if info.ResumeCheckpointID != "" {
		runnerConfig["resumeCheckpointId"] = info.ResumeCheckpointID
		runnerConfig["executionDeadline"] = time.Unix(0, info.ExecutionDeadline).Unix() - spec.EvaluationCollectionGraceSeconds
	}
	configJSON, err := json.Marshal(runnerConfig)
	if err != nil {
		return err
	}
	runner.Env = append(runner.Env, corev1.EnvVar{Name: "ERUUN_JOB_CONFIG", Value: string(configJSON)})
	for _, field := range [][2]string{{"POD_NAME", "metadata.name"}, {"POD_UID", "metadata.uid"}, {"POD_NAMESPACE", "metadata.namespace"}} {
		runner.Env = append(runner.Env, corev1.EnvVar{Name: field[0], ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: field[1]}}})
	}
	runner.VolumeMounts = []corev1.VolumeMount{{Name: "work", MountPath: "/work"}}
	// Bound the long-lived Runner's datasets, logs and archive staging. The
	// kubelet enforces the Pod limit; exhaustion is a failed execution, never a
	// reason to report a truncated archive as complete.
	workStorageMiB := cfg.Jobs.RunnerWorkStorageMiB
	if workStorageMiB == 0 {
		workStorageMiB = spec.DefaultRunnerWorkStorageMiB
	}
	workStorage := *resource.NewQuantity(workStorageMiB*1024*1024, resource.BinarySI)
	runner.Resources.Requests[corev1.ResourceEphemeralStorage] = workStorage.DeepCopy()
	runner.Resources.Limits[corev1.ResourceEphemeralStorage] = workStorage.DeepCopy()
	workload.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "work", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &workStorage}}}}
	workload.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: ptr.To(int64(1000))}
	workload.Spec.Template.Spec.ServiceAccountName = workspace.EvaluationRunnerName
	workload.Spec.Template.Spec.AutomountServiceAccountToken = ptr.To(true)
	workload.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(spec.EvaluationCollectionGraceSeconds))
	workload.Spec.Template.Labels = map[string]string{workspace.EvaluationRunnerLabel: "true", "eruun.io/task-id": job.TaskID}
	// Preserve component ownership for status queries and component cleanup.
	// Runner authentication uses the checkpointed Kubernetes name and UID.
	if previous, ok := job.JobInfo.(*batchv1.Job); ok && previous != nil {
		if workload.Labels == nil {
			workload.Labels = map[string]string{}
		}
		for key, value := range previous.Labels {
			if key == workspace.EvaluationRunnerLabel || key == "eruun.io/task-id" {
				continue
			}
			workload.Labels[key] = value
			workload.Spec.Template.Labels[key] = value
		}
		if name := previous.Annotations[config.AnnotationComponentName]; name != "" {
			if workload.Annotations == nil {
				workload.Annotations = map[string]string{}
			}
			if workload.Spec.Template.Annotations == nil {
				workload.Spec.Template.Annotations = map[string]string{}
			}
			workload.Annotations[config.AnnotationComponentName] = name
			workload.Spec.Template.Annotations[config.AnnotationComponentName] = name
		}
	}
	workload.Spec.TTLSecondsAfterFinished = ptr.To(int32(evaluation.ResultPolicy.RetentionDays * 86400))
	job.Timeout = evaluation.TimeoutSeconds + spec.EvaluationCollectionGraceSeconds
	if info.ExecutionDeadline > 0 {
		if job.Status == config.StatusRunning && job.InternalInfo != "" {
			// Takeover observes the committed attempt, including its collection
			// window. Admission handles an elapsed absolute deadline as Timeout;
			// only a new recovery must still have time to start its agent.
			return workflowjob.RestoreInstantJobRetryCheckpoint(job)
		}
		job.Timeout = int64(time.Until(time.Unix(0, info.ExecutionDeadline)).Seconds())
		if job.Timeout <= spec.EvaluationCollectionGraceSeconds {
			return fmt.Errorf("evaluation recovery deadline elapsed")
		}
		if workload.Annotations == nil {
			workload.Annotations = map[string]string{}
		}
		workload.Annotations[workflowjob.EvaluationDeadlineAnnotation] = fmt.Sprint(info.ExecutionDeadline)
	}
	workload.Spec.ActiveDeadlineSeconds = ptr.To(job.Timeout)
	job.JobType = string(config.JobEval)
	job.JobInfo = workload
	snapshot, err := json.Marshal(info)
	if err != nil {
		return err
	}
	job.EvaluationInfo = string(snapshot)
	return nil
}
