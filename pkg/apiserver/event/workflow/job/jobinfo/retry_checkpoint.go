package jobinfo

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

type InstantJobRetryCheckpoint struct {
	Kind        string          `json:"kind"`
	Version     int             `json:"version"`
	Attempt     uint            `json:"attempt"`
	PreviousUID types.UID       `json:"previousUID,omitempty"`
	CurrentUID  types.UID       `json:"currentUID,omitempty"`
	RetryAt     int64           `json:"retryAt,omitempty"`
	Deadline    int64           `json:"deadline"`
	Job         *batchv1.Job    `json:"job"`
	Runner      json.RawMessage `json:"runner,omitempty"`
}

// EvaluationRunnerCheckpoint returns the runner-owned extension embedded in
// the durable instant Job checkpoint together with that attempt's deadline.
// The caller owns the returned bytes.
func EvaluationRunnerCheckpoint(record *model.JobInfo) (json.RawMessage, int64, error) {
	if record == nil || record.Type != string(config.JobEval) || record.ExecutionKey == nil {
		return nil, 0, fmt.Errorf("evaluation runner checkpoint identity is incomplete")
	}
	task := &model.JobTask{
		TaskID: record.TaskID, ExecutionKey: *record.ExecutionKey, RunGeneration: record.RunGeneration,
		Attempt: record.Attempt, InternalInfo: record.InternalInfo,
	}
	cp, err := DecodeInstantJobRetryCheckpoint(task)
	if err != nil {
		return nil, 0, err
	}
	return append(json.RawMessage(nil), cp.Runner...), cp.Deadline, nil
}

// SetEvaluationRunnerCheckpoint replaces only the runner-owned extension and
// preserves the retry controller's workload identity and deadline.
func SetEvaluationRunnerCheckpoint(record *model.JobInfo, state json.RawMessage) error {
	if len(state) == 0 || !json.Valid(state) {
		return fmt.Errorf("evaluation runner state is invalid")
	}
	if record == nil || record.Type != string(config.JobEval) || record.ExecutionKey == nil {
		return fmt.Errorf("evaluation runner checkpoint identity is incomplete")
	}
	task := &model.JobTask{
		TaskID: record.TaskID, ExecutionKey: *record.ExecutionKey, RunGeneration: record.RunGeneration,
		Attempt: record.Attempt, InternalInfo: record.InternalInfo,
	}
	cp, err := DecodeInstantJobRetryCheckpoint(task)
	if err != nil {
		return err
	}
	cp.Runner = append(json.RawMessage(nil), state...)
	raw, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("encode evaluation runner checkpoint: %w", err)
	}
	record.InternalInfo = string(raw)
	return nil
}

// HasInstantJobRetryCheckpoint includes malformed snapshots so recovery fails
// closed instead of silently granting a fresh retry budget. Instant Jobs do not
// use InternalInfo for other checkpoint kinds.
func HasInstantJobRetryCheckpoint(record *model.JobInfo) bool {
	return record != nil && config.IsInstantJobType(config.JobType(record.Type)) && strings.TrimSpace(record.InternalInfo) != ""
}

// ValidateInstantJobRetryExecution binds a live Kubernetes Job to its durable
// checkpoint. A just-created Job can authenticate before its returned UID has
// been checkpointed, using the exact intended attempt and execution identity.
func ValidateInstantJobRetryExecution(record *model.JobInfo, live *batchv1.Job) error {
	if record == nil || record.ExecutionKey == nil || !HasInstantJobRetryCheckpoint(record) || live == nil || live.UID == "" {
		return fmt.Errorf("Job execution checkpoint identity is incomplete")
	}
	task := &model.JobTask{TaskID: record.TaskID, ExecutionKey: *record.ExecutionKey, RunGeneration: record.RunGeneration,
		Attempt: record.Attempt, InternalInfo: record.InternalInfo}
	cp, err := DecodeInstantJobRetryCheckpoint(task)
	if err != nil {
		return err
	}
	if live.Namespace != cp.Job.Namespace || live.Name != cp.Job.Name || !RetryJobMatchesTask(live, task) ||
		live.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(cp.Attempt), 10) ||
		(cp.CurrentUID != "" && cp.CurrentUID != live.UID) || live.UID == cp.PreviousUID {
		return ErrJobExecutionIdentityChanged
	}
	return nil
}

func DecodeInstantJobRetryCheckpoint(task *model.JobTask) (*InstantJobRetryCheckpoint, error) {
	if task == nil {
		return nil, fmt.Errorf("instant Job retry checkpoint task is nil")
	}
	var cp InstantJobRetryCheckpoint
	if err := json.Unmarshal([]byte(task.InternalInfo), &cp); err != nil {
		return nil, fmt.Errorf("decode instant Job retry checkpoint: %w", err)
	}
	if cp.Kind != "instant_job_retry" || cp.Version != 1 || cp.Job == nil || cp.Job.Name == "" || cp.Job.Namespace == "" || cp.Attempt == 0 || cp.Deadline <= 0 {
		return nil, fmt.Errorf("invalid instant Job retry checkpoint")
	}
	if !RetryJobMatchesTask(cp.Job, task) || cp.Job.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(cp.Attempt), 10) || (task.Attempt > 0 && task.Attempt != cp.Attempt) {
		return nil, fmt.Errorf("instant Job retry checkpoint execution identity or attempt does not match")
	}
	policy, err := RetryPolicyFromJob(cp.Job)
	if err != nil || policy == nil {
		return nil, fmt.Errorf("instant Job retry checkpoint policy is invalid: %v", err)
	}
	if cp.Attempt > policy.MaxRetries+1 || (cp.Attempt > 1 && (cp.PreviousUID == "" || cp.RetryAt <= 0)) {
		return nil, fmt.Errorf("instant Job retry checkpoint exceeds its retry budget or lacks prior identity")
	}
	return &cp, nil
}

func RetryJobMatchesTask(job *batchv1.Job, task *model.JobTask) bool {
	return job != nil && task.TaskID != "" && task.ExecutionKey != "" && task.RunGeneration > 0 &&
		job.Annotations[config.AnnotationJobTaskID] == task.TaskID &&
		job.Annotations[config.AnnotationJobExecutionKey] == task.ExecutionKey &&
		job.Annotations[config.AnnotationJobRunGeneration] == strconv.FormatUint(task.RunGeneration, 10)
}

func RetryPolicyFromJob(job *batchv1.Job) (*workflowconfig.JobRetryPolicy, error) {
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	raw := job.Annotations[workflowconfig.AnnotationJobRetryPolicy]
	if raw == "" {
		return nil, nil
	}
	var policy workflowconfig.JobRetryPolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return nil, fmt.Errorf("decode jobRetryPolicy: %w", err)
	}
	if err := policy.Validate(); err != nil {
		return nil, fmt.Errorf("validate jobRetryPolicy: %w", err)
	}
	if strings.TrimSpace(job.Annotations[config.AnnotationJobStartTime]) != "" {
		return nil, fmt.Errorf("jobRetryPolicy does not support startTime")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return nil, fmt.Errorf("jobRetryPolicy requires backoffLimit 0 and restartPolicy Never")
	}
	if err := ValidateRetryJobResources(job, &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

func ValidateRetryJobResources(job *batchv1.Job, policy *workflowconfig.JobRetryPolicy) error {
	if policy.OnOOM != "resize" {
		return nil
	}
	for _, containers := range [][]corev1.Container{job.Spec.Template.Spec.InitContainers, job.Spec.Template.Spec.Containers} {
		for _, container := range containers {
			for name, factor := range map[corev1.ResourceName]int64{
				corev1.ResourceMemory: policy.MemoryGrowthFactor,
				corev1.ResourceCPU:    policy.CPUGrowthFactor,
			} {
				if factor <= 1 {
					continue
				}
				request := container.Resources.Requests[name]
				limit := container.Resources.Limits[name]
				cap := policy.MaxResources[name]
				if request.Sign() <= 0 || limit.Sign() <= 0 || request.Cmp(limit) > 0 {
					return fmt.Errorf("resize requires positive %s requests <= limits for container %s", name, container.Name)
				}
				if limit.Cmp(cap) > 0 {
					return fmt.Errorf("container %s %s limit exceeds maxResources", container.Name, name)
				}
			}
		}
	}
	return nil
}
