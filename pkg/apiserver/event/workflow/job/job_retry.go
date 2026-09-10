package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type instantJobRetryCheckpoint struct {
	Kind        string       `json:"kind"`
	Version     int          `json:"version"`
	Attempt     uint         `json:"attempt"`
	PreviousUID types.UID    `json:"previousUID,omitempty"`
	CurrentUID  types.UID    `json:"currentUID,omitempty"`
	RetryAt     int64        `json:"retryAt,omitempty"`
	Deadline    int64        `json:"deadline"`
	Job         *batchv1.Job `json:"job"`
}

// HasInstantJobRetryCheckpoint includes malformed snapshots so recovery fails
// closed instead of silently granting a fresh retry budget. Instant Jobs do not
// use InternalInfo for other checkpoint kinds.
func HasInstantJobRetryCheckpoint(record *model.JobInfo) bool {
	return record != nil && record.Type == string(config.JobDeployInstant) && strings.TrimSpace(record.InternalInfo) != ""
}

func RestoreInstantJobRetryCheckpoint(task *model.JobTask) error {
	cp, err := decodeInstantJobRetryCheckpoint(task)
	if err != nil {
		return err
	}
	task.JobInfo = cp.Job.DeepCopy()
	task.Attempt = cp.Attempt
	return nil
}

func decodeInstantJobRetryCheckpoint(task *model.JobTask) (*instantJobRetryCheckpoint, error) {
	if task == nil {
		return nil, fmt.Errorf("instant Job retry checkpoint task is nil")
	}
	var cp instantJobRetryCheckpoint
	if err := json.Unmarshal([]byte(task.InternalInfo), &cp); err != nil {
		return nil, fmt.Errorf("decode instant Job retry checkpoint: %w", err)
	}
	if cp.Kind != "instant_job_retry" || cp.Version != 1 || cp.Job == nil || cp.Job.Name == "" || cp.Job.Namespace == "" || cp.Attempt == 0 || cp.Deadline <= 0 {
		return nil, fmt.Errorf("invalid instant Job retry checkpoint")
	}
	if !retryJobMatchesTask(cp.Job, task) || cp.Job.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(cp.Attempt), 10) || (task.Attempt > 0 && task.Attempt != cp.Attempt) {
		return nil, fmt.Errorf("instant Job retry checkpoint execution identity or attempt does not match")
	}
	policy, err := retryPolicyFromJob(cp.Job)
	if err != nil || policy == nil {
		return nil, fmt.Errorf("instant Job retry checkpoint policy is invalid: %v", err)
	}
	if cp.Attempt > policy.MaxRetries+1 || (cp.Attempt > 1 && (cp.PreviousUID == "" || cp.RetryAt <= 0)) {
		return nil, fmt.Errorf("instant Job retry checkpoint exceeds its retry budget or lacks prior identity")
	}
	return &cp, nil
}

func retryJobMatchesTask(job *batchv1.Job, task *model.JobTask) bool {
	return job != nil && task.TaskID != "" && task.ExecutionKey != "" && task.RunGeneration > 0 &&
		job.Annotations[config.AnnotationJobTaskID] == task.TaskID &&
		job.Annotations[config.AnnotationJobExecutionKey] == task.ExecutionKey &&
		job.Annotations[config.AnnotationJobRunGeneration] == strconv.FormatUint(task.RunGeneration, 10)
}

func retryPolicyFromJob(job *batchv1.Job) (*workflowconfig.JobRetryPolicy, error) {
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
	if err := validateRetryJobResources(job, &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

func (c *InstantJobCtl) ensureRetryWorkflowOwnership(ctx context.Context) error {
	status, err := currentJobWorkflowOwnershipStatus(ctx, c.store, c.job)
	if err != nil {
		return err
	}
	switch status {
	case config.StatusRunning:
		return nil
	case config.StatusCancelled:
		// Cancellation is committed before its signal is published. Recognize
		// it while polling too, and retain the same lease fence for final save.
		c.job.OwnerStatus = config.StatusCancelled
		return context.Canceled
	default:
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("%w: workflow is %s", errWorkflowJobOwnershipChanged, status))
	}
}

func retryJobRetentionSeconds(deadline int64, earliestTermination time.Time) (int32, error) {
	if earliestTermination.IsZero() {
		earliestTermination = time.Now()
	}
	remaining := time.Unix(0, deadline).Sub(earliestTermination)
	seconds := int64(config.DefaultJobTTLSeconds)
	if remaining > 0 {
		seconds += int64(remaining/time.Second) + 1
	}
	if seconds > math.MaxInt32 {
		return 0, fmt.Errorf("job retry recovery deadline exceeds the Kubernetes TTL range")
	}
	return int32(seconds), nil
}

func (c *InstantJobCtl) retainRetryCheckpoint(ctx context.Context, cp *instantJobRetryCheckpoint, createdAt time.Time) error {
	if cp.Job.Spec.TTLSecondsAfterFinished != nil {
		return nil
	}
	// Kubernetes counts TTL from termination, which can precede recovery.
	// Creation is a conservative lower bound for an old attempt's termination.
	ttl, err := retryJobRetentionSeconds(cp.Deadline, createdAt)
	if err != nil {
		return errors.Join(signal.ErrInfrastructureStop, err)
	}
	cp.Job.Spec.TTLSecondsAfterFinished = &ttl
	return c.persistRetryCheckpoint(ctx, cp)
}

func (c *InstantJobCtl) runWithRetryPolicy(ctx context.Context, desired *batchv1.Job, policy *workflowconfig.JobRetryPolicy) error {
	var cp *instantJobRetryCheckpoint
	if c.job.InternalInfo != "" {
		var err error
		cp, err = decodeInstantJobRetryCheckpoint(c.job)
		if err != nil {
			return errors.Join(signal.ErrInfrastructureStop, err)
		}
		policy, err = retryPolicyFromJob(cp.Job)
		if err != nil {
			return errors.Join(signal.ErrInfrastructureStop, err)
		}
	} else {
		var err error
		cp, err = c.newRetryCheckpoint(ctx, desired)
		if err != nil || cp == nil {
			return err
		}
	}
	runCtx, cancel := context.WithDeadline(ctx, time.Unix(0, cp.Deadline))
	defer cancel()
	for {
		if err := c.ensureRetryAttempt(runCtx, cp); err != nil {
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			return err
		}
		live, err := c.waitRetryAttempt(runCtx, cp)
		if err != nil {
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			return err
		}
		status, message, _ := jobTerminalStatus(live)
		if status == config.StatusCompleted {
			c.job.Status = config.StatusCompleted
			c.job.Error = ""
			return nil
		}
		if policy.OnOOM == "stop" || cp.Attempt > policy.MaxRetries {
			return NewStatusError(config.StatusFailed, fmt.Errorf("job failed after attempt %d: %s", cp.Attempt, message))
		}
		oomContainers, err := oomJobContainers(runCtx, c.client, live)
		if err != nil {
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			return errors.Join(signal.ErrInfrastructureStop, err)
		}
		if len(oomContainers) == 0 {
			return NewStatusError(config.StatusFailed, fmt.Errorf("non-OOM job failure: %s", message))
		}
		next := cp.Job.DeepCopy()
		if policy.OnOOM == "resize" {
			next, err = growOOMJobResources(next, policy, oomContainers)
			if err != nil {
				return NewStatusError(config.StatusFailed, err)
			}
		}
		cp = &instantJobRetryCheckpoint{
			Kind: cp.Kind, Version: cp.Version, Attempt: cp.Attempt + 1,
			PreviousUID: live.UID, RetryAt: time.Now().Add(time.Duration(policy.BackoffSeconds) * time.Second).UnixNano(),
			Deadline: cp.Deadline, Job: next,
		}
		cp.Job.Annotations[workflowconfig.AnnotationJobAttempt] = strconv.FormatUint(uint64(cp.Attempt), 10)
		if err := c.persistRetryCheckpoint(runCtx, cp); err != nil {
			return err
		}
		klog.InfoS("Scheduled Job OOM retry", "taskID", c.job.TaskID, "job", cp.Job.Name, "attempt", cp.Attempt, "action", policy.OnOOM)
	}
}

func (c *InstantJobCtl) newRetryCheckpoint(ctx context.Context, desired *batchv1.Job) (*instantJobRetryCheckpoint, error) {
	if c.store == nil || !retryJobMatchesTask(desired, c.job) {
		return nil, fmt.Errorf("jobRetryPolicy requires datastore and workflow execution identity")
	}
	if err := c.ensureRetryWorkflowOwnership(ctx); err != nil {
		return nil, err
	}
	// Reusing an active Job with a different policy would keep Kubernetes'
	// old Pod retry budget. Require an explicit recreate for that transition.
	existing, exists, err := jobExists(ctx, c.client, desired.Namespace, desired.Name)
	if err != nil {
		return nil, err
	}
	if exists && runPolicyFromJob(desired) != workflowconfig.JobRunPolicyRecreate {
		status, _, done := jobTerminalStatus(existing)
		if !(done && status == config.StatusCompleted) {
			return nil, fmt.Errorf("enabling jobRetryPolicy on an existing unfinished Job requires runPolicy recreate")
		}
	}
	action, err := applyJobRunPolicy(ctx, c.client, c.store, desired, config.JobDeployInstant)
	if err != nil {
		return nil, err
	}
	if action == runPolicyActionSkip {
		c.job.Status = config.StatusSkipped
		return nil, nil
	}
	timeout := c.job.Timeout
	if timeout <= 0 {
		timeout = int64(config.DefaultJobTaskTimeout.Seconds())
	}
	cp := &instantJobRetryCheckpoint{
		Kind: "instant_job_retry", Version: 1, Attempt: 1,
		Deadline: time.Now().Add(time.Duration(timeout) * time.Second).UnixNano(), Job: desired.DeepCopy(),
	}
	ttl, err := retryJobRetentionSeconds(cp.Deadline, time.Now())
	if err != nil {
		return nil, err
	}
	cp.Job.Spec.TTLSecondsAfterFinished = &ttl
	cp.Job.Annotations[workflowconfig.AnnotationJobAttempt] = "1"
	if err := c.persistRetryCheckpoint(ctx, cp); err != nil {
		return nil, err
	}
	return cp, nil
}

func (c *InstantJobCtl) persistRetryCheckpoint(ctx context.Context, cp *instantJobRetryCheckpoint) error {
	raw, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("encode instant Job retry checkpoint: %w", err)
	}
	c.job.InternalInfo = string(raw)
	c.job.Attempt = cp.Attempt
	c.job.JobInfo = cp.Job.DeepCopy()
	c.job.Status = config.StatusRunning
	if err := saveJobInfo(ctx, c.store, c.job); err != nil {
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("persist instant Job retry checkpoint: %w", err))
	}
	c.ack()
	return nil
}

func (c *InstantJobCtl) ensureRetryAttempt(ctx context.Context, cp *instantJobRetryCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay := time.Until(time.Unix(0, cp.RetryAt)); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if err := c.ensureRetryWorkflowOwnership(ctx); err != nil {
		return err
	}
	live, exists, err := jobExists(ctx, c.client, cp.Job.Namespace, cp.Job.Name)
	if err != nil {
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("read Job retry attempt: %w", err))
	}
	if exists {
		if !retryJobMatchesTask(live, c.job) {
			return errors.Join(signal.ErrInfrastructureStop, errJobExecutionIdentityChanged)
		}
		if live.Annotations[workflowconfig.AnnotationJobAttempt] == strconv.FormatUint(uint64(cp.Attempt), 10) {
			return c.retainLiveRetryAttempt(ctx, cp, live)
		}
		if cp.PreviousUID == "" || live.UID != cp.PreviousUID || live.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(cp.Attempt-1), 10) || !retryJobFailed(live) {
			return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("%w: refusing to replace unrecognized Job attempt", errJobExecutionIdentityChanged))
		}
		if err := c.deleteRetryJob(ctx, live); err != nil {
			return err
		}
	}
	if cp.CurrentUID != "" {
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("job attempt %d with UID %s disappeared; refusing to replay it", cp.Attempt, cp.CurrentUID))
	}
	if cp.PreviousUID != "" {
		if err := c.waitPreviousRetryAttempt(ctx, cp); err != nil {
			return err
		}
	}
	if err := c.ensureRetryWorkflowOwnership(ctx); err != nil {
		return err
	}
	if err := c.retainRetryCheckpoint(ctx, cp, time.Now()); err != nil {
		return err
	}
	created, err := c.client.BatchV1().Jobs(cp.Job.Namespace).Create(ctx, cp.Job.DeepCopy(), metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		live, getErr := c.client.BatchV1().Jobs(cp.Job.Namespace).Get(ctx, cp.Job.Name, metav1.GetOptions{})
		if getErr != nil {
			return errors.Join(signal.ErrInfrastructureStop, getErr)
		}
		if !retryJobMatchesTask(live, c.job) || live.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(cp.Attempt), 10) {
			return errors.Join(signal.ErrInfrastructureStop, errJobExecutionIdentityChanged)
		}
		return c.persistRetryAttemptUID(ctx, cp, live)
	}
	if err != nil {
		// An uncertain create must resume this checkpoint, not commit a failure
		// that could lead to a second execution while the first still runs.
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("create Job retry attempt: %w", err))
	}
	return c.persistRetryAttemptUID(ctx, cp, created)
}

func (c *InstantJobCtl) retainLiveRetryAttempt(ctx context.Context, cp *instantJobRetryCheckpoint, live *batchv1.Job) error {
	if cp.CurrentUID != "" && live.UID != cp.CurrentUID {
		return errors.Join(signal.ErrInfrastructureStop, errJobExecutionIdentityChanged)
	}
	if err := c.retainRetryCheckpoint(ctx, cp, live.CreationTimestamp.Time); err != nil {
		return err
	}
	if live.Spec.TTLSecondsAfterFinished == nil || *live.Spec.TTLSecondsAfterFinished < *cp.Job.Spec.TTLSecondsAfterFinished {
		if err := c.ensureRetryWorkflowOwnership(ctx); err != nil {
			return err
		}
		updated := live.DeepCopy()
		updated.Spec.TTLSecondsAfterFinished = cp.Job.Spec.TTLSecondsAfterFinished
		var err error
		live, err = c.client.BatchV1().Jobs(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("retain Job retry recovery evidence: %w", err))
		}
	}
	return c.persistRetryAttemptUID(ctx, cp, live)
}

func (c *InstantJobCtl) persistRetryAttemptUID(ctx context.Context, cp *instantJobRetryCheckpoint, live *batchv1.Job) error {
	if live.UID == "" {
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("created Job attempt has no UID"))
	}
	MarkResourceCreated(ctx, domainspec.ResourceJob, live.Namespace, live.Name)
	if cp.CurrentUID == live.UID {
		return nil
	}
	cp.CurrentUID = live.UID
	return c.persistRetryCheckpoint(ctx, cp)
}

func retryJobFailed(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (c *InstantJobCtl) waitRetryAttempt(ctx context.Context, cp *instantJobRetryCheckpoint) (*batchv1.Job, error) {
	var terminal *batchv1.Job
	err := wait.PollUntilContextCancel(ctx, jobPollInterval, true, func(ctx context.Context) (bool, error) {
		if err := c.ensureRetryWorkflowOwnership(ctx); err != nil {
			return false, err
		}
		live, err := c.client.BatchV1().Jobs(cp.Job.Namespace).Get(ctx, cp.Job.Name, metav1.GetOptions{})
		if err != nil {
			return false, errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("observe Job retry attempt: %w", err))
		}
		if !retryJobMatchesTask(live, c.job) || live.UID != cp.CurrentUID || live.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(cp.Attempt), 10) {
			return false, errors.Join(signal.ErrInfrastructureStop, errJobExecutionIdentityChanged)
		}
		status, _, done := jobTerminalStatus(live)
		if (done && status == config.StatusCompleted) || retryJobFailed(live) {
			terminal = live
			return true, nil
		}
		return false, nil
	})
	return terminal, err
}

func (c *InstantJobCtl) deleteRetryJob(ctx context.Context, live *batchv1.Job) error {
	if err := c.ensureRetryWorkflowOwnership(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if live.UID == "" || !retryJobMatchesTask(live, c.job) {
		return errors.Join(signal.ErrInfrastructureStop, errJobExecutionIdentityChanged)
	}
	propagation := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{PropagationPolicy: &propagation, Preconditions: &metav1.Preconditions{UID: &live.UID}}
	if live.ResourceVersion != "" {
		options.Preconditions.ResourceVersion = &live.ResourceVersion
	}
	if err := c.client.BatchV1().Jobs(live.Namespace).Delete(ctx, live.Name, options); err != nil && !k8serrors.IsNotFound(err) {
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("delete previous Job retry attempt: %w", err))
	}
	return nil
}

func (c *InstantJobCtl) cleanRetryAttempt(ctx context.Context) error {
	cp, err := decodeInstantJobRetryCheckpoint(c.job)
	if err != nil {
		return err
	}
	if c.client == nil {
		return fmt.Errorf("clean Job retry attempt: client is nil")
	}
	// A recovered execution has no process-local cleanup tracker. Its durable
	// checkpoint still identifies resources created by a previous lease owner,
	// including the failed attempt retained during retry backoff.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.DelTimeOut)
	defer cancel()
	live, err := c.client.BatchV1().Jobs(cp.Job.Namespace).Get(cleanupCtx, cp.Job.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	wantAttempt := cp.Attempt
	if cp.CurrentUID != "" {
		if live.UID != cp.CurrentUID {
			return errJobExecutionIdentityChanged
		}
	} else if cp.PreviousUID != "" && live.UID == cp.PreviousUID {
		wantAttempt--
	}
	if live.Annotations[workflowconfig.AnnotationJobAttempt] != strconv.FormatUint(uint64(wantAttempt), 10) {
		return errJobExecutionIdentityChanged
	}
	// When create succeeded but its response/checkpoint was lost, the intended
	// attempt's exact execution identity is the same recovery evidence used by
	// ensureRetryAttempt; deletion still pins the UID read above.
	return c.deleteRetryJob(cleanupCtx, live)
}

func (c *InstantJobCtl) waitPreviousRetryAttempt(ctx context.Context, cp *instantJobRetryCheckpoint) error {
	previous := cp.Job.DeepCopy()
	previous.UID = cp.PreviousUID
	return wait.PollUntilContextCancel(ctx, jobPollInterval, true, func(ctx context.Context) (bool, error) {
		if err := c.ensureRetryWorkflowOwnership(ctx); err != nil {
			return false, err
		}
		live, exists, err := jobExists(ctx, c.client, previous.Namespace, previous.Name)
		if err != nil {
			return false, errors.Join(signal.ErrInfrastructureStop, err)
		}
		if exists {
			if live.UID != cp.PreviousUID {
				return false, errors.Join(signal.ErrInfrastructureStop, errJobExecutionIdentityChanged)
			}
			return false, nil
		}
		pods, err := c.client.CoreV1().Pods(previous.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{batchv1.JobNameLabel: previous.Name}.String()})
		if err != nil {
			return false, errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("wait previous Job pods: %w", err))
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if retryPodOwnedByJob(pod, previous) && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
				return false, nil
			}
		}
		return true, nil
	})
}
