package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type InstantJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
}

func GenerateOneTimeJob(component *model.ApplicationComponent, properties *model.Properties, runPolicy string, startTime int64) (*jobexec.GenerateServiceResult, error) {
	job := jobexec.BuildJob(component, properties, jobexec.JobBuildOptions{RunPolicy: runPolicy, StartTime: startTime})
	if job == nil {
		return nil, nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, job)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &jobexec.GenerateServiceResult{
		Service:           job,
		AdditionalObjects: additionalObjects,
	}, nil
}

func NewInstantJobCtl(job *model.JobTask, runtime *jobexec.Runtime) *InstantJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("InstantJobCtl", job, runtime, nil)
	if !ok {
		return nil
	}
	return &InstantJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *InstantJobCtl) Clean(ctx context.Context) {
	if config.IsWorkspaceJobType(config.JobType(c.Job.JobType)) {
		logCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		logs, err := jobexec.CollectJobPodLogs(logCtx, c.Client, c.Namespace, c.Job.Name)
		cancel()
		if err == nil && logs != "" {
			c.Job.Info = logs
		}
	}
	if !c.allowEvaluationTerminalCleanup(ctx) {
		return
	}
	if c.Job.InternalInfo != "" {
		// A settled attempt is the durable checkpoint's recovery evidence until
		// SaveInfo commits its outcome. Cancellation, timeout and panic cleanup
		// still stop active work immediately through the same fenced deletion.
		if c.Job.Status == config.StatusCompleted || c.Job.Status == config.StatusFailed {
			return
		}
		if err := c.cleanRetryAttempt(ctx); err != nil && !k8serrors.IsNotFound(err) {
			klog.ErrorS(err, "clean instant Job retry attempt", "taskID", c.Job.TaskID)
		}
		return
	}
	c.CleanCreatedBatchJobs(ctx, "after failure")
}

func (c *InstantJobCtl) SaveInfo(ctx context.Context) error {
	if err := jobinfo.SaveJobInfo(ctx, c.Store, c.Job); err != nil {
		return err
	}
	// Retry recovery needs the exact live UID until its terminal result,
	// including collected logs, has been committed. Its TTL handles an exit
	// after this commit but before cleanup.
	if (c.Job.Status == config.StatusCompleted || c.Job.Status == config.StatusFailed) && c.Job.InternalInfo != "" && c.allowEvaluationTerminalCleanup(ctx) {
		if err := c.cleanRetryAttempt(ctx); err != nil && !k8serrors.IsNotFound(err) {
			klog.ErrorS(err, "clean terminal instant Job retry attempt", "taskID", c.Job.TaskID)
		}
	}
	return nil
}

func (c *InstantJobCtl) Run(ctx context.Context) error {
	c.Job.Status = config.StatusRunning
	c.Job.Error = ""
	c.Ack()
	if desired, ok := jobinfo.OptionalJobInfo[*batchv1.Job](c.Job); ok && (desired.Annotations[workflowconfig.AnnotationJobRetryPolicy] != "" || c.Job.InternalInfo != "") {
		if desired.Namespace == "" {
			desired.Namespace = c.Namespace
		}
		jobresult.StampJobExecutionIdentity(c.Job, desired)
		policy, err := jobinfo.RetryPolicyFromJob(desired)
		if err == nil {
			err = c.runWithRetryPolicy(ctx, desired, policy)
		}
		if (errors.Is(err, context.Canceled) || errors.Is(err, signal.ErrInfrastructureStop)) && !signal.IsInfrastructureStop(ctx) {
			// A signal between attempts or cancellation winning a checkpoint CAS
			// still needs the same lease's parent status for terminal persistence.
			// Resolve it even when no live Job remains for cleanup to inspect.
			ownershipCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			ownershipErr := c.ensureRetryWorkflowOwnership(ownershipCtx)
			cancel()
			if errors.Is(ownershipErr, context.Canceled) && !errors.Is(ownershipErr, signal.ErrInfrastructureStop) {
				err = context.Canceled
			}
		}
		if err == nil && c.Job.JobType == string(config.JobEval) && c.Job.Status == config.StatusCompleted {
			ready, sourceErr := c.evaluationSourceReady(ctx)
			if sourceErr != nil {
				err = errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("verify evaluation result collection: %w", sourceErr))
			} else if !ready {
				err = fmt.Errorf("evaluation runner completed without a collected result archive")
			}
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				err = jobinfo.NewStatusError(config.StatusCancelled, err)
			} else if errors.Is(err, context.DeadlineExceeded) {
				err = jobinfo.NewStatusError(config.StatusTimeout, err)
			}
			jobinfo.ApplyJobError(c.Job, err, "")
		}
		return err
	}

	if err := c.run(ctx); err != nil {
		jobinfo.ApplyJobError(c.Job, err, "")
		return err
	}
	if c.Job.Status == config.StatusSkipped {
		c.Job.Error = ""
		return nil
	}
	if c.Job.Status == config.StatusDistributed {
		c.Job.Error = ""
		return nil
	}
	status, message, err := c.WaitBatchJob(ctx)
	if err != nil {
		jobinfo.ApplyJobError(c.Job, err, message)
		return err
	}
	if status != "" {
		c.Job.Status = status
	} else {
		c.Job.Status = config.StatusCompleted
	}
	c.Job.Error = ""
	return nil
}

func (c *InstantJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}
	jobObj, err := jobinfo.BatchJobFromJobInfo(c.Job)
	if err != nil {
		return err
	}
	namespace := jobObj.Namespace
	if namespace == "" {
		namespace = c.Namespace
		jobObj.Namespace = namespace
	}
	jobresult.StampJobExecutionIdentity(c.Job, jobObj)

	startTime, hasStart := jobexec.StartTimeFromJob(jobObj)
	now := time.Now().Unix()
	if hasStart && startTime > now {
		jobType := jobexec.JobTypeForTask(c.Job, config.JobDeployInstant)
		payload := &jobinfo.DelayJobPayload{
			ExecuteAt:      startTime,
			Namespace:      namespace,
			JobType:        string(jobType),
			Job:            jobObj,
			TaskID:         c.Job.TaskID,
			ExecutionKey:   c.Job.ExecutionKey,
			RunGeneration:  c.Job.RunGeneration,
			RunToken:       c.Job.RunToken,
			ServiceName:    jobinfo.ResolveJobServiceName(c.Job),
			TimeoutSeconds: c.Job.Timeout,
		}
		if err := jobinfo.PersistDelayJobCheckpoint(ctx, c.Store, c.Job, payload); err != nil {
			return err
		}
		if _, err := jobinfo.EnqueueDelayJob(ctx, c.DelayQueue, payload); err != nil {
			klog.ErrorS(err, "delay queue notification failed; database recovery remains active", "taskID", c.Job.TaskID, "executionKey", c.Job.ExecutionKey)
		}
		c.Ack()
		return nil
	}

	if err := jobexec.EnsureCurrentJobWorkflowOwnership(ctx, c.Store, c.Job); err != nil {
		return err
	}
	action, err := jobexec.ApplyJobRunPolicy(ctx, c.Client, c.Store, jobObj, jobexec.JobTypeForTask(c.Job, config.JobDeployInstant))
	if err != nil {
		return err
	}
	if action == jobexec.RunPolicyActionSkip {
		c.Job.Status = config.StatusSkipped
		c.Job.Error = ""
		c.Ack()
		return nil
	}

	if _, err := c.CreateBatchJob(ctx, jobObj); err != nil {
		return err
	}
	return nil
}

// Terminal evaluation Pods are retained until the complete source archive is
// committed. Cancellation and timeout still stop active work through the normal
// fenced deletion path; the runner's termination grace allows a final upload.
func (c *InstantJobCtl) allowEvaluationTerminalCleanup(ctx context.Context) bool {
	if c.Job.JobType != string(config.JobEval) ||
		(c.Job.Status != config.StatusCompleted && c.Job.Status != config.StatusFailed) || ctx.Err() != nil {
		return true
	}
	ready, err := c.evaluationSourceReady(ctx)
	if err != nil {
		klog.ErrorS(err, "retain evaluation Job after result collection lookup failed", "taskID", c.Job.TaskID)
	}
	return err == nil && ready
}

func (c *InstantJobCtl) evaluationSourceReady(ctx context.Context) (bool, error) {
	if c.Store == nil || c.Job.WorkspaceID == "" || c.Job.TaskID == "" {
		return false, fmt.Errorf("evaluation result collection identity is incomplete")
	}
	record, err := jobinfo.FindExistingJobInfo(ctx, c.Store, c.Job)
	if err != nil {
		return false, err
	}
	outcome, terminalComplete, err := EvaluationRunnerTerminal(record)
	if err != nil {
		return false, err
	}
	if outcome == "" || !terminalComplete || (c.Job.Status == config.StatusCompleted && outcome != "succeeded") {
		return false, nil
	}
	rows, err := c.Store.List(ctx, &model.JobArtifact{WorkspaceID: c.Job.WorkspaceID, TaskID: c.Job.TaskID, ExecutionKey: c.Job.ExecutionKey, Kind: "source"}, &datastore.ListOptions{Page: 1, PageSize: 1})
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		artifact, ok := row.(*model.JobArtifact)
		if ok && artifact.WorkspaceID == c.Job.WorkspaceID && artifact.TaskID == c.Job.TaskID && artifact.ExecutionKey == c.Job.ExecutionKey && artifact.Kind == "source" {
			var summary struct {
				CollectionComplete bool `json:"collectionComplete"`
			}
			if err := json.Unmarshal(artifact.Summary, &summary); err != nil {
				return false, nil
			}
			return summary.CollectionComplete, nil
		}
	}
	return false, nil
}

func (c *InstantJobCtl) CleanCancelled(ctx context.Context) (string, error) {
	if config.IsWorkspaceJobType(config.JobType(c.Job.JobType)) {
		logCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		logs, err := jobexec.CollectJobPodLogs(logCtx, c.Client, c.Namespace, c.Job.Name)
		cancel()
		if err == nil && logs != "" {
			c.Job.Info = logs
		}
	}
	if !c.allowEvaluationTerminalCleanup(ctx) {
		return jobexec.CancelledJobCleanupPreserved, nil
	}
	return jobexec.CancelledJobCleanupComplete, jobexec.DeleteCancelledBatchJob(ctx, c.Client, c.Namespace, c.Job)
}
