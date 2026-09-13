package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type InstantJobCtl struct {
	deployNamespacedResourceJobBase
}

// GenerateInstantJob Job builders.
func GenerateInstantJob(component *model.ApplicationComponent, properties *model.Properties, runPolicy string) *GenerateServiceResult {
	job := buildJob(component, properties, jobBuildOptions{runPolicy: runPolicy})
	if job == nil {
		return nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, job)
	if err != nil {
		klog.ErrorS(err, "instant job traits failed", "component", component.Name)
		return nil
	}
	return &GenerateServiceResult{
		Service:           job,
		AdditionalObjects: additionalObjects,
	}
}

func GenerateOneTimeJob(component *model.ApplicationComponent, properties *model.Properties, runPolicy string, startTime int64) *GenerateServiceResult {
	job := buildJob(component, properties, jobBuildOptions{runPolicy: runPolicy, startTime: startTime})
	if job == nil {
		return nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, job)
	if err != nil {
		klog.ErrorS(err, "scheduled one-time job traits failed", "component", component.Name)
		return nil
	}
	return &GenerateServiceResult{
		Service:           job,
		AdditionalObjects: additionalObjects,
	}
}

func NewInstantJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *InstantJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("InstantJobCtl", job, client, store, ack, nil)
	if !ok {
		return nil
	}
	return &InstantJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *InstantJobCtl) Clean(ctx context.Context) {
	if config.IsWorkspaceJobType(config.JobType(c.job.JobType)) {
		logCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		logs, err := collectJobPodLogs(logCtx, c.client, c.namespace, c.job.Name)
		cancel()
		if err == nil && logs != "" {
			c.job.Info = logs
		}
	}
	if !c.allowEvaluationTerminalCleanup(ctx) {
		return
	}
	if c.job.InternalInfo != "" {
		// A settled attempt is the durable checkpoint's recovery evidence until
		// SaveInfo commits its outcome. Cancellation, timeout and panic cleanup
		// still stop active work immediately through the same fenced deletion.
		if c.job.Status == config.StatusCompleted || c.job.Status == config.StatusFailed {
			return
		}
		if err := c.cleanRetryAttempt(ctx); err != nil && !k8serrors.IsNotFound(err) {
			klog.ErrorS(err, "clean instant Job retry attempt", "taskID", c.job.TaskID)
		}
		return
	}
	c.cleanCreated(ctx, domainspec.ResourceJob, "job", func(ctx context.Context, namespace, name string) error {
		return c.client.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "after failure")
}

func (c *InstantJobCtl) SaveInfo(ctx context.Context) error {
	if err := saveJobInfo(ctx, c.store, c.job); err != nil {
		return err
	}
	// Retry recovery needs the exact live UID until its terminal result,
	// including collected logs, has been committed. Its TTL handles an exit
	// after this commit but before cleanup.
	if (c.job.Status == config.StatusCompleted || c.job.Status == config.StatusFailed) && c.job.InternalInfo != "" && c.allowEvaluationTerminalCleanup(ctx) {
		if err := c.cleanRetryAttempt(ctx); err != nil && !k8serrors.IsNotFound(err) {
			klog.ErrorS(err, "clean terminal instant Job retry attempt", "taskID", c.job.TaskID)
		}
	}
	return nil
}

func (c *InstantJobCtl) Run(ctx context.Context) error {
	c.job.Status = config.StatusRunning
	c.job.Error = ""
	c.ack()
	if desired, ok := optionalJobInfo[*batchv1.Job](c.job); ok && (desired.Annotations[workflowconfig.AnnotationJobRetryPolicy] != "" || c.job.InternalInfo != "") {
		if desired.Namespace == "" {
			desired.Namespace = c.namespace
		}
		stampJobExecutionIdentity(c.job, desired)
		policy, err := retryPolicyFromJob(desired)
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
		if err == nil && c.job.JobType == string(config.JobAgentEvaluation) && c.job.Status == config.StatusCompleted {
			ready, sourceErr := c.evaluationSourceReady(ctx)
			if sourceErr != nil {
				err = errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("verify evaluation result collection: %w", sourceErr))
			} else if !ready {
				err = fmt.Errorf("evaluation runner completed without a collected result archive")
			}
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				err = NewStatusError(config.StatusCancelled, err)
			} else if errors.Is(err, context.DeadlineExceeded) {
				err = NewStatusError(config.StatusTimeout, err)
			}
			applyJobError(c.job, err, "")
		}
		return err
	}

	if err := c.run(ctx); err != nil {
		applyJobError(c.job, err, "")
		return err
	}
	if c.job.Status == config.StatusSkipped {
		c.job.Error = ""
		return nil
	}
	if c.job.Status == config.StatusDistributed {
		c.job.Error = ""
		return nil
	}
	status, message, err := c.wait(ctx)
	if err != nil {
		applyJobError(c.job, err, message)
		return err
	}
	if status != "" {
		c.job.Status = status
	} else {
		c.job.Status = config.StatusCompleted
	}
	c.job.Error = ""
	return nil
}

func (c *InstantJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	jobObj, err := batchJobFromJobInfo(c.job)
	if err != nil {
		return err
	}
	namespace := jobObj.Namespace
	if namespace == "" {
		namespace = c.namespace
		jobObj.Namespace = namespace
	}
	stampJobExecutionIdentity(c.job, jobObj)

	startTime, hasStart := startTimeFromJob(jobObj)
	now := time.Now().Unix()
	if hasStart && startTime > now {
		jobType := jobTypeForTask(c.job, config.JobDeployInstant)
		payload := &DelayJobPayload{
			ExecuteAt:      startTime,
			Namespace:      namespace,
			JobType:        string(jobType),
			Job:            jobObj,
			TaskID:         c.job.TaskID,
			ExecutionKey:   c.job.ExecutionKey,
			RunGeneration:  c.job.RunGeneration,
			RunToken:       c.job.RunToken,
			ServiceName:    resolveJobServiceName(c.job),
			TimeoutSeconds: c.job.Timeout,
		}
		if err := persistDelayJobCheckpoint(ctx, c.store, c.job, payload); err != nil {
			return err
		}
		if _, err := EnqueueDelayJob(ctx, c.delayQueue, payload); err != nil {
			klog.ErrorS(err, "delay queue notification failed; database recovery remains active", "taskID", c.job.TaskID, "executionKey", c.job.ExecutionKey)
		}
		c.ack()
		return nil
	}

	if err := ensureCurrentJobWorkflowOwnership(ctx, c.store, c.job); err != nil {
		return err
	}
	action, err := applyJobRunPolicy(ctx, c.client, c.store, jobObj, jobTypeForTask(c.job, config.JobDeployInstant), validateExistingJobExecutionIdentity(ctx, c.store, jobObj))
	if err != nil {
		return err
	}
	if action == runPolicyActionSkip {
		c.job.Status = config.StatusSkipped
		c.job.Error = ""
		c.ack()
		return nil
	}

	if _, err := c.createJob(ctx, jobObj); err != nil {
		return err
	}
	return nil
}

func (c *InstantJobCtl) createJob(ctx context.Context, jobObj *batchv1.Job) (bool, error) {
	if err := ensureCurrentJobWorkflowOwnership(ctx, c.store, c.job); err != nil {
		return false, err
	}
	validateExisting := validateExistingJobExecutionIdentity(ctx, c.store, jobObj)
	_, created, err := createOrUpdateTrackedResource(ctx, domainspec.ResourceJob, jobObj.Namespace, jobObj.Name, func(ctx context.Context) (*batchv1.Job, error) {
		return c.client.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
	}, func(ctx context.Context) (*batchv1.Job, error) {
		return c.client.BatchV1().Jobs(jobObj.Namespace).Create(ctx, jobObj, metav1.CreateOptions{})
	}, func(_ context.Context, existing *batchv1.Job) error {
		return validateExisting(existing)
	}, k8serrors.IsNotFound, k8serrors.IsAlreadyExists)
	return created, err
}

func (c *InstantJobCtl) wait(ctx context.Context) (config.Status, string, error) {
	timeout := c.job.Timeout
	if timeout <= 0 {
		timeout = int64(config.DefaultJobTaskTimeout.Seconds())
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	namespace := c.namespace
	name := c.job.Name
	if jobObj, ok := optionalJobInfo[*batchv1.Job](c.job); ok {
		if jobObj.Namespace != "" {
			namespace = jobObj.Namespace
		}
		if jobObj.Name != "" {
			name = jobObj.Name
		}
	}
	return waitForJobCompletion(waitCtx, c.client, namespace, name)
}

// Terminal evaluation Pods are retained until the complete source archive is
// committed. Cancellation and timeout still stop active work through the normal
// fenced deletion path; the runner's termination grace allows a final upload.
func (c *InstantJobCtl) allowEvaluationTerminalCleanup(ctx context.Context) bool {
	if c.job.JobType != string(config.JobAgentEvaluation) ||
		(c.job.Status != config.StatusCompleted && c.job.Status != config.StatusFailed) || ctx.Err() != nil {
		return true
	}
	ready, err := c.evaluationSourceReady(ctx)
	if err != nil {
		klog.ErrorS(err, "retain evaluation Job after result collection lookup failed", "taskID", c.job.TaskID)
	}
	return err == nil && ready
}

func (c *InstantJobCtl) evaluationSourceReady(ctx context.Context) (bool, error) {
	if c.store == nil || c.job.WorkspaceID == "" || c.job.TaskID == "" {
		return false, fmt.Errorf("evaluation result collection identity is incomplete")
	}
	record, err := findExistingJobInfo(ctx, c.store, c.job)
	if err != nil {
		return false, err
	}
	outcome, terminalComplete, err := EvaluationRunnerTerminal(record)
	if err != nil {
		return false, err
	}
	if outcome == "" || !terminalComplete || (c.job.Status == config.StatusCompleted && outcome != "succeeded") {
		return false, nil
	}
	rows, err := c.store.List(ctx, &model.JobArtifact{WorkspaceID: c.job.WorkspaceID, TaskID: c.job.TaskID, Kind: "source"}, &datastore.ListOptions{Page: 1, PageSize: 1})
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		artifact, ok := row.(*model.JobArtifact)
		if ok && artifact.WorkspaceID == c.job.WorkspaceID && artifact.TaskID == c.job.TaskID && artifact.Kind == "source" {
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
