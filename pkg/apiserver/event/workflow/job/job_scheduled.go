package job

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type ScheduledJobCtl struct {
	deployNamespacedResourceJobBase
}

func GenerateScheduledCronJob(component *model.ApplicationComponent, properties *model.Properties, schedule string) (*GenerateServiceResult, error) {
	cron := buildCronJob(component, properties, schedule)
	if cron == nil {
		return nil, nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, cron)
	if err != nil {
		return nil, fmt.Errorf("generate component %s: %w", component.Name, err)
	}
	return &GenerateServiceResult{
		Service:           cron,
		AdditionalObjects: additionalObjects,
	}, nil
}

func NewScheduledJobCtl(job *model.JobTask, runtime *Runtime) *ScheduledJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("ScheduledJobCtl", job, runtime, nil)
	if !ok {
		return nil
	}
	return &ScheduledJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *ScheduledJobCtl) Clean(ctx context.Context) {
	c.cleanCreated(ctx, domainspec.ResourceJob, "job", func(ctx context.Context, namespace, name string) error {
		return c.client.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "")
	c.cleanCreated(ctx, domainspec.ResourceCronJob, "cronjob", func(ctx context.Context, namespace, name string) error {
		return c.client.BatchV1().CronJobs(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "")
}

func (c *ScheduledJobCtl) Run(ctx context.Context) error {
	c.job.Status = config.StatusRunning
	c.job.Error = ""
	c.ack()

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
	if _, ok := optionalJobInfo[*batchv1.CronJob](c.job); ok {
		c.job.Status = config.StatusCompleted
		c.job.Error = ""
		return nil
	}
	status, message, err := c.waitBatchJob(ctx)
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

func (c *ScheduledJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	cron, oneTime, err := scheduledJobInfo(c.job)
	if err != nil {
		return err
	}
	if cron != nil {
		return c.runCronJob(ctx, cron)
	}
	return c.runOneTimeJob(ctx, oneTime)
}

func (c *ScheduledJobCtl) runCronJob(ctx context.Context, cron *batchv1.CronJob) error {
	if cron == nil {
		return fmt.Errorf("cronjob is nil")
	}
	namespace := cron.Namespace
	if namespace == "" {
		namespace = c.namespace
		cron.Namespace = namespace
	}
	stampCronJobExecutionIdentity(c.job, cron)
	updateCronJob := func(ctx context.Context, existing *batchv1.CronJob) error {
		cron.ResourceVersion = existing.ResourceVersion
		_, err := c.client.BatchV1().CronJobs(namespace).Update(ctx, cron, metav1.UpdateOptions{})
		return err
	}
	_, _, err := createOrUpdateTrackedResource(ctx, domainspec.ResourceCronJob, namespace, cron.Name, func(ctx context.Context) (*batchv1.CronJob, error) {
		return c.client.BatchV1().CronJobs(namespace).Get(ctx, cron.Name, metav1.GetOptions{})
	}, func(ctx context.Context) (*batchv1.CronJob, error) {
		return c.client.BatchV1().CronJobs(namespace).Create(ctx, cron, metav1.CreateOptions{})
	}, updateCronJob, k8serrors.IsNotFound, k8serrors.IsAlreadyExists)
	if err != nil {
		return err
	}
	return nil
}

func (c *ScheduledJobCtl) runOneTimeJob(ctx context.Context, jobObj *batchv1.Job) error {
	if jobObj == nil {
		return fmt.Errorf("job is nil")
	}
	namespace := jobObj.Namespace
	if namespace == "" {
		namespace = c.namespace
		jobObj.Namespace = namespace
	}
	stampJobExecutionIdentity(c.job, jobObj)

	jobType := jobTypeForTask(c.job, config.JobDeployScheduled)
	if err := ensureCurrentJobWorkflowOwnership(ctx, c.store, c.job); err != nil {
		return err
	}
	action, err := applyJobRunPolicy(ctx, c.client, c.store, jobObj, jobType)
	if err != nil {
		return err
	}
	if action == runPolicyActionSkip {
		c.job.Status = config.StatusSkipped
		c.job.Error = ""
		c.ack()
		return nil
	}

	startTime, hasStart := startTimeFromJob(jobObj)
	now := time.Now().Unix()
	if hasStart && startTime > now {
		jobType := c.job.JobType
		if jobType == "" {
			jobType = string(config.JobDeployScheduled)
		}
		payload := &DelayJobPayload{
			ExecuteAt:      startTime,
			Namespace:      namespace,
			JobType:        jobType,
			TaskID:         c.job.TaskID,
			ExecutionKey:   c.job.ExecutionKey,
			RunGeneration:  c.job.RunGeneration,
			RunToken:       c.job.RunToken,
			ServiceName:    resolveJobServiceName(c.job),
			TimeoutSeconds: c.job.Timeout,
			Job:            jobObj,
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

	if _, err := c.createBatchJob(ctx, jobObj); err != nil {
		return err
	}
	return nil
}
