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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
)

type ScheduledJobCtl struct {
	deployNamespacedResourceJobBase
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
	c.cleanCreatedBatchJobs(ctx, "")
	c.cleanCreated(ctx, domainspec.ResourceCronJob, "cronjob", func(ctx context.Context, namespace, name string) error {
		return c.client.BatchV1().CronJobs(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "")
}

func (c *ScheduledJobCtl) Run(ctx context.Context) error {
	c.job.Status = config.StatusRunning
	c.job.Error = ""
	c.ack()

	if err := c.run(ctx); err != nil {
		jobinfo.ApplyJobError(c.job, err, "")
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
	if _, ok := jobinfo.OptionalJobInfo[*batchv1.CronJob](c.job); ok {
		c.job.Status = config.StatusCompleted
		c.job.Error = ""
		return nil
	}
	status, message, err := c.waitBatchJob(ctx)
	if err != nil {
		jobinfo.ApplyJobError(c.job, err, message)
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
	cron, oneTime, err := jobinfo.ScheduledJobInfo(c.job)
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
		payload := &jobinfo.DelayJobPayload{
			ExecuteAt:      startTime,
			Namespace:      namespace,
			JobType:        jobType,
			TaskID:         c.job.TaskID,
			ExecutionKey:   c.job.ExecutionKey,
			RunGeneration:  c.job.RunGeneration,
			RunToken:       c.job.RunToken,
			ServiceName:    jobinfo.ResolveJobServiceName(c.job),
			TimeoutSeconds: c.job.Timeout,
			Job:            jobObj,
		}
		if err := jobinfo.PersistDelayJobCheckpoint(ctx, c.store, c.job, payload); err != nil {
			return err
		}
		if _, err := jobinfo.EnqueueDelayJob(ctx, c.delayQueue, payload); err != nil {
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

func (c *ScheduledJobCtl) CleanCancelled(ctx context.Context) (string, error) {
	if _, ok := jobinfo.OptionalJobInfo[*batchv1.Job](c.job); ok {
		return cancelledJobCleanupComplete, deleteCancelledBatchJob(ctx, c.client, c.namespace, c.job)
	}
	for _, ref := range resourcesForCleanup(ctx, domainspec.ResourceCronJob) {
		if !ref.Created {
			continue
		}
		namespace := ref.Namespace
		if namespace == "" {
			namespace = c.namespace
		}
		cron, err := c.client.BatchV1().CronJobs(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return cancelledJobCleanupComplete, nil
		}
		if err != nil {
			return "", fmt.Errorf("get cancelled Kubernetes CronJob: %w", err)
		}
		record := jobinfo.BuildJobInfoRecord(c.job)
		if !cancelledCronJobMatches(&record, cron) {
			return "", fmt.Errorf("cancelled Kubernetes CronJob execution identity changed")
		}
		if err := deleteCancelledKubernetesCronJob(ctx, c.client, namespace, cron); err != nil {
			return "", err
		}
		return cancelledJobCleanupComplete, nil
	}
	return cancelledJobCleanupPreserved, nil
}
