package jobdelay

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
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobresult"
)

type ScheduledJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
}

func NewScheduledJobCtl(job *model.JobTask, runtime *jobexec.Runtime) *ScheduledJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("ScheduledJobCtl", job, runtime, nil)
	if !ok {
		return nil
	}
	return &ScheduledJobCtl{
		DeployNamespacedResourceJobBase: base,
	}
}

func (c *ScheduledJobCtl) Clean(ctx context.Context) {
	c.CleanCreatedBatchJobs(ctx, "")
	c.CleanCreated(ctx, domainspec.ResourceCronJob, "cronjob", func(ctx context.Context, namespace, name string) error {
		return c.Client.BatchV1().CronJobs(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, "")
}

func (c *ScheduledJobCtl) Run(ctx context.Context) error {
	c.Job.Status = config.StatusRunning
	c.Job.Error = ""
	c.Ack()

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
	if _, ok := jobinfo.OptionalJobInfo[*batchv1.CronJob](c.Job); ok {
		c.Job.Status = config.StatusCompleted
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

func (c *ScheduledJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}
	cron, oneTime, err := jobinfo.ScheduledJobInfo(c.Job)
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
		namespace = c.Namespace
		cron.Namespace = namespace
	}
	jobresult.StampCronJobExecutionIdentity(c.Job, cron)
	updateCronJob := func(ctx context.Context, existing *batchv1.CronJob) error {
		cron.ResourceVersion = existing.ResourceVersion
		_, err := c.Client.BatchV1().CronJobs(namespace).Update(ctx, cron, metav1.UpdateOptions{})
		return err
	}
	_, _, err := jobexec.CreateOrUpdateTrackedResource(ctx, domainspec.ResourceCronJob, namespace, cron.Name, func(ctx context.Context) (*batchv1.CronJob, error) {
		return c.Client.BatchV1().CronJobs(namespace).Get(ctx, cron.Name, metav1.GetOptions{})
	}, func(ctx context.Context) (*batchv1.CronJob, error) {
		return c.Client.BatchV1().CronJobs(namespace).Create(ctx, cron, metav1.CreateOptions{})
	}, updateCronJob)
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
		namespace = c.Namespace
		jobObj.Namespace = namespace
	}
	jobresult.StampJobExecutionIdentity(c.Job, jobObj)

	jobType := jobexec.JobTypeForTask(c.Job, config.JobDeployScheduled)
	if err := jobexec.EnsureCurrentJobWorkflowOwnership(ctx, c.Store, c.Job); err != nil {
		return err
	}
	action, err := jobexec.ApplyJobRunPolicy(ctx, c.Client, c.Store, jobObj, jobType)
	if err != nil {
		return err
	}
	if action == jobexec.RunPolicyActionSkip {
		c.Job.Status = config.StatusSkipped
		c.Job.Error = ""
		c.Ack()
		return nil
	}

	startTime, hasStart := jobexec.StartTimeFromJob(jobObj)
	now := time.Now().Unix()
	if hasStart && startTime > now {
		jobType := c.Job.JobType
		if jobType == "" {
			jobType = string(config.JobDeployScheduled)
		}
		payload := &jobinfo.DelayJobPayload{
			ExecuteAt:      startTime,
			Namespace:      namespace,
			JobType:        jobType,
			TaskID:         c.Job.TaskID,
			ExecutionKey:   c.Job.ExecutionKey,
			RunGeneration:  c.Job.RunGeneration,
			RunToken:       c.Job.RunToken,
			ServiceName:    jobinfo.ResolveJobServiceName(c.Job),
			TimeoutSeconds: c.Job.Timeout,
			Job:            jobObj,
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

	if _, err := c.CreateBatchJob(ctx, jobObj); err != nil {
		return err
	}
	return nil
}

func (c *ScheduledJobCtl) CleanCancelled(ctx context.Context) (string, error) {
	if _, ok := jobinfo.OptionalJobInfo[*batchv1.Job](c.Job); ok {
		return jobexec.CancelledJobCleanupComplete, jobexec.DeleteCancelledBatchJob(ctx, c.Client, c.Namespace, c.Job)
	}
	for _, ref := range jobexec.ResourcesForCleanup(ctx, domainspec.ResourceCronJob) {
		if !ref.Created {
			continue
		}
		namespace := ref.Namespace
		if namespace == "" {
			namespace = c.Namespace
		}
		cron, err := c.Client.BatchV1().CronJobs(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return jobexec.CancelledJobCleanupComplete, nil
		}
		if err != nil {
			return "", fmt.Errorf("get cancelled Kubernetes CronJob: %w", err)
		}
		record := jobinfo.BuildJobInfoRecord(c.Job)
		if !jobexec.CancelledCronJobMatches(&record, cron) {
			return "", fmt.Errorf("cancelled Kubernetes CronJob execution identity changed")
		}
		if err := jobexec.DeleteCancelledKubernetesCronJob(ctx, c.Client, namespace, cron); err != nil {
			return "", err
		}
		return jobexec.CancelledJobCleanupComplete, nil
	}
	return jobexec.CancelledJobCleanupPreserved, nil
}
