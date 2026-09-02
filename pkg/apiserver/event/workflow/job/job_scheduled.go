package job

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type ScheduledJobCtl struct {
	deployNamespacedResourceJobBase
}

func GenerateScheduledCronJob(component *model.ApplicationComponent, properties *model.Properties, schedule string) *GenerateServiceResult {
	cron := buildCronJob(component, properties, schedule)
	if cron == nil {
		return nil
	}
	additionalObjects, err := traitsPlu.ApplyTraits(component, cron)
	if err != nil {
		klog.ErrorS(err, "scheduled cron job traits failed", "component", component.Name)
		return nil
	}
	return &GenerateServiceResult{
		Service:           cron,
		AdditionalObjects: additionalObjects,
	}
}

func NewScheduledJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *ScheduledJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("ScheduledJobCtl", job, client, store, ack, nil)
	if !ok {
		return nil
	}
	return &ScheduledJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *ScheduledJobCtl) Clean(ctx context.Context) {
	c.cleanCreated(ctx, config.ResourceJob, "job", func(ctx context.Context, namespace, name string) error {
		return c.client.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}, k8serrors.IsNotFound, "")
	c.cleanCreated(ctx, config.ResourceCronJob, "cronjob", func(ctx context.Context, namespace, name string) error {
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
	updateCronJob := func(ctx context.Context, existing *batchv1.CronJob) error {
		cron.ResourceVersion = existing.ResourceVersion
		_, err := c.client.BatchV1().CronJobs(namespace).Update(ctx, cron, metav1.UpdateOptions{})
		return err
	}
	_, _, err := createOrUpdateTrackedResource(ctx, config.ResourceCronJob, namespace, cron.Name, func(ctx context.Context) (*batchv1.CronJob, error) {
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
	action, err := applyJobRunPolicy(ctx, c.client, c.store, jobObj, jobType, validateExistingJobExecutionIdentity(ctx, c.store, jobObj))
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

	if _, err := c.createJob(ctx, jobObj); err != nil {
		return err
	}
	return nil
}

func (c *ScheduledJobCtl) createJob(ctx context.Context, jobObj *batchv1.Job) (bool, error) {
	if err := ensureCurrentJobWorkflowOwnership(ctx, c.store, c.job); err != nil {
		return false, err
	}
	validateExisting := validateExistingJobExecutionIdentity(ctx, c.store, jobObj)
	_, created, err := createOrUpdateTrackedResource(ctx, config.ResourceJob, jobObj.Namespace, jobObj.Name, func(ctx context.Context) (*batchv1.Job, error) {
		return c.client.BatchV1().Jobs(jobObj.Namespace).Get(ctx, jobObj.Name, metav1.GetOptions{})
	}, func(ctx context.Context) (*batchv1.Job, error) {
		return c.client.BatchV1().Jobs(jobObj.Namespace).Create(ctx, jobObj, metav1.CreateOptions{})
	}, func(_ context.Context, existing *batchv1.Job) error {
		return validateExisting(existing)
	}, k8serrors.IsNotFound, k8serrors.IsAlreadyExists)
	return created, err
}

func (c *ScheduledJobCtl) wait(ctx context.Context) (config.Status, string, error) {
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
