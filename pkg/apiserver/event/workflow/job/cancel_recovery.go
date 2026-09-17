package job

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

const (
	cancelledJobCleanupPending   = "parent workflow cancelled"
	cancelledJobCleanupComplete  = "cancelled execution cleanup complete"
	cancelledJobCleanupPreserved = "cancelled scheduled resource preserved: ownership origin unavailable"
)

func IsCancelledJobCleanupPending(record *model.JobInfo) bool {
	return record != nil && recoverableKubernetesJobType(record.Type) &&
		record.Status == string(config.StatusCancelled) && record.SchedulingReason == cancelledJobCleanupPending
}

// CleanupRecoveredCancelledJobs deletes durable Kubernetes Job attempts after
// the database recovery fence has made their JobInfo terminal.
func CleanupRecoveredCancelledJobs(ctx context.Context, client kubernetes.Interface, store datastore.DataStore) (int, error) {
	cleaned, _, err := CleanupRecoveredCancelledJobsPage(ctx, client, store, 1, 100)
	return cleaned, err
}

// CleanupRecoveredCancelledJobsPage lets the controller rotate through durable
// cleanup intents so a bad record cannot hide newer work.
func CleanupRecoveredCancelledJobsPage(ctx context.Context, client kubernetes.Interface, store datastore.DataStore, page, pageSize int) (int, int, error) {
	if client == nil || store == nil {
		return 0, 0, nil
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}
	entities, err := store.List(ctx, &model.JobInfo{Status: string(config.StatusCancelled)}, &datastore.ListOptions{
		FilterOptions: datastore.FilterOptions{
			In: []datastore.InQueryOption{{
				Key: "type", Values: []string{
					string(config.JobDeployInstant), string(config.JobCommand),
					string(config.JobEval), string(config.JobDeployScheduled),
				},
			}},
			Queries: []datastore.FuzzyQueryOption{{Key: "scheduling_reason", Query: cancelledJobCleanupPending}},
		},
		Page: page, PageSize: pageSize,
		SortBy: []datastore.SortOption{{Key: "update_time", Order: datastore.SortOrderAscending}},
	})
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("list recovered cancelled jobs: %w", err)
	}
	cleaned := 0
	var cleanupErr error
	for _, entity := range entities {
		record, ok := entity.(*model.JobInfo)
		if !ok || record == nil {
			cleanupErr = errors.Join(cleanupErr, datastore.ErrEntityInvalid)
			continue
		}
		if !IsCancelledJobCleanupPending(record) {
			continue
		}
		if err := cleanupRecoveredCancelledJob(ctx, client, store, record); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("job %d: %w", record.ID, err))
			continue
		}
		cleaned++
	}
	return cleaned, len(entities), cleanupErr
}

func cleanupRecoveredCancelledJob(ctx context.Context, client kubernetes.Interface, store datastore.DataStore, record *model.JobInfo) error {
	if record == nil || record.ExecutionKey == nil || strings.TrimSpace(*record.ExecutionKey) == "" {
		return fmt.Errorf("cancelled Kubernetes Job identity is incomplete")
	}
	space := &model.Workspace{ID: record.WorkspaceID}
	if space.ID == "" && record.AppID != "" {
		app := &model.Applications{ID: record.AppID}
		if err := store.Get(ctx, app); err != nil {
			return fmt.Errorf("load cancelled Job application: %w", err)
		}
		space.ID = app.WorkspaceID
	}
	if space.ID == "" {
		return fmt.Errorf("cancelled Kubernetes Job workspace is missing")
	}
	if err := store.Get(ctx, space); err != nil {
		return fmt.Errorf("load cancelled Job workspace: %w", err)
	}
	if strings.TrimSpace(space.Namespace) == "" {
		return fmt.Errorf("cancelled Kubernetes Job namespace is missing")
	}
	resourceName := strings.TrimSpace(record.ServiceName)
	if HasInstantJobRetryCheckpoint(record) {
		task := &model.JobTask{TaskID: record.TaskID, ExecutionKey: *record.ExecutionKey, RunGeneration: record.RunGeneration, Attempt: record.Attempt, InternalInfo: record.InternalInfo}
		checkpoint, checkpointErr := decodeInstantJobRetryCheckpoint(task)
		if checkpointErr != nil {
			return checkpointErr
		}
		if checkpoint.Job.Namespace != space.Namespace {
			return fmt.Errorf("cancelled Kubernetes Job checkpoint namespace changed")
		}
		resourceName = checkpoint.Job.Name
	}
	if resourceName == "" {
		return fmt.Errorf("cancelled Kubernetes Job name is missing")
	}
	live, err := client.BatchV1().Jobs(space.Namespace).Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get cancelled Kubernetes Job: %w", err)
	}
	replacementJob := false
	if err == nil {
		if !cancelledKubernetesJobMatches(record, live) {
			replacementJob = true
		} else {
			if HasInstantJobRetryCheckpoint(record) {
				if err := ValidateInstantJobRetryExecution(record, live); err != nil {
					return fmt.Errorf("validate cancelled Kubernetes Job checkpoint: %w", err)
				}
			}
			if err := deleteCancelledKubernetesJob(ctx, client, space.Namespace, live); err != nil {
				return err
			}
			return finishCancelledJobCleanup(ctx, store, record, cancelledJobCleanupComplete)
		}
	}
	if record.Type == string(config.JobDeployScheduled) {
		cron, cronErr := client.BatchV1().CronJobs(space.Namespace).Get(ctx, record.ServiceName, metav1.GetOptions{})
		if cronErr != nil && !k8serrors.IsNotFound(cronErr) {
			return fmt.Errorf("get cancelled Kubernetes CronJob: %w", cronErr)
		}
		if cronErr == nil {
			if !cancelledCronJobMatches(record, cron) {
				return finishCancelledJobCleanup(ctx, store, record, cancelledJobCleanupPreserved)
			}
			if err := deleteCancelledKubernetesCronJob(ctx, client, space.Namespace, cron); err != nil {
				return err
			}
			return finishCancelledJobCleanup(ctx, store, record, cancelledJobCleanupComplete)
		}
	}
	if replacementJob {
		return finishCancelledJobCleanup(ctx, store, record, cancelledJobCleanupPreserved)
	}
	return finishCancelledJobCleanup(ctx, store, record, cancelledJobCleanupComplete)
}

func beginCancelledJobCleanup(ctx context.Context, store datastore.DataStore, job *model.JobTask) error {
	if job == nil || !recoverableKubernetesJobType(job.JobType) {
		return nil
	}
	return updateCancelledJobCleanup(ctx, store, job, "", cancelledJobCleanupPending)
}

func finishOnlineCancelledJobCleanup(ctx context.Context, store datastore.DataStore, job *model.JobTask, outcome string) error {
	if job == nil || !recoverableKubernetesJobType(job.JobType) {
		return nil
	}
	return updateCancelledJobCleanup(ctx, store, job, cancelledJobCleanupPending, outcome)
}

func updateCancelledJobCleanup(ctx context.Context, store datastore.DataStore, job *model.JobTask, expectedReason, outcome string) error {
	return withJobInfoOwnership(ctx, store, job, func(tx datastore.DataStore) error {
		record, err := findExistingJobInfo(ctx, tx, job)
		if err != nil {
			return fmt.Errorf("load cancelled Job cleanup checkpoint: %w", err)
		}
		if record == nil {
			return datastore.ErrRecordNotExist
		}
		conditional, ok := tx.(datastore.ConditionalCompareAndSwap)
		if !ok {
			return fmt.Errorf("cancelled Job cleanup requires conditional compare-and-swap")
		}
		conditions := map[string]interface{}{
			"status": record.Status, "run_generation": record.RunGeneration, "attempt": record.Attempt,
		}
		if record.ExecutionKey != nil {
			conditions["execution_key"] = *record.ExecutionKey
		}
		if expectedReason != "" {
			conditions["scheduling_reason"] = expectedReason
		}
		updates := map[string]interface{}{"scheduling_reason": outcome}
		if outcome == cancelledJobCleanupPending {
			updates["status"] = string(config.StatusCancelled)
			updates["error"] = job.Error
			updates["end_time"] = job.EndTime
		} else if record.SchedulingState != "" {
			updates["scheduling_state"] = workflowconfig.JobSchedulingReleased
		}
		updated, err := conditional.CompareAndSwapWithConditions(ctx, record, conditions, updates)
		if err != nil {
			return err
		}
		if !updated {
			return fmt.Errorf("cancelled Job cleanup checkpoint changed")
		}
		return nil
	})
}

func finishCancelledJobCleanup(ctx context.Context, store datastore.DataStore, record *model.JobInfo, outcome string) error {
	conditional, ok := store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("mark cancelled Kubernetes Job cleanup: datastore does not support fencing")
	}
	conditions := map[string]interface{}{
		"status": string(config.StatusCancelled), "execution_key": *record.ExecutionKey,
		"run_generation": record.RunGeneration, "attempt": record.Attempt,
		"scheduling_reason": cancelledJobCleanupPending,
	}
	updates := map[string]interface{}{"scheduling_reason": outcome}
	if record.SchedulingState != "" {
		updates["scheduling_state"] = workflowconfig.JobSchedulingReleased
	}
	updated, err := conditional.CompareAndSwapWithConditions(ctx, record, conditions, updates)
	if err != nil {
		return fmt.Errorf("mark cancelled Kubernetes Job cleanup: %w", err)
	}
	if !updated {
		return fmt.Errorf("cancelled Kubernetes Job checkpoint changed")
	}
	return nil
}

type cancelledJobCleaner interface {
	CleanCancelled(context.Context) (string, error)
}

func (c *InstantJobCtl) CleanCancelled(ctx context.Context) (string, error) {
	if config.IsWorkspaceJobType(config.JobType(c.job.JobType)) {
		logCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		logs, err := collectJobPodLogs(logCtx, c.client, c.namespace, c.job.Name)
		cancel()
		if err == nil && logs != "" {
			c.job.Info = logs
		}
	}
	if !c.allowEvaluationTerminalCleanup(ctx) {
		return cancelledJobCleanupPreserved, nil
	}
	return cancelledJobCleanupComplete, deleteCancelledBatchJob(ctx, c.client, c.namespace, c.job)
}

func (c *ScheduledJobCtl) CleanCancelled(ctx context.Context) (string, error) {
	if _, ok := optionalJobInfo[*batchv1.Job](c.job); ok {
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
		record := buildJobInfoRecord(c.job)
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

func deleteCancelledBatchJob(ctx context.Context, client kubernetes.Interface, namespace string, task *model.JobTask) error {
	if client == nil || task == nil {
		return fmt.Errorf("cancelled Kubernetes Job dependencies are incomplete")
	}
	record := buildJobInfoRecord(task)
	if record.ExecutionKey == nil || strings.TrimSpace(*record.ExecutionKey) == "" {
		return fmt.Errorf("cancelled Kubernetes Job identity is incomplete")
	}
	resourceName := strings.TrimSpace(record.ServiceName)
	if desired, ok := optionalJobInfo[*batchv1.Job](task); ok {
		if desired.Namespace != "" {
			namespace = desired.Namespace
		}
		resourceName = desired.Name
	}
	if HasInstantJobRetryCheckpoint(&record) {
		checkpoint, err := decodeInstantJobRetryCheckpoint(task)
		if err != nil {
			return err
		}
		namespace, resourceName = checkpoint.Job.Namespace, checkpoint.Job.Name
	}
	if resourceName == "" {
		return fmt.Errorf("cancelled Kubernetes Job name is missing")
	}
	live, err := client.BatchV1().Jobs(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get cancelled Kubernetes Job: %w", err)
	}
	if !cancelledKubernetesJobMatches(&record, live) {
		return fmt.Errorf("cancelled Kubernetes Job execution identity changed")
	}
	if HasInstantJobRetryCheckpoint(&record) {
		if err := ValidateInstantJobRetryExecution(&record, live); err != nil {
			return fmt.Errorf("validate cancelled Kubernetes Job checkpoint: %w", err)
		}
	}
	return deleteCancelledKubernetesJob(ctx, client, namespace, live)
}

func deleteCancelledKubernetesJob(ctx context.Context, client kubernetes.Interface, namespace string, live *batchv1.Job) error {
	uid := live.UID
	policy := metav1.DeletePropagationForeground
	if err := client.BatchV1().Jobs(namespace).Delete(ctx, live.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid}, PropagationPolicy: &policy,
	}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete cancelled Kubernetes Job: %w", err)
	}
	return waitForCancelledKubernetesResourceDeletion(ctx, func(ctx context.Context) (types.UID, error) {
		current, err := client.BatchV1().Jobs(namespace).Get(ctx, live.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return current.UID, nil
	}, uid, "Job")
}

func deleteCancelledKubernetesCronJob(ctx context.Context, client kubernetes.Interface, namespace string, live *batchv1.CronJob) error {
	uid := live.UID
	policy := metav1.DeletePropagationForeground
	if err := client.BatchV1().CronJobs(namespace).Delete(ctx, live.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid}, PropagationPolicy: &policy,
	}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete cancelled Kubernetes CronJob: %w", err)
	}
	return waitForCancelledKubernetesResourceDeletion(ctx, func(ctx context.Context) (types.UID, error) {
		current, err := client.BatchV1().CronJobs(namespace).Get(ctx, live.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return current.UID, nil
	}, uid, "CronJob")
}

func waitForCancelledKubernetesResourceDeletion(ctx context.Context, currentUID func(context.Context) (types.UID, error), deletedUID types.UID, kind string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		uid, err := currentUID(ctx)
		if k8serrors.IsNotFound(err) || (err == nil && uid != deletedUID) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("confirm cancelled Kubernetes %s deletion: %w", kind, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("confirm cancelled Kubernetes %s deletion: %w", kind, ctx.Err())
		case <-ticker.C:
		}
	}
}

func recoverableKubernetesJobType(jobType string) bool {
	return config.IsInstantJobType(config.JobType(jobType)) || jobType == string(config.JobDeployScheduled)
}

func cancelledKubernetesJobMatches(record *model.JobInfo, live *batchv1.Job) bool {
	return record != nil && record.ExecutionKey != nil && live != nil && live.UID != "" &&
		live.Annotations[config.AnnotationJobTaskID] == record.TaskID &&
		live.Annotations[config.AnnotationJobExecutionKey] == *record.ExecutionKey &&
		live.Annotations[config.AnnotationJobRunGeneration] == strconv.FormatUint(record.RunGeneration, 10) &&
		(record.Attempt == 0 || live.Annotations[workflowconfig.AnnotationJobAttempt] == strconv.FormatUint(uint64(record.Attempt), 10))
}

func cancelledCronJobMatches(record *model.JobInfo, live *batchv1.CronJob) bool {
	return record != nil && record.ExecutionKey != nil && live != nil && live.UID != "" &&
		live.Annotations[config.AnnotationJobTaskID] == record.TaskID &&
		live.Annotations[config.AnnotationJobExecutionKey] == *record.ExecutionKey &&
		live.Annotations[config.AnnotationJobRunGeneration] == strconv.FormatUint(record.RunGeneration, 10) &&
		live.Annotations[workflowconfig.AnnotationJobAttempt] == strconv.FormatUint(uint64(record.Attempt), 10)
}
