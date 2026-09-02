package job

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/signal"
)

type jobBuildOptions struct {
	runPolicy string
	startTime int64
}

func buildJob(component *model.ApplicationComponent, properties *model.Properties, opts jobBuildOptions) *batchv1.Job {
	if component == nil {
		return nil
	}
	labels := BuildLabels(component, properties)
	annotations := map[string]string{}
	policy, _ := workflowconfig.NormalizeJobRunPolicy(opts.runPolicy)
	annotations[config.AnnotationJobRunPolicy] = string(policy)
	if opts.startTime > 0 {
		annotations[config.AnnotationJobStartTime] = fmt.Sprintf("%d", opts.startTime)
	}
	for k, v := range BuildAnnotations(component) {
		annotations[k] = v
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        buildJobName(component.Name, component.ResourceNameKey()),
			Namespace:   component.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: jobTTLSeconds(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: BuildAnnotations(component),
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						buildJobContainer(component, properties),
					},
				},
			},
		},
	}
	return job
}

func buildCronJob(component *model.ApplicationComponent, properties *model.Properties, schedule string) *batchv1.CronJob {
	if component == nil {
		return nil
	}
	labels := BuildLabels(component, properties)
	successfulLimit := cronHistoryLimit(properties, true)
	failedLimit := cronHistoryLimit(properties, false)
	jobSpec := batchv1.JobSpec{
		TTLSecondsAfterFinished: jobTTLSeconds(),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{
					buildJobContainer(component, properties),
				},
			},
		},
	}
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        buildCronJobName(component.Name, component.ResourceNameKey()),
			Namespace:   component.Namespace,
			Labels:      labels,
			Annotations: BuildAnnotations(component),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   schedule,
			SuccessfulJobsHistoryLimit: successfulLimit,
			FailedJobsHistoryLimit:     failedLimit,
			JobTemplate:                batchv1.JobTemplateSpec{Spec: jobSpec},
		},
	}
}

func buildJobContainer(component *model.ApplicationComponent, properties *model.Properties) corev1.Container {
	containerName := utils.NormalizeLowerStrip(component.Name)
	if containerName == "" {
		containerName = "job"
	}
	var envs []corev1.EnvVar
	if properties != nil {
		for k, v := range properties.Env {
			envs = append(envs, corev1.EnvVar{Name: k, Value: v})
		}
	}
	container := corev1.Container{
		Name:            containerName,
		Image:           component.Image,
		Env:             envs,
		ImagePullPolicy: workflowconfig.DefaultWorkflowImagePullPolicy,
	}
	if properties != nil && len(properties.Command) > 0 {
		container.Command = properties.Command
	}
	return container
}

func jobTTLSeconds() *int32 {
	ttl := config.DefaultJobTTLSeconds
	return &ttl
}

func cronHistoryLimit(properties *model.Properties, success bool) *int32 {
	var provided *int32
	if properties != nil {
		if success {
			provided = properties.SuccessfulJobsHistoryLimit
		} else {
			provided = properties.FailedJobsHistoryLimit
		}
	}
	if provided != nil {
		return provided
	}
	if success {
		limit := config.DefaultCronJobSuccessfulLimit
		return &limit
	}
	limit := config.DefaultCronJobFailedLimit
	return &limit
}

// Job annotations and status helpers.
func runPolicyFromJob(job *batchv1.Job) workflowconfig.JobRunPolicy {
	if job == nil {
		policy, _ := workflowconfig.NormalizeJobRunPolicy("")
		return policy
	}
	raw := strings.TrimSpace(job.GetAnnotations()[config.AnnotationJobRunPolicy])
	policy, _ := workflowconfig.NormalizeJobRunPolicy(raw)
	return policy
}

// Job run policy helpers.
type runPolicyAction int

const (
	runPolicyActionCreate runPolicyAction = iota
	runPolicyActionSkip
)

// applyJobRunPolicy decides whether to create, skip, or recreate a job based on its run policy and current state.
func applyJobRunPolicy(
	ctx context.Context,
	client kubernetes.Interface,
	store datastore.DataStore,
	jobObj *batchv1.Job,
	jobType config.JobType,
	validators ...func(*batchv1.Job) error,
) (runPolicyAction, error) {
	if jobObj == nil {
		return runPolicyActionCreate, fmt.Errorf("job is nil")
	}
	if client == nil {
		return runPolicyActionCreate, fmt.Errorf("client is nil")
	}
	namespace := jobObj.Namespace
	if namespace == "" {
		return runPolicyActionCreate, fmt.Errorf("job namespace is empty")
	}
	jobName := jobObj.Name
	if jobName == "" {
		return runPolicyActionCreate, fmt.Errorf("job name is empty")
	}

	policy := runPolicyFromJob(jobObj)
	existing, exists, err := jobExists(ctx, client, namespace, jobName)
	if err != nil {
		return runPolicyActionCreate, err
	}
	if !exists {
		if policy == workflowconfig.JobRunPolicySkipIfCompleted {
			completed, err := jobCompletedInStore(ctx, store, jobType, jobObj)
			if err != nil {
				klog.Warningf("job runPolicy skip check failed: %v", err)
				return runPolicyActionCreate, nil
			}
			if completed {
				return runPolicyActionSkip, nil
			}
		}
		return runPolicyActionCreate, nil
	}

	validateExisting := validateExistingJobExecutionIdentity(ctx, store, jobObj)
	if len(validators) > 0 && validators[0] != nil {
		validateExisting = validators[0]
	}
	if err := validateExisting(existing); err != nil {
		return runPolicyActionCreate, err
	}

	status, message, done := jobTerminalStatus(existing)
	switch policy {
	case workflowconfig.JobRunPolicySkipIfCompleted:
		if done && status == config.StatusCompleted {
			return runPolicyActionSkip, nil
		}
		if done && status == config.StatusFailed {
			klog.Errorf("job %s already failed: %s", jobName, message)
			return runPolicyActionCreate, NewStatusError(config.StatusFailed, fmt.Errorf("existing job failed: %s", message))
		}
		if _, err := adoptReusableJobExecution(ctx, client, jobObj, existing); err != nil {
			return runPolicyActionCreate, err
		}
		return runPolicyActionCreate, nil
	case workflowconfig.JobRunPolicyRecreate:
		deleteOptions := metav1.DeleteOptions{}
		if existing.UID != "" {
			uid := existing.UID
			deleteOptions.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		deleteErr := client.BatchV1().Jobs(namespace).Delete(ctx, jobName, deleteOptions)
		if deleteErr != nil && !k8serrors.IsNotFound(deleteErr) {
			return runPolicyActionCreate, fmt.Errorf("delete existing job %s/%s: %w", namespace, jobName, deleteErr)
		}
		if deleteErr == nil {
			if existing.UID != "" {
				if err := waitForJobUIDDeletion(ctx, client, namespace, jobName, existing.UID); err != nil {
					return runPolicyActionCreate, err
				}
			} else if err := waitForJobDeletion(ctx, client, namespace, jobName); err != nil {
				return runPolicyActionCreate, fmt.Errorf("wait for job deletion %s/%s: %w", namespace, jobName, err)
			}
		}
		return runPolicyActionCreate, nil
	default:
		return runPolicyActionCreate, NewStatusError(config.StatusFailed, fmt.Errorf("unsupported run policy %q", policy))
	}
}

func adoptReusableJobExecution(ctx context.Context, client kubernetes.Interface, desired, existing *batchv1.Job) (*batchv1.Job, error) {
	if client == nil || desired == nil || existing == nil {
		return nil, fmt.Errorf("adopt reusable job: client and jobs are required")
	}
	desiredAnnotations := desired.GetAnnotations()
	desiredTaskID := strings.TrimSpace(desiredAnnotations[config.AnnotationJobTaskID])
	desiredExecutionKey := strings.TrimSpace(desiredAnnotations[config.AnnotationJobExecutionKey])
	desiredGenerationText := strings.TrimSpace(desiredAnnotations[config.AnnotationJobRunGeneration])
	desiredGeneration, err := strconv.ParseUint(desiredGenerationText, 10, 64)
	if desiredTaskID == "" || desiredExecutionKey == "" || err != nil || desiredGeneration == 0 {
		return nil, fmt.Errorf("job %s/%s requires task, execution key, and run generation", desired.Namespace, desired.Name)
	}

	existingAnnotations := existing.GetAnnotations()
	existingTaskID := strings.TrimSpace(existingAnnotations[config.AnnotationJobTaskID])
	existingExecutionKey := strings.TrimSpace(existingAnnotations[config.AnnotationJobExecutionKey])
	existingGenerationText := strings.TrimSpace(existingAnnotations[config.AnnotationJobRunGeneration])
	existingGeneration, err := strconv.ParseUint(existingGenerationText, 10, 64)
	if existingTaskID == "" || existingExecutionKey == "" || err != nil || existingGeneration == 0 {
		return nil, fmt.Errorf("job %s/%s has incomplete execution identity", existing.Namespace, existing.Name)
	}
	if existingExecutionKey == desiredExecutionKey && existingGeneration == desiredGeneration && existingTaskID == desiredTaskID {
		return existing, nil
	}
	if existingTaskID == desiredTaskID && existingGeneration >= desiredGeneration {
		return nil, fmt.Errorf(
			"%w: job %s/%s belongs to task %q execution %q generation %d; requested task %q execution %q generation %d",
			errJobExecutionIdentityChanged,
			existing.Namespace,
			existing.Name,
			existingTaskID,
			existingExecutionKey,
			existingGeneration,
			desiredTaskID,
			desiredExecutionKey,
			desiredGeneration,
		)
	}

	updated := existing.DeepCopy()
	annotations := maps.Clone(existingAnnotations)
	annotations[config.AnnotationJobTaskID] = desiredTaskID
	annotations[config.AnnotationJobExecutionKey] = desiredExecutionKey
	annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(desiredGeneration, 10)
	updated.SetAnnotations(annotations)
	live, err := client.BatchV1().Jobs(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("adopt reusable job %s/%s: %w", updated.Namespace, updated.Name, err)
	}
	klog.InfoS("Adopted reusable job execution",
		"namespace", live.Namespace,
		"name", live.Name,
		"previousGeneration", existingGeneration,
		"runGeneration", desiredGeneration)
	return live, nil
}

var errWorkflowJobOwnershipChanged = errors.New("workflow job ownership changed")

func jobOwnerGeneration(jobTask *model.JobTask) uint64 {
	if jobTask == nil {
		return 0
	}
	if jobTask.OwnerRunGeneration > 0 {
		return jobTask.OwnerRunGeneration
	}
	return jobTask.RunGeneration
}

func ensureCurrentJobWorkflowOwnership(ctx context.Context, store datastore.DataStore, jobTask *model.JobTask) error {
	if jobTask == nil || (jobTask.OwnerRunGeneration == 0 && strings.TrimSpace(jobTask.WorkerID) == "") {
		return nil
	}
	generation := jobOwnerGeneration(jobTask)
	if store == nil || strings.TrimSpace(jobTask.TaskID) == "" || generation == 0 ||
		strings.TrimSpace(jobTask.RunToken) == "" || strings.TrimSpace(jobTask.WorkerID) == "" {
		return errors.Join(signal.ErrInfrastructureStop, fmt.Errorf("%w: incomplete ownership identity", errWorkflowJobOwnershipChanged))
	}
	task, err := repository.TaskByID(ctx, store, strings.TrimSpace(jobTask.TaskID))
	if err != nil {
		return errors.Join(
			signal.ErrInfrastructureStop,
			fmt.Errorf("load workflow ownership before Kubernetes update: %w", err),
		)
	}
	if task.Status != config.StatusRunning ||
		task.RunGeneration != generation ||
		task.RunToken != jobTask.RunToken ||
		task.WorkerID != jobTask.WorkerID {
		return errors.Join(
			signal.ErrInfrastructureStop,
			fmt.Errorf("%w: task %s generation %d is no longer owned by worker %s", errWorkflowJobOwnershipChanged, jobTask.TaskID, generation, jobTask.WorkerID),
		)
	}
	return nil
}

func ensureJobWorkflowOwnership(ctx context.Context, jobTask *model.JobTask, store datastore.DataStore, operation string) error {
	if err := ensureCurrentJobWorkflowOwnership(ctx, store, jobTask); err != nil {
		return fmt.Errorf("verify workflow ownership before %s: %w", operation, err)
	}
	return nil
}

func validateExistingJobExecutionIdentity(ctx context.Context, store datastore.DataStore, desired *batchv1.Job) func(existing *batchv1.Job) error {
	return func(existing *batchv1.Job) error {
		if desired == nil || existing == nil {
			return nil
		}
		desiredTaskID := strings.TrimSpace(desired.Annotations[config.AnnotationJobTaskID])
		existingTaskID := strings.TrimSpace(existing.Annotations[config.AnnotationJobTaskID])
		if desiredTaskID != "" && existingTaskID != "" && desiredTaskID != existingTaskID {
			if store == nil {
				return errors.Join(
					signal.ErrInfrastructureStop,
					fmt.Errorf("load workflow task %s before replacing Kubernetes Job: datastore is nil", existingTaskID),
				)
			}
			existingTask, err := repository.TaskByID(ctx, store, existingTaskID)
			if err != nil {
				return errors.Join(
					signal.ErrInfrastructureStop,
					fmt.Errorf("load workflow task %s before replacing Kubernetes Job: %w", existingTaskID, err),
				)
			}
			if !isTerminalWorkflowJobTaskStatus(existingTask.Status) {
				return fmt.Errorf("%w: existing Job belongs to active workflow task %s", errJobExecutionIdentityChanged, existingTaskID)
			}
			return nil
		}
		desiredKey := strings.TrimSpace(desired.Annotations[config.AnnotationJobExecutionKey])
		desiredGenerationRaw := strings.TrimSpace(desired.Annotations[config.AnnotationJobRunGeneration])
		if desiredKey == "" || desiredGenerationRaw == "" {
			return nil
		}
		desiredGeneration, err := strconv.ParseUint(desiredGenerationRaw, 10, 64)
		if err != nil || desiredGeneration == 0 {
			return fmt.Errorf("%w: desired Job has invalid run generation %q", errJobExecutionIdentityChanged, desiredGenerationRaw)
		}
		existingKey := strings.TrimSpace(existing.Annotations[config.AnnotationJobExecutionKey])
		existingGenerationRaw := strings.TrimSpace(existing.Annotations[config.AnnotationJobRunGeneration])
		if existingGenerationRaw == "" {
			if existingKey != "" && existingKey != desiredKey {
				return fmt.Errorf("%w: existing Job has a different execution key without a generation", errJobExecutionIdentityChanged)
			}
			return nil
		}
		existingGeneration, err := strconv.ParseUint(existingGenerationRaw, 10, 64)
		if err != nil || existingGeneration == 0 {
			return fmt.Errorf("%w: existing Job has invalid run generation %q", errJobExecutionIdentityChanged, existingGenerationRaw)
		}
		if existingGeneration > desiredGeneration || (existingGeneration == desiredGeneration && existingKey != desiredKey) {
			return fmt.Errorf("%w: existing Job generation %d does not belong to desired generation %d", errJobExecutionIdentityChanged, existingGeneration, desiredGeneration)
		}
		return nil
	}
}

func isTerminalWorkflowJobTaskStatus(status config.Status) bool {
	switch status {
	case config.StatusCompleted, config.StatusPassed, config.StatusSkipped, config.StatusFailed, config.StatusTimeout, config.StatusCancelled, config.StatusReject:
		return true
	default:
		return false
	}
}

// jobTypeForTask returns the job type for policy checks.
func jobTypeForTask(job *model.JobTask, fallback config.JobType) config.JobType {
	if job == nil {
		return fallback
	}
	if job.JobType == "" {
		return fallback
	}
	return config.JobType(job.JobType)
}

// jobCompletedInStore checks whether a completed job of the same component is already recorded.
func jobCompletedInStore(ctx context.Context, store datastore.DataStore, jobType config.JobType, jobObj *batchv1.Job) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("store is nil")
	}
	if jobObj == nil {
		return false, fmt.Errorf("job is nil")
	}
	if jobType == "" {
		return false, fmt.Errorf("job type is empty")
	}
	labels := jobObj.GetLabels()
	appID := strings.TrimSpace(labels[config.LabelAppID])
	componentName := strings.TrimSpace(labels[config.LabelComponentName])
	if appID == "" || componentName == "" {
		return false, nil
	}
	filters := completedJobFilters(appID, componentName, jobType)
	count, err := store.Count(ctx, &model.JobInfo{}, &filters)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// completedJobFilters builds datastore filters for completed job lookup.
func completedJobFilters(appID, componentName string, jobType config.JobType) datastore.FilterOptions {
	return datastore.FilterOptions{
		In: []datastore.InQueryOption{
			{Key: "status", Values: []string{string(config.StatusCompleted)}},
			{Key: "type", Values: []string{string(jobType)}},
			{Key: "app_id", Values: []string{appID}},
			{Key: "service_name", Values: []string{componentName}},
		},
	}
}

func startTimeFromJob(job *batchv1.Job) (int64, bool) {
	if job == nil {
		return 0, false
	}
	raw := strings.TrimSpace(job.GetAnnotations()[config.AnnotationJobStartTime])
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func jobTerminalStatus(job *batchv1.Job) (config.Status, string, bool) {
	if job == nil {
		return "", "", false
	}
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			return config.StatusCompleted, "", true
		case batchv1.JobFailed:
			msg := strings.TrimSpace(condition.Message)
			if msg == "" {
				msg = strings.TrimSpace(condition.Reason)
			}
			if msg == "" {
				msg = fmt.Sprintf("job %s failed", job.Name)
			}
			return config.StatusFailed, msg, true
		}
	}
	if job.Status.Succeeded >= jobRequiredCompletions(job) {
		return config.StatusCompleted, "", true
	}
	if job.Status.Failed > 0 && job.Status.Active == 0 {
		msg := fmt.Sprintf("job %s failed", job.Name)
		return config.StatusFailed, msg, true
	}
	return "", "", false
}

func jobRequiredCompletions(job *batchv1.Job) int32 {
	if job == nil || job.Spec.Completions == nil || *job.Spec.Completions <= 0 {
		return 1
	}
	return *job.Spec.Completions
}

func jobSucceededOwnedPodsCompleted(ctx context.Context, client kubernetes.Interface, namespace string, job *batchv1.Job) (bool, error) {
	if client == nil {
		return false, fmt.Errorf("client is nil")
	}
	if job == nil || job.Name == "" || job.UID == "" {
		return false, nil
	}
	selector := labels.Set{batchv1.JobNameLabel: job.Name}.String()
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, fmt.Errorf("list pods for job %s/%s: %w", namespace, job.Name, err)
	}

	var succeeded int32
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}
		if !podOwnedByJob(pod, job) {
			continue
		}
		succeeded++
	}
	return succeeded >= jobRequiredCompletions(job), nil
}

func podOwnedByJob(pod *corev1.Pod, job *batchv1.Job) bool {
	if pod == nil || job == nil || job.UID == "" {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" && owner.UID == job.UID {
			return true
		}
	}
	return false
}

// Job wait helpers.
const jobPollInterval = 2 * time.Second

const jobDeleteTimeout = 30 * time.Second

var errJobExecutionIdentityChanged = errors.New("job execution identity changed")

func waitForJobCompletion(ctx context.Context, client kubernetes.Interface, namespace, name string) (config.Status, string, error) {
	return waitForJobCompletionMatching(ctx, client, namespace, name, nil)
}

func waitForJobCompletionMatching(ctx context.Context, client kubernetes.Interface, namespace, name string, matches func(*batchv1.Job) bool) (config.Status, string, error) {
	if client == nil {
		return config.StatusFailed, "client is nil", fmt.Errorf("client is nil")
	}
	var finalStatus config.Status
	var finalMessage string
	err := wait.PollUntilContextCancel(ctx, jobPollInterval, true, func(checkCtx context.Context) (bool, error) {
		job, err := client.BatchV1().Jobs(namespace).Get(checkCtx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if matches != nil && !matches(job) {
			return false, errJobExecutionIdentityChanged
		}
		status, message, done := jobTerminalStatus(job)
		if done {
			finalStatus = status
			finalMessage = message
			return true, nil
		}
		completed, err := jobSucceededOwnedPodsCompleted(checkCtx, client, namespace, job)
		if err != nil {
			return false, err
		}
		if completed {
			finalStatus = config.StatusCompleted
			finalMessage = ""
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return config.StatusTimeout, err.Error(), NewStatusError(config.StatusTimeout, err)
		}
		return config.StatusFailed, err.Error(), err
	}
	if finalStatus == config.StatusFailed {
		return finalStatus, finalMessage, NewStatusError(config.StatusFailed, fmt.Errorf("%s", finalMessage))
	}
	return finalStatus, finalMessage, nil
}

func jobExists(ctx context.Context, client kubernetes.Interface, namespace, name string) (*batchv1.Job, bool, error) {
	if client == nil {
		return nil, false, fmt.Errorf("client is nil")
	}
	job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return job, true, nil
}

func waitForJobDeletion(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
	if client == nil {
		return fmt.Errorf("client is nil")
	}
	deleteCtx, cancel := context.WithTimeout(ctx, jobDeleteTimeout)
	defer cancel()
	return wait.PollUntilContextCancel(deleteCtx, jobPollInterval, true, func(checkCtx context.Context) (bool, error) {
		_, err := client.BatchV1().Jobs(namespace).Get(checkCtx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
}
