package job

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

var errResultDispatchNoRetry = errors.New("result dispatch no retry")

const defaultResultProcessingConcurrency = 16

type JobResultPayload struct {
	TaskID         string `json:"taskId"`
	ExecutionKey   string `json:"executionKey"`
	RunGeneration  uint64 `json:"runGeneration"`
	JobType        string `json:"jobType,omitempty"`
	Namespace      string `json:"namespace,omitempty"`
	Name           string `json:"name"`
	ServiceName    string `json:"serviceName,omitempty"`
	TimeoutSeconds int64  `json:"timeoutSeconds,omitempty"`
}

type ResultDispatcher struct {
	client                kubernetes.Interface
	store                 datastore.DataStore
	pollInterval          time.Duration
	recoveryBatchSize     int
	heartbeatInterval     time.Duration
	processingConcurrency int
	pendingBeforeID       string
}

func NewResultDispatcher(client kubernetes.Interface, store datastore.DataStore) *ResultDispatcher {
	return &ResultDispatcher{
		client: client, store: store,
		pollInterval:          workflowconfig.DefaultDispatchPollInterval,
		recoveryBatchSize:     resultOutboxBatchSize,
		processingConcurrency: defaultResultProcessingConcurrency,
	}
}

func (d *ResultDispatcher) Run(ctx context.Context) {
	if d == nil || d.client == nil || d.store == nil {
		klog.ErrorS(fmt.Errorf("Kubernetes client and datastore are required"), "result dispatcher dependencies missing")
		return
	}
	concurrency := d.processingConcurrency
	if concurrency <= 0 {
		concurrency = defaultResultProcessingConcurrency
	}
	interval := d.pollInterval
	if interval <= 0 {
		interval = workflowconfig.DefaultDispatchPollInterval
	}
	slots := make(chan struct{}, concurrency)
	var processing sync.WaitGroup
	defer processing.Wait()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		// Recovery is independent of free processing slots. A full pool must not
		// prevent expired owners from being fenced and returned to pending.
		if err := d.recoverResultOutboxes(ctx); err != nil {
			klog.ErrorS(err, "recover result outboxes")
		}
		if err := d.dispatchPendingResults(ctx, slots, &processing); err != nil {
			klog.ErrorS(err, "dispatch pending results")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *ResultDispatcher) dispatchPendingResults(ctx context.Context, slots chan struct{}, processing *sync.WaitGroup) error {
	available := cap(slots) - len(slots)
	if available == 0 {
		return nil
	}
	outboxes, err := listPendingResultOutboxes(ctx, d.store, d.pendingBeforeID, available)
	if err != nil {
		return err
	}
	for _, outbox := range outboxes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case slots <- struct{}{}:
		}
		// Advance only past rows actually attempted, including claim errors.
		// Advancing an entire batch could hide healthy rows behind a failing CAS.
		d.pendingBeforeID = outbox.ID
		claimed, err := claimResultOutbox(ctx, d.store, outbox)
		if err != nil || !claimed {
			<-slots
			if err != nil {
				return err
			}
			continue
		}
		processing.Add(1)
		go func() {
			defer processing.Done()
			defer func() { <-slots }()
			if err := d.processResult(ctx, outbox); err != nil {
				klog.ErrorS(err, "process result outbox", "outboxID", outbox.ID)
			}
		}()
	}
	if len(outboxes) < available {
		d.pendingBeforeID = ""
	}
	return nil
}

// processResult only accepts an already-claimed outbox. All result writes and
// cleanup keep their independent database ownership checks.
func (d *ResultDispatcher) processResult(ctx context.Context, outbox *model.JobResultOutbox) error {
	payload := jobResultPayloadFromOutbox(outbox)
	var err error
	if payload == nil {
		err = errResultDispatchNoRetry
	} else {
		err = d.processOwnedResult(ctx, outbox, payload)
	}
	persistCtx, cancel := resultOutboxPersistenceContext()
	defer cancel()
	if err != nil {
		if errors.Is(err, errResultOutboxOwnershipLost) {
			return err
		}
		if errors.Is(err, errResultDispatchNoRetry) {
			return errors.Join(err, markJobResultOutboxFailed(persistCtx, d.store, outbox, err.Error()))
		}
		return errors.Join(err, retryResultOutbox(persistCtx, d.store, outbox, err.Error()))
	}
	return withResultOutboxOwnership(persistCtx, d.store, outbox, func(tx datastore.DataStore, _ *model.JobResultOutbox) error {
		return deleteJobResultOutbox(persistCtx, tx, outbox.ID)
	})
}

func claimResultOutbox(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox) (bool, error) {
	if outbox == nil || outbox.State != config.JobResultOutboxStateResultPending {
		return false, nil
	}
	now, err := resultOutboxDatabaseTime(ctx, store)
	if err != nil {
		return false, err
	}
	deadline := now.Add(resultOutboxProcessGrace)
	token := uuid.NewString()
	claimed, err := compareAndSwapJobResultOutboxWithConditions(ctx, store, outbox, map[string]interface{}{
		"state": string(config.JobResultOutboxStateResultPending), "claim_token": outbox.ClaimToken, "attempts": outbox.Attempts,
	}, map[string]interface{}{
		"state": config.JobResultOutboxStateResultProcessing, "claim_token": token, "lease_expires_at": &deadline, "last_error": "",
	})
	if claimed {
		outbox.State = config.JobResultOutboxStateResultProcessing
		outbox.ClaimToken = token
		outbox.LeaseExpiresAt = &deadline
	}
	return claimed, err
}

func (d *ResultDispatcher) processOwnedResult(ctx context.Context, outbox *model.JobResultOutbox, payload *JobResultPayload) error {
	processCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := d.heartbeatInterval
		if interval <= 0 {
			interval = resultOutboxHeartbeatInterval
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-processCtx.Done():
				return
			case <-ticker.C:
				renewCtx, stop := context.WithTimeout(processCtx, resultOutboxPersistTimeout)
				err := renewResultOutboxLease(renewCtx, d.store, outbox)
				stop()
				if err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	err := processJobResultWithOutbox(processCtx, d.client, d.store, payload, outbox)
	cause := context.Cause(processCtx)
	cancel(context.Canceled)
	<-done
	if cause != nil {
		return errors.Join(err, cause)
	}
	return err
}

func isResultPayloadProcessable(payload *JobResultPayload) bool {
	return validateJobResultPayload(payload) == nil
}

func validateJobResultPayload(payload *JobResultPayload) error {
	if payload == nil || strings.TrimSpace(payload.Name) == "" || strings.TrimSpace(payload.Namespace) == "" {
		return fmt.Errorf("result payload job name and namespace are required")
	}
	if strings.TrimSpace(payload.TaskID) == "" {
		return fmt.Errorf("result payload task ID is required")
	}
	if strings.TrimSpace(payload.ExecutionKey) == "" {
		return fmt.Errorf("result payload execution key is required")
	}
	if payload.RunGeneration == 0 {
		return fmt.Errorf("result payload run generation is required")
	}
	return nil
}

func processJobResultWithOutbox(ctx context.Context, client kubernetes.Interface, store datastore.DataStore, payload *JobResultPayload, outbox *model.JobResultOutbox) error {
	if !isResultPayloadProcessable(payload) || outbox == nil || strings.TrimSpace(outbox.ID) == "" {
		return errResultDispatchNoRetry
	}
	if client == nil || store == nil {
		return errResultDispatchNoRetry
	}
	namespace := strings.TrimSpace(payload.Namespace)
	if namespace == "" {
		return errResultDispatchNoRetry
	}

	timeout := payload.TimeoutSeconds
	if timeout <= 0 {
		timeout = int64(config.DefaultJobTaskTimeout.Seconds())
	}
	record, recordErr := findJobInfoForResult(ctx, store, payload)
	if recordErr != nil {
		return recordErr
	}
	if record != nil && isSettledDelayedExecutionStatus(config.Status(record.Status)) {
		if config.Status(record.Status) != config.StatusCompleted {
			return nil
		}
		if outbox.JobUID == "" {
			// Completed rows without a recorded UID have no cleanup identity. Do not bind
			// them to whichever object now happens to have the same name.
			klog.InfoS("keep completed result without recorded cleanup UID", "taskID", payload.TaskID, "name", payload.Name)
			return nil
		}
		return cleanupPersistedResult(ctx, client, store, payload, outbox, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: payload.Name, Namespace: namespace, UID: types.UID(outbox.JobUID)}})
	}
	currentJob, getErr := client.BatchV1().Jobs(namespace).Get(ctx, payload.Name, metav1.GetOptions{})
	if k8serrors.IsNotFound(getErr) {
		currentJob = nil
	} else if getErr != nil {
		return fmt.Errorf("get job before processing result: %w", getErr)
	}
	if currentJob != nil && !jobResultMatchesExecutionIdentity(payload, currentJob) {
		klog.InfoS("discard stale job result before waiting for newer Kubernetes Job", "namespace", namespace, "name", payload.Name, "taskID", payload.TaskID, "runGeneration", payload.RunGeneration)
		return nil
	}
	expectedUID := ""
	if currentJob != nil {
		expectedUID = string(currentJob.UID)
	}
	if err := bindResultOutboxJobUID(ctx, store, outbox, expectedUID); err != nil {
		return err
	}
	if outbox.JobUID != "" {
		expectedUID = outbox.JobUID
	}
	matchesExpectedJob := func(jobObj *batchv1.Job) bool {
		if !jobResultMatchesExecutionIdentity(payload, jobObj) {
			return false
		}
		if expectedUID == "" {
			expectedUID = string(jobObj.UID)
		}
		return string(jobObj.UID) == expectedUID
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	status, message, err := waitForJobCompletionMatching(waitCtx, client, namespace, payload.Name, matchesExpectedJob)
	if errors.Is(err, errJobExecutionIdentityChanged) {
		klog.InfoS("discard stale job result after Kubernetes Job identity changed", "namespace", namespace, "name", payload.Name, "taskID", payload.TaskID, "runGeneration", payload.RunGeneration)
		return nil
	}
	if err != nil {
		if _, terminal := ExtractStatusError(err); !terminal {
			return fmt.Errorf("observe result job: %w", err)
		}
	}
	status, message = jobCompletionResult(status, message, err)
	if bindErr := bindResultOutboxJobUID(ctx, store, outbox, expectedUID); bindErr != nil {
		return bindErr
	}

	jobObj, getErr := client.BatchV1().Jobs(namespace).Get(ctx, payload.Name, metav1.GetOptions{})
	if k8serrors.IsNotFound(getErr) {
		jobObj = nil
	} else if getErr != nil {
		return fmt.Errorf("get job after completion: %w", getErr)
	}
	if jobObj != nil && !matchesExpectedJob(jobObj) {
		klog.InfoS("discard stale job result for newer Kubernetes Job", "namespace", namespace, "name", payload.Name, "taskID", payload.TaskID, "runGeneration", payload.RunGeneration)
		return nil
	}
	if jobObj == nil && expectedUID != "" {
		// The observed object may have disappeared after completion. Preserve
		// its UID for pod log selection and cleanup even if a name is reused.
		jobObj = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: payload.Name, Namespace: namespace, UID: types.UID(expectedUID)}}
	}
	serviceName := strings.TrimSpace(payload.ServiceName)
	if serviceName == "" && jobObj != nil {
		serviceName = componentNameFromJobInfo(jobObj)
	}
	payload.ServiceName = serviceName

	startTime, endTime := jobTimesFromStatus(jobObj)
	if endTime == 0 && status != config.StatusRunning {
		endTime = time.Now().Unix()
	}

	var logs string
	if status == config.StatusCompleted {
		logText, logErr := collectJobPodLogsForJob(ctx, client, namespace, payload.Name, jobObj)
		if logErr != nil {
			return fmt.Errorf("collect result logs before cleanup: %w", logErr)
		}
		logs = logText
	}

	var persistedStatus config.Status
	persist := func(tx datastore.DataStore, _ *model.JobResultOutbox) error {
		if err := updateJobInfoStatus(ctx, tx, payload, status, message, startTime, endTime, logs); err != nil {
			return err
		}
		saved, err := findJobInfoForResult(ctx, tx, payload)
		if err != nil {
			return err
		}
		if saved == nil || !isSettledDelayedExecutionStatus(config.Status(saved.Status)) {
			return fmt.Errorf("result status was not persisted for execution %s", payload.ExecutionKey)
		}
		persistedStatus = config.Status(saved.Status)
		return nil
	}
	if upErr := withResultOutboxOwnership(ctx, store, outbox, persist); upErr != nil {
		return upErr
	}
	if persistedStatus == config.StatusCompleted {
		if cleanupErr := cleanupPersistedResult(ctx, client, store, payload, outbox, jobObj); cleanupErr != nil {
			return cleanupErr
		}
	}

	if err != nil {
		if _, ok := ExtractStatusError(err); ok {
			return nil
		}
		return err
	}
	return nil
}

// A Job's UID is recorded before waiting or removing its runtime evidence.
func bindResultOutboxJobUID(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, uid string) error {
	if uid == "" {
		return nil
	}
	if outbox.JobUID != "" {
		if outbox.JobUID != uid {
			return fmt.Errorf("%w: result Job UID changed", errResultDispatchNoRetry)
		}
		return nil
	}
	if err := withResultOutboxOwnership(ctx, store, outbox, func(tx datastore.DataStore, current *model.JobResultOutbox) error {
		if current.JobUID != "" && current.JobUID != uid {
			return fmt.Errorf("%w: result Job UID changed", errResultDispatchNoRetry)
		}
		bound, err := compareAndSwapJobResultOutbox(ctx, tx, outbox, config.JobResultOutboxStateResultProcessing, map[string]interface{}{"job_uid": uid})
		if err != nil {
			return err
		}
		if !bound {
			return errResultOutboxOwnershipLost
		}
		return nil
	}); err != nil {
		return err
	}
	outbox.JobUID = uid
	return nil
}

// Database ownership is checked before cleanup; Kubernetes UID preconditions
// protect the irreversible operation after the transaction has released its lock.
func cleanupPersistedResult(ctx context.Context, client kubernetes.Interface, store datastore.DataStore, payload *JobResultPayload, outbox *model.JobResultOutbox, jobObj *batchv1.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := withResultOutboxOwnership(ctx, store, outbox, func(datastore.DataStore, *model.JobResultOutbox) error { return nil }); err != nil {
		return err
	}
	if err := deleteCompletedJobAndPods(ctx, client, payload.Namespace, payload.Name, jobObj); err != nil {
		return fmt.Errorf("clean committed result: %w", err)
	}
	return nil
}

func jobCompletionResult(status config.Status, message string, err error) (config.Status, string) {
	if status == "" {
		if err == nil {
			status = config.StatusFailed
			message = "job status unknown"
		} else {
			status = statusFromError(err)
			message = jobErrorMessage(err, message)
		}
	} else if err != nil {
		message = jobErrorMessage(err, message)
	}

	return status, message
}

func stampJobExecutionIdentity(jobTask *model.JobTask, jobObj *batchv1.Job) {
	if jobTask == nil || jobObj == nil {
		return
	}
	if jobObj.Annotations == nil {
		jobObj.Annotations = make(map[string]string)
	}
	if jobObj.Labels == nil {
		jobObj.Labels = make(map[string]string)
	}
	jobObj.Labels[config.LabelManagedBy] = config.ManagedByEruun
	if strings.TrimSpace(jobTask.TaskID) != "" {
		jobObj.Annotations[config.AnnotationJobTaskID] = strings.TrimSpace(jobTask.TaskID)
	}
	if strings.TrimSpace(jobTask.ExecutionKey) == "" {
		return
	}
	jobObj.Annotations[config.AnnotationJobExecutionKey] = strings.TrimSpace(jobTask.ExecutionKey)
	if jobTask.RunGeneration > 0 {
		jobObj.Annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(jobTask.RunGeneration, 10)
	}
	attempt := jobTask.Attempt
	if attempt == 0 {
		attempt = uint(jobTask.RetryCount + 1)
	}
	jobObj.Annotations[workflowconfig.AnnotationJobAttempt] = strconv.FormatUint(uint64(attempt), 10)
	stampWorkspaceJobPodIdentity(jobTask, jobObj)
}

func stampCronJobExecutionIdentity(jobTask *model.JobTask, cron *batchv1.CronJob) {
	if jobTask == nil || cron == nil {
		return
	}
	if cron.Annotations == nil {
		cron.Annotations = make(map[string]string)
	}
	cron.Annotations[config.AnnotationJobTaskID] = strings.TrimSpace(jobTask.TaskID)
	cron.Annotations[config.AnnotationJobExecutionKey] = strings.TrimSpace(jobTask.ExecutionKey)
	if jobTask.RunGeneration > 0 {
		cron.Annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(jobTask.RunGeneration, 10)
	}
	attempt := jobTask.Attempt
	if attempt == 0 {
		attempt = uint(jobTask.RetryCount + 1)
	}
	cron.Annotations[workflowconfig.AnnotationJobAttempt] = strconv.FormatUint(uint64(attempt), 10)
}

func stampWorkspaceJobPodIdentity(task *model.JobTask, workload *batchv1.Job) {
	if !config.IsWorkspaceJobType(config.JobType(task.JobType)) {
		return
	}
	if workload.Spec.Template.Annotations == nil {
		workload.Spec.Template.Annotations = map[string]string{}
	}
	workload.Spec.Template.Annotations[config.AnnotationJobTaskID] = task.TaskID
	workload.Spec.Template.Annotations[config.AnnotationJobExecutionKey] = task.ExecutionKey
	workload.Spec.Template.Annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(task.RunGeneration, 10)
}

func jobResultMatchesExecutionIdentity(payload *JobResultPayload, jobObj *batchv1.Job) bool {
	if validateJobResultPayload(payload) != nil || jobObj == nil {
		return false
	}
	if strings.TrimSpace(jobObj.Annotations[config.AnnotationJobTaskID]) != strings.TrimSpace(payload.TaskID) {
		return false
	}
	if strings.TrimSpace(jobObj.Annotations[config.AnnotationJobExecutionKey]) != strings.TrimSpace(payload.ExecutionKey) {
		return false
	}
	return strings.TrimSpace(jobObj.Annotations[config.AnnotationJobRunGeneration]) == strconv.FormatUint(payload.RunGeneration, 10)
}

// The outbox lease excludes competing result consumers, but cancellation can
// settle JobInfo independently. Preserve the first terminal result atomically.
func updateJobInfoStatus(ctx context.Context, store datastore.DataStore, payload *JobResultPayload, status config.Status, message string, startTime, endTime int64, info string) error {
	if store == nil || validateJobResultPayload(payload) != nil {
		return errResultDispatchNoRetry
	}
	conditionalStore, ok := store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("update job info: datastore does not support conditional compare-and-swap")
	}
	for attempt := 1; attempt <= jobInfoSaveMaxAttempts; attempt++ {
		jobInfo, err := findJobInfoForResult(ctx, store, payload)
		if err != nil {
			return err
		}
		if jobInfo == nil {
			klog.InfoS("job info not found while updating result status", "taskID", payload.TaskID, "serviceName", payload.ServiceName, "status", status)
			return nil
		}
		if isSettledDelayedExecutionStatus(config.Status(jobInfo.Status)) {
			return nil
		}
		updates := map[string]interface{}{"status": string(status)}
		switch status {
		case config.StatusCompleted, config.StatusSkipped, config.StatusPassed:
			updates["error"] = ""
		default:
			updates["error"] = strings.TrimSpace(message)
		}
		if startTime > 0 && jobInfo.StartTime == 0 {
			updates["start_time"] = startTime
		}
		if endTime > 0 {
			updates["end_time"] = endTime
		} else if jobInfo.EndTime == 0 && status != config.StatusRunning {
			updates["end_time"] = time.Now().Unix()
		}
		if status == config.StatusCompleted && info != "" {
			updates["info"] = info
		}
		updated, err := conditionalStore.CompareAndSwapWithConditions(ctx, jobInfo, map[string]interface{}{
			"status":         jobInfo.Status,
			"execution_key":  payload.ExecutionKey,
			"run_generation": payload.RunGeneration,
			"attempt":        jobInfo.Attempt,
		}, updates)
		if err != nil {
			return fmt.Errorf("update job info: %w", err)
		}
		if updated {
			return nil
		}
	}
	return fmt.Errorf("update job info: concurrent execution state changes did not converge after %d attempts", jobInfoSaveMaxAttempts)
}

func findJobInfoForResult(ctx context.Context, store datastore.DataStore, payload *JobResultPayload) (*model.JobInfo, error) {
	query := &model.JobInfo{TaskID: payload.TaskID}
	filters := datastore.FilterOptions{}
	if payload.JobType != "" {
		filters.In = append(filters.In, datastore.InQueryOption{Key: "type", Values: []string{payload.JobType}})
	}
	if payload.ServiceName != "" {
		filters.In = append(filters.In, datastore.InQueryOption{Key: "service_name", Values: []string{payload.ServiceName}})
	}
	filters.In = append(filters.In,
		datastore.InQueryOption{Key: "execution_key", Values: []string{payload.ExecutionKey}},
		datastore.InQueryOption{Key: "run_generation", Values: []string{fmt.Sprint(payload.RunGeneration)}})
	opts := datastore.ListOptions{
		FilterOptions: filters,
		SortBy:        []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderDescending}},
		Page:          1,
		PageSize:      1,
	}
	entities, err := store.List(ctx, query, &opts)
	if err != nil {
		return nil, fmt.Errorf("list job info: %w", err)
	}
	if len(entities) == 0 {
		return nil, nil
	}
	jobInfo, ok := entities[0].(*model.JobInfo)
	if !ok || jobInfo == nil {
		return nil, fmt.Errorf("job info type assertion failed")
	}
	return jobInfo, nil
}

func shouldKeepExistingJobInfoStatus(current string, next config.Status) bool {
	currentStatus := config.Status(strings.TrimSpace(current))
	if !isSuccessfulTerminalJobStatus(currentStatus) {
		return false
	}
	return !isSuccessfulTerminalJobStatus(next)
}

func isSuccessfulTerminalJobStatus(status config.Status) bool {
	switch status {
	case config.StatusCompleted, config.StatusPassed, config.StatusSkipped:
		return true
	default:
		return false
	}
}

func newJobResultPayloadFromDelay(payload *DelayJobPayload, jobObj *batchv1.Job) *JobResultPayload {
	if payload == nil || jobObj == nil {
		return nil
	}
	name := strings.TrimSpace(jobObj.Name)
	namespace := strings.TrimSpace(jobObj.Namespace)
	if name == "" || namespace == "" {
		return nil
	}
	serviceName := strings.TrimSpace(payload.ServiceName)
	if serviceName == "" {
		serviceName = componentNameFromJobInfo(jobObj)
	}
	timeout := payload.TimeoutSeconds
	if timeout <= 0 {
		timeout = int64(config.DefaultJobTaskTimeout.Seconds())
	}
	return &JobResultPayload{
		TaskID:         payload.TaskID,
		ExecutionKey:   payload.ExecutionKey,
		RunGeneration:  payload.RunGeneration,
		JobType:        payload.JobType,
		Namespace:      namespace,
		Name:           name,
		ServiceName:    serviceName,
		TimeoutSeconds: timeout,
	}
}

func jobTimesFromStatus(jobObj *batchv1.Job) (int64, int64) {
	if jobObj == nil {
		return 0, 0
	}
	var startTime, endTime int64
	if jobObj.Status.StartTime != nil {
		startTime = jobObj.Status.StartTime.Unix()
	}
	if jobObj.Status.CompletionTime != nil {
		endTime = jobObj.Status.CompletionTime.Unix()
	}
	return startTime, endTime
}
