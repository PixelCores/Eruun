package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

var (
	ErrResultQueueUnavailable = errors.New("result queue unavailable")
	errResultDispatchNoRetry  = errors.New("result dispatch no retry")
)

const defaultResultProcessingConcurrency = 16

type JobResultPayload struct {
	OutboxID       string `json:"outboxId,omitempty"`
	TaskID         string `json:"taskId"`
	ExecutionKey   string `json:"executionKey"`
	RunGeneration  uint64 `json:"runGeneration"`
	JobType        string `json:"jobType,omitempty"`
	Namespace      string `json:"namespace,omitempty"`
	Name           string `json:"name"`
	ServiceName    string `json:"serviceName,omitempty"`
	TimeoutSeconds int64  `json:"timeoutSeconds,omitempty"`
	RunToken       string `json:"runToken,omitempty"`
	WorkerID       string `json:"workerId,omitempty"`
}

type ResultDispatcher struct {
	queue                 msg.Queue
	client                kubernetes.Interface
	store                 datastore.DataStore
	group                 string
	consumer              string
	readCount             int
	readBlock             time.Duration
	autoClaimInterval     time.Duration
	autoClaimIdle         time.Duration
	autoClaimCount        int
	backoffMin            time.Duration
	backoffMax            time.Duration
	heartbeatInterval     time.Duration
	processingConcurrency int
	inFlightMu            sync.Mutex
	inFlight              map[string]struct{}
	ackFailures           atomic.Int64
	ensureFailures        atomic.Int64
}

func NewResultDispatcher(queue msg.Queue, client kubernetes.Interface, store datastore.DataStore, group, consumer string) *ResultDispatcher {
	return &ResultDispatcher{
		queue:                 queue,
		client:                client,
		store:                 store,
		group:                 group,
		consumer:              consumer,
		readCount:             workflowconfig.DefaultWorkerReadCount,
		readBlock:             workflowconfig.DefaultWorkerReadBlock,
		autoClaimInterval:     workflowconfig.DefaultWorkerStaleInterval,
		autoClaimIdle:         workflowconfig.DefaultWorkerAutoClaimIdle,
		autoClaimCount:        workflowconfig.DefaultWorkerAutoClaimCount,
		backoffMin:            workflowconfig.DefaultWorkerBackoffMin,
		backoffMax:            workflowconfig.DefaultWorkerBackoffMax,
		processingConcurrency: defaultResultProcessingConcurrency,
	}
}

func (d *ResultDispatcher) Start(ctx context.Context) {
	if !d.prepare(ctx) {
		return
	}
	go d.runLoops(ctx)
}

func (d *ResultDispatcher) Run(ctx context.Context) {
	if !d.prepare(ctx) {
		return
	}
	d.runLoops(ctx)
}

func (d *ResultDispatcher) prepare(ctx context.Context) bool {
	if d == nil {
		return false
	}
	if d.queue == nil || d.client == nil || d.store == nil {
		klog.ErrorS(fmt.Errorf("queue, client, or store is nil"), "result dispatcher dependencies missing", "queueNil", d.queue == nil, "clientNil", d.client == nil, "storeNil", d.store == nil)
		return false
	}
	if d.group == "" {
		d.group = config.ResultQueueGroup
	}
	if d.consumer == "" {
		d.consumer = "result-dispatcher"
	}
	if err := d.queue.EnsureGroup(ctx, d.group); err != nil {
		failures := d.ensureFailures.Add(1)
		klog.ErrorS(err, "result dispatcher ensure group failed", "group", d.group, "failureCount", failures)
	}
	return true
}

func (d *ResultDispatcher) runLoops(ctx context.Context) {
	concurrency := d.processingConcurrency
	if concurrency <= 0 {
		concurrency = defaultResultProcessingConcurrency
	}
	slots := make(chan struct{}, concurrency)
	var loopWG sync.WaitGroup
	var processingWG sync.WaitGroup
	loopWG.Add(2)
	go func() {
		defer loopWG.Done()
		d.readLoop(ctx, slots, &processingWG)
	}()
	go func() {
		defer loopWG.Done()
		d.claimLoop(ctx, slots, &processingWG)
	}()
	loopWG.Wait()
	processingWG.Wait()
	d.releaseInFlightMessages()
}

func (d *ResultDispatcher) readLoop(ctx context.Context, slots chan struct{}, processingWG *sync.WaitGroup) {
	currentDelay := d.backoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		messages, err := d.queue.ReadGroup(ctx, d.group, d.consumer, d.readCount, d.readBlock)
		if err != nil {
			wait := d.backoffDelay(currentDelay)
			currentDelay = wait
			klog.ErrorS(err, "result dispatcher read failed", "group", d.group, "consumer", d.consumer, "retryAfter", wait)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}
		currentDelay = d.backoffMin
		d.dispatchMessages(ctx, messages, slots, processingWG)
	}
}

func (d *ResultDispatcher) claimLoop(ctx context.Context, slots chan struct{}, processingWG *sync.WaitGroup) {
	ticker := time.NewTicker(d.autoClaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		messages, err := d.queue.AutoClaim(ctx, d.group, d.consumer, d.autoClaimIdle, d.autoClaimCount)
		if err != nil {
			klog.ErrorS(err, "result dispatcher auto-claim failed", "group", d.group, "consumer", d.consumer)
			continue
		}
		d.dispatchMessages(ctx, messages, slots, processingWG)
	}
}

func (d *ResultDispatcher) dispatchMessages(ctx context.Context, messages []msg.Message, slots chan struct{}, processingWG *sync.WaitGroup) {
	for i, message := range messages {
		if !d.markMessageInFlight(message.ID) {
			klog.V(4).InfoS("skip result message already being handled", "msgID", message.ID)
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			for _, pending := range messages[i+1:] {
				d.markMessageInFlight(pending.ID)
			}
			return
		}
		message := message
		msg.MarkMessageHandlingStart(d.queue, message.ID)
		processingWG.Add(1)
		go func() {
			defer processingWG.Done()
			defer func() { <-slots }()
			defer d.markMessageDone(message.ID)
			if !d.handleMessage(ctx, message) {
				msg.MarkMessageHandlingDone(d.queue, message.ID, false)
			}
		}()
	}
}

func (d *ResultDispatcher) releaseInFlightMessages() {
	d.inFlightMu.Lock()
	ids := make([]string, 0, len(d.inFlight))
	for id := range d.inFlight {
		ids = append(ids, id)
	}
	d.inFlight = nil
	d.inFlightMu.Unlock()

	for _, id := range ids {
		msg.MarkMessageHandlingDone(d.queue, id, false)
	}
}

func (d *ResultDispatcher) markMessageInFlight(id string) bool {
	if id == "" {
		return true
	}
	d.inFlightMu.Lock()
	defer d.inFlightMu.Unlock()
	if d.inFlight == nil {
		d.inFlight = make(map[string]struct{})
	}
	if _, exists := d.inFlight[id]; exists {
		return false
	}
	d.inFlight[id] = struct{}{}
	return true
}

func (d *ResultDispatcher) markMessageDone(id string) {
	if id == "" {
		return
	}
	d.inFlightMu.Lock()
	delete(d.inFlight, id)
	d.inFlightMu.Unlock()
}

func (d *ResultDispatcher) handleMessage(ctx context.Context, message msg.Message) bool {
	if message.ID == "" {
		return true
	}
	if len(message.Payload) == 0 {
		return d.ackMessage(ctx, message.ID, "empty_payload") == nil
	}
	payload, err := decodeResultPayload(message.Payload)
	if err != nil {
		klog.ErrorS(err, "result dispatcher decode payload failed", "msgID", message.ID)
		return d.ackMessage(ctx, message.ID, "decode_payload_failed") == nil
	}
	if payload.Name == "" || payload.TaskID == "" {
		klog.ErrorS(fmt.Errorf("task or name is empty"), "result dispatcher payload missing task or name", "msgID", message.ID, "taskID", payload.TaskID, "name", payload.Name)
		return d.ackMessage(ctx, message.ID, "missing_task_or_name") == nil
	}
	if payload.OutboxID != "" {
		return d.handleOutboxMessage(ctx, message, payload)
	}
	if err := processJobResult(ctx, d.client, d.store, payload); err != nil {
		if errors.Is(err, errResultDispatchNoRetry) {
			klog.ErrorS(err, "result dispatcher process failed without retry", "msgID", message.ID, "taskID", payload.TaskID, "name", payload.Name)
			return d.ackMessage(ctx, message.ID, "no_retry_process_error") == nil
		}
		klog.ErrorS(err, "result dispatcher process failed", "msgID", message.ID, "taskID", payload.TaskID, "name", payload.Name)
		return false
	}
	return d.ackMessage(ctx, message.ID, "processed") == nil
}

func (d *ResultDispatcher) handleOutboxMessage(ctx context.Context, message msg.Message, payload *JobResultPayload) bool {
	if d == nil || d.store == nil {
		return false
	}
	persistCtx, cancel := resultOutboxPersistenceContext()
	outbox, err := getJobResultOutboxByID(persistCtx, d.store, strings.TrimSpace(payload.OutboxID))
	if err != nil {
		cancel()
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return d.ackMessage(ctx, message.ID, "outbox_missing") == nil
		}
		klog.ErrorS(err, "load result outbox", "msgID", message.ID)
		return false
	}
	claimed, err := claimResultOutbox(persistCtx, d.store, outbox, message.ID)
	cancel()
	if err != nil {
		klog.ErrorS(err, "claim result outbox", "outboxID", outbox.ID)
		return false
	}
	if !claimed {
		// A processing delivery has its own token and lease. Replayed broker IDs
		// must never resume or invalidate a live owner's work.
		return d.ackMessage(ctx, message.ID, "outbox_not_claimed") == nil
	}
	// The durable outbox owns the result identity, not the notification body.
	payload = jobResultPayloadFromOutbox(outbox)
	if payload == nil {
		err = errResultDispatchNoRetry
	} else {
		err = d.processOwnedResult(ctx, outbox, payload)
	}
	persistCtx, cancel = resultOutboxPersistenceContext()
	defer cancel()
	if err != nil {
		klog.ErrorS(err, "process result outbox", "outboxID", outbox.ID)
		if errors.Is(err, errResultOutboxOwnershipLost) {
			return false
		}
		if errors.Is(err, errResultDispatchNoRetry) {
			if markErr := markJobResultOutboxFailed(persistCtx, d.store, outbox, err.Error()); markErr != nil {
				return false
			}
			return d.ackMessage(ctx, message.ID, "no_retry_process_error") == nil
		}
		if retryErr := retryResultOutbox(persistCtx, d.store, outbox, err.Error()); retryErr != nil {
			klog.ErrorS(retryErr, "return result outbox to pending", "outboxID", outbox.ID)
		}
		return false
	}
	if err := withResultOutboxOwnership(persistCtx, d.store, outbox, func(tx datastore.DataStore, _ *model.JobResultOutbox) error {
		return deleteJobResultOutbox(persistCtx, tx, outbox.ID)
	}); err != nil {
		return false
	}
	return d.ackMessage(ctx, message.ID, "processed") == nil
}

func claimResultOutbox(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, messageID string) (bool, error) {
	for attempt := 0; attempt < 2; attempt++ {
		conditions := map[string]interface{}{"state": string(outbox.State), "message_id": outbox.MessageID}
		switch outbox.State {
		case config.JobResultOutboxStateResultQueued:
			if outbox.MessageID != strings.TrimSpace(messageID) {
				return false, nil
			}
		case config.JobResultOutboxStateResultDispatching:
			// Enqueue may deliver before the producer persists its broker ID.
		default:
			return false, nil
		}
		now, err := resultOutboxDatabaseTime(ctx, store)
		if err != nil {
			return false, err
		}
		deadline := now.Add(resultOutboxProcessGrace)
		token := uuid.NewString()
		claimed, err := compareAndSwapJobResultOutboxWithConditions(ctx, store, outbox, conditions, map[string]interface{}{
			"state": config.JobResultOutboxStateResultProcessingQueue, "message_id": token, "lease_expires_at": &deadline, "last_error": "",
		})
		if err != nil {
			return false, err
		}
		if claimed {
			outbox.State = config.JobResultOutboxStateResultProcessingQueue
			outbox.MessageID = token
			outbox.LeaseExpiresAt = &deadline
			return true, nil
		}
		current, err := getJobResultOutboxByID(ctx, store, outbox.ID)
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		*outbox = *current
	}
	return false, nil
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

func decodeResultPayload(raw []byte) (*JobResultPayload, error) {
	var payload JobResultPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if err := validateJobResultPayload(&payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

func (d *ResultDispatcher) ackMessage(ctx context.Context, msgID, reason string) error {
	if d == nil || d.queue == nil || msgID == "" {
		return nil
	}
	if err := d.queue.Ack(ctx, d.group, msgID); err != nil {
		msg.MarkMessageHandlingDone(d.queue, msgID, false)
		failures := d.ackFailures.Add(1)
		klog.ErrorS(err, "result dispatcher ack failed", "group", d.group, "msgID", msgID, "reason", reason, "failureCount", failures)
		return err
	}
	msg.MarkMessageHandlingDone(d.queue, msgID, true)
	klog.V(4).InfoS("result dispatcher ack succeeded", "group", d.group, "msgID", msgID, "reason", reason)
	return nil
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

func processJobResult(ctx context.Context, client kubernetes.Interface, store datastore.DataStore, payload *JobResultPayload) error {
	return processJobResultWithOutbox(ctx, client, store, payload, nil)
}

func processJobResultWithOutbox(ctx context.Context, client kubernetes.Interface, store datastore.DataStore, payload *JobResultPayload, outbox *model.JobResultOutbox) error {
	if !isResultPayloadProcessable(payload) {
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
		if outbox == nil || outbox.JobUID == "" {
			// Old completed rows have no trustworthy cleanup identity. Do not bind
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
	if outbox != nil && outbox.JobUID != "" {
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

	persist := func(tx datastore.DataStore, _ *model.JobResultOutbox) error {
		if err := updateJobInfoStatus(ctx, tx, payload, status, message, startTime, endTime, logs); err != nil {
			return err
		}
		saved, err := findJobInfoForResult(ctx, tx, payload)
		if err != nil {
			return err
		}
		if saved == nil || saved.Status != string(status) {
			return fmt.Errorf("result status was not persisted for execution %s", payload.ExecutionKey)
		}
		return nil
	}
	if upErr := withResultOutboxOwnership(ctx, store, outbox, persist); upErr != nil {
		return upErr
	}
	if status == config.StatusCompleted {
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
	if outbox == nil || uid == "" {
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
		bound, err := compareAndSwapJobResultOutbox(ctx, tx, outbox, config.JobResultOutboxStateResultProcessingQueue, map[string]interface{}{"job_uid": uid})
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
	if outbox != nil {
		if err := withResultOutboxOwnership(ctx, store, outbox, func(datastore.DataStore, *model.JobResultOutbox) error { return nil }); err != nil {
			return err
		}
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

func updateJobInfoStatus(ctx context.Context, store datastore.DataStore, payload *JobResultPayload, status config.Status, message string, startTime, endTime int64, info string) error {
	if store == nil || validateJobResultPayload(payload) != nil {
		return errResultDispatchNoRetry
	}
	if hasResultPayloadFencingIdentity(payload) {
		return updateFencedJobInfoStatus(ctx, store, payload, status, message, startTime, endTime, info)
	}
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
		SortBy: []datastore.SortOption{
			{Key: "create_time", Order: datastore.SortOrderDescending},
		},
		Page:     1,
		PageSize: 1,
	}
	entities, err := store.List(ctx, query, &opts)
	if err != nil {
		return fmt.Errorf("list job info: %w", err)
	}
	if len(entities) == 0 {
		klog.InfoS("job info not found while updating result status", "taskID", payload.TaskID, "serviceName", payload.ServiceName, "status", status)
		return nil
	}
	jobInfo, ok := entities[0].(*model.JobInfo)
	if !ok || jobInfo == nil {
		return fmt.Errorf("job info type assertion failed")
	}
	if shouldKeepExistingJobInfoStatus(jobInfo.Status, status) {
		klog.V(4).InfoS("skip stale job status update", "taskID", payload.TaskID, "current", jobInfo.Status, "next", status)
		return nil
	}

	jobInfo.Status = string(status)
	switch status {
	case config.StatusCompleted, config.StatusSkipped, config.StatusPassed:
		jobInfo.Error = ""
	default:
		jobInfo.Error = strings.TrimSpace(message)
	}
	if startTime > 0 && jobInfo.StartTime == 0 {
		jobInfo.StartTime = startTime
	}
	if endTime > 0 {
		jobInfo.EndTime = endTime
	} else if jobInfo.EndTime == 0 && status != config.StatusRunning {
		jobInfo.EndTime = time.Now().Unix()
	}
	if status == config.StatusCompleted && info != "" {
		jobInfo.Info = info
	}

	if err := store.Put(ctx, jobInfo); err != nil {
		return fmt.Errorf("update job info: %w", err)
	}
	return nil
}

func hasResultPayloadFencingIdentity(payload *JobResultPayload) bool {
	return payload != nil && (strings.TrimSpace(payload.RunToken) != "" || strings.TrimSpace(payload.WorkerID) != "")
}

func hasCompleteResultPayloadExecutionIdentity(payload *JobResultPayload) bool {
	return payload != nil &&
		payload.RunGeneration > 0 &&
		strings.TrimSpace(payload.ExecutionKey) != "" &&
		strings.TrimSpace(payload.RunToken) != "" &&
		strings.TrimSpace(payload.WorkerID) != ""
}

func updateFencedJobInfoStatus(
	ctx context.Context,
	store datastore.DataStore,
	payload *JobResultPayload,
	status config.Status,
	message string,
	startTime, endTime int64,
	info string,
) error {
	owner, err := resultPayloadJobTask(payload)
	if err != nil {
		return errors.Join(errResultDispatchNoRetry, err)
	}
	err = withJobInfoOwnership(ctx, store, owner, func(tx datastore.DataStore) error {
		conditionalStore, ok := tx.(datastore.ConditionalCompareAndSwap)
		if !ok {
			return fmt.Errorf("update job info: datastore does not support conditional compare-and-swap")
		}
		for attempt := 1; attempt <= jobInfoSaveMaxAttempts; attempt++ {
			jobInfo, err := findJobInfoForResult(ctx, tx, payload)
			if err != nil {
				return err
			}
			if jobInfo == nil {
				klog.InfoS("job info not found while updating result status", "taskID", payload.TaskID, "serviceName", payload.ServiceName, "status", status)
				return nil
			}
			if shouldKeepExistingJobInfoStatus(jobInfo.Status, status) {
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
	})
	if errors.Is(err, repository.ErrWorkflowOwnershipLost) {
		return errors.Join(errResultDispatchNoRetry, err)
	}
	return err
}

func resultPayloadJobTask(payload *JobResultPayload) (*model.JobTask, error) {
	if payload == nil {
		return nil, fmt.Errorf("result payload is nil")
	}
	if !hasCompleteResultPayloadExecutionIdentity(payload) {
		return nil, fmt.Errorf("result payload execution identity is incomplete")
	}
	return &model.JobTask{
		TaskID:        strings.TrimSpace(payload.TaskID),
		JobType:       strings.TrimSpace(payload.JobType),
		ExecutionKey:  strings.TrimSpace(payload.ExecutionKey),
		RunGeneration: payload.RunGeneration,
		RunToken:      strings.TrimSpace(payload.RunToken),
		WorkerID:      strings.TrimSpace(payload.WorkerID),
	}, nil
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

func (d *ResultDispatcher) backoffDelay(current time.Duration) time.Duration {
	if current <= 0 {
		current = d.backoffMin
	}
	next := current * 2
	if next > d.backoffMax {
		next = d.backoffMax
	}
	if next < d.backoffMin {
		next = d.backoffMin
	}
	return next
}
