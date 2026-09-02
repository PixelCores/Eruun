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

	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	msg "github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/messaging"
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
	processingConcurrency int
	inFlightMu            sync.Mutex
	inFlight              map[string]struct{}
	ackFailures           atomic.Int64
	ensureFailures        atomic.Int64
}

func EnqueueResultJob(ctx context.Context, queue msg.Queue, payload *JobResultPayload) (string, error) {
	return enqueueResultJob(ctx, queue, payload)
}

func NewResultDispatcher(queue msg.Queue, client kubernetes.Interface, store datastore.DataStore, group, consumer string) *ResultDispatcher {
	return &ResultDispatcher{
		queue:                 queue,
		client:                client,
		store:                 store,
		group:                 group,
		consumer:              consumer,
		readCount:             config.DefaultWorkerReadCount,
		readBlock:             config.DefaultWorkerReadBlock,
		autoClaimInterval:     config.DefaultWorkerStaleInterval,
		autoClaimIdle:         config.DefaultWorkerAutoClaimIdle,
		autoClaimCount:        config.DefaultWorkerAutoClaimCount,
		backoffMin:            config.DefaultWorkerBackoffMin,
		backoffMax:            config.DefaultWorkerBackoffMax,
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
	for _, message := range messages {
		if !d.markMessageInFlight(message.ID) {
			klog.V(4).InfoS("skip result message already being handled", "msgID", message.ID)
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			d.markMessageDone(message.ID)
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

	outboxID := strings.TrimSpace(payload.OutboxID)
	outbox, err := getJobResultOutboxByID(persistCtx, d.store, outboxID)
	if err != nil {
		cancel()
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return d.ackMessage(ctx, message.ID, "outbox_missing") == nil
		}
		klog.ErrorS(err, "result dispatcher load outbox failed", "outboxID", outboxID, "msgID", message.ID)
		return false
	}

	messageID := strings.TrimSpace(message.ID)
	switch outbox.State {
	case config.JobResultOutboxStateResultQueued:
		claimed, claimErr := claimQueuedOutboxForMessage(persistCtx, d.store, outbox, messageID)
		if claimErr != nil {
			cancel()
			klog.ErrorS(claimErr, "result dispatcher claim outbox failed", "outboxID", outbox.ID, "msgID", message.ID)
			return false
		}
		if !claimed {
			cancel()
			return d.ackMessage(ctx, message.ID, "outbox_claim_lost") == nil
		}
		outbox.State = config.JobResultOutboxStateResultProcessingQueue
		outbox.MessageID = messageID
		outbox.LastError = ""
	case config.JobResultOutboxStateResultProcessingQueue:
		if strings.TrimSpace(outbox.MessageID) != messageID {
			cancel()
			return d.ackMessage(ctx, message.ID, "outbox_message_mismatch") == nil
		}
	case config.JobResultOutboxStateResultDispatching:
		claimed, claimErr := compareAndSwapJobResultOutboxWithConditions(persistCtx, d.store, outbox, map[string]interface{}{
			"state": string(config.JobResultOutboxStateResultDispatching),
		}, map[string]interface{}{
			"state":      config.JobResultOutboxStateResultProcessingQueue,
			"message_id": messageID,
			"attempts":   outbox.Attempts,
			"last_error": "",
		})
		if claimErr != nil {
			cancel()
			klog.ErrorS(claimErr, "result dispatcher claim dispatching outbox failed", "outboxID", outbox.ID, "msgID", message.ID)
			return false
		}
		if !claimed {
			refreshed, refreshErr := getJobResultOutboxByID(persistCtx, d.store, outbox.ID)
			if refreshErr != nil {
				cancel()
				if errors.Is(refreshErr, datastore.ErrRecordNotExist) {
					return d.ackMessage(ctx, message.ID, "outbox_missing") == nil
				}
				klog.ErrorS(refreshErr, "result dispatcher refresh dispatching outbox failed", "outboxID", outbox.ID, "msgID", message.ID)
				return false
			}
			outbox = refreshed
			switch outbox.State {
			case config.JobResultOutboxStateResultQueued:
				if strings.TrimSpace(outbox.MessageID) != messageID {
					cancel()
					return d.ackMessage(ctx, message.ID, "outbox_message_mismatch") == nil
				}
				reclaimed, reclaimErr := claimQueuedOutboxForMessage(persistCtx, d.store, outbox, messageID)
				if reclaimErr != nil {
					cancel()
					klog.ErrorS(reclaimErr, "result dispatcher reclaim queued outbox failed", "outboxID", outbox.ID, "msgID", message.ID)
					return false
				}
				if !reclaimed {
					cancel()
					return d.ackMessage(ctx, message.ID, "outbox_claim_lost") == nil
				}
			case config.JobResultOutboxStateResultProcessingQueue:
				if strings.TrimSpace(outbox.MessageID) != messageID {
					cancel()
					return d.ackMessage(ctx, message.ID, "outbox_message_mismatch") == nil
				}
				cancel()
				return d.ackMessage(ctx, message.ID, "outbox_claim_lost") == nil
			case config.JobResultOutboxStateResultDispatching:
				cancel()
				return false
			default:
				cancel()
				return d.ackMessage(ctx, message.ID, "outbox_state_"+string(outbox.State)) == nil
			}
		}
		outbox.State = config.JobResultOutboxStateResultProcessingQueue
		outbox.MessageID = messageID
		outbox.LastError = ""
	default:
		cancel()
		return d.ackMessage(ctx, message.ID, "outbox_state_"+string(outbox.State)) == nil
	}
	cancel()

	if err := processJobResult(ctx, d.client, d.store, payload); err != nil {
		persistCtx, cancel = resultOutboxPersistenceContext()
		defer cancel()
		if errors.Is(err, errResultDispatchNoRetry) {
			klog.ErrorS(err, "result dispatcher process failed without retry", "outboxID", outbox.ID, "msgID", message.ID, "taskID", payload.TaskID, "name", payload.Name)
			if markErr := markJobResultOutboxFailed(persistCtx, d.store, outbox, err.Error()); markErr != nil {
				klog.ErrorS(markErr, "result dispatcher mark outbox failed", "outboxID", outbox.ID, "msgID", message.ID)
				return false
			}
			return d.ackMessage(ctx, message.ID, "no_retry_process_error") == nil
		}
		klog.ErrorS(err, "result dispatcher process failed", "outboxID", outbox.ID, "msgID", message.ID, "taskID", payload.TaskID, "name", payload.Name)
		if moveErr := moveQueueResultOutboxToQueued(persistCtx, d.store, outbox, err.Error(), outbox.Attempts+1); moveErr != nil {
			klog.ErrorS(moveErr, "result dispatcher move outbox back to queued failed", "outboxID", outbox.ID, "msgID", message.ID)
		}
		return false
	}
	persistCtx, cancel = resultOutboxPersistenceContext()
	defer cancel()
	if err := deleteJobResultOutbox(persistCtx, d.store, outbox.ID); err != nil {
		klog.ErrorS(err, "result dispatcher delete outbox failed", "outboxID", outbox.ID, "msgID", message.ID)
		return false
	}
	return d.ackMessage(ctx, message.ID, "processed") == nil
}

func claimQueuedOutboxForMessage(ctx context.Context, store datastore.DataStore, outbox *model.JobResultOutbox, messageID string) (bool, error) {
	if outbox == nil {
		return false, nil
	}
	claimed, err := compareAndSwapJobResultOutboxWithConditions(ctx, store, outbox, map[string]interface{}{
		"state":      string(config.JobResultOutboxStateResultQueued),
		"message_id": strings.TrimSpace(messageID),
	}, map[string]interface{}{
		"state":      config.JobResultOutboxStateResultProcessingQueue,
		"message_id": strings.TrimSpace(messageID),
		"attempts":   outbox.Attempts,
		"last_error": "",
	})
	if err != nil || !claimed {
		return claimed, err
	}
	outbox.State = config.JobResultOutboxStateResultProcessingQueue
	outbox.MessageID = strings.TrimSpace(messageID)
	outbox.LastError = ""
	return true, nil
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

func dispatchJobResult(ctx context.Context, queue msg.Queue, payload *JobResultPayload) error {
	if payload == nil {
		return fmt.Errorf("result payload is nil")
	}
	_, err := EnqueueResultJob(ctx, queue, payload)
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

func processJobResult(ctx context.Context, client kubernetes.Interface, store datastore.DataStore, payload *JobResultPayload) error {
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
	currentJob, getErr := client.BatchV1().Jobs(namespace).Get(ctx, payload.Name, metav1.GetOptions{})
	if getErr != nil && !k8serrors.IsNotFound(getErr) {
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
	matchesExpectedJob := func(jobObj *batchv1.Job) bool {
		if expectedUID != "" && string(jobObj.UID) != expectedUID {
			return false
		}
		return jobResultMatchesExecutionIdentity(payload, jobObj)
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	status, message, err := waitForJobCompletionMatching(waitCtx, client, namespace, payload.Name, matchesExpectedJob)
	if errors.Is(err, errJobExecutionIdentityChanged) {
		klog.InfoS("discard stale job result after Kubernetes Job identity changed", "namespace", namespace, "name", payload.Name, "taskID", payload.TaskID, "runGeneration", payload.RunGeneration)
		return nil
	}
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

	jobObj, getErr := client.BatchV1().Jobs(namespace).Get(ctx, payload.Name, metav1.GetOptions{})
	if getErr != nil && !k8serrors.IsNotFound(getErr) {
		klog.ErrorS(getErr, "get job failed while processing result", "namespace", namespace, "name", payload.Name, "taskID", payload.TaskID)
	}
	if jobObj != nil && !jobResultMatchesExecutionIdentity(payload, jobObj) {
		klog.InfoS("discard stale job result for newer Kubernetes Job", "namespace", namespace, "name", payload.Name, "taskID", payload.TaskID, "runGeneration", payload.RunGeneration)
		return nil
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
		logText, logErr := collectJobPodLogs(ctx, client, namespace, payload.Name)
		if logErr != nil {
			klog.ErrorS(logErr, "collect job logs failed", "namespace", namespace, "name", payload.Name, "taskID", payload.TaskID)
		} else {
			logs = logText
		}
		if cleanupErr := deleteCompletedJobAndPods(ctx, client, namespace, payload.Name, jobObj); cleanupErr != nil {
			klog.Warningf("clean completed job %s/%s failed: %v", namespace, payload.Name, cleanupErr)
		}
	}

	if upErr := updateJobInfoStatus(ctx, store, payload, status, message, startTime, endTime, logs); upErr != nil {
		return upErr
	}

	if err != nil {
		if _, ok := ExtractStatusError(err); ok {
			return nil
		}
		return err
	}
	return nil
}

func stampJobExecutionIdentity(jobTask *model.JobTask, jobObj *batchv1.Job) {
	if jobTask == nil || jobObj == nil {
		return
	}
	if jobObj.Annotations == nil {
		jobObj.Annotations = make(map[string]string)
	}
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

func newJobResultPayload(jobTask *model.JobTask, jobObj *batchv1.Job) *JobResultPayload {
	if jobTask == nil || jobObj == nil {
		return nil
	}
	name := strings.TrimSpace(jobObj.Name)
	if name == "" {
		return nil
	}
	namespace := strings.TrimSpace(jobObj.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(jobTask.Namespace)
	}
	if namespace == "" {
		return nil
	}
	serviceName := resolveJobServiceName(jobTask)
	if serviceName == "" {
		serviceName = componentNameFromJobInfo(jobObj)
	}
	timeout := jobTask.Timeout
	if timeout <= 0 {
		timeout = int64(config.DefaultJobTaskTimeout.Seconds())
	}
	return &JobResultPayload{
		TaskID:         jobTask.TaskID,
		ExecutionKey:   jobTask.ExecutionKey,
		RunGeneration:  jobTask.RunGeneration,
		JobType:        jobTask.JobType,
		Namespace:      namespace,
		Name:           name,
		ServiceName:    serviceName,
		TimeoutSeconds: timeout,
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
